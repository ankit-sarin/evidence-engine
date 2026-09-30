"""Extraction-entry import — a review entered at extraction (R204, 11c).

A corpus screened elsewhere arrives as one JSON file naming each paper and one
parsed text per paper (9e-SE-A §4a, route (a) of 11c R-b). The import writes what
the skipped stages would have written: each paper at FT_ELIGIBLE, its parsed text
under the review's `parsed_text/` with a `parsed_text_refs` row, an `adjudicated`
→ `eligible` event under an `import` manifest (R202), and the eight screening
stages of `workflow_state` complete. No model is called and no `run_calls` row
is written.

Input (`import_extraction_entry`'s `input_path`)::

    {"source": "<non-empty>",
     "papers": [{"title": str, "pmid": str | null, "doi": str | null,
                 "abstract"?: str | null, "authors"?: [str] | null,
                 "journal"?: str | null, "year"?: int | null,
                 "text_path": "<relative to the JSON file's directory>"}]}

An imported ref is identified as imported by its `parsed_text_sha256` appearing
among the import manifest's `inputs` (11c R-c): no `parsed_text_refs` column can
carry the mark, and `source_full_text_assets_id` stays NULL (R-b).

No CLI (the R294 pattern). Invocation::

    import_extraction_entry(ReviewDatabase(<id>), <json>, spec=load_spec_for(<id>))
"""

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path

from engine.adjudication.ft_screening_adjudicator import _stored_path
from engine.adjudication.workflow import (
    WORKFLOW_STAGES,
    complete_stage,
    ensure_workflow_table,
)
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook_beside
from engine.core.database import (
    ImportStatusRefused,
    ReviewDatabase,
    RetiredTransition,
    insert_paper_at_status,
)
from engine.core.events import write_paper_event
from engine.core.parsed_text import next_version, record_parsed_text

logger = logging.getLogger(__name__)

#: The status every imported paper enters at (R204).
ENTRY_STATUS = "FT_ELIGIBLE"

#: The workflow stages the import completes: the ones a screened corpus has passed.
SCREENING_WORKFLOW_STAGES = WORKFLOW_STAGES[:8]

#: `paper_events.stage_name` on every event this import writes.
STAGE_NAME = "extraction_entry_import"

_OPTIONAL_TYPES = {"abstract": str, "journal": str, "year": int}

#: Refusals the import recognises inside its transaction: they close the
#: manifest 'aborted'; any other exception closes it 'failed' (11c R-k).
_REFUSALS = (ImportStatusRefused, RetiredTransition, FileExistsError)


class ExtractionEntryRejected(ValueError):
    """The input failed validation. Nothing was written and no manifest opened."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("extraction-entry import rejected — no database changes were "
                         "made:\n  " + "\n  ".join(errors))


@dataclass(frozen=True)
class _Entry:
    n: int
    title: str
    pmid: str | None
    doi: str | None
    abstract: str | None
    authors: list[str] | None
    journal: str | None
    year: int | None
    text_path: Path
    data: bytes

    @property
    def label(self) -> str:
        return f"entry {self.n} ({self.pmid or self.doi}): "


def _parsed_text_dir(review_db: ReviewDatabase) -> Path:
    return Path(review_db.db_path).parent / "parsed_text"


# ── Validators shared with the screening-entry import (11c R-t) ──────────


def _load_document(input_path: Path, rejected: type[ValueError]) -> tuple[str, list, list[str]]:
    """Read the file and check its top-level shape. Returns (source, papers,
    errors-so-far); raises `rejected` at once when nothing further can be checked."""
    try:
        doc = json.loads(input_path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise rejected([f"input file unreadable as JSON: {exc}"]) from exc

    errors: list[str] = []
    if not isinstance(doc, dict) or set(doc) != {"source", "papers"}:
        raise rejected([
            "top-level shape: the file must be an object with exactly the keys "
            "'source' and 'papers'"])
    source = doc["source"]
    if not isinstance(source, str) or not source.strip():
        errors.append("source: must be a non-empty string")
    papers = doc["papers"]
    if not isinstance(papers, list) or not papers:
        errors.append("papers: must be a non-empty list")
        raise rejected(errors)
    return source, papers, errors


def _check_title_and_ids(rec: dict, where: str,
                         errors: list[str]) -> tuple[object, str | None, str | None]:
    """The title rule and the pmid/doi type rules (R-o). Returns (title, pmid,
    doi), each identifier None when absent or blank."""
    title = rec.get("title")
    if not isinstance(title, str) or not title.strip():
        errors.append(f"{where}: title must be a non-empty string")
    pmid, doi = rec.get("pmid"), rec.get("doi")
    for key, val in (("pmid", pmid), ("doi", doi)):
        if val is not None and not isinstance(val, str):
            errors.append(f"{where}: {key} must be a string or null")
    pmid = pmid if isinstance(pmid, str) and pmid.strip() else None
    doi = doi if isinstance(doi, str) and doi.strip() else None
    return title, pmid, doi


def _check_id_duplicates(n: int, where: str, pmid: str | None, doi: str | None,
                         seen_pmid: dict[str, int], seen_doi: dict[str, int],
                         errors: list[str]) -> None:
    """11c R-m: pmid compared stripped, doi stripped and lower-cased; stored as given."""
    if pmid is not None:
        key = pmid.strip()
        if key in seen_pmid:
            errors.append(f"{where}: duplicate pmid {pmid!r} (also entry {seen_pmid[key]})")
        seen_pmid.setdefault(key, n)
    if doi is not None:
        key = doi.strip().lower()
        if key in seen_doi:
            errors.append(f"{where}: duplicate doi {doi!r} (also entry {seen_doi[key]})")
        seen_doi.setdefault(key, n)


def _check_optional_fields(rec: dict, where: str, errors: list[str]) -> tuple:
    """The optional-field type rules (R-o). Returns (abstract, authors, journal, year)."""
    for key, typ in _OPTIONAL_TYPES.items():
        val = rec.get(key)
        if val is not None and (not isinstance(val, typ) or isinstance(val, bool)):
            errors.append(f"{where}: {key} must be {typ.__name__} or null")
    authors = rec.get("authors")
    if authors is not None and not (isinstance(authors, list)
                                    and all(isinstance(a, str) for a in authors)):
        errors.append(f"{where}: authors must be a list of strings or null")
    return rec.get("abstract"), authors, rec.get("journal"), rec.get("year")


def _validate(review_db: ReviewDatabase, input_path: Path) -> tuple[str, list[_Entry]]:
    """The whole file, and the review, before any write. Raises
    `ExtractionEntryRejected` listing every rule that failed."""
    source, papers, errors = _load_document(input_path, ExtractionEntryRejected)

    base = input_path.parent
    entries: list[_Entry] = []
    seen_pmid: dict[str, int] = {}
    seen_doi: dict[str, int] = {}
    seen_text: dict[Path, int] = {}
    for n, rec in enumerate(papers, 1):
        where = f"entry {n}"
        if not isinstance(rec, dict):
            errors.append(f"{where}: must be an object")
            continue
        title, pmid, doi = _check_title_and_ids(rec, where, errors)
        if pmid is None and doi is None:
            errors.append(f"{where}: at least one of pmid and doi is required")
        _check_id_duplicates(n, where, pmid, doi, seen_pmid, seen_doi, errors)
        abstract, authors, journal, year = _check_optional_fields(rec, where, errors)

        text_path, data = rec.get("text_path"), None
        if not isinstance(text_path, str) or not text_path:
            errors.append(f"{where}: text_path must be a non-empty string")
            text_path = None
        else:
            text_path = (base / text_path).resolve()
            if not text_path.is_file():
                errors.append(f"{where}: text_path {rec['text_path']!r} is not an "
                              "existing regular file")
            else:
                data = text_path.read_bytes()
                if not data:
                    errors.append(f"{where}: text_path {rec['text_path']!r} is empty")
                else:
                    try:
                        data.decode("utf-8")
                    except UnicodeDecodeError:
                        errors.append(f"{where}: text_path {rec['text_path']!r} is not "
                                      "UTF-8 decodable")
            if text_path in seen_text:
                errors.append(f"{where}: text_path {rec['text_path']!r} is also "
                              f"entry {seen_text[text_path]}'s")
            seen_text.setdefault(text_path, n)

        entries.append(_Entry(n, title, pmid, doi, abstract, authors,
                              journal, year, text_path, data))

    # 11c R-e: a review entered at extraction starts empty.
    (n_papers,) = review_db._conn.execute("SELECT COUNT(*) FROM papers").fetchone()
    if n_papers:
        errors.append(f"review: holds {n_papers} papers row(s); an extraction-entry "
                      "import enters an empty review only (R-e)")
    # 11c R-l: nothing already under parsed_text/ that a placement could meet.
    ptd = _parsed_text_dir(review_db)
    if ptd.exists() and any(ptd.iterdir()):
        errors.append(f"review: {ptd} is not empty; an extraction-entry import places "
                      "its texts into an empty parsed_text/ only (R-l)")

    if errors:
        raise ExtractionEntryRejected(errors)
    return source, entries


def _place(data: bytes, md_path: Path, placed: list[Path]) -> None:
    """Write `<md_path>.tmp`, then move it to `md_path` without ever replacing an
    existing file: `os.link` raises FileExistsError where `rename` would clobber.
    Every path this call creates is appended to `placed`."""
    tmp = md_path.with_suffix(".md.tmp")
    fh = open(tmp, "xb")           # FileExistsError if tmp exists: never tracked
    placed.append(tmp)
    with fh:
        fh.write(data)
    os.link(tmp, md_path)          # FileExistsError if md_path exists (R-l, R-k)
    placed.append(md_path)
    tmp.unlink()


def import_extraction_entry(review_db: ReviewDatabase, input_path: str | Path, *,
                            spec, git=None, digest_fn=None) -> dict:
    """Enter a screened corpus into an empty review at FT_ELIGIBLE (R204, 11c).

    Validates the whole file and the review first; a rejection
    (`ExtractionEntryRejected`) opens no manifest and writes nothing. A valid file
    is applied under one `import` manifest whose `inputs` pin the JSON file and
    every text by stored path (R288), in ONE transaction: per paper the papers row
    (`insert_paper_at_status`, R-d), its text placed under `parsed_text/` and its
    ref (`record_parsed_text`, `source_asset_id=None`), and an `adjudicated` →
    `eligible` event (actor human / reviewer, actor_name the JSON's stored path);
    then the eight screening workflow stages complete. On any exception after the
    manifest opens: roll back, unlink every file this call placed, close the
    manifest 'aborted' for a recognised refusal or 'failed' otherwise (R-k), and
    re-raise.

    `spec` is the loaded ReviewSpec the manifest records; `git` and `digest_fn`
    pass through to `open_run` (a test seam; production defaults).

    Returns {"run_id", "paper_ids", "imported"}.
    """
    input_path = Path(input_path)
    source, entries = _validate(review_db, input_path)

    conn = review_db._conn
    stored = _stored_path(input_path)
    inputs = {stored: hashlib.sha256(input_path.read_bytes()).hexdigest()}
    for e in entries:
        inputs[_stored_path(e.text_path)] = hashlib.sha256(e.data).hexdigest()

    # Its executescript commits, so it runs before the transaction opens (D4).
    ensure_workflow_table(conn)
    # DirtyTree and every other open_run refusal raise here, before any write.
    run_id = rm.open_run(conn, spec, kind="import", stages=(),
                         codebook=load_codebook_beside(review_db.db_path),
                         inputs=inputs, git=git, digest_fn=digest_fn).run_id

    ptd = _parsed_text_dir(review_db)
    ptd.mkdir(exist_ok=True)
    placed: list[Path] = []
    paper_ids: list[int] = []
    refused = loop_reason = None
    try:
        for e in entries:
            try:
                pid = insert_paper_at_status(
                    conn, status=ENTRY_STATUS, title=e.title, source=source,
                    pmid=e.pmid, doi=e.doi, abstract=e.abstract, authors=e.authors,
                    journal=e.journal, year=e.year)
                version = next_version(conn, pid)
                md_path = ptd / f"{pid}_v{version}.md"
                _place(e.data, md_path, placed)
                record_parsed_text(conn, paper_id=pid, path=md_path, version=version,
                                   data=e.data, source_asset_id=None)
                write_paper_event(
                    conn, event_type="adjudicated", paper_id=pid, to_state="eligible",
                    from_state=None, actor_kind="human", actor_role="reviewer",
                    actor_name=stored, actor_digest=None, run_id=run_id,
                    stage_name=STAGE_NAME, commit=False)
            except _REFUSALS as exc:
                refused = e.label + str(exc)
                raise
            except Exception as exc:
                loop_reason = e.label + str(exc)
                raise
            paper_ids.append(pid)

        for stage in SCREENING_WORKFLOW_STAGES:
            complete_stage(conn, stage, commit=False,
                           metadata=f"extraction-entry import, run {run_id}: "
                                    f"{len(paper_ids)} papers from {stored}")
        conn.commit()
    except Exception as exc:
        conn.rollback()
        for path in reversed(placed):
            path.unlink(missing_ok=True)
        rm.close_run(conn, run_id, "aborted" if refused else "failed",
                     reason=refused or loop_reason or str(exc))
        raise
    rm.close_run(conn, run_id, "completed")

    logger.info("Extraction-entry import: %d papers at %s under run %d from %s",
                len(paper_ids), ENTRY_STATUS, run_id, stored)
    return {"run_id": run_id, "paper_ids": paper_ids, "imported": len(paper_ids)}
