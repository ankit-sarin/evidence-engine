"""Screening-entry import — a review entered at abstract screening (R202, 11c).

A citation set identified elsewhere arrives as one JSON file. The import writes
what the search stage writes and nothing more: one `papers` row per entry at
INGESTED, under an `import` manifest whose `inputs` pin the file (R288). No
paper event (INGESTED carries no eligibility), no parsed text, no workflow
stage (no stage stands for search), no model call. Abstract screening then
selects the rows as it selects a search's (`get_papers_by_status("INGESTED")`).

Input (`import_screening_entry`'s `input_path`)::

    {"source": "<non-empty>",
     "papers": [{"title": str, "pmid"?: str | null, "doi"?: str | null,
                 "abstract"?: str | null, "authors"?: [str] | null,
                 "journal"?: str | null, "year"?: int | null}]}

Validation is extraction-entry's (R-o types included) without `text_path` and
without "at least one of pmid/doi" (11c R-t, R-s). Every within-file duplicate is
refused, never merged (R-s): pmid stripped; doi stripped and lower-cased (R-m);
and, for an entry with neither identifier only, its normalised title against
every other entry's. The review must hold no papers row (R-r).

No CLI (the R294 pattern). Invocation::

    import_screening_entry(ReviewDatabase(<id>), <json>, spec=load_spec_for(<id>))
"""

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path

from engine.adjudication.ft_screening_adjudicator import _stored_path
from engine.adjudication.import_extraction_entry import (
    _check_id_duplicates,
    _check_optional_fields,
    _check_title_and_ids,
    _load_document,
)
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook_beside
from engine.core.database import (
    ImportStatusRefused,
    ReviewDatabase,
    RetiredTransition,
    insert_paper_at_status,
)

logger = logging.getLogger(__name__)

#: The status every imported record enters at: the search stage's (R202).
ENTRY_STATUS = "INGESTED"

#: Refusals recognised inside the transaction close the manifest 'aborted';
#: anything else closes it 'failed' (11c R-k).
_REFUSALS = (ImportStatusRefused, RetiredTransition)


class ScreeningEntryRejected(ValueError):
    """The input failed validation. Nothing was written and no manifest opened."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("screening-entry import rejected — no database changes were "
                         "made:\n  " + "\n  ".join(errors))


def normalise_title(title: str) -> str:
    """11c R-s: stripped, lower-cased, internal whitespace collapsed to one space."""
    return " ".join(title.split()).lower()


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

    @property
    def label(self) -> str:
        """The in-loop reason prefix (R-k); an id-less entry shows its title (I3)."""
        ident = self.pmid or self.doi or f"title: {self.title[:40]}"
        return f"entry {self.n} ({ident}): "


def _validate(review_db: ReviewDatabase, input_path: Path) -> tuple[str, list[_Entry]]:
    """The whole file, and the review, before any write. Raises
    `ScreeningEntryRejected` listing every rule that failed."""
    source, papers, errors = _load_document(input_path, ScreeningEntryRejected)

    entries: list[_Entry] = []
    seen_pmid: dict[str, int] = {}
    seen_doi: dict[str, int] = {}
    for n, rec in enumerate(papers, 1):
        where = f"entry {n}"
        if not isinstance(rec, dict):
            errors.append(f"{where}: must be an object")
            continue
        title, pmid, doi = _check_title_and_ids(rec, where, errors)
        _check_id_duplicates(n, where, pmid, doi, seen_pmid, seen_doi, errors)
        abstract, authors, journal, year = _check_optional_fields(rec, where, errors)
        entries.append(_Entry(n, title, pmid, doi, abstract, authors, journal, year))

    # R-s: an entry with neither identifier is a duplicate if its normalised
    # title is any other entry's; entries with an identifier are not
    # title-deduplicated against each other.
    titles = [(e.n, normalise_title(e.title)) for e in entries if isinstance(e.title, str)]
    for e in entries:
        if e.pmid is not None or e.doi is not None or not isinstance(e.title, str):
            continue
        norm = normalise_title(e.title)
        if not norm:
            continue
        others = [m for m, t in titles if t == norm and m != e.n]
        if others:
            errors.append(f"entry {e.n}: has neither pmid nor doi and its title "
                          f"duplicates entry {others[0]}'s")

    # 11c R-r: a review entered at screening starts empty.
    (n_papers,) = review_db._conn.execute("SELECT COUNT(*) FROM papers").fetchone()
    if n_papers:
        errors.append(f"review: holds {n_papers} papers row(s); a screening-entry "
                      "import enters an empty review only (R-r)")

    if errors:
        raise ScreeningEntryRejected(errors)
    return source, entries


def import_screening_entry(review_db: ReviewDatabase, input_path: str | Path, *,
                           spec, git=None, digest_fn=None) -> dict:
    """Enter an identified citation set into an empty review at INGESTED (11c).

    Validates the whole file and the review first; a rejection
    (`ScreeningEntryRejected`) opens no manifest and writes nothing. A valid file
    is applied under one `import` manifest whose `inputs` pin the file by stored
    path (R288), in ONE transaction: one `insert_paper_at_status` row per entry
    at INGESTED with the file's `source`. On any exception after the manifest
    opens: roll back, close the manifest 'aborted' for a recognised refusal or
    'failed' otherwise (R-k), and re-raise.

    `spec` is the loaded ReviewSpec the manifest records; `git` and `digest_fn`
    pass through to `open_run` (a test seam; production defaults).

    Returns {"run_id", "paper_ids", "imported"}.
    """
    input_path = Path(input_path)
    source, entries = _validate(review_db, input_path)

    conn = review_db._conn
    stored = _stored_path(input_path)
    inputs = {stored: hashlib.sha256(input_path.read_bytes()).hexdigest()}
    # DirtyTree and every other open_run refusal raise here, before any write.
    run_id = rm.open_run(conn, spec, kind="import", stages=(),
                         codebook=load_codebook_beside(review_db.db_path),
                         inputs=inputs, git=git, digest_fn=digest_fn).run_id

    paper_ids: list[int] = []
    refused = loop_reason = None
    try:
        for e in entries:
            try:
                pid = insert_paper_at_status(
                    conn, status=ENTRY_STATUS, title=e.title, source=source,
                    pmid=e.pmid, doi=e.doi, abstract=e.abstract, authors=e.authors,
                    journal=e.journal, year=e.year)
            except _REFUSALS as exc:
                refused = e.label + str(exc)
                raise
            except Exception as exc:
                loop_reason = e.label + str(exc)
                raise
            paper_ids.append(pid)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        rm.close_run(conn, run_id, "aborted" if refused else "failed",
                     reason=refused or loop_reason or str(exc))
        raise
    rm.close_run(conn, run_id, "completed")

    logger.info("Screening-entry import: %d papers at %s under run %d from %s",
                len(paper_ids), ENTRY_STATUS, run_id, stored)
    return {"run_id": run_id, "paper_ids": paper_ids, "imported": len(paper_ids)}
