"""Loader: disagreement CSV + codebook YAML → list[JudgeInput].

Pure data-shaping. Reads the DB for spans and parsed paper text,
computes pre-check flags, and returns JudgeInput objects ready
for judge.run_pass1. No DB writes, no LLM calls.

Arm names mirror what engine.analysis.concordance and
analysis.paper1.export_disagreement_pairs already canonicalize:
  - "local"
  - "openai_o4_mini_high"
  - "anthropic_sonnet_4_6"
These are lexicographically sortable (anthropic < local < openai).
"""

from __future__ import annotations

import csv
import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

import yaml

from analysis.paper1.judge_schema import ArmOutput, FieldType, JudgeInput
from analysis.paper1.precheck import PreCheckFlags, compute_precheck_flags
from engine.core.codebook import (
    VALID_FIELD_TYPES,
    compute_codebook_sha256,
)

logger = logging.getLogger(__name__)

InputScope = Literal["AI_TRIPLES", "HUMAN_PAIRS", "ALL", "SMOKE_TEST"]

# Column name in the exporter CSV → arm_name emitted into ArmOutput.
# The exporter writes short-form value columns; we expand to the canonical
# storage arm names used by cloud_extractions and judge_pair_ratings.
CSV_VALUE_COLS = (
    ("local_value", "local"),
    ("o4mini_value", "openai_o4_mini_high"),
    ("sonnet_value", "anthropic_sonnet_4_6"),
)

#: Relocated to engine/core/codebook.py so that engine code does not import
#: from analysis/ to learn what a field type is. Aliased, not copied: the
#: judge loader, the codebook loader and ReviewSpec.ExtractionField all
#: validate against one tuple.
_VALID_FIELD_TYPES = VALID_FIELD_TYPES


class LoaderError(Exception):
    """Parse / load failure (bad codebook, CSV shape, etc.)."""


@dataclass(frozen=True)
class CodebookEntry:
    field_name: str
    field_type: FieldType
    definition: str
    valid_values: Optional[list[str]]
    numeric_tolerance: float




def _parse_field_type(raw: object, field_name: str) -> FieldType:
    if raw not in _VALID_FIELD_TYPES:
        raise LoaderError(
            f"Unknown field type {raw!r} for field {field_name!r}; "
            f"expected one of {_VALID_FIELD_TYPES}"
        )
    return raw  # type: ignore[return-value]


def _parse_valid_values(raw, field_name: str) -> Optional[list[str]]:
    if raw is None:
        return None
    if not isinstance(raw, list):
        raise LoaderError(
            f"valid_values for {field_name!r} must be a list, got {type(raw).__name__}"
        )
    out: list[str] = []
    for item in raw:
        # Codebook may store either plain strings or {value, definition} dicts.
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, dict) and "value" in item:
            out.append(str(item["value"]))
        else:
            raise LoaderError(
                f"valid_values entry for {field_name!r} must be str or "
                f"{{value, definition}}, got {item!r}"
            )
    return out


def _document(path: Path) -> dict:
    """The codebook document, from the one loader (CODEBOOK-AUTH-01 C8).

    Its CodebookError is re-raised as LoaderError so this module's callers
    keep the single error type they already catch; what changes is that the
    document is now validated once, in one place, rather than by whichever of
    eleven readers happened to open the file.
    """
    from engine.core.codebook import CodebookError, load_codebook as _load

    try:
        return _load(path).raw
    except CodebookError as exc:
        raise LoaderError(str(exc)) from exc


def load_codebook(path: Path) -> dict[str, CodebookEntry]:
    """Parse extraction_codebook.yaml into CodebookEntry objects keyed by field_name."""
    doc = _document(path)

    entries: dict[str, CodebookEntry] = {}
    for field in doc["fields"] or []:
        if not isinstance(field, dict):
            raise LoaderError(f"codebook field entry must be a mapping, got {field!r}")
        name = field.get("name")
        if not name or not isinstance(name, str):
            raise LoaderError(f"codebook field missing 'name': {field!r}")
        if "type" not in field:
            raise LoaderError(f"codebook field {name!r} missing required 'type'")
        if "definition" not in field:
            raise LoaderError(
                f"codebook field {name!r} missing required 'definition'"
            )

        field_type = _parse_field_type(field["type"], name)
        definition = str(field["definition"]).strip()
        valid_values = _parse_valid_values(field.get("valid_values"), name)
        tolerance_raw = field.get("tolerance", 0.0)
        try:
            tolerance = float(tolerance_raw)
        except (TypeError, ValueError) as exc:
            raise LoaderError(
                f"tolerance for {name!r} must be numeric, got {tolerance_raw!r}"
            ) from exc

        entries[name] = CodebookEntry(
            field_name=name,
            field_type=field_type,
            definition=definition,
            valid_values=valid_values,
            numeric_tolerance=tolerance,
        )
    return entries


def load_raw_codebook(path: Path) -> dict[str, dict]:
    """Return the raw YAML field dicts keyed by field_name.

    Unlike :func:`load_codebook`, this preserves the full codebook entry —
    including ``judge_rubric_family``, ``ordered_values``, ``dimension`` and
    the verbatim ``valid_values``/``decision_criteria`` — which the
    production Pass 2 prompt builder (``build_judge_prompt``) dispatches on.
    """
    doc = _document(path)
    return {
        f["name"]: f
        for f in (doc.get("fields") or [])
        if isinstance(f, dict) and "name" in f
    }


# ── Paper text + span lookups ───────────────────────────────────────


def _paper_text(review_dir: Path, paper_id: int | str) -> Optional[str]:
    """Return parsed markdown text for a paper, or None if not available.

    Matches engine/agents/auditor.py pattern: data/<review>/parsed_text/{pid}_v*.md,
    highest version wins.
    """
    parsed_dir = review_dir / "parsed_text"
    md_files = sorted(parsed_dir.glob(f"{paper_id}_v*.md"), reverse=True)
    if not md_files:
        return None
    try:
        return md_files[0].read_text()
    except OSError:
        return None


def _fetch_spans_for_paper(conn, paper_id: int, codebook_path, arms):
    """`{(arm_name, field_name): source_snippet}` for one paper, VIA THE READER.

    READERS-01 Phase 2a (R30, R38). Two queries lived here, each a copy of the
    latest-extraction rule — `AND e.id = (SELECT MAX(e2.id) ...)` for local and
    `AND ce.id = (SELECT MAX(ce2.id) ...)` per cloud arm — making this the
    FOURTH copy of a resolution rule in the repository (A1). They are gone.

    They also reached the live database through `db._conn`, a private attribute
    of an open read-write `ReviewDatabase`: inventory row I13, which the I5 grep
    could not see because it is not a `sqlite3.connect` call. The loader now
    receives a `mode=ro` connection and opens nothing.
    """
    from engine.core.codebook import load_codebook
    from engine.core.effective import iter_grid

    codebook = load_codebook(codebook_path)
    spans: dict[tuple[str, str], str] = {}
    for _pid, field_name, arm_name, ev in iter_grid(
            conn, codebook=codebook, papers=(paper_id,), arms=arms):
        located = ev.provenance.get("located")
        snippet = located.get("snippet") if isinstance(located, dict) else None
        if snippet:
            spans[(arm_name, field_name)] = snippet
    return spans


def load_grid(
    conn,
    review_dir: Path,
    codebook: dict[str, CodebookEntry],
    codebook_path: Path,
    *,
    verdicts: Optional[dict] = None,
    limit: Optional[int] = None,
) -> list[JudgeInput]:
    """The FULL cell grid as `list[JudgeInput]` — R28.

    **The scorer's verdict is a feature, not a filter.** `load_ai_triples_csv`
    took its universe from the disagreement CSV, which holds only the
    `(paper, field)` rows where at least one arm pair disagreed. B3 is the
    consequence: 2,266 of 3,802 cells were judged and 1,535 (40.4%) never were,
    one-directionally — false matches were removed from the judge's view and
    fabrications in agreeing cells were not. Every rate derived from that pass
    inherits the bias.

    Here the universe is the grid — corpus papers x codebook fields x registered
    arms — and every cell is emitted whatever the scorer said about it. A verdict,
    when one is supplied through `verdicts`, is attached to the record as a
    feature the judge prompt may use.

    Arms come from the registry (R12). `CSV_VALUE_COLS`, which hard-listed three
    of them, is retained only for reading legacy CSVs.
    """
    from engine.core.effective import iter_grid, registered_arms

    arms = registered_arms(conn)
    cb = None
    from engine.core.codebook import load_codebook as _load_cb
    cb = _load_cb(codebook_path)

    by_cell: dict[tuple[int, str], dict[str, "object"]] = {}
    for paper_id, field_name, arm_name, ev in iter_grid(
            conn, codebook=cb, arms=arms):
        by_cell.setdefault((paper_id, field_name), {})[arm_name] = ev

    inputs: list[JudgeInput] = []
    text_cache: dict[int, Optional[str]] = {}

    for (paper_id, field_name) in sorted(by_cell):
        if limit is not None and len(inputs) >= limit:
            break
        entry = codebook.get(field_name)
        if entry is None:
            logger.warning("skip: field %r not in codebook (paper_id=%s)",
                           field_name, paper_id)
            continue
        if paper_id not in text_cache:
            text_cache[paper_id] = _paper_text(review_dir, paper_id)
        paper_text = text_cache[paper_id]
        if paper_text is None:
            logger.warning("skip: no parsed text for paper_id=%s field=%s",
                           paper_id, field_name)
            continue

        cell = by_cell[(paper_id, field_name)]
        arm_outputs: list[ArmOutput] = []
        for arm_name in arms:
            ev = cell.get(arm_name)
            value = _coerce_value(ev.value if ev is not None else None)
            located = ev.provenance.get("located") if ev is not None else None
            span = located.get("snippet") if isinstance(located, dict) else None
            flags: PreCheckFlags = compute_precheck_flags(
                value=value, span=span, source_text=paper_text,
                field_type=entry.field_type,
                numeric_tolerance=entry.numeric_tolerance,
            )
            arm_outputs.append(ArmOutput(arm_name=arm_name, value=value,
                                         span=span, precheck_flags=flags))

        inputs.append(JudgeInput(
            paper_id=str(paper_id), field_name=field_name,
            field_type=entry.field_type, field_definition=entry.definition,
            field_valid_values=entry.valid_values, arms=arm_outputs,
            scorer_verdict=(verdicts or {}).get((paper_id, field_name)),
        ))

    return inputs


# ── CSV loader ──────────────────────────────────────────────────────


def _coerce_value(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    stripped = raw.strip()
    if stripped == "" or stripped.lower() in {"none", "null"}:
        return None
    return raw


def load_ai_triples_csv(
    csv_path: Path,
    db,
    codebook: dict[str, CodebookEntry],
    limit: Optional[int] = None,
    *,
    codebook_path: Optional[Path] = None,
) -> list[JudgeInput]:
    """LEGACY. Read a 3-arm disagreement CSV and produce list[JudgeInput].

    **Superseded by `load_grid` (R28).** This reads the SCORER'S DISAGREEMENT
    SET, not the review's cells: its universe is whatever rows
    `export_disagreement_pairs.py` wrote, which is exactly B3. It is kept
    because the committed Run 6 judge runs were produced through it and a
    frozen study whose loader has been deleted cannot be read for what it
    measured — telemetry under R30/R31, not a supported path.

    `db` may be a `ReviewDatabase` or a bare `mode=ro` connection.

    Rows are skipped (with WARNING logs, not exceptions) when:
      - field_name is not in the codebook,
      - paper text is missing on disk.
    """
    csv_path = Path(csv_path)
    conn = getattr(db, "_conn", db)
    review_dir = Path(
        conn.execute("PRAGMA database_list").fetchone()[2]).parent
    codebook_path = Path(codebook_path or (review_dir / "extraction_codebook.yaml"))

    with csv_path.open(newline="") as f:
        rows = list(csv.DictReader(f))

    rows.sort(key=lambda r: (int(r["paper_id"]), r["field_name"]))

    inputs: list[JudgeInput] = []
    span_cache: dict[int, dict[tuple[str, str], str]] = {}
    text_cache: dict[int, Optional[str]] = {}

    for row in rows:
        if limit is not None and len(inputs) >= limit:
            break

        field_name = row["field_name"]
        entry = codebook.get(field_name)
        if entry is None:
            logger.warning(
                "skip: field %r not in codebook (paper_id=%s)",
                field_name, row["paper_id"],
            )
            continue

        try:
            paper_id_int = int(row["paper_id"])
        except (KeyError, ValueError) as exc:
            logger.warning("skip: bad paper_id row=%r (%s)", row, exc)
            continue

        if paper_id_int not in text_cache:
            text_cache[paper_id_int] = _paper_text(review_dir, paper_id_int)
        paper_text = text_cache[paper_id_int]
        if paper_text is None:
            logger.warning(
                "skip: no parsed text for paper_id=%s field=%s",
                paper_id_int, field_name,
            )
            continue

        if paper_id_int not in span_cache:
            span_cache[paper_id_int] = _fetch_spans_for_paper(
                conn, paper_id_int, codebook_path,
                tuple(a for _c, a in CSV_VALUE_COLS))
        spans = span_cache[paper_id_int]

        arms: list[ArmOutput] = []
        for col_name, arm_name in CSV_VALUE_COLS:
            value = _coerce_value(row.get(col_name))
            span = spans.get((arm_name, field_name))
            flags: PreCheckFlags = compute_precheck_flags(
                value=value,
                span=span,
                source_text=paper_text,
                field_type=entry.field_type,
                numeric_tolerance=entry.numeric_tolerance,
            )
            arms.append(
                ArmOutput(
                    arm_name=arm_name,
                    value=value,
                    span=span,
                    precheck_flags=flags,
                )
            )

        inputs.append(
            JudgeInput(
                paper_id=str(paper_id_int),
                field_name=field_name,
                field_type=entry.field_type,
                field_definition=entry.definition,
                field_valid_values=entry.valid_values,
                arms=arms,
            )
        )

    return inputs


__all__ = [
    "CSV_VALUE_COLS",
    "load_grid",
    "CodebookEntry",
    "InputScope",
    "LoaderError",
    "compute_codebook_sha256",
    "load_ai_triples_csv",
    "load_codebook",
]
