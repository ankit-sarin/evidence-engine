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
from engine.core.database import ReviewDatabase

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

_VALID_FIELD_TYPES = ("categorical", "numeric", "free_text")


class LoaderError(Exception):
    """Parse / load failure (bad codebook, CSV shape, etc.)."""


@dataclass(frozen=True)
class CodebookEntry:
    field_name: str
    field_type: FieldType
    definition: str
    valid_values: Optional[list[str]]
    numeric_tolerance: float


def compute_codebook_sha256(path: Path) -> str:
    """SHA-256 hex digest of the codebook file bytes (no normalization)."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


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


def load_codebook(path: Path) -> dict[str, CodebookEntry]:
    """Parse extraction_codebook.yaml into CodebookEntry objects keyed by field_name."""
    path = Path(path)
    try:
        doc = yaml.safe_load(path.read_text())
    except yaml.YAMLError as exc:
        raise LoaderError(f"codebook YAML parse failed: {exc}") from exc

    if not isinstance(doc, dict) or "fields" not in doc:
        raise LoaderError("codebook missing top-level 'fields' list")

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


def _fetch_spans_for_paper(
    db: ReviewDatabase, paper_id: int
) -> dict[tuple[str, str], str]:
    """Return {(arm_name, field_name): source_snippet} for one paper.

    Joins the latest extraction for the local arm and all cloud arms.
    Missing snippets are absent from the dict (caller treats as None).
    """
    spans: dict[tuple[str, str], str] = {}

    # Local arm — latest extraction wins.
    local_rows = db._conn.execute(
        """SELECT es.field_name, es.source_snippet
           FROM evidence_spans es
           JOIN extractions e ON e.id = es.extraction_id
           WHERE e.paper_id = ?
           AND e.id = (
               SELECT MAX(e2.id) FROM extractions e2
               WHERE e2.paper_id = e.paper_id
           )""",
        (paper_id,),
    ).fetchall()
    for r in local_rows:
        if r["source_snippet"] is not None:
            spans[("local", r["field_name"])] = r["source_snippet"]

    # Cloud arms — latest per (paper_id, arm).
    cloud_rows = db._conn.execute(
        """SELECT ce.arm, cs.field_name, cs.source_snippet
           FROM cloud_evidence_spans cs
           JOIN cloud_extractions ce ON ce.id = cs.cloud_extraction_id
           WHERE ce.paper_id = ?
           AND ce.id = (
               SELECT MAX(ce2.id) FROM cloud_extractions ce2
               WHERE ce2.paper_id = ce.paper_id AND ce2.arm = ce.arm
           )""",
        (paper_id,),
    ).fetchall()
    for r in cloud_rows:
        if r["source_snippet"] is not None:
            spans[(r["arm"], r["field_name"])] = r["source_snippet"]

    return spans


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
    db: ReviewDatabase,
    codebook: dict[str, CodebookEntry],
    limit: Optional[int] = None,
) -> list[JudgeInput]:
    """Read a 3-arm disagreement CSV and produce list[JudgeInput].

    Rows are skipped (with WARNING logs, not exceptions) when:
      - field_name is not in the codebook,
      - paper text is missing on disk.
    """
    csv_path = Path(csv_path)
    review_dir = db.db_path.parent

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
            span_cache[paper_id_int] = _fetch_spans_for_paper(db, paper_id_int)
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
    "CodebookEntry",
    "InputScope",
    "LoaderError",
    "compute_codebook_sha256",
    "load_ai_triples_csv",
    "load_codebook",
]
