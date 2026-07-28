"""Autopsy for the single-span storage collapse (SPANLOSS-01).

19 Run 6 extractions stored one span instead of ~20. This module reconstructs
the diagnosis from stored data alone — no re-extraction, no model calls.

The pivotal question is **extraction defect vs. storage defect**, and the stored
`extracted_data` column answers it directly, because both extractors persist the
*parsed model response verbatim*:

  * `engine/cloud/openai_extractor.py:100` — `extracted_data = json.loads(content)`
  * `engine/cloud/base.py:226`             — `json.dumps(extracted_data)` on write

So if `extracted_data` holds a syntactically complete single-span object, that is
what the model returned, and storage was faithful. If it holds a full 20-element
list while only one span row exists, the loss happened between parse and write.

The two arms turn out to have different mechanisms with the same symptom, so the
classifier below reports a shape per extraction rather than a single verdict.

Read-only. No Ollama, no cloud API, no repairs.

Usage:
    PYTHONPATH=. python -m analysis.paper1.spanloss_autopsy --review surgical_autonomy
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sqlite3
import statistics
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

EXPECTED_SPANS = 20

# Response shapes, in the order the salvage ladder in
# engine/cloud/base.py:88-146 encounters them.
SHAPE_WRAPPED_LIST = "wrapped_list"        # {"fields": [...]} / {"extractions": [...]}
SHAPE_BARE_LIST = "bare_list"              # [ {...}, {...} ]
SHAPE_SINGLE_SPAN_DICT = "single_span_dict"  # {"field_name": ..., "value": ...}
SHAPE_SINGLE_ELEMENT_LIST = "single_element_list"  # [ {...} ]
SHAPE_RAW_FALLBACK = "raw_fallback"        # {"fields": [], "raw": "..."} — parse failed
SHAPE_UNKNOWN = "unknown"


def response_shape(extracted_data: str | None) -> tuple[str, int]:
    """Classify the stored raw response and count the span objects it contains.

    Returns (shape, n_span_objects). A syntactically complete single-span shape
    means the model emitted one span; it is not evidence of truncation, because
    a truncated response would not have survived `json.loads`.
    """
    if not extracted_data:
        return SHAPE_UNKNOWN, 0
    try:
        d = json.loads(extracted_data)
    except json.JSONDecodeError:
        return SHAPE_UNKNOWN, 0

    if isinstance(d, list):
        n = sum(1 for x in d if isinstance(x, dict) and "field_name" in x)
        return (SHAPE_SINGLE_ELEMENT_LIST if len(d) == 1 else SHAPE_BARE_LIST), n
    if isinstance(d, dict):
        if "raw" in d and isinstance(d.get("fields"), list) and not d["fields"]:
            return SHAPE_RAW_FALLBACK, 0
        for key in ("fields", "extractions", "extracted_fields", "data", "results"):
            if isinstance(d.get(key), list):
                return SHAPE_WRAPPED_LIST, len(d[key])
        if "field_name" in d:
            return SHAPE_SINGLE_SPAN_DICT, 1
    return SHAPE_UNKNOWN, 0


def fork_verdict(shape: str, n_in_response: int, n_stored: int) -> str:
    """Extraction defect vs. storage defect for one extraction.

    EXTRACTION — the response itself carried n_stored spans; storage was faithful.
    STORAGE    — the response carried more spans than were stored.
    UNDETERMINED — the response could not be parsed from what was persisted.
    """
    if shape == SHAPE_UNKNOWN:
        return "UNDETERMINED"
    if n_in_response > n_stored:
        return "STORAGE"
    return "EXTRACTION"


# ── loading ──────────────────────────────────────────────────────────────


def _ro(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def load_extractions(db_path: Path) -> list[dict]:
    """Every Run 6 extraction with its stored span count and raw response."""
    conn = _ro(db_path)
    try:
        rows = [
            {
                "arm": "local_deepseek_r1_32b",
                "paper_id": r["paper_id"],
                "extracted_data": r["extracted_data"],
                "extracted_at": r["extracted_at"],
                "input_tokens": None,
                "output_tokens": None,
                "reasoning_tokens": None,
                "spans_stored": r["spans"],
            }
            for r in conn.execute(
                "SELECT e.paper_id, e.extracted_data, e.extracted_at, "
                "       (SELECT COUNT(*) FROM evidence_spans s WHERE s.extraction_id = e.id) spans "
                "  FROM extractions e"
            )
        ]
        rows += [
            {
                "arm": r["arm"],
                "paper_id": r["paper_id"],
                "extracted_data": r["extracted_data"],
                "extracted_at": r["extracted_at"],
                "input_tokens": r["input_tokens"],
                "output_tokens": r["output_tokens"],
                "reasoning_tokens": r["reasoning_tokens"],
                "spans_stored": r["spans"],
            }
            for r in conn.execute(
                "SELECT c.paper_id, c.arm, c.extracted_data, c.extracted_at, "
                "       c.input_tokens, c.output_tokens, c.reasoning_tokens, "
                "       (SELECT COUNT(*) FROM cloud_evidence_spans s "
                "         WHERE s.cloud_extraction_id = c.id) spans "
                "  FROM cloud_extractions c"
            )
        ]
    finally:
        conn.close()
    for r in rows:
        r["shape"], r["spans_in_response"] = response_shape(r["extracted_data"])
        r["verdict"] = fork_verdict(r["shape"], r["spans_in_response"], r["spans_stored"])
        r["visible_tokens"] = (
            (r["output_tokens"] or 0) - (r["reasoning_tokens"] or 0)
            if r["output_tokens"] is not None else None
        )
    return rows


def affected(rows: list[dict], threshold: int = EXPECTED_SPANS) -> list[dict]:
    return sorted(
        (r for r in rows if r["spans_stored"] < threshold),
        key=lambda r: (r["arm"], r["spans_stored"], r["paper_id"]),
    )


def surviving_fields(db_path: Path, rows: list[dict]) -> dict:
    """Which field survived on each single-span extraction."""
    conn = _ro(db_path)
    out: dict[str, list[str]] = {}
    try:
        for r in rows:
            if r["spans_stored"] != 1:
                continue
            if r["arm"] == "local_deepseek_r1_32b":
                q = ("SELECT s.field_name FROM evidence_spans s JOIN extractions e "
                     "ON e.id = s.extraction_id WHERE e.paper_id = ?")
                args = (r["paper_id"],)
            else:
                q = ("SELECT s.field_name FROM cloud_evidence_spans s JOIN cloud_extractions c "
                     "ON c.id = s.cloud_extraction_id WHERE c.paper_id = ? AND c.arm = ?")
                args = (r["paper_id"], r["arm"])
            names = [x[0] for x in conn.execute(q, args)]
            out.setdefault(r["arm"], []).extend(names)
    finally:
        conn.close()
    return {arm: dict(Counter(v)) for arm, v in out.items()}


def clustering(rows: list[dict], aff_ids: set, arm: str, paper_chars: dict) -> dict:
    """Do affected extractions differ from healthy ones on any observable?"""
    arm_rows = [r for r in rows if r["arm"] == arm]
    a = [r for r in arm_rows if r["paper_id"] in aff_ids]
    h = [r for r in arm_rows if r["paper_id"] not in aff_ids]

    def med(rs, key):
        vals = [r[key] for r in rs if r.get(key) is not None]
        return round(statistics.median(vals), 1) if vals else None

    order = sorted(arm_rows, key=lambda r: r["extracted_at"])
    positions = [i for i, r in enumerate(order, 1) if r["paper_id"] in aff_ids]
    return {
        "n_affected": len(a),
        "n_healthy": len(h),
        "median_paper_chars": {
            "affected": round(statistics.median([paper_chars.get(r["paper_id"], 0) for r in a]), 1) if a else None,
            "healthy": round(statistics.median([paper_chars.get(r["paper_id"], 0) for r in h]), 1) if h else None,
        },
        "median_input_tokens": {"affected": med(a, "input_tokens"), "healthy": med(h, "input_tokens")},
        "median_reasoning_tokens": {"affected": med(a, "reasoning_tokens"), "healthy": med(h, "reasoning_tokens")},
        "median_visible_tokens": {"affected": med(a, "visible_tokens"), "healthy": med(h, "visible_tokens")},
        "run_positions": positions,
        "run_length": len(order),
        "position_gaps": [b - a_ for a_, b in zip(positions, positions[1:])],
        "hourly": {
            h_: {"total": sum(1 for r in arm_rows if r["extracted_at"][11:13] == h_),
                 "affected": sum(1 for r in a if r["extracted_at"][11:13] == h_)}
            for h_ in sorted({r["extracted_at"][11:13] for r in arm_rows})
        },
    }


def pairs_csv_exposure(csv_path: Path, aff_ids: set, value_col: str = "o4mini_value") -> dict:
    """Concordance blast radius: rows whose value is empty only because of the loss."""
    if not csv_path.exists():
        return {}
    rows = list(csv.DictReader(csv_path.open()))
    sub = [r for r in rows if r["paper_id"] in {str(p) for p in aff_ids}]
    empty = [r for r in sub if not (r.get(value_col) or "").strip()]
    out = {
        "csv_rows": len(rows),
        "rows_from_affected_papers": len(sub),
        "affected_share_of_csv_pct": round(100.0 * len(sub) / len(rows), 1) if rows else None,
        "rows_with_empty_value": len(empty),
        "score_impact": {},
    }
    for col in [c for c in (rows[0] if rows else {}) if c.endswith("_score")]:
        total_mm = sum(1 for r in rows if r[col] == "MISMATCH")
        from_empty = sum(1 for r in empty if r[col] == "MISMATCH")
        out["score_impact"][col] = {
            "mismatch_total": total_mm,
            "mismatch_from_empty_cells": from_empty,
            "pct_of_all_mismatches": round(100.0 * from_empty / total_mm, 1) if total_mm else None,
        }
    return out


def build_report(db_path: Path, review_dir: Path) -> dict:
    rows = load_extractions(db_path)
    aff = affected(rows)
    parsed = review_dir / "parsed_text"

    def chars(pid):
        fs = sorted(parsed.glob(f"{pid}_v*.md"), key=lambda p: int(p.stem.rsplit("_v", 1)[1]))
        return len(fs[-1].read_text()) if fs else 0

    paper_chars = {r["paper_id"]: chars(r["paper_id"]) for r in rows}
    openai_aff = {r["paper_id"] for r in aff if r["arm"] == "openai_o4_mini_high"}

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "expected_spans_per_extraction": EXPECTED_SPANS,
        "affected": [
            {k: r[k] for k in
             ("arm", "paper_id", "spans_stored", "spans_in_response", "shape",
              "verdict", "extracted_at", "output_tokens", "reasoning_tokens",
              "visible_tokens")}
            for r in aff
        ],
        "fork_verdict_by_arm": {
            arm: dict(Counter(r["verdict"] for r in aff if r["arm"] == arm))
            for arm in sorted({r["arm"] for r in aff})
        },
        "shape_by_arm": {
            arm: dict(Counter(r["shape"] for r in aff if r["arm"] == arm))
            for arm in sorted({r["arm"] for r in aff})
        },
        "surviving_fields": surviving_fields(db_path, aff),
        "clustering_openai": clustering(rows, openai_aff, "openai_o4_mini_high", paper_chars),
        "pairs_csv_exposure": pairs_csv_exposure(
            review_dir / "exports" / "disagreement_pairs_3arm.csv", openai_aff
        ),
        "span_deficit": {
            arm: EXPECTED_SPANS * len({r["paper_id"] for r in rows if r["arm"] == arm})
                 - sum(r["spans_stored"] for r in rows if r["arm"] == arm)
            for arm in sorted({r["arm"] for r in rows})
        },
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Single-span collapse autopsy (read-only)")
    p.add_argument("--review", required=True)
    p.add_argument("--data-root", default="data")
    p.add_argument("--out-dir", default=None)
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    review_dir = Path(args.data_root) / args.review
    db_path = review_dir / "review.db"
    if not db_path.exists():
        logger.error("No review DB at %s", db_path)
        return 2

    report = build_report(db_path, review_dir)
    out_dir = Path(args.out_dir) if args.out_dir else review_dir / "analysis" / "provenance"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = out_dir / f"spanloss_autopsy_{ts}.json"
    out.write_text(json.dumps(report, indent=2))
    logger.info("%d affected extractions -> %s", len(report["affected"]), out.name)
    print(json.dumps({k: report[k] for k in
                      ("fork_verdict_by_arm", "shape_by_arm", "surviving_fields",
                       "span_deficit")}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
