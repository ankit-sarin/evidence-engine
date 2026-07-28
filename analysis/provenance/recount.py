"""Re-tabulate an existing provenance census under a different field-class axis.

Nothing here re-classifies a span. The census run is read as-is and only the
grouping changes, so the three-way (STATED/INFERABLE/JUDGMENT) and binary
(extractive/interpretive) cross-tabs are guaranteed to describe the same
11,017 spans.

Read-only against the DB. No Ollama, no network.

Usage:
    PYTHONPATH=. python -m analysis.provenance.recount --review surgical_autonomy
    PYTHONPATH=. python -m analysis.provenance.recount --review surgical_autonomy \\
        --run-id provcensus_surgical_autonomy_20260727T194748Z
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from analysis.provenance import classifier as C
from analysis.provenance.field_class import field_class as field_class_binary
from analysis.provenance.field_class3 import (
    CLASSES,
    FIELD_CLASS3,
    PAPER_VARIABLE,
    VERSION,
    field_class3,
)

logger = logging.getLogger(__name__)

# The class carved out in taxonomy v1.1. Excluded from no-basis denominators
# because it is a separated population, not a traceability outcome.
ABSENCE = C.ABSENCE_CLAIM
NO_BASIS = C.UNTRACEABLE_NO_BASIS


# ── loading ──────────────────────────────────────────────────────────────


def load_census(db_path: Path, run_id: str | None = None) -> tuple[str, list[dict]]:
    """Load one census run's rows. Defaults to the most recent run."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        if run_id is None:
            row = conn.execute(
                "SELECT census_run_id FROM provenance_census_runs "
                "ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if row is None:
                raise RuntimeError("No provenance census runs found")
            run_id = row[0]
        rows = [
            dict(r)
            for r in conn.execute(
                "SELECT arm, paper_id, field_name, taxonomy_class, snippet_chars, "
                "       field_class AS field_class_binary "
                "  FROM provenance_classifications WHERE census_run_id = ?",
                (run_id,),
            )
        ]
    finally:
        conn.close()
    if not rows:
        raise RuntimeError(f"Census run {run_id} has no rows")
    return run_id, rows


def annotate(rows: list[dict]) -> list[dict]:
    """Attach the three-way class. Does not touch taxonomy_class."""
    for r in rows:
        r["field_class3"] = field_class3(r["field_name"])
    return rows


# ── tabulation (pure; unit-tested without a DB) ──────────────────────────


def distribution(rows: list[dict], key: str) -> dict[str, dict[str, int]]:
    """taxonomy_class counts grouped by rows[key]. Groups with '' key included."""
    out: dict[str, dict[str, int]] = {}
    for r in rows:
        group = r.get(key) or ""
        bucket = out.setdefault(group, {c: 0 for c in C.ALL_CLASSES})
        bucket[r["taxonomy_class"]] += 1
    return out


def cross_tab(rows: list[dict], key: str) -> dict[str, dict[str, dict[str, int]]]:
    """arm -> group -> taxonomy_class counts."""
    out: dict[str, dict[str, dict[str, int]]] = {}
    for arm in sorted({r["arm"] for r in rows}):
        out[arm] = distribution([r for r in rows if r["arm"] == arm], key)
    return out


def no_basis_rate(rows: list[dict], key: str, group: str, arm: str | None = None) -> dict:
    """No-basis rate for one group.

    Denominator = spans with a snippet, EXCLUDING ABSENCE_CLAIM. Absence claims
    are a separated population under taxonomy v1.1 and counting them here would
    reintroduce exactly the contamination TAXONOMY-CENSUS-02 removed.
    """
    sub = [
        r for r in rows
        if (r.get(key) or "") == group
        and (arm is None or r["arm"] == arm)
        and r["snippet_chars"] > 0
        and r["taxonomy_class"] != ABSENCE
    ]
    n = len(sub)
    hits = sum(1 for r in sub if r["taxonomy_class"] == NO_BASIS)
    return {
        "no_basis": hits,
        "denominator": n,
        "rate_pct": round(100.0 * hits / n, 2) if n else None,
    }


def no_basis_by_class(rows: list[dict], key: str, groups) -> dict:
    """Pooled + per-arm no-basis rates for each group of a class axis."""
    arms = sorted({r["arm"] for r in rows})
    out: dict[str, dict] = {}
    for g in groups:
        out[g] = {"POOLED": no_basis_rate(rows, key, g)}
        for arm in arms:
            out[g][arm] = no_basis_rate(rows, key, g, arm)
    return out


def per_field_table(rows: list[dict]) -> list[dict]:
    """One row per field: class, no-basis rate, per-arm rates, absence count."""
    arms = sorted({r["arm"] for r in rows})
    fields = sorted({r["field_name"] for r in rows})
    table = []
    for f in fields:
        sub = [r for r in rows if r["field_name"] == f]
        scored = [r for r in sub if r["snippet_chars"] > 0 and r["taxonomy_class"] != ABSENCE]
        hits = sum(1 for r in scored if r["taxonomy_class"] == NO_BASIS)
        entry = {
            "field": f,
            "class3": field_class3(f),
            "class_binary": field_class_binary(f),
            "paper_variable": f in PAPER_VARIABLE,
            "no_basis": hits,
            "denominator": len(scored),
            "rate_pct": round(100.0 * hits / len(scored), 2) if scored else None,
            "absence_claims": sum(1 for r in sub if r["taxonomy_class"] == ABSENCE),
        }
        for arm in arms:
            a = [r for r in scored if r["arm"] == arm]
            ah = sum(1 for r in a if r["taxonomy_class"] == NO_BASIS)
            entry[arm] = round(100.0 * ah / len(a), 2) if a else None
        table.append(entry)
    return sorted(table, key=lambda e: (e["rate_pct"] is None, -(e["rate_pct"] or 0)))


def coverage_check(rows: list[dict]) -> dict:
    """Every span must land in exactly one group on each axis."""
    unknown3 = Counter(r["field_name"] for r in rows if not r.get("field_class3"))
    unknown_bin = Counter(r["field_name"] for r in rows if not r.get("field_class_binary"))
    return {
        "spans": len(rows),
        "unclassified_three_way": dict(unknown3),
        "unclassified_binary": dict(unknown_bin),
        "axes_agree_on_coverage": set(unknown3) == set(unknown_bin),
    }


def build_report(run_id: str, rows: list[dict]) -> dict:
    return {
        "census_run_id": run_id,
        "field_class_version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "coverage": coverage_check(rows),
        "three_way": {
            "distribution_by_arm": cross_tab(rows, "field_class3"),
            "pooled": distribution(rows, "field_class3"),
            "no_basis": no_basis_by_class(rows, "field_class3", CLASSES),
        },
        "binary": {
            "distribution_by_arm": cross_tab(rows, "field_class_binary"),
            "pooled": distribution(rows, "field_class_binary"),
            "no_basis": no_basis_by_class(
                rows, "field_class_binary", ("extractive", "interpretive")
            ),
        },
        "per_field": per_field_table(rows),
        "paper_variable_fields": sorted(PAPER_VARIABLE),
        "field_assignments": {
            k: {"class": v[0], "basis": v[1], "justification": v[2]}
            for k, v in FIELD_CLASS3.items()
        },
    }


# ── CLI ──────────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Recount a provenance census by field class")
    p.add_argument("--review", required=True)
    p.add_argument("--data-root", default="data")
    p.add_argument("--run-id", default=None, help="default: most recent census run")
    p.add_argument("--out-dir", default=None)
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    review_dir = Path(args.data_root) / args.review
    db_path = review_dir / "review.db"
    if not db_path.exists():
        logger.error("No review DB at %s", db_path)
        return 2

    run_id, rows = load_census(db_path, args.run_id)
    annotate(rows)
    report = build_report(run_id, rows)

    out_dir = Path(args.out_dir) if args.out_dir else review_dir / "analysis" / "provenance"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = out_dir / f"recount_fieldclass3_{ts}.json"
    out.write_text(json.dumps(report, indent=2))
    logger.info("Recounted %d spans from %s -> %s", len(rows), run_id, out.name)

    print(json.dumps(report["three_way"]["no_basis"], indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
