"""Restate Pass 2 judge verdicts against the frozen provenance taxonomy.

Two instruments, two dimensions — this module joins them and must not be read
as scoring one against the other:

  * The **judge** (Pass 2, `fabrication_verifications`) is an NLI-style
    *supportedness* instrument: given an arm's value and its snippet, does the
    snippet support the value? It asks a semantic question and knows nothing
    about where the snippet came from.
  * The **census** (`provenance_classifications`, taxonomy v1.1) is a lexical
    *provenance* instrument: can the snippet be located in the paper? It asks a
    string question and knows nothing about whether the snippet supports
    anything.

Consequently `SUPPORTED` on a `STITCHED` or `DRIFTED` span is **expected
inter-instrument divergence, not judge failure**: the text is real, the arm
joined or lightly edited it, and it does support the value. The cell that is a
genuine judge failure is `SUPPORTED` on `UNTRACEABLE_NO_BASIS` — the judge
endorsing evidence the arm authored.

Read-only. No Ollama, no re-scoring, no judge or prompt changes.

Usage:
    PYTHONPATH=. python -m analysis.paper1.judge_provenance --review surgical_autonomy
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from analysis.provenance import classifier as C
from analysis.provenance.field_class3 import CLASSES as FIELD_CLASSES
from analysis.provenance.field_class3 import field_class3

logger = logging.getLogger(__name__)

# The judge stores a short arm label; the census stores the full arm id used
# throughout the extraction tables. This is the only key transformation.
ARM_ALIASES = {"local": "local_deepseek_r1_32b"}

VERDICTS = ("SUPPORTED", "PARTIALLY_SUPPORTED", "UNSUPPORTED")

# The one cell that is a real judge failure under the two-instrument framing.
TRUE_FAILURE = (("SUPPORTED",), (C.UNTRACEABLE_NO_BASIS,))

# Real-but-nonconforming provenance: the text exists in the paper, the arm
# joined or lightly edited it. SUPPORTED here is divergence, not error.
REAL_BUT_NONCONFORMING = (C.STITCHED, C.DRIFTED)


def canonical_arm(arm_name: str) -> str:
    return ARM_ALIASES.get(arm_name, arm_name)


# ── loading ──────────────────────────────────────────────────────────────


def _ro(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def latest_judge_run(db_path: Path) -> str:
    conn = _ro(db_path)
    try:
        row = conn.execute(
            "SELECT run_id FROM judge_runs WHERE pass_number = 2 "
            "ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise RuntimeError("No Pass 2 judge runs found")
    return row[0]


def load_verdicts(db_path: Path, judge_run_id: str) -> list[dict]:
    conn = _ro(db_path)
    try:
        rows = [
            {
                "paper_id": int(r["paper_id"]),
                "field_name": r["field_name"],
                "arm": canonical_arm(r["arm_name"]),
                "arm_raw": r["arm_name"],
                "verdict": r["verdict"],
                "pre_check_short_circuit": r["pre_check_short_circuit"],
            }
            for r in conn.execute(
                "SELECT paper_id, field_name, arm_name, verdict, pre_check_short_circuit "
                "FROM fabrication_verifications WHERE judge_run_id = ?",
                (judge_run_id,),
            )
        ]
    finally:
        conn.close()
    if not rows:
        raise RuntimeError(f"Judge run {judge_run_id} has no verifications")
    return rows


def load_spans(db_path: Path, census_run_id: str) -> dict[tuple, dict]:
    conn = _ro(db_path)
    try:
        rows = conn.execute(
            "SELECT arm, paper_id, field_name, taxonomy_class, absence_pattern, "
            "       snippet_chars, value "
            "  FROM provenance_classifications WHERE census_run_id = ?",
            (census_run_id,),
        ).fetchall()
    finally:
        conn.close()
    return {(r["arm"], r["paper_id"], r["field_name"]): dict(r) for r in rows}


def load_legacy(csv_path: Path | None) -> dict[tuple, bool]:
    """Legacy grep_verify verdicts, for the revision disclosure only."""
    if csv_path is None or not csv_path.exists():
        return {}
    out: dict[tuple, bool] = {}
    with csv_path.open() as fh:
        for r in csv.DictReader(fh):
            raw = (r.get("legacy_grep_verify") or "").strip()
            if raw in ("True", "False"):
                out[(r["arm"], int(r["paper_id"]), r["field_name"])] = raw == "True"
    return out


# ── join ─────────────────────────────────────────────────────────────────


def join(verdicts: list[dict], spans: dict[tuple, dict]) -> tuple[list[dict], list[dict]]:
    """Join on (arm, paper_id, field_name). Returns (joined, unjoined).

    Unjoined rows are judged arm-rows with no stored evidence span. They are
    returned rather than dropped: the judge having rendered a supportedness
    verdict where the arm produced no span at all is a finding about the judge's
    input, not a join defect to be swept up.
    """
    joined, unjoined = [], []
    for v in verdicts:
        key = (v["arm"], v["paper_id"], v["field_name"])
        span = spans.get(key)
        if span is None:
            unjoined.append(v)
        else:
            joined.append({**v, **{
                "taxonomy_class": span["taxonomy_class"],
                "absence_pattern": span["absence_pattern"],
                "snippet_chars": span["snippet_chars"],
                "value": span["value"],
                "field_class3": field_class3(v["field_name"]),
            }})
    return joined, unjoined


# ── tabulation (pure) ────────────────────────────────────────────────────


def verdict_by_class(rows: list[dict]) -> dict[str, dict[str, int]]:
    """verdict -> taxonomy_class -> count."""
    out = {v: {c: 0 for c in C.ALL_CLASSES} for v in VERDICTS}
    for r in rows:
        out.setdefault(r["verdict"], {c: 0 for c in C.ALL_CLASSES})
        out[r["verdict"]][r["taxonomy_class"]] += 1
    return out


def supported_breakdown(rows: list[dict], arm: str | None = None) -> dict:
    """Of SUPPORTED verdicts, the fraction resting on each provenance kind."""
    sup = [r for r in rows if r["verdict"] == "SUPPORTED" and (arm is None or r["arm"] == arm)]
    n = len(sup)

    def frac(pred):
        k = sum(1 for r in sup if pred(r))
        return {"n": k, "pct": round(100.0 * k / n, 2) if n else None}

    return {
        "supported_total": n,
        "on_no_basis": frac(lambda r: r["taxonomy_class"] == C.UNTRACEABLE_NO_BASIS),
        "on_any_non_anchored": frac(lambda r: r["taxonomy_class"] != C.ANCHORED),
        "on_stitched_or_drifted": frac(lambda r: r["taxonomy_class"] in REAL_BUT_NONCONFORMING),
        "on_untraceable_either": frac(
            lambda r: r["taxonomy_class"] in (C.UNTRACEABLE_NO_BASIS, C.UNTRACEABLE_PARTIAL)
        ),
        "on_anchored": frac(lambda r: r["taxonomy_class"] == C.ANCHORED),
        "on_absence_claim": frac(lambda r: r["taxonomy_class"] == C.ABSENCE_CLAIM),
    }


def true_failures(rows: list[dict]) -> list[dict]:
    verdicts, classes = TRUE_FAILURE
    return [r for r in rows if r["verdict"] in verdicts and r["taxonomy_class"] in classes]


def true_failure_crosstab(rows: list[dict]) -> dict:
    """SUPPORTED-on-no-basis counts by arm x field class, with denominators."""
    fails = true_failures(rows)
    arms = sorted({r["arm"] for r in rows})
    out: dict[str, dict] = {}
    for arm in arms + ["POOLED"]:
        cell: dict[str, dict] = {}
        for fc in FIELD_CLASSES:
            denom = [
                r for r in rows
                if (arm == "POOLED" or r["arm"] == arm)
                and r["field_class3"] == fc and r["verdict"] == "SUPPORTED"
            ]
            hits = [
                r for r in fails
                if (arm == "POOLED" or r["arm"] == arm) and r["field_class3"] == fc
            ]
            cell[fc] = {
                "failures": len(hits),
                "supported_in_class": len(denom),
                "pct_of_supported": round(100.0 * len(hits) / len(denom), 2) if denom else None,
            }
        cell["ALL"] = {
            "failures": sum(c["failures"] for c in cell.values()),
            "supported_in_class": sum(c["supported_in_class"] for c in cell.values()),
        }
        out[arm] = cell
    return out


def symmetry_check(rows: list[dict]) -> dict:
    """UNSUPPORTED on ANCHORED — the judge rejecting verbatim-present evidence."""
    arms = sorted({r["arm"] for r in rows})
    out = {}
    for arm in arms + ["POOLED"]:
        sub = [r for r in rows if (arm == "POOLED" or r["arm"] == arm)]
        anchored = [r for r in sub if r["taxonomy_class"] == C.ANCHORED]
        rejected = [r for r in anchored if r["verdict"] == "UNSUPPORTED"]
        partial = [r for r in anchored if r["verdict"] == "PARTIALLY_SUPPORTED"]
        out[arm] = {
            "anchored_rows": len(anchored),
            "unsupported_on_anchored": len(rejected),
            "pct": round(100.0 * len(rejected) / len(anchored), 2) if anchored else None,
            "partially_supported_on_anchored": len(partial),
        }
    return out


def absence_interaction(rows: list[dict]) -> dict:
    """How the judge scores ABSENCE_CLAIM spans (feeds the NOT_FOUND fix design)."""
    sub = [r for r in rows if r["taxonomy_class"] == C.ABSENCE_CLAIM]
    by_arm: dict[str, Counter] = defaultdict(Counter)
    for r in sub:
        by_arm[r["arm"]][r["verdict"]] += 1
    return {
        "absence_claim_rows_judged": len(sub),
        "verdicts": dict(Counter(r["verdict"] for r in sub)),
        "by_arm": {a: dict(c) for a, c in by_arm.items()},
        "by_pattern": dict(Counter(r["absence_pattern"] for r in sub)),
        "by_field": dict(Counter(r["field_name"] for r in sub).most_common(10)),
    }


def legacy_restatement(rows: list[dict], legacy: dict[tuple, bool]) -> dict | None:
    """The retired figure: SUPPORTED verdicts whose span failed legacy grep_verify."""
    if not legacy:
        return None
    sup = [r for r in rows if r["verdict"] == "SUPPORTED"]
    scored = [r for r in sup if (r["arm"], r["paper_id"], r["field_name"]) in legacy]
    unanchored = [
        r for r in scored if not legacy[(r["arm"], r["paper_id"], r["field_name"])]
    ]
    by_arm = {}
    for arm in sorted({r["arm"] for r in scored}):
        a = [r for r in scored if r["arm"] == arm]
        u = [r for r in unanchored if r["arm"] == arm]
        by_arm[arm] = {
            "supported": len(a),
            "legacy_unanchored": len(u),
            "pct": round(100.0 * len(u) / len(a), 2) if a else None,
        }
    return {
        "supported_with_legacy_verdict": len(scored),
        "legacy_unanchored": len(unanchored),
        "pct": round(100.0 * len(unanchored) / len(scored), 2) if scored else None,
        "by_arm": by_arm,
    }


def unjoined_summary(unjoined: list[dict], db_path: Path) -> dict:
    """Characterize judged arm-rows that have no stored span."""
    conn = _ro(db_path)
    try:
        local_counts = {
            r[0]: r[1] for r in conn.execute(
                "SELECT e.paper_id, COUNT(*) FROM evidence_spans s "
                "JOIN extractions e ON e.id = s.extraction_id GROUP BY 1"
            )
        }
        cloud_counts = {
            (r[0], r[1]): r[2] for r in conn.execute(
                "SELECT c.arm, c.paper_id, COUNT(*) FROM cloud_evidence_spans s "
                "JOIN cloud_extractions c ON c.id = s.cloud_extraction_id GROUP BY 1,2"
            )
        }
    finally:
        conn.close()

    def spans_for(row):
        if row["arm"] == "local_deepseek_r1_32b":
            return local_counts.get(row["paper_id"], 0)
        return cloud_counts.get((row["arm"], row["paper_id"]), 0)

    return {
        "rows": len(unjoined),
        "by_arm": dict(Counter(r["arm"] for r in unjoined)),
        "by_verdict": dict(Counter(r["verdict"] for r in unjoined)),
        "distinct_papers": len({(r["arm"], r["paper_id"]) for r in unjoined}),
        "span_count_of_affected_extraction": dict(
            Counter(spans_for(r) for r in unjoined)
        ),
    }


def build_report(
    judge_run_id: str,
    census_run_id: str,
    joined: list[dict],
    unjoined: list[dict],
    legacy: dict,
    db_path: Path,
) -> dict:
    arms = sorted({r["arm"] for r in joined})
    return {
        "judge_run_id": judge_run_id,
        "census_run_id": census_run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "join": {
            "key": "(arm, paper_id, field_name); judge arm 'local' -> 'local_deepseek_r1_32b'; "
                   "judge paper_id is TEXT and is cast to INTEGER",
            "judge_rows": len(joined) + len(unjoined),
            "joined": len(joined),
            "unjoined": unjoined_summary(unjoined, db_path),
        },
        "verdict_by_taxonomy_class": verdict_by_class(joined),
        "headline": {
            "POOLED": supported_breakdown(joined),
            **{a: supported_breakdown(joined, a) for a in arms},
        },
        "legacy": legacy_restatement(joined, legacy),
        "true_failures": {
            "total": len(true_failures(joined)),
            "crosstab": true_failure_crosstab(joined),
            "by_field": dict(Counter(r["field_name"] for r in true_failures(joined))),
        },
        "symmetry": symmetry_check(joined),
        "absence_interaction": absence_interaction(joined),
    }


# ── CLI ──────────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Judge verdict x provenance restatement")
    p.add_argument("--review", required=True)
    p.add_argument("--data-root", default="data")
    p.add_argument("--judge-run-id", default=None, help="default: latest Pass 2 run")
    p.add_argument("--census-run-id", default=None, help="default: latest census run")
    p.add_argument("--legacy-csv", default=None, help="census_spans_*.csv for the legacy figure")
    p.add_argument("--out-dir", default=None)
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    review_dir = Path(args.data_root) / args.review
    db_path = review_dir / "review.db"
    if not db_path.exists():
        logger.error("No review DB at %s", db_path)
        return 2

    judge_run_id = args.judge_run_id or latest_judge_run(db_path)
    conn = _ro(db_path)
    try:
        census_run_id = args.census_run_id or conn.execute(
            "SELECT census_run_id FROM provenance_census_runs ORDER BY created_at DESC LIMIT 1"
        ).fetchone()[0]
    finally:
        conn.close()

    out_dir = Path(args.out_dir) if args.out_dir else review_dir / "analysis" / "provenance"
    legacy_csv = Path(args.legacy_csv) if args.legacy_csv else next(
        iter(sorted(out_dir.glob("census_spans_*.csv"), reverse=True)), None
    )

    verdicts = load_verdicts(db_path, judge_run_id)
    spans = load_spans(db_path, census_run_id)
    joined, unjoined = join(verdicts, spans)
    report = build_report(
        judge_run_id, census_run_id, joined, unjoined, load_legacy(legacy_csv), db_path
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = out_dir / f"judge_provenance_{ts}.json"
    out.write_text(json.dumps(report, indent=2))
    logger.info(
        "Judge %s x census %s: %d joined, %d unjoined -> %s",
        judge_run_id, census_run_id, len(joined), len(unjoined), out.name,
    )
    print(json.dumps({k: report[k] for k in ("headline", "true_failures")}, indent=2)[:4000])
    return 0


if __name__ == "__main__":
    sys.exit(main())
