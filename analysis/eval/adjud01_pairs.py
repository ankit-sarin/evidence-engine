"""ADJUD-01 — adjudicate the SCHEMA-EVAL-02 value-disagreement clause.

SCHEMA-EVAL-02's pre-registered rule rejected condition C on one clause:
C-vs-B value disagreement 11.4% > 10%. That metric is exact match on the
normalized value string, so it cannot distinguish "the model extracted a
different fact" from "the model worded the same fact differently".

This module enumerates the C-vs-B disagreement pairs the clause fired on,
splits them by codebook field type, draws a seeded sample of the free-text
pairs for human adjudication, and scores hand-assigned labels against the
pre-registered 80% SAME_FACT threshold.

Read-only. Zero model calls: the labels in adjud01_labels.json were assigned
by reading the pairs, not by asking a model.

    # enumerate + draw the sample (writes raw dumps to gitignored data/)
    PYTHONPATH=. python -m analysis.eval.adjud01_pairs --review surgical_autonomy

    # score the hand-assigned labels
    PYTHONPATH=. python -m analysis.eval.adjud01_pairs --review surgical_autonomy --score
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
from collections import Counter
from pathlib import Path

import yaml

from analysis.eval.analyze_schema_eval2 import _norm
from analysis.eval.schema_eval2 import COND_B, COND_C, read_results

logger = logging.getLogger(__name__)

# Pre-registered in the ADJUD-01 brief, before any pair was read.
SAMPLE_SEED = 20260731
SAMPLE_N = 60
SAME_FACT_THRESHOLD_PCT = 80.0

LABELS = ("SAME_FACT", "DIFFERENT_FACT", "UNCLEAR")

LABELS_PATH = Path(__file__).with_name("adjud01_labels.json")


def field_types(codebook_path: Path) -> dict[str, str]:
    """field_name -> codebook `type` (categorical / free_text / numeric)."""
    cb = yaml.safe_load(codebook_path.read_text())
    return {f["name"]: f.get("type", "unknown") for f in cb["fields"]}


def disagreement_pairs(rows: list[dict], types: dict[str, str]) -> list[dict]:
    """Every (paper, field) where B and C both produced a value and they differ.

    Mirrors analyze_schema_eval2.agreement() exactly — same intersection of
    papers, same intersection of fields, same _norm comparison — so the pairs
    enumerated here are precisely the ones the 11.4% counted.
    """

    def by_field(cond: str) -> dict[int, dict[str, dict]]:
        return {
            r["paper_id"]: {s["field_name"]: s for s in r.get("spans", []) if s.get("field_name")}
            for r in rows
            if r["ok"] and r["condition"] == cond
        }

    b, c = by_field(COND_B), by_field(COND_C)
    pairs: list[dict] = []
    for pid in sorted(set(b) & set(c)):
        for f in sorted(set(b[pid]) & set(c[pid])):
            bv, cv = b[pid][f].get("value"), c[pid][f].get("value")
            if _norm(bv) == _norm(cv):
                continue
            pairs.append(
                {
                    "pair_id": f"{pid}::{f}",
                    "paper_id": pid,
                    "field_name": f,
                    "field_type": types.get(f, "unknown"),
                    "b_value": bv,
                    "c_value": cv,
                    "b_snippet": b[pid][f].get("source_snippet"),
                    "c_snippet": c[pid][f].get("source_snippet"),
                }
            )
    return pairs


def compared_counts(rows: list[dict], types: dict[str, str]) -> Counter:
    """Denominator per field type — every compared (paper, field), agreeing or not."""

    def by_field(cond: str) -> dict[int, set[str]]:
        return {
            r["paper_id"]: {s["field_name"] for s in r.get("spans", []) if s.get("field_name")}
            for r in rows
            if r["ok"] and r["condition"] == cond
        }

    b, c = by_field(COND_B), by_field(COND_C)
    n: Counter = Counter()
    for pid in set(b) & set(c):
        for f in b[pid] & c[pid]:
            n[types.get(f, "unknown")] += 1
    return n


def draw_sample(pairs: list[dict], n: int = SAMPLE_N, seed: int = SAMPLE_SEED) -> list[dict]:
    """Seeded sample, or all pairs when fewer than n exist. Ordered by pair_id first
    so the draw does not depend on dict iteration order."""
    ordered = sorted(pairs, key=lambda p: p["pair_id"])
    if len(ordered) <= n:
        return ordered
    return sorted(random.Random(seed).sample(ordered, n), key=lambda p: p["pair_id"])


def score(sample: list[dict], labels: dict[str, str]) -> dict:
    """Apply the pre-registered 80% rule to the hand-assigned labels."""
    missing = [p["pair_id"] for p in sample if p["pair_id"] not in labels]
    if missing:
        raise SystemExit(f"unlabelled pairs: {missing}")
    bad = {k: v for k, v in labels.items() if v not in LABELS}
    if bad:
        raise SystemExit(f"labels outside the fixed vocabulary: {bad}")

    counts = Counter(labels[p["pair_id"]] for p in sample)
    total = len(sample)
    same_pct = 100.0 * counts["SAME_FACT"] / total if total else 0.0
    return {
        "adjudicated": total,
        "counts": dict(counts),
        "same_fact_pct": round(same_pct, 1),
        "threshold_pct": SAME_FACT_THRESHOLD_PCT,
        "outcome": "ADOPT_C_WITH_DOCUMENTED_OVERRIDE"
        if same_pct >= SAME_FACT_THRESHOLD_PCT
        else "RETAIN_B_STANDS",
        "by_field": {
            f: dict(Counter(labels[p["pair_id"]] for p in sample if p["field_name"] == f))
            for f in sorted({p["field_name"] for p in sample})
        },
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="ADJUD-01 pair enumeration and scoring")
    p.add_argument("--review", required=True)
    p.add_argument("--data-root", default="data")
    p.add_argument("--score", action="store_true", help="score adjud01_labels.json against the rule")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    review_dir = Path(args.data_root) / args.review
    rows = read_results(review_dir)
    if not rows:
        logger.error("no results under %s", review_dir / "eval/schema_eval2")
        return 2

    types = field_types(review_dir / "extraction_codebook.yaml")
    pairs = disagreement_pairs(rows, types)
    denom = compared_counts(rows, types)
    by_type = Counter(p["field_type"] for p in pairs)

    free_text = [p for p in pairs if p["field_type"] == "free_text"]
    sample = draw_sample(free_text)

    summary = {
        "seed": SAMPLE_SEED,
        "sample_n_requested": SAMPLE_N,
        "sample_n_drawn": len(sample),
        "compared_total": sum(denom.values()),
        "disagree_total": len(pairs),
        "disagreement_pct": round(100.0 * len(pairs) / sum(denom.values()), 1),
        "by_field_type": {
            t: {
                "compared": denom[t],
                "disagree": by_type[t],
                "pct": round(100.0 * by_type[t] / denom[t], 1) if denom[t] else None,
            }
            for t in sorted(denom)
        },
        "free_text_disagreements_by_field": dict(Counter(p["field_name"] for p in free_text)),
    }

    out_dir = review_dir / "eval" / "schema_eval2"
    (out_dir / "adjud01_all_pairs.json").write_text(json.dumps(pairs, indent=2))
    (out_dir / "adjud01_sample.json").write_text(json.dumps(sample, indent=2))
    (out_dir / "adjud01_summary.json").write_text(json.dumps(summary, indent=2))
    logger.info("wrote adjud01_{all_pairs,sample,summary}.json to %s", out_dir)

    if args.score:
        summary["adjudication"] = score(sample, json.loads(LABELS_PATH.read_text())["labels"])
        (out_dir / "adjud01_summary.json").write_text(json.dumps(summary, indent=2))

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
