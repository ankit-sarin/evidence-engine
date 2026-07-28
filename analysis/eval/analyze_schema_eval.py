"""SCHEMA-EVAL-01 analysis: the five pre-registered measures and the decision rule.

Measures, per condition per arm:
  1. Shape/completeness — guard pass rate, parse path, retry-equivalent count.
  2. Value agreement — A vs B per field, and each condition vs Run 6 (drift context).
  3. Evidence quality — the frozen v1.1 provenance ladder over every snippet.
     This is the tokenization-drift detector: constrained decoding forces
     non-canonical token boundaries, so if it damages verbatim quoting it shows
     up as anchored→drifted/no-basis movement.
  4. NOT_FOUND / absence-assertion rate.
  5. Latency and token usage.

Decision rule, pinned before the run and applied verbatim in `decide()`.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

from analysis.eval.schema_eval import CONDITION_A, CONDITION_B, read_results
from analysis.provenance import classifier as C
from analysis.provenance.absence import match_absence_pattern

logger = logging.getLogger(__name__)

# ── pre-registered decision rule ─────────────────────────────────────────
ANCHORED_DROP_PP = 3.0        # >3pp absolute A→B drop in anchored rate ⇒ reject
DISAGREEMENT_PCT = 10.0       # >10% of fields disagreeing A vs B ⇒ reject
# Third clause (local only): think-channel degraded or absent ⇒ reject.


def _norm(v) -> str:
    return " ".join(str(v or "").strip().lower().split())


ABSENCE_VALUES = {"", "nr", "n/a", "na", "not_found", "not found", "not reported", "none"}


# ── measure 1: shape / completeness ──────────────────────────────────────


def shape_measures(rows: list[dict]) -> dict:
    ok = [r for r in rows if r["ok"]]
    return {
        "calls": len(rows),
        "errors": len(rows) - len(ok),
        "complete": sum(1 for r in ok if r["complete"]),
        "guard_pass_rate_pct": round(100.0 * sum(1 for r in ok if r["complete"]) / len(ok), 1) if ok else None,
        "median_spans": statistics.median([r["n_spans"] for r in ok]) if ok else None,
        "parse_paths": dict(Counter(r.get("parse_path") for r in ok)),
        "would_retry": sum(1 for r in ok if not r["complete"]),
    }


# ── measure 2: value agreement ───────────────────────────────────────────


def _spans_by_field(row: dict) -> dict[str, dict]:
    return {s["field_name"]: s for s in row.get("spans", []) if s.get("field_name")}


def value_agreement(rows_a: list[dict], rows_b: list[dict]) -> dict:
    a_by = {r["paper_id"]: _spans_by_field(r) for r in rows_a if r["ok"]}
    b_by = {r["paper_id"]: _spans_by_field(r) for r in rows_b if r["ok"]}
    common = sorted(set(a_by) & set(b_by))
    agree = disagree = 0
    only_a = only_b = 0
    per_field: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for pid in common:
        fields = set(a_by[pid]) | set(b_by[pid])
        for f in fields:
            av, bv = a_by[pid].get(f), b_by[pid].get(f)
            if av is None:
                only_b += 1
                continue
            if bv is None:
                only_a += 1
                continue
            if _norm(av.get("value")) == _norm(bv.get("value")):
                agree += 1
                per_field[f][0] += 1
            else:
                disagree += 1
                per_field[f][1] += 1
    total = agree + disagree
    return {
        "papers_compared": len(common),
        "fields_compared": total,
        "agree": agree,
        "disagree": disagree,
        "disagreement_pct": round(100.0 * disagree / total, 1) if total else None,
        "present_only_in_A": only_a,
        "present_only_in_B": only_b,
        "worst_fields": sorted(
            ({"field": f, "agree": v[0], "disagree": v[1]} for f, v in per_field.items()),
            key=lambda d: -d["disagree"],
        )[:6],
    }


def agreement_vs_run6(rows: list[dict], db_path: Path, arm_table: str = "local") -> dict:
    """Drift context: each condition against the Run 6 stored values."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        if arm_table == "local":
            q = ("SELECT e.paper_id, s.field_name, s.value FROM evidence_spans s "
                 "JOIN extractions e ON e.id = s.extraction_id")
            stored = {(r["paper_id"], r["field_name"]): r["value"] for r in conn.execute(q)}
        else:
            q = ("SELECT c.paper_id, s.field_name, s.value FROM cloud_evidence_spans s "
                 "JOIN cloud_extractions c ON c.id = s.cloud_extraction_id WHERE c.arm = ?")
            stored = {(r["paper_id"], r["field_name"]): r["value"]
                      for r in conn.execute(q, (arm_table,))}
    finally:
        conn.close()

    agree = disagree = missing = 0
    for r in rows:
        if not r["ok"]:
            continue
        for f, s in _spans_by_field(r).items():
            key = (r["paper_id"], f)
            if key not in stored:
                missing += 1
                continue
            if _norm(s.get("value")) == _norm(stored[key]):
                agree += 1
            else:
                disagree += 1
    total = agree + disagree
    return {
        "compared": total, "agree": agree, "disagree": disagree,
        "disagreement_pct": round(100.0 * disagree / total, 1) if total else None,
        "no_run6_counterpart": missing,
    }


# ── measure 3: evidence quality (provenance ladder) ──────────────────────


def provenance_measures(rows: list[dict], parsed_dir: Path) -> dict:
    cache: dict[int, C.PaperIndex] = {}
    counts: Counter = Counter()
    for r in rows:
        if not r["ok"]:
            continue
        pid = r["paper_id"]
        if pid not in cache:
            files = sorted(parsed_dir.glob(f"{pid}_v*.md"),
                           key=lambda f: int(f.stem.rsplit("_v", 1)[1]))
            cache[pid] = C.PaperIndex.build(pid, files[-1].read_text())
        for s in r.get("spans", []):
            res = C.classify_span(s.get("source_snippet"), s.get("value"), cache[pid])
            counts[res.taxonomy_class] += 1
    total = sum(counts.values())
    out = {"spans_classified": total, "counts": dict(counts)}
    for cls in C.ALL_CLASSES:
        out[f"{cls}_pct"] = round(100.0 * counts[cls] / total, 1) if total else None
    return out


# ── measure 4: absence rate ──────────────────────────────────────────────


def absence_measures(rows: list[dict]) -> dict:
    n = absent_value = absent_snippet = empty_snippet = 0
    for r in rows:
        if not r["ok"]:
            continue
        for s in r.get("spans", []):
            n += 1
            if _norm(s.get("value")) in ABSENCE_VALUES:
                absent_value += 1
            snip = s.get("source_snippet") or ""
            if not snip.strip():
                empty_snippet += 1
            elif match_absence_pattern(snip):
                absent_snippet += 1
    return {
        "spans": n,
        "absence_values": absent_value,
        "absence_value_pct": round(100.0 * absent_value / n, 1) if n else None,
        "absence_assertion_snippets": absent_snippet,
        "absence_assertion_pct": round(100.0 * absent_snippet / n, 1) if n else None,
        "empty_snippets": empty_snippet,
    }


# ── measure 5: latency / tokens ──────────────────────────────────────────


def cost_measures(rows: list[dict]) -> dict:
    ok = [r for r in rows if r["ok"]]

    def med(key):
        vals = [r[key] for r in ok if r.get(key) is not None]
        return round(statistics.median(vals), 1) if vals else None

    return {
        "median_latency_s": med("latency_s"),
        "total_latency_s": round(sum(r["latency_s"] for r in ok), 1),
        "median_input_tokens": med("input_tokens"),
        "median_output_tokens": med("output_tokens"),
        "median_reasoning_tokens": med("reasoning_tokens"),
        "median_think_chars": med("think_chars"),
        "think_present": sum(1 for r in ok if (r.get("think_chars") or 0) > 0),
    }


# ── decision rule ────────────────────────────────────────────────────────


def decide(arm: str, a_prov: dict, b_prov: dict, agreement: dict,
           think_ok: bool | None) -> dict:
    """Apply the pre-registered rule verbatim."""
    reasons, ambiguous = [], []
    a_anch = a_prov.get("ANCHORED_pct")
    b_anch = b_prov.get("ANCHORED_pct")
    drop = round(a_anch - b_anch, 1) if (a_anch is not None and b_anch is not None) else None
    if drop is None:
        ambiguous.append("anchored rate not computable for both conditions")
    elif drop > ANCHORED_DROP_PP:
        reasons.append(f"anchored rate dropped {drop}pp A→B (> {ANCHORED_DROP_PP}pp)")

    dis = agreement.get("disagreement_pct")
    if dis is None:
        ambiguous.append("value disagreement not computable")
    elif dis > DISAGREEMENT_PCT:
        reasons.append(f"A-vs-B value disagreement {dis}% (> {DISAGREEMENT_PCT}%)")

    if think_ok is False:
        reasons.append("think-channel degraded or absent under constraint")

    verdict = "ADOPT" if not reasons else "REJECT"
    if ambiguous and not reasons:
        verdict = "AMBIGUOUS"
    return {
        "arm": arm, "verdict": verdict,
        "anchored_pct_A": a_anch, "anchored_pct_B": b_anch, "anchored_drop_pp": drop,
        "disagreement_pct": dis, "think_ok": think_ok,
        "reject_reasons": reasons, "ambiguous_notes": ambiguous,
        "thresholds": {"anchored_drop_pp": ANCHORED_DROP_PP,
                       "disagreement_pct": DISAGREEMENT_PCT},
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Analyze the schema A/B eval")
    p.add_argument("--review", required=True)
    p.add_argument("--data-root", default="data")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    review_dir = Path(args.data_root) / args.review
    db_path = review_dir / "review.db"
    parsed = review_dir / "parsed_text"
    rows = read_results(review_dir)
    if not rows:
        logger.error("No eval results found under %s", review_dir / "eval/schema_eval")
        return 2

    report: dict = {"n_rows": len(rows), "by_arm": {}}
    for arm in sorted({r["arm"] for r in rows}):
        arm_rows = [r for r in rows if r["arm"] == arm]
        a = [r for r in arm_rows if r["condition"] == CONDITION_A]
        b = [r for r in arm_rows if r["condition"] == CONDITION_B]
        entry = {
            "A": {"shape": shape_measures(a), "provenance": provenance_measures(a, parsed),
                  "absence": absence_measures(a), "cost": cost_measures(a)} if a else None,
            "B": {"shape": shape_measures(b), "provenance": provenance_measures(b, parsed),
                  "absence": absence_measures(b), "cost": cost_measures(b)} if b else None,
        }
        if a and b:
            entry["agreement_A_vs_B"] = value_agreement(a, b)
            think_ok = None
            b_think = entry["B"]["cost"]["think_present"]
            a_think = entry["A"]["cost"]["think_present"]
            if a_think or b_think:
                think_ok = not (a_think > 0 and b_think == 0)
            entry["decision"] = decide(arm, entry["A"]["provenance"],
                                       entry["B"]["provenance"],
                                       entry["agreement_A_vs_B"], think_ok)
        arm_table = "local" if "deepseek" in arm else arm
        for cond, rws in (("A", a), ("B", b)):
            if rws:
                entry.setdefault("vs_run6", {})[cond] = agreement_vs_run6(rws, db_path, arm_table)
        report["by_arm"][arm] = entry

    out = review_dir / "eval" / "schema_eval" / "analysis_summary.json"
    out.write_text(json.dumps(report, indent=2, default=str))
    logger.info("Wrote %s", out)
    print(json.dumps(report, indent=2, default=str)[:6000])
    return 0


if __name__ == "__main__":
    sys.exit(main())
