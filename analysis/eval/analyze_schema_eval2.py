"""SCHEMA-EVAL-02 analysis: six measures and the pre-registered decision rule.

Measure 6 is the restoration read: condition B is the current production contract,
so B's anchored rate against Run 6's on the same papers answers "did fixing the
thinking channel restore Run 6 quality?" independently of the contract question.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import statistics
import sys
from collections import Counter
from pathlib import Path

from analysis.eval.schema_eval2 import COND_A, COND_B, COND_C, CONDITIONS, read_results
from analysis.provenance import classifier as C
from analysis.provenance.absence import match_absence_pattern

logger = logging.getLogger(__name__)

# ── pre-registered thresholds ────────────────────────────────────────────
ANCHORED_MARGIN_PP = 3.0
DISAGREEMENT_PCT = 10.0

ABSENCE_VALUES = {"", "nr", "n/a", "na", "not_found", "not found", "not reported", "none"}


def _norm(v) -> str:
    return " ".join(str(v or "").strip().lower().split())


# ── measure 1: provenance ladder ─────────────────────────────────────────


def provenance(rows: list[dict], parsed_dir: Path, cache: dict) -> dict:
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
            counts[C.classify_span(s.get("source_snippet"), s.get("value"),
                                   cache[pid]).taxonomy_class] += 1
    total = sum(counts.values())
    out = {"spans": total, "counts": dict(counts)}
    for cls in C.ALL_CLASSES:
        out[f"{cls}_pct"] = round(100.0 * counts[cls] / total, 1) if total else None
    return out


def provenance_by_paper(rows: list[dict], parsed_dir: Path, cache: dict) -> dict[int, tuple[int, int]]:
    """paper_id -> (anchored, spans). Enables a paired comparison across conditions."""
    out: dict[int, tuple[int, int]] = {}
    for r in rows:
        if not r["ok"]:
            continue
        pid = r["paper_id"]
        if pid not in cache:
            files = sorted(parsed_dir.glob(f"{pid}_v*.md"),
                           key=lambda f: int(f.stem.rsplit("_v", 1)[1]))
            cache[pid] = C.PaperIndex.build(pid, files[-1].read_text())
        a = sum(1 for s in r["spans"]
                if C.classify_span(s.get("source_snippet"), s.get("value"),
                                   cache[pid]).taxonomy_class == C.ANCHORED)
        out[pid] = (a, len(r["spans"]))
    return out


# ── measure 2: completeness guard ────────────────────────────────────────


def shape(rows: list[dict]) -> dict:
    ok = [r for r in rows if r["ok"]]
    return {
        "calls": len(rows),
        "errors": len(rows) - len(ok),
        "complete": sum(1 for r in ok if r["complete"]),
        "guard_pass_pct": round(100.0 * sum(1 for r in ok if r["complete"]) / len(ok), 1) if ok else None,
        "would_retry": sum(r["retries"] for r in ok),
        "median_spans": statistics.median([r["n_spans"] for r in ok]) if ok else None,
        "parse_paths": dict(Counter(r["parse_path"] for r in ok)),
        "parse_branches": dict(Counter(r["parse_branch"] for r in ok)),
    }


# ── measure 3: value agreement ───────────────────────────────────────────


def _by_field(r: dict) -> dict[str, dict]:
    return {s["field_name"]: s for s in r.get("spans", []) if s.get("field_name")}


def agreement(rows_x: list[dict], rows_y: list[dict]) -> dict:
    x = {r["paper_id"]: _by_field(r) for r in rows_x if r["ok"]}
    y = {r["paper_id"]: _by_field(r) for r in rows_y if r["ok"]}
    agree = disagree = 0
    worst: Counter = Counter()
    for pid in sorted(set(x) & set(y)):
        for f in set(x[pid]) & set(y[pid]):
            if _norm(x[pid][f].get("value")) == _norm(y[pid][f].get("value")):
                agree += 1
            else:
                disagree += 1
                worst[f] += 1
    total = agree + disagree
    return {
        "papers": len(set(x) & set(y)), "fields": total, "agree": agree,
        "disagree": disagree,
        "disagreement_pct": round(100.0 * disagree / total, 1) if total else None,
        "worst_fields": worst.most_common(5),
    }


def vs_run6(rows: list[dict], db_path: Path) -> dict:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        stored = {
            (r[0], r[1]): r[2] for r in conn.execute(
                "SELECT e.paper_id, s.field_name, s.value FROM evidence_spans s "
                "JOIN extractions e ON e.id = s.extraction_id"
            )
        }
    finally:
        conn.close()
    agree = disagree = missing = 0
    for r in rows:
        if not r["ok"]:
            continue
        for f, s in _by_field(r).items():
            key = (r["paper_id"], f)
            if key not in stored:
                missing += 1
            elif _norm(s.get("value")) == _norm(stored[key]):
                agree += 1
            else:
                disagree += 1
    total = agree + disagree
    return {
        "compared": total, "agree": agree, "disagree": disagree,
        "disagreement_pct": round(100.0 * disagree / total, 1) if total else None,
        "no_run6_counterpart": missing,
    }


# ── measure 4: absence ───────────────────────────────────────────────────


def absence(rows: list[dict]) -> dict:
    n = av = aa = empty = 0
    for r in rows:
        if not r["ok"]:
            continue
        for s in r.get("spans", []):
            n += 1
            if _norm(s.get("value")) in ABSENCE_VALUES:
                av += 1
            snip = s.get("source_snippet") or ""
            if not snip.strip():
                empty += 1
            elif match_absence_pattern(snip):
                aa += 1
    return {
        "spans": n, "absence_values": av,
        "absence_value_pct": round(100.0 * av / n, 1) if n else None,
        "absence_assertions": aa,
        "absence_assertion_pct": round(100.0 * aa / n, 1) if n else None,
        "empty_snippets": empty,
    }


# ── measure 5: cost ──────────────────────────────────────────────────────


def cost(rows: list[dict]) -> dict:
    ok = [r for r in rows if r["ok"]]

    def med(k):
        v = [r[k] for r in ok if r.get(k) is not None]
        return round(statistics.median(v), 1) if v else None

    return {
        "median_pass1_s": med("pass1_latency_s"),
        "median_pass2_s": med("pass2_latency_s"),
        "median_total_s": med("total_latency_s"),
        "total_hours": round(sum(r["total_latency_s"] for r in ok) / 3600, 2),
        "median_prompt_eval": med("prompt_eval_count"),
        "median_eval": med("eval_count"),
        "median_thinking_chars": med("thinking_chars"),
        "done_reasons": dict(Counter(r["done_reason"] for r in ok)),
    }


# ── measure 6: restoration ───────────────────────────────────────────────


def restoration(rows_b: list[dict], db_path: Path, parsed_dir: Path, cache: dict) -> dict:
    """Condition B (= production contract) anchored vs Run 6 on the SAME papers."""
    pids = sorted({r["paper_id"] for r in rows_b if r["ok"]})
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        run6: dict[int, list[dict]] = {}
        for pid in pids:
            run6[pid] = [
                dict(r) for r in conn.execute(
                    "SELECT s.field_name, s.value, s.source_snippet FROM evidence_spans s "
                    "JOIN extractions e ON e.id = s.extraction_id WHERE e.paper_id = ?",
                    (pid,),
                )
            ]
    finally:
        conn.close()

    def anch(spans, pid):
        if pid not in cache:
            files = sorted(parsed_dir.glob(f"{pid}_v*.md"),
                           key=lambda f: int(f.stem.rsplit("_v", 1)[1]))
            cache[pid] = C.PaperIndex.build(pid, files[-1].read_text())
        return sum(1 for s in spans
                   if C.classify_span(s.get("source_snippet"), s.get("value"),
                                      cache[pid]).taxonomy_class == C.ANCHORED)

    paired = [pid for pid in pids if run6.get(pid)]
    r6_a = sum(anch(run6[pid], pid) for pid in paired)
    r6_n = sum(len(run6[pid]) for pid in paired)
    b_by = {r["paper_id"]: r for r in rows_b if r["ok"]}
    b_a = sum(anch(b_by[pid]["spans"], pid) for pid in paired)
    b_n = sum(len(b_by[pid]["spans"]) for pid in paired)
    return {
        "papers_with_run6_counterpart": len(paired),
        "papers_without": len(pids) - len(paired),
        "run6_anchored": r6_a, "run6_spans": r6_n,
        "run6_anchored_pct": round(100.0 * r6_a / r6_n, 1) if r6_n else None,
        "conditionB_anchored": b_a, "conditionB_spans": b_n,
        "conditionB_anchored_pct": round(100.0 * b_a / b_n, 1) if b_n else None,
        "delta_pp": round(100.0 * b_a / b_n - 100.0 * r6_a / r6_n, 1) if (b_n and r6_n) else None,
    }


# ── decision rule ────────────────────────────────────────────────────────


def decide(prov: dict[str, dict], agree_cb: dict) -> dict:
    """Apply the pre-registered rule verbatim.

    Adopt C unless C's anchored is >3pp below the best condition, or C-vs-B value
    disagreement exceeds 10%. If C fails, retain B unless B is >3pp below A. Adopt
    A only if it beats both constrained conditions by >3pp. Ties go to constrained;
    between constrained conditions ties go to C.
    """
    a = prov[COND_A].get("ANCHORED_pct")
    b = prov[COND_B].get("ANCHORED_pct")
    c = prov[COND_C].get("ANCHORED_pct")
    if None in (a, b, c):
        return {"verdict": "AMBIGUOUS", "reason": "anchored rate missing for a condition",
                "anchored": {"A": a, "B": b, "C": c}}

    best = max(a, b, c)
    dis = agree_cb.get("disagreement_pct")
    notes, rejected = [], []

    if best - c > ANCHORED_MARGIN_PP:
        rejected.append(f"C anchored {c}% is {round(best - c, 1)}pp below best {best}% (> {ANCHORED_MARGIN_PP}pp)")
    if dis is None:
        notes.append("C-vs-B disagreement not computable")
    elif dis > DISAGREEMENT_PCT:
        rejected.append(f"C-vs-B value disagreement {dis}% (> {DISAGREEMENT_PCT}%)")

    if not rejected:
        verdict, chosen = "ADOPT_C", COND_C
        why = (f"C anchored {c}% within {ANCHORED_MARGIN_PP}pp of best ({best}%) and "
               f"C-vs-B disagreement {dis}% within {DISAGREEMENT_PCT}%")
    else:
        # C failed; retain B unless B is >3pp below A
        if a - b > ANCHORED_MARGIN_PP:
            # A beats B; adopt A only if it beats BOTH constrained by >3pp
            if a - c > ANCHORED_MARGIN_PP:
                verdict, chosen = "ADOPT_A", COND_A
                why = f"A anchored {a}% beats both B ({b}%) and C ({c}%) by > {ANCHORED_MARGIN_PP}pp"
            else:
                verdict, chosen = "RETAIN_B", COND_B
                why = (f"A beats B by {round(a - b, 1)}pp but not C by > {ANCHORED_MARGIN_PP}pp; "
                       f"ties go to constrained")
        else:
            verdict, chosen = "RETAIN_B", COND_B
            why = f"C rejected; B anchored {b}% is within {ANCHORED_MARGIN_PP}pp of A ({a}%)"

    return {
        "verdict": verdict, "chosen": chosen, "why": why,
        "anchored": {"A": a, "B": b, "C": c}, "best": best,
        "c_vs_b_disagreement_pct": dis,
        "reject_reasons": rejected, "ambiguous_notes": notes,
        "thresholds": {"anchored_margin_pp": ANCHORED_MARGIN_PP,
                       "disagreement_pct": DISAGREEMENT_PCT},
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Analyze SCHEMA-EVAL-02")
    p.add_argument("--review", required=True)
    p.add_argument("--data-root", default="data")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    review_dir = Path(args.data_root) / args.review
    parsed = review_dir / "parsed_text"
    db = review_dir / "review.db"
    rows = read_results(review_dir)
    if not rows:
        logger.error("no results under %s", review_dir / "eval/schema_eval2")
        return 2

    cache: dict = {}
    by_cond = {c: [r for r in rows if r["condition"] == c] for c in CONDITIONS}
    prov = {c: provenance(by_cond[c], parsed, cache) for c in CONDITIONS}

    report = {
        "n_rows": len(rows),
        "per_condition": {
            c: {"shape": shape(by_cond[c]), "provenance": prov[c],
                "absence": absence(by_cond[c]), "cost": cost(by_cond[c]),
                "vs_run6": vs_run6(by_cond[c], db)}
            for c in CONDITIONS
        },
        "pairwise_agreement": {
            "A_vs_B": agreement(by_cond[COND_A], by_cond[COND_B]),
            "A_vs_C": agreement(by_cond[COND_A], by_cond[COND_C]),
            "B_vs_C": agreement(by_cond[COND_B], by_cond[COND_C]),
        },
        "restoration": restoration(by_cond[COND_B], db, parsed, cache),
        "anchored_by_paper": {
            c: {str(k): v for k, v in provenance_by_paper(by_cond[c], parsed, cache).items()}
            for c in CONDITIONS
        },
    }
    report["decision"] = decide(prov, report["pairwise_agreement"]["B_vs_C"])

    out = review_dir / "eval" / "schema_eval2" / "analysis_summary.json"
    out.write_text(json.dumps(report, indent=2, default=str))
    logger.info("wrote %s", out)
    print(json.dumps({"decision": report["decision"],
                      "restoration": report["restoration"]}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
