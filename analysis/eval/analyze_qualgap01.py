"""QUALGAP-01 analysis: does the Ollama runtime version explain the Run 6 gap?

Four comparison arms, only two of which cost model calls:

    Run 6            0.17.7, pre-fix code (Pass 2 primed from the CONTENT channel)
    cond B           0.21.0, post-fix code (Pass 2 primed from the THINKING channel)
    V1 / V2          0.17.7, post-fix code (Pass 2 primed from the THINKING channel)

Run 6 and condition B are read from disk, never re-run. Restricting every arm to
the papers all four have in common is what makes the anchored rates comparable at
all, so the matched-paper set is computed once and applied everywhere.

The channel analysis (§`channels`) is the part the pre-flight made possible. Run 6
did not read the thinking channel — its stored `reasoning_trace` rows are
first-draft answers carrying quoted snippets, because the pre-fix parser returned
the whole content. This module classifies the snippets embedded in each Pass-1
channel against the paper, so "is the content channel quote-rich and the thinking
channel not?" is answered from data already on disk.

Usage:
    PYTHONPATH=. python -m analysis.eval.analyze_qualgap01 --review surgical_autonomy
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import statistics
import sys
from collections import Counter
from pathlib import Path

from analysis.eval.qualgap01 import CELL_V1, CELL_V2, CELLS, read_results
from analysis.eval.schema_eval2 import COND_B
from analysis.eval.schema_eval2 import read_results as read_eval2
from analysis.provenance import classifier as C

logger = logging.getLogger(__name__)

# Pinned in the brief. A cell within this many points of an arm is "at" that arm.
PINNED_MARGIN_PP = 5.0

# `"source_snippet": "…"` as emitted inside a Pass-1 draft. Tolerates single
# quotes and markdown bolding because the draft is free-form text, not JSON.
_SNIPPET_RE = re.compile(
    r'["\'*]{0,3}source[_ ]snippet["\'*]{0,3}\s*[:=]\s*["“]([^"”]{10,})["”]',
    re.IGNORECASE,
)


def _norm(v) -> str:
    return " ".join(str(v or "").strip().lower().split())


class Papers:
    """Lazy PaperIndex cache — building one is the expensive part of this module."""

    def __init__(self, parsed_dir: Path):
        self.parsed_dir = parsed_dir
        self._cache: dict[int, C.PaperIndex] = {}

    def __getitem__(self, pid: int) -> C.PaperIndex:
        if pid not in self._cache:
            files = sorted(self.parsed_dir.glob(f"{pid}_v*.md"),
                           key=lambda f: int(f.stem.rsplit("_v", 1)[1]))
            self._cache[pid] = C.PaperIndex.build(pid, files[-1].read_text())
        return self._cache[pid]


# ── ladder ───────────────────────────────────────────────────────────────


def ladder(spans_by_paper: dict[int, list[dict]], papers: Papers) -> dict:
    """Frozen v1.1 provenance ladder over one arm, restricted to `spans_by_paper`."""
    counts: Counter = Counter()
    for pid, spans in spans_by_paper.items():
        for s in spans:
            counts[C.classify_span(s.get("source_snippet"), s.get("value"),
                                   papers[pid]).taxonomy_class] += 1
    total = sum(counts.values())
    out = {"papers": len(spans_by_paper), "spans": total,
           "anchored": counts[C.ANCHORED],
           "anchored_pct": round(100.0 * counts[C.ANCHORED] / total, 1) if total else None}
    for cls in C.ALL_CLASSES:
        out[f"{cls}_pct"] = round(100.0 * counts[cls] / total, 1) if total else None
    return out


def per_paper_anchored(spans_by_paper: dict[int, list[dict]], papers: Papers) -> dict[int, tuple[int, int]]:
    return {
        pid: (sum(1 for s in spans
                  if C.classify_span(s.get("source_snippet"), s.get("value"),
                                     papers[pid]).taxonomy_class == C.ANCHORED),
              len(spans))
        for pid, spans in spans_by_paper.items()
    }


# ── arm loaders ──────────────────────────────────────────────────────────


def cell_spans(rows: list[dict], cell: str) -> dict[int, list[dict]]:
    return {r["paper_id"]: r["spans"] for r in rows
            if r["condition"] == cell and r["ok"]}


def run6_spans(db_path: Path, pids) -> dict[int, list[dict]]:
    conn = sqlite3.connect(f"file:{db_path}?immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        out = {}
        for pid in pids:
            rows = [dict(r) for r in conn.execute(
                "SELECT s.field_name, s.value, s.source_snippet FROM evidence_spans s "
                "JOIN extractions e ON e.id = s.extraction_id WHERE e.paper_id = ?", (pid,))]
            if rows:
                out[pid] = rows
        return out
    finally:
        conn.close()


def run6_traces(db_path: Path, pids) -> dict[int, str]:
    """Run 6's stored `reasoning_trace` — the pre-fix Pass-2 input, verbatim."""
    conn = sqlite3.connect(f"file:{db_path}?immutable=1", uri=True)
    try:
        return {
            pid: t for pid, t in (
                (pid, (conn.execute(
                    "SELECT reasoning_trace FROM extractions WHERE paper_id=? ORDER BY id LIMIT 1",
                    (pid,)).fetchone() or [None])[0])
                for pid in pids
            ) if t
        }
    finally:
        conn.close()


# ── channel analysis ─────────────────────────────────────────────────────


def channel(texts: dict[int, str], papers: Papers) -> dict:
    """Classify the snippets a Pass-1 channel quotes, as evidence of quote-fidelity.

    A channel that hands Pass 2 verbatim paper text is a channel Pass 2 can copy
    from; one that hands it paraphrase is not. Papers contributing no extractable
    snippet are counted separately rather than dropped — "this channel quotes
    nothing" is itself the finding for the thinking channel.
    """
    counts: Counter = Counter()
    n_snip, papers_with, chars = 0, 0, []
    for pid, text in texts.items():
        chars.append(len(text or ""))
        found = _SNIPPET_RE.findall(text or "")
        if found:
            papers_with += 1
        for snip in found:
            n_snip += 1
            counts[C.classify_span(snip, None, papers[pid]).taxonomy_class] += 1
    return {
        "papers": len(texts), "papers_with_quoted_snippets": papers_with,
        "snippets": n_snip,
        "median_chars": round(statistics.median(chars), 0) if chars else None,
        "anchored": counts[C.ANCHORED],
        "anchored_pct": round(100.0 * counts[C.ANCHORED] / n_snip, 1) if n_snip else None,
        "no_basis_pct": round(100.0 * counts[C.UNTRACEABLE_NO_BASIS] / n_snip, 1) if n_snip else None,
    }


# ── telemetry / agreement ────────────────────────────────────────────────


def telemetry(rows: list[dict], cell: str, pids: set[int]) -> dict:
    sel = [r for r in rows if r["condition"] == cell and r["paper_id"] in pids]
    ok = [r for r in sel if r["ok"]]

    def med(k):
        v = [r[k] for r in ok if r.get(k) is not None]
        return round(statistics.median(v), 1) if v else None

    return {
        "calls": len(sel), "errors": len(sel) - len(ok),
        "complete": sum(1 for r in ok if r["complete"]),
        "guard_pass_pct": round(100.0 * sum(1 for r in ok if r["complete"]) / len(ok), 1) if ok else None,
        "would_retry": sum(r["retries"] for r in ok),
        "parse_paths": dict(Counter(r["parse_path"] for r in ok)),
        "parse_branches": dict(Counter(r["parse_branch"] for r in ok)),
        "median_total_s": med("total_latency_s"), "median_pass1_s": med("pass1_latency_s"),
        "median_pass2_s": med("pass2_latency_s"),
        "median_thinking_chars": med("thinking_chars"),
        "median_eval_count": med("eval_count"),
        "total_hours": round(sum(r["total_latency_s"] for r in ok) / 3600, 2),
    }


def agreement(x: dict[int, list[dict]], y: dict[int, list[dict]]) -> dict:
    """Exact normalized value agreement, same measure SCHEMA-EVAL-02 used."""
    def by_field(spans):
        return {s["field_name"]: s for s in spans if s.get("field_name")}

    agree = disagree = 0
    worst: Counter = Counter()
    for pid in sorted(set(x) & set(y)):
        fx, fy = by_field(x[pid]), by_field(y[pid])
        for f in set(fx) & set(fy):
            if _norm(fx[f].get("value")) == _norm(fy[f].get("value")):
                agree += 1
            else:
                disagree += 1
                worst[f] += 1
    total = agree + disagree
    return {"fields": total, "agree": agree, "disagree": disagree,
            "disagreement_pct": round(100.0 * disagree / total, 1) if total else None,
            "worst_fields": worst.most_common(5)}


# ── pinned reads ─────────────────────────────────────────────────────────


def pinned_read(v1_pct: float | None, run6_pct: float | None, cond_b_pct: float | None) -> dict:
    """The brief's pre-registered thresholds, applied verbatim and reported as-is."""
    if None in (v1_pct, run6_pct, cond_b_pct):
        return {"outcome": "NOT_COMPUTABLE", "reason": "an arm has no spans"}
    d_run6, d_b = round(v1_pct - run6_pct, 1), round(v1_pct - cond_b_pct, 1)
    near_run6, near_b = abs(d_run6) <= PINNED_MARGIN_PP, abs(d_b) <= PINNED_MARGIN_PP
    if near_run6 and not near_b:
        outcome = "RUNTIME_CONVICTED"
        why = (f"V1 {v1_pct}% is within {PINNED_MARGIN_PP}pp of Run 6 ({run6_pct}%) "
               f"and {abs(d_b)}pp from the 0.21.0 arm ({cond_b_pct}%)")
    elif near_b and not near_run6:
        outcome = "HYPOTHESIS_DEAD"
        why = (f"V1 {v1_pct}% is within {PINNED_MARGIN_PP}pp of the 0.21.0 arm "
               f"({cond_b_pct}%) and {abs(d_run6)}pp from Run 6 ({run6_pct}%)")
    elif near_run6 and near_b:
        outcome = "INDETERMINATE"
        why = (f"the two reference arms are {round(abs(run6_pct - cond_b_pct), 1)}pp apart, "
               f"so V1 {v1_pct}% is within {PINNED_MARGIN_PP}pp of both")
    else:
        outcome = "BETWEEN"
        why = (f"V1 {v1_pct}% sits {abs(d_b)}pp above the 0.21.0 arm ({cond_b_pct}%) and "
               f"{abs(d_run6)}pp below Run 6 ({run6_pct}%) — no auto-conclusion")
    return {"outcome": outcome, "why": why, "v1_anchored_pct": v1_pct,
            "run6_anchored_pct": run6_pct, "cond_b_anchored_pct": cond_b_pct,
            "delta_vs_run6_pp": d_run6, "delta_vs_cond_b_pp": d_b,
            "margin_pp": PINNED_MARGIN_PP}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Analyze QUALGAP-01")
    p.add_argument("--review", required=True)
    p.add_argument("--data-root", default="data")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    review_dir = Path(args.data_root) / args.review
    papers = Papers(review_dir / "parsed_text")
    db = review_dir / "review.db"

    rows = read_results(review_dir)
    if not rows:
        logger.error("no results under %s", review_dir / "eval/qualgap01")
        return 2
    eval2 = read_eval2(review_dir)

    arms_raw = {c: cell_spans(rows, c) for c in CELLS}
    arms_raw[COND_B] = cell_spans(eval2, COND_B)
    r6 = run6_spans(db, {r["paper_id"] for r in rows})
    arms_raw["run6"] = r6

    # Matched-paper set: every arm that produced results must have the paper, so
    # no arm's rate is computed over a different corpus than another's.
    present = [set(v) for v in arms_raw.values() if v]
    matched = sorted(set.intersection(*present)) if present else []
    logger.info("matched papers across all arms: %d", len(matched))

    arms = {k: {pid: v[pid] for pid in matched if pid in v} for k, v in arms_raw.items()}
    ladders = {k: ladder(v, papers) for k, v in arms.items() if v}
    pp = {k: {str(a): b for a, b in per_paper_anchored(v, papers).items()}
          for k, v in arms.items() if v}

    v1_pct = ladders.get(CELL_V1, {}).get("anchored_pct")
    v2_pct = ladders.get(CELL_V2, {}).get("anchored_pct")

    by_pid = {}
    for r in rows:
        if r["ok"] and r["paper_id"] in set(matched):
            by_pid.setdefault(r["condition"], {})[r["paper_id"]] = r

    report = {
        "matched_papers": matched,
        "n_matched": len(matched),
        "rows_collected": len(rows),
        "ladder": ladders,
        "telemetry": {c: telemetry(rows, c, set(matched)) for c in CELLS},
        "anchored_by_paper": pp,
        "value_agreement_vs_run6": {
            c: agreement(arms.get(c, {}), arms.get("run6", {})) for c in CELLS
        },
        "value_agreement_v1_vs_v2": agreement(arms.get(CELL_V1, {}), arms.get(CELL_V2, {})),
        "value_agreement_v1_vs_condB": agreement(arms.get(CELL_V1, {}), arms.get(COND_B, {})),
        "channels": {
            "run6_stored_trace_content_channel": channel(
                {k: v for k, v in run6_traces(db, matched).items()}, papers),
            **{
                f"{c}_pass1_content": channel(
                    {pid: (r.get("pass1_content") or "") for pid, r in by_pid.get(c, {}).items()},
                    papers)
                for c in CELLS
            },
            **{
                f"{c}_pass1_thinking": channel(
                    {pid: (r.get("pass1_trace") or "") for pid, r in by_pid.get(c, {}).items()},
                    papers)
                for c in CELLS
            },
        },
    }
    report["pinned_read"] = pinned_read(
        v1_pct, ladders.get("run6", {}).get("anchored_pct"),
        ladders.get(COND_B, {}).get("anchored_pct"),
    )
    report["v2_vs_v1_pp"] = (round(v2_pct - v1_pct, 1)
                             if None not in (v1_pct, v2_pct) else None)
    report["v2_vs_v1_within_margin"] = (
        abs(report["v2_vs_v1_pp"]) <= PINNED_MARGIN_PP
        if report["v2_vs_v1_pp"] is not None else None
    )

    out = review_dir / "eval" / "qualgap01" / "analysis_summary.json"
    out.write_text(json.dumps(report, indent=2, default=str))
    logger.info("wrote %s", out)
    print(json.dumps({
        "n_matched": report["n_matched"],
        "anchored_pct": {k: v["anchored_pct"] for k, v in ladders.items()},
        "pinned_read": report["pinned_read"],
        "v2_vs_v1_pp": report["v2_vs_v1_pp"],
        "channels": report["channels"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
