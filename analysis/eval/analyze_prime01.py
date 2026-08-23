"""PRIME-01 analysis: four-channel quote-richness + does it predict anchoring?

Reports source availability first and refuses to invent the channel that is
missing. Measure 4 (the cross-runtime residual) is emitted as NOT_COMPUTABLE
with the reason, rather than silently substituting the length proxy for the
richness measure the pre-registered bands are defined on.

Usage:
    PYTHONPATH=. python -m analysis.eval.analyze_prime01 --review surgical_autonomy
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
from pathlib import Path

from analysis.eval.prime01 import (
    DocStats,
    load_papers,
    load_qualgap,
    load_run6_spans,
    load_run6_traces,
    load_schema_eval1_draft_lengths,
    measure,
    spearman,
)
from analysis.eval.qualgap01 import CELL_V1, CELL_V2
from analysis.provenance import classifier as C

logger = logging.getLogger(__name__)

BAND_PP = 10.0  # pre-registered: 0.21.0 within 10pp of 0.17.7 => runtime-stable


# ── source availability (gate 1) ─────────────────────────────────────────


def survey_sources(review_dir: Path) -> dict:
    """State what exists BEFORE analyzing. The 0.21.0 draft is the gating one."""
    ev = review_dir / "eval"
    qg = ev / "qualgap01" / "runtime_v12.jsonl"
    se2 = ev / "schema_eval2" / "local_abc.jsonl"
    se1 = ev / "schema_eval" / "local_ab_20260728T200410Z.jsonl"
    telemetry = review_dir / "telemetry" / "extraction_calls.jsonl"

    out: dict = {}

    # Source 1 — SCHEMA-EVAL-02 condition B Pass-1 drafts on 0.21.0.
    se2_rows = load_qualgap(se2) if se2.exists() else []
    condb = [r for r in se2_rows if r.get("condition") == "B_array_schema"]
    has_pass1_text = any("pass1_content" in r for r in se2_rows)
    out["source_1_0210_pass1_drafts"] = {
        "status": "ABSENT",
        "reason": (
            "SCHEMA-EVAL-02 stores raw_content = the PASS-2 response and "
            "think_chars = an integer LENGTH of the Pass-1 trace; the Pass-1 text "
            "is discarded. record_call telemetry would hold it but is never "
            "reached — the eval runners bypass extract_paper(), which is what "
            "calls it."
        ),
        "telemetry_file_exists": telemetry.exists(),
        "condition_B_rows": len(condb),
        "rows_with_pass1_text": sum(1 for r in se2_rows if r.get("pass1_content")),
        "truncated_rows": 0,
        "truncation_note": "not applicable — never captured, so nothing to truncate",
        "field_inventory": sorted(se2_rows[0].keys()) if se2_rows else [],
        "available_for_analysis": has_pass1_text,
    }

    # Source 2 — QUALGAP-01, both channels, 0.17.7.
    qg_rows = load_qualgap(qg) if qg.exists() else []
    ok = [r for r in qg_rows if r.get("ok")]
    out["source_2_0177_both_channels"] = {
        "status": "PRESENT" if ok else "ABSENT",
        "rows": len(qg_rows), "ok_rows": len(ok),
        "with_pass1_content": sum(1 for r in ok if r.get("pass1_content")),
        "with_pass1_trace": sum(1 for r in ok if r.get("pass1_trace")),
        "papers": len({r["paper_id"] for r in ok}),
    }

    # Source 3 — Run 6 stored drafts.
    pids = sorted({r["paper_id"] for r in ok})
    traces = load_run6_traces(review_dir / "review.db", pids) if pids else {}
    out["source_3_run6_traces"] = {
        "status": "PRESENT" if traces else "ABSENT",
        "papers_with_trace": len(traces),
        "papers_queried": len(pids),
        "median_chars": int(statistics.median([len(t) for t in traces.values()])) if traces else None,
    }

    # Partial: 0.21.0 draft LENGTHS from the pre-fix SCHEMA-EVAL-01 run.
    se1_lengths = load_schema_eval1_draft_lengths(se1) if se1.exists() else {}
    out["partial_0210_draft_lengths"] = {
        "status": "PRESENT (length only)" if se1_lengths else "ABSENT",
        "why_valid": (
            "SCHEMA-EVAL-01 (2026-07-28) predates the REGRESSION-01 fix "
            "(2026-07-29), so its pre-fix extract_pass1_reasoning returned whole "
            "content and think_chars is the DRAFT length. Post-fix runs record "
            "thinking length under the same key and are excluded."
        ),
        "papers": len(se1_lengths),
        "calls": sum(len(v) for v in se1_lengths.values()),
        "limitation": "length is not richness; the pre-registered bands are on rate",
    }
    return out


# ── measures ─────────────────────────────────────────────────────────────


def channel_table(stats: list[DocStats]) -> dict:
    """Pooled + per-document summary for one channel."""
    if not stats:
        return {"documents": 0}
    hits = sum(s.hits for s in stats)
    windows = sum(s.windows for s in stats)
    rates = [s.rate for s in stats]
    return {
        "documents": len(stats),
        "hits": hits, "windows": windows,
        "pooled_rate_pct": round(100.0 * hits / windows, 1) if windows else None,
        "median_doc_rate_pct": round(statistics.median(rates), 1),
        "median_chars": int(statistics.median([s.chars for s in stats])),
        "fenced_json_share_pct": round(100.0 * sum(s.fenced_json for s in stats) / len(stats), 1),
        "docs_enumerating_snippets": sum(1 for s in stats if s.snippet_labels > 0),
        "median_snippet_labels": statistics.median([s.snippet_labels for s in stats]),
    }


def anchored_by_paper(spans_by_paper: dict[int, list[dict]], papers_raw: dict[int, object]) -> dict[int, float]:
    out = {}
    for pid, spans in spans_by_paper.items():
        if not spans:
            continue
        idx = papers_raw[pid]
        a = sum(1 for s in spans
                if C.classify_span(s.get("source_snippet"), s.get("value"), idx).taxonomy_class == C.ANCHORED)
        out[pid] = 100.0 * a / len(spans)
    return out


def relate(draft_rates: dict[int, float], anchored: dict[int, float], label: str) -> dict:
    """Measure 3: does Pass-1 richness predict the Pass-2 anchored rate?"""
    pids = sorted(set(draft_rates) & set(anchored))
    if len(pids) < 3:
        return {"pairs": len(pids), "spearman_rho": None, "note": "too few papers"}
    xs = [draft_rates[p] for p in pids]
    ys = [anchored[p] for p in pids]
    rho = spearman(xs, ys)
    # Quartile scatter: cheap, and shows shape a single rho can hide.
    ordered = sorted(pids, key=lambda p: draft_rates[p])
    q = max(1, len(ordered) // 4)
    buckets = [ordered[i:i + q] for i in range(0, len(ordered), q)][:4]
    return {
        "label": label,
        "pairs": len(pids),
        "spearman_rho": round(rho, 3) if rho is not None else None,
        "quartiles": [
            {
                "n": len(b),
                "draft_rate_pct": round(statistics.mean(draft_rates[p] for p in b), 1),
                "anchored_pct": round(statistics.mean(anchored[p] for p in b), 1),
            }
            for b in buckets if b
        ],
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="PRIME-01: Pass-1 channel quote-richness")
    p.add_argument("--review", required=True)
    p.add_argument("--data-root", default="data")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    review_dir = Path(args.data_root) / args.review
    db = review_dir / "review.db"
    parsed = review_dir / "parsed_text"

    sources = survey_sources(review_dir)
    logger.info("source 1 (0.21.0 drafts): %s", sources["source_1_0210_pass1_drafts"]["status"])
    logger.info("source 2 (0.17.7 both):   %s", sources["source_2_0177_both_channels"]["status"])
    logger.info("source 3 (Run 6 drafts):  %s", sources["source_3_run6_traces"]["status"])

    qg_rows = [r for r in load_qualgap(review_dir / "eval" / "qualgap01" / "runtime_v12.jsonl")
               if r.get("ok")]
    qg_pids = sorted({r["paper_id"] for r in qg_rows})

    run6_traces = load_run6_traces(db, qg_pids)
    run6_spans = load_run6_spans(db, qg_pids)

    # Same matching QUALGAP-01 used: papers every arm has.
    v1 = {r["paper_id"]: r for r in qg_rows if r["condition"] == CELL_V1}
    v2 = {r["paper_id"]: r for r in qg_rows if r["condition"] == CELL_V2}
    matched = sorted(set(v1) & set(v2) & set(run6_traces) & set(run6_spans))
    logger.info("matched papers: %d", len(matched))

    norm = load_papers(parsed, matched)
    # Index built once per paper, from the same newest-version file the verbatim
    # rates use, so both measures see identical text.
    idx = {}
    for pid in matched:
        files = sorted(parsed.glob(f"{pid}_v*.md"), key=lambda f: int(f.stem.rsplit("_v", 1)[1]))
        idx[pid] = C.PaperIndex.build(pid, files[-1].read_text())

    channels: dict[str, list[DocStats]] = {
        "run6_draft_0177_prefix": [
            measure(pid, "run6_draft", run6_traces[pid], norm[pid]) for pid in matched],
        "v1_draft_0177": [
            measure(pid, "v1_draft", v1[pid].get("pass1_content"), norm[pid]) for pid in matched],
        "v2_draft_0177": [
            measure(pid, "v2_draft", v2[pid].get("pass1_content"), norm[pid]) for pid in matched],
        "v1_thinking_0177": [
            measure(pid, "v1_thinking", v1[pid].get("pass1_trace"), norm[pid]) for pid in matched],
        "v2_thinking_0177": [
            measure(pid, "v2_thinking", v2[pid].get("pass1_trace"), norm[pid]) for pid in matched],
    }

    m1 = {k: channel_table(v) for k, v in channels.items()}
    m1["drafts_0210"] = {
        "documents": 0,
        "status": "NOT_MEASURABLE",
        "reason": sources["source_1_0210_pass1_drafts"]["reason"],
    }

    # Measure 3 — input (Pass-1 richness) vs output (Pass-2 anchored), per arm.
    anchored = {
        "run6": anchored_by_paper({p: run6_spans[p] for p in matched}, idx),
        "v1": anchored_by_paper({p: v1[p]["spans"] for p in matched}, idx),
        "v2": anchored_by_paper({p: v2[p]["spans"] for p in matched}, idx),
    }
    rate_of = lambda key: {s.paper_id: s.rate for s in channels[key]}  # noqa: E731

    m3 = {
        # Run 6: the draft WAS the primer — this tests the hypothesis directly.
        "run6_draft_vs_run6_anchored__causal": relate(
            rate_of("run6_draft_0177_prefix"), anchored["run6"],
            "Run 6 draft richness -> Run 6 anchored (draft was the actual primer)"),
        # V1/V2: thinking WAS the primer.
        "v1_thinking_vs_v1_anchored__causal": relate(
            rate_of("v1_thinking_0177"), anchored["v1"],
            "V1 thinking richness -> V1 anchored (thinking was the actual primer)"),
        "v2_thinking_vs_v2_anchored__causal": relate(
            rate_of("v2_thinking_0177"), anchored["v2"],
            "V2 thinking richness -> V2 anchored (thinking was the actual primer)"),
        # Counterfactual: the draft existed on the same call but was discarded.
        "v1_draft_vs_v1_anchored__counterfactual": relate(
            rate_of("v1_draft_0177"), anchored["v1"],
            "V1 draft richness -> V1 anchored (draft was NOT the primer)"),
    }

    # Measure 4 — the residual. Not computable; report the length proxy separately.
    se1 = review_dir / "eval" / "schema_eval" / "local_ab_20260728T200410Z.jsonl"
    lengths_0210 = load_schema_eval1_draft_lengths(se1) if se1.exists() else {}
    shared = sorted(set(lengths_0210) & set(matched))
    m4 = {
        "status": "NOT_COMPUTABLE",
        "reason": ("the 0.21.0 Pass-1 draft text was never persisted; the "
                   "pre-registered bands are defined on verbatim-window RATE, "
                   "which cannot be derived from a length"),
        "band_pp": BAND_PP,
        "length_proxy": {
            "note": "LENGTH ONLY — not richness. Weak evidence, stated as such.",
            "papers_shared_with_matched_set": len(shared),
            "median_0210_draft_chars_prefix": (
                int(statistics.median([statistics.median(lengths_0210[p]) for p in shared]))
                if shared else None),
            "median_0177_v1_draft_chars": (
                int(statistics.median([s.chars for s in channels["v1_draft_0177"]
                                       if s.paper_id in shared])) if shared else None),
            "median_run6_draft_chars": (
                int(statistics.median([s.chars for s in channels["run6_draft_0177_prefix"]
                                       if s.paper_id in shared])) if shared else None),
        },
    }

    report = {
        "sources": sources,
        "matched_papers": matched,
        "n_matched": len(matched),
        "measure_1_and_2_channel_richness_and_character": m1,
        "measure_3_input_output_relationship": m3,
        "measure_4_residual": m4,
        "per_document": {k: [s.to_json() for s in v] for k, v in channels.items()},
        "anchored_by_paper": {k: {str(a): round(b, 1) for a, b in v.items()}
                              for k, v in anchored.items()},
    }

    out_dir = review_dir / "eval" / "prime01"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "analysis_summary.json"
    out.write_text(json.dumps(report, indent=2, default=str))
    logger.info("wrote %s", out)

    print(json.dumps({
        "sources": {k: v.get("status") for k, v in sources.items()},
        "n_matched": len(matched),
        "channel_richness": m1,
        "input_output": m3,
        "residual": m4,
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
