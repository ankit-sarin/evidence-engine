"""CAPTURE-01 analysis — 0.21.0 Pass-1 channel richness, PRIME-01 method verbatim.

Reads only `eval/capture01/capture01.jsonl` and the parsed papers. **Zero model
calls, zero DB writes.**

The metric is imported, not restated: `verbatim_window_rate`, `measure`,
`SNIPPET_RE` and `WINDOW_WORDS` come from `analysis.eval.prime01` unchanged, which
is what makes the 0.21.0 numbers tabulate against PRIME-01's 0.17.7 figures and
QUALGAP-01's published 0.3% / 37.7% rather than merely resemble them.
`tests/test_prime01.py` pins that method; nothing here may fork it.

The 0.17.7 comparison column is read out of PRIME-01's stored
`analysis_summary.json`, not hardcoded, so the side-by-side cannot drift from the
study it cites.
"""

from __future__ import annotations

import argparse
import json
import statistics as st
import sys
from pathlib import Path

from analysis.eval.capture01 import LABEL, read_results, store_dir
from analysis.eval.prime01 import (
    WINDOW_WORDS,
    load_papers,
    load_schema_eval1_draft_lengths,
    measure,
)

CHANNELS = (("draft_0210", "pass1_content"), ("thinking_0210", "pass1_thinking"))


def summarize(stats: list) -> dict:
    """Pooled and per-doc aggregate for one channel — PRIME-01's summary shape."""
    if not stats:
        return {"documents": 0, "status": "NO_DATA"}
    hits = sum(s.hits for s in stats)
    windows = sum(s.windows for s in stats)
    rates = [s.rate for s in stats]
    return {
        "documents": len(stats),
        "hits": hits,
        "windows": windows,
        "pooled_rate_pct": round(100.0 * hits / windows, 1) if windows else 0.0,
        "median_doc_rate_pct": round(st.median(rates), 1),
        "min_doc_rate_pct": round(min(rates), 1),
        "max_doc_rate_pct": round(max(rates), 1),
        "median_chars": int(st.median([s.chars for s in stats])),
        "fenced_json_share_pct": round(100.0 * sum(s.fenced_json for s in stats) / len(stats), 1),
        "docs_enumerating_snippets": sum(1 for s in stats if s.snippet_labels > 0),
        "median_snippet_labels": st.median([s.snippet_labels for s in stats]),
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="CAPTURE-01 analysis")
    p.add_argument("--review", required=True)
    p.add_argument("--data-root", default="data")
    p.add_argument("--label", default=LABEL)
    p.add_argument("--out", default=None, help="summary filename (default analysis_summary.json)")
    args = p.parse_args(argv)

    review_dir = Path(args.data_root) / args.review
    rows = read_results(review_dir, args.label)
    ok_rows = [r for r in rows if r.get("ok")]
    failures = [
        {"paper_id": r["paper_id"], "error": r.get("error"), "attempts": r.get("attempts")}
        for r in rows if not r.get("ok")
    ]
    if not ok_rows:
        print("no successful captures", file=sys.stderr)
        return 1

    pids = [r["paper_id"] for r in ok_rows]
    papers = load_papers(review_dir / "parsed_text", pids)
    missing_text = sorted(set(pids) - set(papers))

    per_channel: dict[str, list] = {}
    per_document: dict[str, list] = {}
    for name, key in CHANNELS:
        stats = [measure(r["paper_id"], name, r.get(key), papers[r["paper_id"]])
                 for r in ok_rows if r["paper_id"] in papers]
        per_channel[name] = stats
        per_document[name] = [s.to_json() for s in stats]

    # ── draft length vs the surviving pre-fix 0.21.0 measurement ──────────
    se1 = review_dir / "eval" / "schema_eval" / "local_ab_20260728T200410Z.jsonl"
    se1_lengths = load_schema_eval1_draft_lengths(se1) if se1.exists() else {}
    captured_len = {r["paper_id"]: r["pass1_content_chars"] for r in ok_rows}
    shared = sorted(set(se1_lengths) & set(captured_len))
    length_check = {
        "note": (
            "SCHEMA-EVAL-01 predates the REGRESSION-01 fix, so its think_chars is a genuine "
            "0.21.0 DRAFT length. This is the one prior 0.21.0 draft measurement that survived, "
            "and it is length only. Comparing it to the captured draft lengths is a sanity "
            "check on the capture, not a richness result."
        ),
        "papers_shared": len(shared),
        "median_prefix_schema_eval1_chars": (
            int(st.median([st.median(se1_lengths[p]) for p in shared])) if shared else None
        ),
        "median_captured_draft_chars": (
            int(st.median([captured_len[p] for p in shared])) if shared else None
        ),
        "median_captured_draft_chars_all": int(st.median(list(captured_len.values()))),
    }

    # ── side-by-side against PRIME-01's stored 0.17.7 figures ─────────────
    prime = review_dir / "eval" / "prime01" / "analysis_summary.json"
    prior = {}
    if prime.exists():
        pj = json.loads(prime.read_text())
        prior = pj.get("measure_1_and_2_channel_richness_and_character", {})

    def col(d: dict) -> dict:
        return {k: d.get(k) for k in (
            "documents", "pooled_rate_pct", "median_doc_rate_pct", "median_chars",
            "fenced_json_share_pct", "docs_enumerating_snippets", "median_snippet_labels")}

    side_by_side = {
        "capture01_draft_0210": col(summarize(per_channel["draft_0210"])),
        "capture01_thinking_0210": col(summarize(per_channel["thinking_0210"])),
        "prime01_run6_draft_0177_prefix": col(prior.get("run6_draft_0177_prefix", {})),
        "prime01_v1_draft_0177": col(prior.get("v1_draft_0177", {})),
        "prime01_v2_draft_0177": col(prior.get("v2_draft_0177", {})),
        "prime01_v1_thinking_0177": col(prior.get("v1_thinking_0177", {})),
        "prime01_v2_thinking_0177": col(prior.get("v2_thinking_0177", {})),
    }

    summary = {
        "study": "CAPTURE-01",
        "question": "Does the 0.21.0 Pass-1 draft channel share the 0.17.7 draft's quote-richness?",
        "method": {
            "measure": "verbatim non-overlapping 8-word windows vs normalized parsed text",
            "window_words": WINDOW_WORDS,
            "source": "analysis.eval.prime01 (imported unmodified; pinned by tests/test_prime01.py)",
            "paper_text": "data/{review}/parsed_text/{pid}_v*.md, newest version",
            "model_calls": 0,
        },
        "capture": {
            "rows": len(rows),
            "ok": len(ok_rows),
            "failed": len(failures),
            "failures": failures,
            "papers_measured": len(per_channel["draft_0210"]),
            "papers_missing_parsed_text": missing_text,
            "server_version": sorted({r.get("server_version") for r in ok_rows}),
            "model": sorted({r.get("model") for r in ok_rows}),
            "model_digest": sorted({r.get("model_digest") for r in ok_rows}),
            "options_sha256": sorted({r.get("options_sha256") for r in ok_rows}),
            "think": sorted({r.get("think") for r in ok_rows}),
            "done_reasons": sorted({r.get("done_reason") for r in ok_rows}),
        },
        "channel_richness": {n: summarize(per_channel[n]) for n, _ in CHANNELS},
        "draft_length_check": length_check,
        "side_by_side_vs_0177": side_by_side,
        "per_document": per_document,
    }

    out = store_dir(review_dir) / (args.out or "analysis_summary.json")
    out.write_text(json.dumps(summary, indent=2))

    d = summary["channel_richness"]["draft_0210"]
    t = summary["channel_richness"]["thinking_0210"]
    print(f"CAPTURE-01 — {d['documents']} papers on Ollama "
          f"{summary['capture']['server_version']}")
    print(f"  draft    : pooled {d['pooled_rate_pct']}%  median/doc {d['median_doc_rate_pct']}%  "
          f"snippet-docs {d['docs_enumerating_snippets']}/{d['documents']}  "
          f"median labels {d['median_snippet_labels']}  median chars {d['median_chars']}")
    print(f"  thinking : pooled {t['pooled_rate_pct']}%  median/doc {t['median_doc_rate_pct']}%  "
          f"snippet-docs {t['docs_enumerating_snippets']}/{t['documents']}  "
          f"median labels {t['median_snippet_labels']}  median chars {t['median_chars']}")
    print(f"  wrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
