"""PARSE-01 Phase 1 — corpus-wide parse-quality and input-truncation sweep.

Read-only. No model calls, no writes to review.db, no re-parsing. Emits one JSON
row per EXTRACTED paper to data/{review}/eval/parse01/sweep.jsonl.

Token accounting, per arm:
  * anthropic_sonnet_4_6 / openai_o4_mini_high — MEASURED. cloud_extractions
    stores the provider's own `input_tokens` per paper.
  * local deepseek-r1:32b — ESTIMATED. Run 6 (2026-03) predates INSTRUMENT-01,
    so no per-call telemetry exists and no prompt_eval was ever persisted; the
    run log carries none either. The estimate uses the chars->tokens ratio
    measured on CAPTURE-01's 38 non-truncated rows against the *same* model and
    the *same* prompt builder. Reported as an estimate everywhere, never as a
    measurement.

The local prompt is build_extraction_prompt(paper_text, spec), whose non-paper
scaffold is a fixed ~N chars for a given codebook; the sweep measures that once
and adds it, so the estimate is of the real prompt rather than of the paper.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import statistics as st
import sys
from pathlib import Path

from analysis.provenance.segment import sentences

LOCAL_CTX = 131_072  # deepseek-r1:32b native context length (ollama show)

RE_REFERENCES = re.compile(r"^#{1,6}\s*references\b", re.IGNORECASE | re.MULTILINE)
RE_GLYPH = re.compile(r"GLYPH<[^>]*>|GLYPH&lt;[^&]*&gt;")
RE_IMAGE = re.compile(r"<!--\s*image\s*-->")
RE_FORMULA = re.compile(r"<!--\s*formula-not-decoded\s*-->")


def newest_parsed(parsed_dir: Path, pid: int) -> Path | None:
    files = sorted(parsed_dir.glob(f"{pid}_v*.md"),
                   key=lambda f: int(f.stem.rsplit("_v", 1)[1]))
    return files[-1] if files else None


def measure_paper(pid: int, path: Path) -> dict:
    raw = path.read_text(errors="replace")
    lines = raw.splitlines()
    nonblank = [l for l in lines if l.strip()]
    units = sentences(raw)
    unit_tokens = [len(u.split()) for u in units]
    nonascii = sum(1 for ch in raw if ord(ch) > 127)
    return {
        "paper_id": pid,
        "parsed_file": path.name,
        "chars": len(raw),
        "lines": len(lines),
        "nonblank_lines": len(nonblank),
        "unique_nonblank_lines": len(set(nonblank)),
        "max_line_chars": max((len(l) for l in lines), default=0),
        "median_line_chars": int(st.median([len(l) for l in nonblank])) if nonblank else 0,
        "chars_per_line": round(len(raw) / len(nonblank), 1) if nonblank else 0.0,
        "references_sections": len(RE_REFERENCES.findall(raw)),
        "glyph_artifacts": len(RE_GLYPH.findall(raw)),
        "image_comments": len(RE_IMAGE.findall(raw)),
        "formula_comments": len(RE_FORMULA.findall(raw)),
        "replacement_chars": raw.count("�"),
        "nonascii_density_pct": round(100.0 * nonascii / len(raw), 3) if raw else 0.0,
        "pysbd_units": len(units),
        "median_unit_tokens": st.median(unit_tokens) if unit_tokens else 0,
        "chars_per_unit": round(len(raw) / len(units), 1) if units else 0.0,
        "short_unit_share_pct": (
            round(100.0 * sum(1 for t in unit_tokens if t < 3) / len(unit_tokens), 1)
            if unit_tokens else 0.0
        ),
    }


def local_ratio_and_scaffold(review_dir: Path) -> tuple[float, int]:
    """chars->tokens ratio and prompt scaffold size, both measured, not assumed."""
    cap = review_dir / "eval" / "capture01" / "capture01.jsonl"
    rows = [json.loads(l) for l in cap.read_text().splitlines() if l.strip()]
    ok = [r for r in rows if r.get("ok")]
    # Rows at the context ceiling are truncated: their ratio is capped, not real.
    clean = [r for r in ok if r["prompt_eval_count"] < LOCAL_CTX - 100]
    ratio = st.median([r["prompt_eval_count"] / r["prompt_chars"] for r in clean])
    # scaffold = prompt_chars - paper chars, constant for a given codebook
    parsed = review_dir / "parsed_text"
    diffs = []
    for r in clean:
        p = newest_parsed(parsed, r["paper_id"])
        if p:
            diffs.append(r["prompt_chars"] - len(p.read_text(errors="replace")))
    return ratio, int(st.median(diffs)) if diffs else 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="PARSE-01 Phase 1 sweep")
    ap.add_argument("--review", required=True)
    ap.add_argument("--data-root", default="data")
    args = ap.parse_args(argv)

    review_dir = Path(args.data_root) / args.review
    db = review_dir / "review.db"
    conn = sqlite3.connect(f"file:{db}?immutable=1", uri=True)
    conn.row_factory = sqlite3.Row

    pids = [r[0] for r in conn.execute(
        "SELECT DISTINCT paper_id FROM extractions ORDER BY paper_id")]
    cloud: dict[tuple[int, str], int | None] = {
        (r["paper_id"], r["arm"]): r["input_tokens"]
        for r in conn.execute(
            "SELECT paper_id, arm, input_tokens FROM cloud_extractions")
    }
    pdfs = {r["paper_id"]: r["pdf_path"] for r in conn.execute(
        "SELECT paper_id, pdf_path FROM full_text_assets WHERE pdf_path IS NOT NULL")}

    ratio, scaffold = local_ratio_and_scaffold(review_dir)
    parsed_dir = review_dir / "parsed_text"

    out_path = review_dir / "eval" / "parse01" / "sweep.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    missing = []
    for pid in pids:
        p = newest_parsed(parsed_dir, pid)
        if p is None:
            missing.append(pid)
            continue
        row = measure_paper(pid, p)
        est = (row["chars"] + scaffold) * ratio
        row.update({
            "local_est_prompt_tokens": int(est),
            "local_token_method": "ESTIMATED_from_capture01_ratio",
            "local_ratio_used": round(ratio, 4),
            "local_scaffold_chars": scaffold,
            "local_exceeds_ctx": bool(est > LOCAL_CTX),
            "sonnet_input_tokens": cloud.get((pid, "anthropic_sonnet_4_6")),
            "openai_input_tokens": cloud.get((pid, "openai_o4_mini_high")),
            "openai_extracted": (pid, "openai_o4_mini_high") in cloud,
            "cloud_token_method": "MEASURED_provider_reported",
            "pdf_path": pdfs.get(pid),
        })
        rows.append(row)

    with out_path.open("w") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")

    print(f"PARSE-01 sweep: {len(rows)}/{len(pids)} papers -> {out_path}")
    print(f"  local ratio {ratio:.4f} tok/char (median of "
          f"CAPTURE-01 non-truncated rows), scaffold {scaffold:,} chars")
    if missing:
        print(f"  MISSING parsed_text: {missing}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
