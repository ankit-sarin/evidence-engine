#!/usr/bin/env python3
"""Extraction validation: parse 3 PDFs, run two-pass DeepSeek-R1:32b extraction,
validate JSON against Pydantic schema, verify source_snippets against paper text."""

import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from engine.agents.extractor import (
    MODEL as EXTRACTOR_MODEL,
    build_extraction_prompt,
    extract_pass1_reasoning,
    extract_pass2_structured,
)
from engine.agents.models import ExtractionResult
from engine.core.review_spec import load_review_spec
from engine.parsers.pdf_parser import is_scanned_pdf, parse_with_docling, parse_with_vision

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("extraction_test")

SPEC_PATH = PROJECT_ROOT / "review_specs" / "surgical_autonomy_v1.yaml"
PDF_DIR = PROJECT_ROOT / "data" / "surgical_autonomy" / "pdfs"
ARTIFACT_DIR = PROJECT_ROOT / "data" / "surgical_autonomy" / "extraction_test_artifacts"
LOG_PATH = PROJECT_ROOT / "tests" / "extraction_test_log.md"

PAPERS = [
    {"file": "shademan_2016.pdf", "label": "Shademan 2016", "difficulty": "easy"},
    {"file": "saeidi_2022.pdf", "label": "Saeidi 2022", "difficulty": "medium"},
    {"file": "kim_2025.pdf", "label": "Kim 2025", "difficulty": "hard"},
]

# Success criteria
MAX_TIME_PER_PAPER = 600  # 10 minutes
MIN_SNIPPET_RATE = 0.80   # 80% source_snippets must verify


def normalize(text: str) -> str:
    """Collapse whitespace, strip, lowercase."""
    return re.sub(r"\s+", " ", text.strip().lower())


def token_set(text: str) -> set[str]:
    """Split into lowercase word tokens."""
    return set(re.findall(r"[a-z0-9]+", text.lower()))


def verify_snippet(snippet: str, paper_text: str) -> dict:
    """Check source_snippet against paper text with three methods."""
    exact = snippet in paper_text
    norm_match = normalize(snippet) in normalize(paper_text)

    s_tokens = token_set(snippet)
    p_tokens = token_set(paper_text)
    if s_tokens:
        overlap = len(s_tokens & p_tokens) / len(s_tokens)
    else:
        overlap = 0.0

    return {
        "exact_match": exact,
        "normalized_match": norm_match,
        "token_overlap": round(overlap, 3),
        "verified": exact or norm_match or overlap >= 0.8,
    }


def process_paper(pdf_path: str, label: str, spec, paper_id: int) -> dict:
    """Parse PDF, run two-pass extraction, validate, verify snippets."""
    result = {
        "label": label,
        "pdf_path": pdf_path,
        "parse_ok": False,
        "pass1_ok": False,
        "pass2_ok": False,
        "json_valid": False,
        "field_count": 0,
        "snippet_results": [],
        "snippets_verified": 0,
        "snippets_total": 0,
        "snippet_rate": 0.0,
        "parse_time": 0.0,
        "pass1_time": 0.0,
        "pass2_time": 0.0,
        "total_time": 0.0,
        "error": None,
        "parser_used": None,
    }

    t_total_start = time.time()

    # ── Step 1: Parse PDF ────────────────────────────────────────
    logger.info("[%s] Parsing PDF...", label)
    t0 = time.time()
    try:
        scanned = is_scanned_pdf(pdf_path)
        result["parser_used"] = "qwen2.5vl" if scanned else "docling"
        if scanned:
            logger.info("[%s] Detected scanned PDF, using Qwen2.5-VL", label)
            paper_text = parse_with_vision(pdf_path)
        else:
            logger.info("[%s] Digital PDF, using Docling", label)
            paper_text = parse_with_docling(pdf_path)
        result["parse_ok"] = bool(paper_text and len(paper_text) > 100)
        result["parse_time"] = round(time.time() - t0, 1)
        logger.info("[%s] Parsed: %d chars in %.1fs", label, len(paper_text), result["parse_time"])
    except Exception as e:
        result["error"] = f"Parse failed: {e}"
        result["parse_time"] = round(time.time() - t0, 1)
        result["total_time"] = round(time.time() - t_total_start, 1)
        logger.error("[%s] %s", label, result["error"])
        return result

    if not result["parse_ok"]:
        result["error"] = f"Parse returned insufficient text ({len(paper_text)} chars)"
        result["total_time"] = round(time.time() - t_total_start, 1)
        return result

    # ── Step 2: Build prompt ─────────────────────────────────────
    prompt = build_extraction_prompt(paper_text, spec)
    logger.info("[%s] Prompt built: %d chars", label, len(prompt))

    # ── Step 3: Pass 1 — free reasoning ──────────────────────────
    logger.info("[%s] Pass 1: free reasoning (DeepSeek-R1:32b)...", label)
    t0 = time.time()
    try:
        reasoning_trace = extract_pass1_reasoning(prompt)
        result["pass1_ok"] = bool(reasoning_trace and len(reasoning_trace) > 50)
        result["pass1_time"] = round(time.time() - t0, 1)
        logger.info("[%s] Pass 1 done: %d chars in %.1fs", label, len(reasoning_trace), result["pass1_time"])
    except Exception as e:
        result["error"] = f"Pass 1 failed: {e}"
        result["pass1_time"] = round(time.time() - t0, 1)
        result["total_time"] = round(time.time() - t_total_start, 1)
        logger.error("[%s] %s", label, result["error"])
        return result

    # ── Step 4: Pass 2 — structured JSON ─────────────────────────
    logger.info("[%s] Pass 2: structured output (grammar-constrained)...", label)
    t0 = time.time()
    try:
        extraction = extract_pass2_structured(prompt, reasoning_trace, spec, paper_id)
        result["pass2_ok"] = True
        result["pass2_time"] = round(time.time() - t0, 1)
        logger.info("[%s] Pass 2 done: %d fields in %.1fs", label, len(extraction.fields), result["pass2_time"])
    except Exception as e:
        result["error"] = f"Pass 2 failed: {e}"
        result["pass2_time"] = round(time.time() - t0, 1)
        result["total_time"] = round(time.time() - t_total_start, 1)
        logger.error("[%s] %s", label, result["error"])
        return result

    # ── Step 5: Validate JSON via Pydantic ───────────────────────
    try:
        # Re-validate by roundtripping through Pydantic
        json_data = extraction.model_dump()
        validated = ExtractionResult.model_validate(json_data)
        result["json_valid"] = True
        result["field_count"] = len(validated.fields)
        logger.info("[%s] Pydantic validation passed: %d fields", label, result["field_count"])
    except Exception as e:
        result["error"] = f"Pydantic validation failed: {e}"
        result["json_valid"] = False
        result["total_time"] = round(time.time() - t_total_start, 1)
        logger.error("[%s] %s", label, result["error"])
        return result

    # ── Step 6: Verify source_snippets ───────────────────────────
    for span in extraction.fields:
        sv = verify_snippet(span.source_snippet, paper_text)
        sv["field_name"] = span.field_name
        sv["snippet_preview"] = span.source_snippet[:80]
        result["snippet_results"].append(sv)

    result["snippets_total"] = len(result["snippet_results"])
    result["snippets_verified"] = sum(1 for s in result["snippet_results"] if s["verified"])
    if result["snippets_total"] > 0:
        result["snippet_rate"] = round(result["snippets_verified"] / result["snippets_total"], 3)

    logger.info(
        "[%s] Snippet verification: %d/%d (%.0f%%)",
        label, result["snippets_verified"], result["snippets_total"],
        result["snippet_rate"] * 100,
    )

    result["total_time"] = round(time.time() - t_total_start, 1)

    # ── Step 7: Save artifacts ───────────────────────────────────
    paper_dir = ARTIFACT_DIR / label.lower().replace(" ", "_")
    paper_dir.mkdir(parents=True, exist_ok=True)

    (paper_dir / "parsed_text.md").write_text(paper_text)
    (paper_dir / "reasoning_trace.txt").write_text(reasoning_trace)
    (paper_dir / "extraction.json").write_text(
        json.dumps(extraction.model_dump(), indent=2, default=str)
    )
    (paper_dir / "snippet_verification.json").write_text(
        json.dumps(result["snippet_results"], indent=2)
    )

    logger.info("[%s] Artifacts saved to %s", label, paper_dir)
    return result


def write_log(results: list[dict], total_elapsed: float, spec):
    """Write markdown test log."""
    now = datetime.now(timezone.utc)

    valid_json_count = sum(1 for r in results if r["json_valid"])
    all_snippet_rates = [r["snippet_rate"] for r in results if r["snippets_total"] > 0]
    avg_snippet_rate = sum(all_snippet_rates) / len(all_snippet_rates) if all_snippet_rates else 0
    over_time = [r for r in results if r["total_time"] > MAX_TIME_PER_PAPER]

    pass_json = valid_json_count == len(results)
    pass_snippets = all(r["snippet_rate"] >= MIN_SNIPPET_RATE for r in results if r["snippets_total"] > 0)
    pass_time = len(over_time) == 0
    overall_pass = pass_json and pass_snippets and pass_time

    lines = [
        "# Extraction Validation Test Log",
        "",
        f"**Date:** {now.strftime('%Y-%m-%d %H:%M UTC')}",
        f"**Extractor model:** {EXTRACTOR_MODEL}",
        f"**Spec:** {spec.title} v{spec.version}",
        f"**Total elapsed:** {total_elapsed:.1f}s",
        "",
        "## Success Criteria",
        "",
        f"| Criterion | Target | Actual | Pass |",
        f"|-----------|--------|--------|------|",
        f"| Valid JSON (all papers) | 3/3 | {valid_json_count}/3 | {'YES' if pass_json else 'NO'} |",
        f"| Source snippet verify rate | >80% | {avg_snippet_rate:.0%} | {'YES' if pass_snippets else 'NO'} |",
        f"| Time per paper | <10 min | {'all OK' if pass_time else f'{len(over_time)} exceeded'} | {'YES' if pass_time else 'NO'} |",
        f"| **Overall** | | | **{'PASS' if overall_pass else 'FAIL'}** |",
        "",
        "## Per-Paper Results",
        "",
    ]

    for r in results:
        status = "PASS" if (r["json_valid"] and r["snippet_rate"] >= MIN_SNIPPET_RATE and r["total_time"] <= MAX_TIME_PER_PAPER) else "FAIL"
        lines.extend([
            f"### {r['label']} ({r.get('difficulty', '?')}) — {status}",
            "",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Parser | {r['parser_used'] or 'N/A'} |",
            f"| Parse time | {r['parse_time']:.1f}s |",
            f"| Pass 1 time | {r['pass1_time']:.1f}s |",
            f"| Pass 2 time | {r['pass2_time']:.1f}s |",
            f"| Total time | {r['total_time']:.1f}s |",
            f"| JSON valid | {'YES' if r['json_valid'] else 'NO'} |",
            f"| Fields extracted | {r['field_count']} |",
            f"| Snippets verified | {r['snippets_verified']}/{r['snippets_total']} ({r['snippet_rate']:.0%}) |",
        ])

        if r["error"]:
            lines.append(f"| Error | {r['error']} |")

        lines.append("")

        # Snippet detail table
        if r["snippet_results"]:
            lines.extend([
                "**Snippet Verification Detail:**",
                "",
                "| Field | Exact | Normalized | Token Overlap | Verified |",
                "|-------|-------|------------|---------------|----------|",
            ])
            for s in r["snippet_results"]:
                lines.append(
                    f"| {s['field_name']} | {'Y' if s['exact_match'] else 'N'} "
                    f"| {'Y' if s['normalized_match'] else 'N'} "
                    f"| {s['token_overlap']:.0%} "
                    f"| {'YES' if s['verified'] else 'NO'} |"
                )
            lines.append("")

    # Pivot plan
    if not overall_pass:
        lines.extend([
            "## Pivot Plan",
            "",
            "Test **FAILED** — consider swapping extractor to `qwen2.5:32b`:",
            "1. Update `MODEL` in `engine/agents/extractor.py` to `qwen2.5:32b`",
            "2. Re-run this validation script",
            "3. Compare snippet verification rates between models",
            "",
        ])

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(lines))


def main():
    t_start = time.time()

    logger.info("Loading review spec: %s", SPEC_PATH)
    spec = load_review_spec(SPEC_PATH)

    results = []
    for i, paper in enumerate(PAPERS):
        pdf_path = str(PDF_DIR / paper["file"])
        logger.info("=" * 60)
        logger.info("Processing %d/3: %s (%s)", i + 1, paper["label"], paper["difficulty"])
        logger.info("=" * 60)

        r = process_paper(pdf_path, paper["label"], spec, paper_id=i + 1)
        r["difficulty"] = paper["difficulty"]
        results.append(r)

        logger.info(
            "[%s] Done — JSON=%s, snippets=%d/%d, time=%.1fs",
            r["label"],
            "valid" if r["json_valid"] else "INVALID",
            r["snippets_verified"], r["snippets_total"],
            r["total_time"],
        )

    total_elapsed = round(time.time() - t_start, 1)
    write_log(results, total_elapsed, spec)

    logger.info("=" * 60)
    valid = sum(1 for r in results if r["json_valid"])
    logger.info("RESULTS: %d/3 valid JSON", valid)
    for r in results:
        logger.info(
            "  %s: snippets %d/%d (%.0f%%), %.1fs",
            r["label"], r["snippets_verified"], r["snippets_total"],
            r["snippet_rate"] * 100, r["total_time"],
        )
    logger.info("Log written to %s", LOG_PATH)
    logger.info("Total time: %.1fs", total_elapsed)

    overall = all(
        r["json_valid"] and r["snippet_rate"] >= MIN_SNIPPET_RATE and r["total_time"] <= MAX_TIME_PER_PAPER
        for r in results
    )
    if overall:
        logger.info("OVERALL: PASS")
    else:
        logger.warning("OVERALL: FAIL — see pivot plan in log")

    sys.exit(0 if overall else 1)


if __name__ == "__main__":
    main()
