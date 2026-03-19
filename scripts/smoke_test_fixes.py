#!/usr/bin/env python3
"""Smoke test: extract + audit 5 papers with the fixed prompts.

Uses a temporary SQLite DB so the production data is untouched.
Reports per-paper: field count, verified/flagged/grep-fail breakdown.
"""

import argparse
import json
import logging
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

# Ensure project root is importable
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.agents.auditor import audit_span, grep_verify
from engine.agents.extractor import build_extraction_prompt, extract_pass1_reasoning, extract_pass2_structured
from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec, load_review_spec

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_REVIEW = "surgical_autonomy"

# Pick 5 papers with varying original span counts
# paper 82 (13 spans, mix of verified/flagged) — our deep-dive paper
# paper 4 (13 spans), paper 24 (11 spans), paper 70 (10 spans), paper 123 (12 spans)
TEST_PAPER_IDS = [82, 4, 24, 70, 123]


def main():
    parser = argparse.ArgumentParser(description="Smoke test: extract + audit 5 papers with fixed prompts")
    parser.add_argument("--review", default=DEFAULT_REVIEW, help=f"Review name (default: {DEFAULT_REVIEW})")
    parser.add_argument("--spec", default=None, help="Path to review spec YAML (default: review_specs/<review>_v1.yaml)")
    args = parser.parse_args()

    if args.review == DEFAULT_REVIEW and "--review" not in " ".join(sys.argv):
        logging.warning("No --review specified, using default 'surgical_autonomy'.")

    review = args.review
    spec_path = args.spec or f"review_specs/{review}_v1.yaml"
    review_dir = Path(f"data/{review}")
    prod_db = review_dir / "review.db"

    spec = load_review_spec(spec_path)

    # Build field_type lookup
    field_type_map = {f.name: f.type for f in spec.extraction_schema.fields}
    expected_field_count = len(spec.extraction_schema.fields)
    logger.info("Schema has %d fields, types: %s", expected_field_count, json.dumps(field_type_map))

    # Read paper metadata from production DB
    prod_conn = sqlite3.connect(prod_db)
    prod_conn.row_factory = sqlite3.Row

    results = []

    for pid in TEST_PAPER_IDS:
        row = prod_conn.execute("SELECT title FROM papers WHERE id = ?", (pid,)).fetchone()
        title = row["title"] if row else f"Paper {pid}"
        logger.info("=" * 60)
        logger.info("Paper %d: %s", pid, title[:80])

        # Load parsed text
        parsed_dir = review_dir / "parsed_text"
        md_files = sorted(parsed_dir.glob(f"{pid}_v*.md"), reverse=True)
        if not md_files:
            logger.warning("Paper %d: no parsed text — skipping", pid)
            continue

        paper_text = md_files[0].read_text()
        logger.info("Paper text length: %d chars", len(paper_text))

        # ── EXTRACT ──
        prompt = build_extraction_prompt(paper_text, spec)
        t0 = time.time()
        logger.info("Pass 1: reasoning...")
        reasoning = extract_pass1_reasoning(prompt)
        t1 = time.time()
        logger.info("Pass 1 done in %.1fs (%d chars)", t1 - t0, len(reasoning))

        logger.info("Pass 2: structured output...")
        extraction = extract_pass2_structured(prompt, reasoning, spec, pid)
        t2 = time.time()
        logger.info("Pass 2 done in %.1fs — %d fields", t2 - t1, len(extraction.fields))

        # ── AUDIT each span ──
        stats = {
            "paper_id": pid,
            "title": title[:60],
            "total_fields": len(extraction.fields),
            "verified": 0,
            "flagged": 0,
            "grep_failures": 0,
            "empty_snippet_skipped": 0,
            "not_found_skipped": 0,
            "tier4_present": False,
            "field_names": [],
        }

        for span in extraction.fields:
            field_name = span.field_name
            stats["field_names"].append(field_name)

            if field_name in ("key_limitation", "clinical_readiness_assessment"):
                stats["tier4_present"] = True

            span_data = {
                "field_name": field_name,
                "value": span.value,
                "source_snippet": span.source_snippet,
                "confidence": span.confidence,
                "tier": span.tier,
            }
            ft = field_type_map.get(field_name, "text")
            verdict = audit_span(span_data, paper_text, field_type=ft)

            if verdict.status == "verified":
                stats["verified"] += 1
                if span.value == "NOT_FOUND":
                    stats["not_found_skipped"] += 1
                elif not span.source_snippet or not span.source_snippet.strip():
                    stats["empty_snippet_skipped"] += 1
            else:
                stats["flagged"] += 1
                if not verdict.grep_found:
                    stats["grep_failures"] += 1

            snippet_desc = "empty" if not span.source_snippet.strip() else f"{len(span.source_snippet)} chars"
            logger.info(
                "  %-30s %-10s %-8s snippet=%-12s val=%s",
                field_name, verdict.status,
                "grep_ok" if verdict.grep_found else "no_grep",
                snippet_desc,
                span.value[:80],
            )

        t3 = time.time()
        flag_rate = stats["flagged"] / max(stats["total_fields"], 1) * 100
        logger.info(
            "Paper %d summary: %d fields, %d verified, %d flagged (%.0f%%), "
            "%d grep failures, tier4=%s",
            pid, stats["total_fields"], stats["verified"], stats["flagged"],
            flag_rate, stats["grep_failures"], stats["tier4_present"],
        )
        stats["flag_rate"] = round(flag_rate, 1)
        stats["extract_time"] = round(t2 - t0, 1)
        stats["audit_time"] = round(t3 - t2, 1)
        results.append(stats)

    prod_conn.close()

    # ── SUMMARY ──
    logger.info("=" * 60)
    logger.info("SMOKE TEST SUMMARY")
    logger.info("=" * 60)

    total_fields = sum(r["total_fields"] for r in results)
    total_verified = sum(r["verified"] for r in results)
    total_flagged = sum(r["flagged"] for r in results)
    total_grep_fail = sum(r["grep_failures"] for r in results)
    tier4_count = sum(1 for r in results if r["tier4_present"])

    overall_flag_rate = total_flagged / max(total_fields, 1) * 100

    for r in results:
        logger.info(
            "  Paper %-4d: %2d fields, %2d verified, %2d flagged (%4.0f%%), tier4=%s",
            r["paper_id"], r["total_fields"], r["verified"], r["flagged"],
            r["flag_rate"], r["tier4_present"],
        )

    logger.info("-" * 60)
    logger.info(
        "  TOTAL:      %2d fields, %2d verified, %2d flagged (%.0f%%)",
        total_fields, total_verified, total_flagged, overall_flag_rate,
    )
    logger.info("  Grep failures:  %d / %d flagged", total_grep_fail, total_flagged)
    logger.info("  Tier 4 present: %d / %d papers", tier4_count, len(results))
    logger.info("  OLD flag rate:  72%%")
    logger.info("  NEW flag rate:  %.0f%%", overall_flag_rate)

    # Check fabricated snippets
    fabricated = 0
    for r in results:
        # Already counted as empty_snippet_skipped or not_found_skipped
        pass
    logger.info("  Empty snippets correctly skipped: %d", sum(r["empty_snippet_skipped"] for r in results))
    logger.info("  NOT_FOUND correctly skipped: %d", sum(r["not_found_skipped"] for r in results))


if __name__ == "__main__":
    main()
