#!/usr/bin/env python3
"""Full extraction + audit on eligible papers.

Designed for unattended tmux execution. Per-paper DB commits mean
crash recovery is automatic on restart — already-extracted papers
are skipped via schema-hash check.

Usage:
    python scripts/run5_extract_and_audit.py
    python scripts/run5_extract_and_audit.py --retry-failed
"""

import argparse
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from engine.agents.auditor import run_audit
from engine.agents.extractor import run_extraction
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run5")

REVIEW = "surgical_autonomy"
SPEC_PATH = "review_specs/surgical_autonomy_v1.yaml"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run extraction + audit pipeline"
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Re-attempt extraction on EXTRACT_FAILED papers only",
    )
    parser.add_argument(
        "--paper-ids",
        type=int,
        nargs="+",
        help="Extract specific paper IDs only",
    )
    return parser.parse_args(argv)


def reset_failed_papers(db: ReviewDatabase) -> list[int]:
    """Find EXTRACT_FAILED papers, reset to FT_ELIGIBLE, return their IDs."""
    failed = db.get_papers_by_status("EXTRACT_FAILED")

    if not failed:
        logger.info("No EXTRACT_FAILED papers found — nothing to retry")
        return []

    ids = [p["id"] for p in failed]
    logger.info(
        "Found %d EXTRACT_FAILED papers to retry: %s",
        len(ids), ids,
    )

    for pid in ids:
        db.update_status(pid, "FT_ELIGIBLE")
    logger.info("Reset %d papers to FT_ELIGIBLE", len(ids))

    return ids


def main(argv: list[str] | None = None):
    args = parse_args(argv)

    if args.retry_failed and args.paper_ids:
        logger.error("--retry-failed and --paper-ids are mutually exclusive")
        sys.exit(1)

    db = ReviewDatabase(REVIEW)
    spec = load_review_spec(SPEC_PATH)

    # ── Retry-failed: reset EXTRACT_FAILED → FT_ELIGIBLE ──
    if args.retry_failed:
        reset_ids = reset_failed_papers(db)
        if not reset_ids:
            db.close()
            return

    # ── Phase 1: Extraction ──
    logger.info("=" * 60)
    logger.info("PHASE 1: EXTRACTION")
    logger.info("=" * 60)

    t0 = time.time()
    extract_stats = run_extraction(db, spec, REVIEW)
    extract_elapsed = time.time() - t0

    logger.info(
        "Extraction finished in %.1f min: %d extracted, %d skipped, %d failed",
        extract_elapsed / 60,
        extract_stats["extracted"],
        extract_stats["skipped"],
        extract_stats["failed"],
    )

    # ── Phase 2: Audit ──
    logger.info("=" * 60)
    logger.info("PHASE 2: AUDIT")
    logger.info("=" * 60)

    t1 = time.time()
    audit_stats = run_audit(db, REVIEW, spec)
    audit_elapsed = time.time() - t1

    logger.info(
        "Audit finished in %.1f min: %s",
        audit_elapsed / 60,
        audit_stats,
    )

    # ── Summary ──
    total_elapsed = time.time() - t0
    logger.info("=" * 60)
    logger.info("COMPLETE — %.1f hours total", total_elapsed / 3600)
    logger.info("  Extraction: %.1f min", extract_elapsed / 60)
    logger.info("  Audit:      %.1f min", audit_elapsed / 60)
    logger.info("  Stats:      %s", extract_stats)
    logger.info("=" * 60)

    db.close()


if __name__ == "__main__":
    main()
