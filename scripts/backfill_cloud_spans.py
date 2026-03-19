#!/usr/bin/env python3
"""Backfill cloud_evidence_spans for extractions that have valid extracted_data
but no corresponding span rows.

Usage:
    python scripts/backfill_cloud_spans.py --dry-run
    python scripts/backfill_cloud_spans.py --confirm
"""

import argparse
import json
import logging
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from engine.cloud.base import CloudExtractorBase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_REVIEW = "surgical_autonomy"


def main():
    parser = argparse.ArgumentParser(description="Backfill missing cloud evidence spans")
    parser.add_argument("--review", default=DEFAULT_REVIEW, help=f"Review name (default: {DEFAULT_REVIEW})")
    parser.add_argument("--confirm", action="store_true", help="Actually write spans (default is dry-run)")
    parser.add_argument("--db", default=None, help="Database path (default: data/<review>/review.db)")
    args = parser.parse_args()

    if args.review == DEFAULT_REVIEW and "--review" not in " ".join(sys.argv):
        logging.warning("No --review specified, using default 'surgical_autonomy'.")

    db_path = args.db or f"data/{args.review}/review.db"

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    cur = conn.cursor()

    # Find extractions with no spans
    cur.execute("""
        SELECT ce.id, ce.paper_id, ce.arm, ce.extracted_data
        FROM cloud_extractions ce
        WHERE ce.id NOT IN (SELECT DISTINCT cloud_extraction_id FROM cloud_evidence_spans)
    """)
    rows = cur.fetchall()
    logger.info("Found %d extractions with no spans", len(rows))

    base = CloudExtractorBase.__new__(CloudExtractorBase)
    backfilled = 0
    failed = 0
    total_spans = 0

    for row in rows:
        ext_id, pid, arm = row["id"], row["paper_id"], row["arm"]
        data = json.loads(row["extracted_data"])
        spans = CloudExtractorBase.parse_response_to_spans(base, data)

        if not spans:
            logger.warning("Paper %d (%s): still 0 spans after parse — skipping", pid, arm)
            failed += 1
            continue

        if args.confirm:
            for span in spans:
                conn.execute(
                    """INSERT INTO cloud_evidence_spans
                       (cloud_extraction_id, field_name, value, source_snippet,
                        confidence, tier, notes)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        ext_id,
                        span["field_name"],
                        span.get("value"),
                        span.get("source_snippet"),
                        span.get("confidence"),
                        span.get("tier"),
                        span.get("notes"),
                    ),
                )
            conn.commit()

        backfilled += 1
        total_spans += len(spans)
        logger.info("Paper %d (%s): %d spans %s",
                     pid, arm, len(spans),
                     "written" if args.confirm else "(dry-run)")

    logger.info("Done: %d backfilled (%d spans), %d failed, %d total",
                backfilled, total_spans, failed, len(rows))

    if not args.confirm and backfilled:
        logger.info("Re-run with --confirm to write spans")

    # Verify
    if args.confirm:
        cur.execute("""
            SELECT arm, COUNT(DISTINCT ce.paper_id)
            FROM cloud_evidence_spans ces
            JOIN cloud_extractions ce ON ces.cloud_extraction_id = ce.id
            GROUP BY ce.arm
        """)
        logger.info("Post-backfill span counts:")
        for r in cur.fetchall():
            logger.info("  %s: %d papers with spans", r[0], r[1])

    conn.close()


if __name__ == "__main__":
    main()
