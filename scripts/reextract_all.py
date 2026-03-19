#!/usr/bin/env python3
"""Full re-extraction + re-audit of all 96 screened-in papers.

Resets AI_AUDIT_COMPLETE/HUMAN_AUDIT_COMPLETE papers to PARSED, deletes old
extractions/spans, runs extraction with the updated prompts, then audits.

Usage:
    python scripts/reextract_all.py
"""

import argparse
import json
import logging
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.agents.auditor import run_audit
from engine.agents.extractor import run_extraction
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_REVIEW = "surgical_autonomy"


def main():
    parser = argparse.ArgumentParser(description="Full re-extraction + re-audit of all screened-in papers")
    parser.add_argument("--review", default=DEFAULT_REVIEW, help=f"Review name (default: {DEFAULT_REVIEW})")
    parser.add_argument("--spec", default=None, help="Path to review spec YAML (default: review_specs/<review>_v1.yaml)")
    args = parser.parse_args()

    if args.review == DEFAULT_REVIEW and "--review" not in " ".join(sys.argv):
        logging.warning("No --review specified, using default 'surgical_autonomy'.")

    review_name = args.review
    spec_path = args.spec or f"review_specs/{review_name}_v1.yaml"

    spec = load_review_spec(spec_path)
    db = ReviewDatabase(review_name, data_root=Path("data"))
    schema_hash = spec.extraction_hash()

    # ── Step 1: Identify papers to re-extract ──
    ai_complete = db.get_papers_by_status("AI_AUDIT_COMPLETE")
    human_complete = db.get_papers_by_status("HUMAN_AUDIT_COMPLETE")
    extracted = db.get_papers_by_status("EXTRACTED")
    targets = ai_complete + human_complete + extracted
    logger.info("Papers to re-extract: %d AI_AUDIT_COMPLETE + %d HUMAN_AUDIT_COMPLETE + %d EXTRACTED = %d total",
                len(ai_complete), len(human_complete), len(extracted), len(targets))

    if not targets:
        logger.info("No papers to re-extract.")
        return

    # ── Step 2: Reset status to PARSED and clean old data ──
    # Use reset_for_reaudit() first to handle AI_AUDIT_COMPLETE → EXTRACTED,
    # then manually step to PARSED and delete extractions
    logger.info("Resetting papers to PARSED and cleaning old extractions...")
    for paper in targets:
        pid = paper["id"]
        # Delete old evidence_spans (via extraction_id)
        ext_ids = db._conn.execute(
            "SELECT id FROM extractions WHERE paper_id = ?", (pid,)
        ).fetchall()
        for ext_row in ext_ids:
            db._conn.execute(
                "DELETE FROM evidence_spans WHERE extraction_id = ?",
                (ext_row["id"],),
            )
        # Delete old extractions
        db._conn.execute("DELETE FROM extractions WHERE paper_id = ?", (pid,))
        # Reset status directly (bypass state machine — administrative override)
        db._conn.execute(
            "UPDATE papers SET status = 'PARSED', updated_at = datetime('now') WHERE id = ?",
            (pid,),
        )
    db._conn.commit()

    parsed_count = len(db.get_papers_by_status("PARSED"))
    logger.info("Reset complete. %d papers now at PARSED.", parsed_count)

    # ── Step 3: Run extraction ──
    t0 = time.time()
    logger.info("=" * 60)
    logger.info("STARTING EXTRACTION (%d papers, schema hash: %s)",
                parsed_count, schema_hash[:12])
    extract_stats = run_extraction(db, spec, review_name)
    t1 = time.time()
    logger.info("Extraction complete in %.1f min — %s",
                (t1 - t0) / 60, json.dumps(extract_stats))

    # ── Step 4: Run audit ──
    logger.info("=" * 60)
    logger.info("STARTING AUDIT")
    audit_stats = run_audit(db, review_name, spec=spec)
    t2 = time.time()
    logger.info("Audit complete in %.1f min — %s",
                (t2 - t1) / 60, json.dumps(audit_stats))

    # ── Step 5: Report ──
    logger.info("=" * 60)
    logger.info("FINAL REPORT")
    logger.info("=" * 60)

    # Total spans
    total_spans = db._conn.execute("SELECT COUNT(*) FROM evidence_spans").fetchone()[0]
    verified = db._conn.execute(
        "SELECT COUNT(*) FROM evidence_spans WHERE audit_status = 'verified'"
    ).fetchone()[0]
    contested = db._conn.execute(
        "SELECT COUNT(*) FROM evidence_spans WHERE audit_status = 'contested'"
    ).fetchone()[0]
    flagged = db._conn.execute(
        "SELECT COUNT(*) FROM evidence_spans WHERE audit_status = 'flagged'"
    ).fetchone()[0]
    invalid = db._conn.execute(
        "SELECT COUNT(*) FROM evidence_spans WHERE audit_status = 'invalid_snippet'"
    ).fetchone()[0]
    pending = db._conn.execute(
        "SELECT COUNT(*) FROM evidence_spans WHERE audit_status = 'pending'"
    ).fetchone()[0]

    logger.info("Total evidence spans: %d", total_spans)
    logger.info("  Verified:        %d (%.1f%%)", verified, 100 * verified / max(total_spans, 1))
    logger.info("  Contested:       %d (%.1f%%)", contested, 100 * contested / max(total_spans, 1))
    logger.info("  Flagged:         %d (%.1f%%)", flagged, 100 * flagged / max(total_spans, 1))
    logger.info("  Invalid snippet: %d (%.1f%%)", invalid, 100 * invalid / max(total_spans, 1))
    logger.info("  Pending:         %d", pending)

    # Field coverage
    field_counts = db._conn.execute(
        "SELECT field_name, COUNT(*) as cnt FROM evidence_spans GROUP BY field_name ORDER BY field_name"
    ).fetchall()
    logger.info("\nField coverage (%d distinct fields):", len(field_counts))
    for row in field_counts:
        logger.info("  %-30s %d papers", row["field_name"], row["cnt"])

    # Papers with fewer than 13 spans
    low_span = db._conn.execute("""
        SELECT e.paper_id, p.title, COUNT(es.id) as span_count
        FROM extractions e
        JOIN evidence_spans es ON e.id = es.extraction_id
        JOIN papers p ON e.paper_id = p.id
        GROUP BY e.paper_id
        HAVING span_count < 13
        ORDER BY span_count
    """).fetchall()
    if low_span:
        logger.info("\nPapers with < 13 spans:")
        for row in low_span:
            logger.info("  Paper %-4d (%2d spans): %s", row["paper_id"], row["span_count"], row["title"][:60])
    else:
        logger.info("\nNo papers with < 13 spans.")

    total_time = (t2 - t0) / 60
    logger.info("\nTotal time: %.1f min (extract: %.1f min, audit: %.1f min)",
                total_time, (t1 - t0) / 60, (t2 - t1) / 60)

    db.close()


if __name__ == "__main__":
    main()
