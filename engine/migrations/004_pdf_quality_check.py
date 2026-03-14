"""Migration 004: Add PDF quality check columns and PDF_EXCLUDED status.

Adds columns to the papers table for tracking PDF quality checks:
  - pdf_exclusion_reason: NON_ENGLISH | NOT_MANUSCRIPT | INACCESSIBLE | OTHER
  - pdf_exclusion_detail: free-text detail
  - pdf_quality_check_status: PENDING | AI_CHECKED | HUMAN_CONFIRMED
  - pdf_ai_language: detected language
  - pdf_ai_content_type: detected content type
  - pdf_ai_confidence: AI confidence score

Adds PDF_EXCLUDED as a terminal paper status (papers do not advance).

Idempotent: skips columns that already exist.

Usage:
    python -m engine.migrations.004_pdf_quality_check [--review NAME]
"""

import argparse
import logging
import sqlite3
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

_NEW_COLUMNS = [
    ("papers", "pdf_exclusion_reason", "TEXT"),
    ("papers", "pdf_exclusion_detail", "TEXT"),
    ("papers", "pdf_quality_check_status", "TEXT"),
    ("papers", "pdf_ai_language", "TEXT"),
    ("papers", "pdf_ai_content_type", "TEXT"),
    ("papers", "pdf_ai_confidence", "REAL"),
]


def run_migration(db_path: str | Path) -> dict:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    added = 0
    skipped = 0

    for table, col, col_type in _NEW_COLUMNS:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
            conn.commit()
            logger.info("Added column %s.%s (%s)", table, col, col_type)
            added += 1
        except sqlite3.OperationalError:
            skipped += 1  # column already exists

    # Verify all columns exist
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(papers)").fetchall()}
    for _, col, _ in _NEW_COLUMNS:
        assert col in cols, f"Column {col} missing after migration"

    # Report current PDF_EXCLUDED count (should be 0 on first run)
    pdf_excluded = conn.execute(
        "SELECT COUNT(*) FROM papers WHERE status = 'PDF_EXCLUDED'"
    ).fetchone()[0]

    conn.close()

    stats = {"columns_added": added, "columns_skipped": skipped, "pdf_excluded": pdf_excluded}
    logger.info(
        "Migration 004 complete: %d columns added, %d skipped, %d PDF_EXCLUDED papers",
        added, skipped, pdf_excluded,
    )
    return stats


def main():
    parser = argparse.ArgumentParser(description="Migration 004: PDF quality check columns")
    parser.add_argument("--review", default="surgical_autonomy", help="Review name")
    args = parser.parse_args()

    db_path = DATA_DIR / args.review / "review.db"
    if not db_path.exists():
        logger.error("Database not found: %s", db_path)
        return

    result = run_migration(db_path)
    print(f"\n{'=' * 50}")
    print("MIGRATION 004 REPORT")
    print(f"{'=' * 50}")
    print(f"  Columns added:   {result['columns_added']}")
    print(f"  Columns skipped: {result['columns_skipped']}")
    print(f"  PDF_EXCLUDED:    {result['pdf_excluded']}")


if __name__ == "__main__":
    main()
