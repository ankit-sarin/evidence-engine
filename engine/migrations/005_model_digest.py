"""Migration 005: Add model digest columns to extractions table.

Records the exact Ollama model digest (hash) at extraction time for both
the extractor and auditor models.  Guards against silent model updates
via ``ollama pull``.

Adds columns:
  - extractions.model_digest: digest of the extraction model
  - extractions.auditor_model_digest: digest of the auditor model

Idempotent: skips columns that already exist.

Usage:
    python -m engine.migrations.005_model_digest [--review NAME]
"""

import argparse
import logging
import sqlite3
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

_NEW_COLUMNS = [
    ("extractions", "model_digest", "TEXT"),
    ("extractions", "auditor_model_digest", "TEXT"),
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
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(extractions)").fetchall()}
    for _, col, _ in _NEW_COLUMNS:
        assert col in cols, f"Column {col} missing after migration"

    conn.close()

    stats = {"columns_added": added, "columns_skipped": skipped}
    logger.info(
        "Migration 005 complete: %d columns added, %d skipped",
        added, skipped,
    )
    return stats


def main():
    parser = argparse.ArgumentParser(description="Migration 005: model digest columns")
    parser.add_argument("--review", default="surgical_autonomy", help="Review name")
    args = parser.parse_args()

    db_path = DATA_DIR / args.review / "review.db"
    if not db_path.exists():
        logger.error("Database not found: %s", db_path)
        return

    result = run_migration(db_path)
    print(f"\n{'=' * 50}")
    print("MIGRATION 005 REPORT")
    print(f"{'=' * 50}")
    print(f"  Columns added:   {result['columns_added']}")
    print(f"  Columns skipped: {result['columns_skipped']}")


if __name__ == "__main__":
    main()
