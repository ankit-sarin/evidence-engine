#!/usr/bin/env python3
"""Advance SCREENED_IN papers to PDF_ACQUIRED where a matching PDF exists on disk.

For each SCREENED_IN paper, checks for data/surgical_autonomy/pdfs/<paper_id>.pdf.
If found, computes the SHA-256 hash, inserts a full_text_assets row, and
transitions the paper status to PDF_ACQUIRED via the standard state machine.
"""

import argparse
import hashlib
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_REVIEW = "surgical_autonomy"


def sha256_file(path: Path) -> str:
    """Compute SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description="Advance SCREENED_IN papers to PDF_ACQUIRED")
    parser.add_argument("--review", default=DEFAULT_REVIEW, help=f"Review name (default: {DEFAULT_REVIEW})")
    args = parser.parse_args()

    if args.review == DEFAULT_REVIEW and "--review" not in " ".join(__import__("sys").argv):
        logging.warning("No --review specified, using default 'surgical_autonomy'.")

    review_name = args.review
    data_root = Path("data")
    review_dir = data_root / review_name
    db_path = review_dir / "review.db"
    pdf_dir = review_dir / "pdfs"

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")

    # Get all SCREENED_IN papers
    rows = conn.execute(
        "SELECT id, title FROM papers WHERE status = 'SCREENED_IN'"
    ).fetchall()

    print(f"Found {len(rows)} SCREENED_IN papers\n")

    advanced = 0
    skipped = 0
    missing_pdfs = []

    for row in rows:
        paper_id = row["id"]
        title = row["title"]
        pdf_path = pdf_dir / f"{paper_id}.pdf"

        if not pdf_path.is_file():
            missing_pdfs.append((paper_id, title))
            skipped += 1
            continue

        pdf_hash = sha256_file(pdf_path)
        now = datetime.now(timezone.utc).isoformat()

        # Use a transaction for atomicity
        conn.execute("BEGIN")
        try:
            # Insert full_text_assets row
            conn.execute(
                """INSERT INTO full_text_assets (paper_id, pdf_path, pdf_hash)
                   VALUES (?, ?, ?)""",
                (paper_id, str(pdf_path), pdf_hash),
            )
            # Transition status via direct UPDATE (mirrors ReviewDatabase.update_status)
            conn.execute(
                "UPDATE papers SET status = 'PDF_ACQUIRED', updated_at = ? WHERE id = ?",
                (now, paper_id),
            )
            conn.execute("COMMIT")
            advanced += 1
        except sqlite3.Error as e:
            conn.execute("ROLLBACK")
            print(f"  ERROR paper {paper_id}: {e}")
            skipped += 1

    conn.close()

    # ── Summary ─────────────────────────────────────────────────────

    print("=" * 56)
    print(f"  SCREENED_IN papers found:   {len(rows)}")
    print(f"  Advanced to PDF_ACQUIRED:   {advanced}")
    print(f"  Skipped (no PDF on disk):   {skipped}")
    print("=" * 56)

    if missing_pdfs:
        print(f"\n  Papers missing PDFs ({len(missing_pdfs)}):")
        for pid, t in missing_pdfs:
            print(f"    id={pid}  \"{t[:72]}\"")
    else:
        print("\n  All SCREENED_IN papers had matching PDFs.")


if __name__ == "__main__":
    main()
