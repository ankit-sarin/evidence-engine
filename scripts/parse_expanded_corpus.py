"""Parse expanded corpus PDFs — advance ABSTRACT_SCREENED_IN → PDF_ACQUIRED → PARSED.

For papers at ABSTRACT_SCREENED_IN with a PDF on disk and no parsed text,
advances status to PDF_ACQUIRED then runs Docling parsing.

Usage:
    python scripts/parse_expanded_corpus.py
"""

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.core.database import ReviewDatabase
from engine.parsers.pdf_parser import parse_all_pdfs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_REVIEW = "surgical_autonomy"


def main():
    parser = argparse.ArgumentParser(description="Parse expanded corpus PDFs")
    parser.add_argument("--review", default=DEFAULT_REVIEW, help=f"Review name (default: {DEFAULT_REVIEW})")
    args = parser.parse_args()

    if args.review == DEFAULT_REVIEW and "--review" not in " ".join(sys.argv):
        logging.warning("No --review specified, using default 'surgical_autonomy'.")

    review = args.review
    db = ReviewDatabase(review)

    # Find ABSTRACT_SCREENED_IN papers with PDFs but no parsed text
    rows = db._conn.execute("""
        SELECT p.id, p.ee_identifier,
               COALESCE(ft.pdf_path, p.pdf_local_path) AS pdf_path,
               ft.parsed_text_path
        FROM papers p
        LEFT JOIN full_text_assets ft ON ft.paper_id = p.id
        WHERE p.status = 'ABSTRACT_SCREENED_IN'
          AND p.pdf_quality_check_status = 'HUMAN_CONFIRMED'
          AND COALESCE(ft.pdf_path, p.pdf_local_path) IS NOT NULL
          AND (ft.parsed_text_path IS NULL)
        ORDER BY p.id
    """).fetchall()

    logger.info("Found %d papers to advance and parse", len(rows))

    # Step 1: Advance all to PDF_ACQUIRED
    advanced = 0
    for row in rows:
        pid = row["id"]
        pdf_path = row["pdf_path"]
        if pdf_path and Path(pdf_path).exists():
            try:
                db.update_status(pid, "PDF_ACQUIRED")
                advanced += 1
            except ValueError as e:
                logger.warning("Skip %d: %s", pid, e)
        else:
            logger.warning("Paper %d (%s): PDF not on disk — skipping",
                           pid, row["ee_identifier"])

    logger.info("Advanced %d papers to PDF_ACQUIRED", advanced)

    # Step 2: Parse all PDF_ACQUIRED papers
    t0 = time.time()
    stats = parse_all_pdfs(db, review)
    elapsed = time.time() - t0

    print(f"\n{'=' * 60}")
    print("PARSING COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Advanced to PDF_ACQUIRED: {advanced}")
    print(f"  Parsed:       {stats['parsed']}")
    print(f"    Docling:    {stats['docling']}")
    print(f"    Qwen2.5-VL: {stats['qwen2.5vl']}")
    print(f"  Skipped:      {stats['skipped_existing']}")
    print(f"  Failed:       {stats['failed']}")
    print(f"  Elapsed:      {elapsed/60:.1f} min")


if __name__ == "__main__":
    main()
