#!/usr/bin/env python3
"""Retry parsing 6 stuck PDF_ACQUIRED papers: Docling first, PyMuPDF fallback."""

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import fitz  # PyMuPDF

from engine.core.database import ReviewDatabase
from engine.parsers.pdf_parser import compute_pdf_hash, parse_with_docling

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_REVIEW = "surgical_autonomy"
STUCK_IDS = [274, 368, 378, 455, 748, 780]


def parse_with_pymupdf(pdf_path: str) -> str:
    """Extract text from PDF using PyMuPDF and format as Markdown."""
    doc = fitz.open(pdf_path)
    pages = []
    try:
        for i, page in enumerate(doc):
            text = page.get_text("text")
            pages.append(f"<!-- Page {i + 1} -->\n{text}")
    finally:
        doc.close()
    return "\n\n---\n\n".join(pages)


def retry_paper(db: ReviewDatabase, paper_id: int) -> str:
    """Try Docling, fall back to PyMuPDF. Returns parser used or raises."""
    # Get PDF path
    row = db._conn.execute(
        "SELECT pdf_path FROM full_text_assets WHERE paper_id = ? ORDER BY id DESC LIMIT 1",
        (paper_id,),
    ).fetchone()
    if not row:
        # Also check papers.pdf_local_path
        p_row = db._conn.execute(
            "SELECT pdf_local_path FROM papers WHERE id = ?", (paper_id,),
        ).fetchone()
        if not p_row or not p_row["pdf_local_path"]:
            raise FileNotFoundError(f"No PDF path for paper {paper_id}")
        pdf_path = p_row["pdf_local_path"]
    else:
        pdf_path = row["pdf_path"]
    if not Path(pdf_path).exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    pdf_hash = compute_pdf_hash(pdf_path)

    # Determine version
    last_v = db._conn.execute(
        "SELECT MAX(parsed_text_version) FROM full_text_assets WHERE paper_id = ?",
        (paper_id,),
    ).fetchone()[0]
    version = (last_v or 0) + 1

    # Try Docling
    parser_used = "docling"
    try:
        logger.info("Paper %d: trying Docling...", paper_id)
        markdown = parse_with_docling(pdf_path)
        if len(markdown.strip()) < 100:
            raise ValueError(f"Docling output too sparse ({len(markdown.strip())} chars)")
        logger.info("Paper %d: Docling succeeded (%d chars)", paper_id, len(markdown))
    except Exception as exc:
        logger.warning("Paper %d: Docling failed (%s), falling back to PyMuPDF", paper_id, exc)
        parser_used = "pymupdf"
        markdown = parse_with_pymupdf(pdf_path)
        logger.info("Paper %d: PyMuPDF extracted %d chars", paper_id, len(markdown))

    # Save markdown
    review_dir = Path(db.db_path).parent
    md_path = review_dir / "parsed_text" / f"{paper_id}_v{version}.md"
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(markdown)

    # Update DB
    now = datetime.now(timezone.utc).isoformat()
    db._conn.execute(
        """UPDATE full_text_assets
           SET pdf_hash = ?, parsed_text_path = ?, parsed_text_version = ?,
               parser_used = ?, parsed_at = ?
           WHERE paper_id = ? AND id = (
               SELECT id FROM full_text_assets WHERE paper_id = ? ORDER BY id DESC LIMIT 1
           )""",
        (pdf_hash, str(md_path), version, parser_used, now, paper_id, paper_id),
    )
    db._conn.commit()
    db.update_status(paper_id, "PARSED")

    return parser_used


def main():
    parser = argparse.ArgumentParser(description="Retry parsing stuck PDF_ACQUIRED papers")
    parser.add_argument("--review", default=DEFAULT_REVIEW, help=f"Review name (default: {DEFAULT_REVIEW})")
    args = parser.parse_args()

    if args.review == DEFAULT_REVIEW and "--review" not in " ".join(sys.argv):
        logging.warning("No --review specified, using default 'surgical_autonomy'.")

    db = ReviewDatabase(args.review)
    results = {"docling": [], "pymupdf": [], "failed": []}

    for pid in STUCK_IDS:
        try:
            parser = retry_paper(db, pid)
            results[parser].append(pid)
        except Exception as exc:
            logger.error("Paper %d: FAILED — %s", pid, exc)
            results["failed"].append(pid)

    # Report
    print("\n" + "=" * 50)
    print("RETRY RESULTS")
    print("=" * 50)
    print(f"Docling:  {len(results['docling'])}  {results['docling']}")
    print(f"PyMuPDF:  {len(results['pymupdf'])}  {results['pymupdf']}")
    print(f"Failed:   {len(results['failed'])}  {results['failed']}")

    # Final counts
    cur = db._conn.cursor()
    cur.execute("SELECT status, COUNT(*) FROM papers GROUP BY status ORDER BY status")
    print("\n" + "=" * 50)
    print("FINAL PAPER STATUS COUNTS")
    print("=" * 50)
    for row in cur.fetchall():
        print(f"  {row[0]:30s} {row[1]:>5d}")

    cur.execute("SELECT COUNT(*) FROM full_text_assets WHERE parsed_text_path IS NOT NULL")
    print(f"\n  Total parsed assets:           {cur.fetchone()[0]}")
    cur.execute("SELECT parser_used, COUNT(*) FROM full_text_assets WHERE parsed_text_path IS NOT NULL GROUP BY parser_used")
    for row in cur.fetchall():
        print(f"    {row[0] or 'unknown':20s} {row[1]:>5d}")


if __name__ == "__main__":
    main()
