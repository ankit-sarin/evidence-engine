"""PDF-to-Markdown parser: Docling → PyMuPDF fallback → Qwen2.5-VL for scanned.

CLI:
    python -m engine.parsers.pdf_parser --verify-hashes --review surgical_autonomy
"""

import argparse
import base64
import hashlib
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import fitz  # PyMuPDF
from engine.utils.ollama_client import ollama_chat
from docling.document_converter import DocumentConverter

from engine.core.database import ReviewDatabase
from engine.parsers.models import ParsedDocument

logger = logging.getLogger(__name__)

_SCANNED_THRESHOLD = 100  # chars per page — below this, assume scanned
_VISION_MODEL = "qwen2.5vl:7b"


# ── Public API ───────────────────────────────────────────────────────


def compute_pdf_hash(pdf_path: str) -> str | None:
    """SHA-256 hash of the PDF file contents. Returns None if file doesn't exist."""
    path = Path(pdf_path)
    if not path.exists():
        return None
    h = hashlib.sha256()
    with open(pdf_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def is_scanned_pdf(pdf_path: str) -> bool:
    """Heuristic: if extractable text is sparse relative to page count, it's scanned."""
    doc = fitz.open(pdf_path)
    try:
        num_pages = len(doc)
        if num_pages == 0:
            return True
        total_chars = sum(len(page.get_text()) for page in doc)
        chars_per_page = total_chars / num_pages
        return chars_per_page < _SCANNED_THRESHOLD
    finally:
        doc.close()


def parse_with_docling(pdf_path: str) -> str:
    """Parse a digital PDF to Markdown using Docling."""
    converter = DocumentConverter()
    result = converter.convert(pdf_path)
    return result.document.export_to_markdown()


def parse_with_pymupdf(pdf_path: str) -> str:
    """Extract text from a digital PDF using PyMuPDF as a structural fallback."""
    doc = fitz.open(pdf_path)
    pages: list[str] = []
    try:
        for i, page in enumerate(doc):
            text = page.get_text("text")
            pages.append(f"<!-- Page {i + 1} -->\n{text}")
    finally:
        doc.close()
    return "\n\n---\n\n".join(pages)


def parse_with_vision(pdf_path: str) -> str:
    """Parse a scanned PDF by sending page images to Qwen2.5-VL via Ollama."""
    doc = fitz.open(pdf_path)
    pages_md: list[str] = []

    try:
        for page_num in range(len(doc)):
            page = doc[page_num]
            # Render page to PNG at 200 DPI
            pix = page.get_pixmap(dpi=200)
            img_bytes = pix.tobytes("png")
            img_b64 = base64.b64encode(img_bytes).decode()

            response = ollama_chat(
                model=_VISION_MODEL,
                messages=[
                    {
                        "role": "user",
                        "content": (
                            "Extract all text from this page. Preserve tables, "
                            "headings, and formatting. Output as Markdown."
                        ),
                        "images": [img_b64],
                    }
                ],
                options={"temperature": 0},
            )
            page_text = response.message.content
            pages_md.append(f"<!-- Page {page_num + 1} -->\n{page_text}")
            logger.info("Qwen2.5-VL parsed page %d/%d", page_num + 1, len(doc))
    finally:
        doc.close()

    return "\n\n---\n\n".join(pages_md)


def parse_pdf(
    pdf_path: str,
    paper_id: int,
    review_name: str,
    db: ReviewDatabase,
) -> ParsedDocument:
    """Parse a PDF, route between Docling and Qwen2.5-VL, save and record."""
    pdf_hash = compute_pdf_hash(pdf_path)

    # Check for existing parse with same hash
    existing = db._conn.execute(
        "SELECT parsed_text_version FROM full_text_assets "
        "WHERE paper_id = ? AND pdf_hash = ? ORDER BY parsed_text_version DESC LIMIT 1",
        (paper_id, pdf_hash),
    ).fetchone()

    if existing:
        version = existing[0]
        md_path = (
            Path(db.db_path).parent / "parsed_text" / f"{paper_id}_v{version}.md"
        )
        logger.info("Paper %d already parsed (v%d, same hash) — skipping", paper_id, version)
        return ParsedDocument(
            paper_id=paper_id,
            source_pdf_path=pdf_path,
            pdf_hash=pdf_hash,
            parsed_markdown=md_path.read_text() if md_path.exists() else "",
            parser_used="docling",
            parsed_at=datetime.now(timezone.utc),
            version=version,
        )

    # Determine version number
    last_version = db._conn.execute(
        "SELECT MAX(parsed_text_version) FROM full_text_assets WHERE paper_id = ?",
        (paper_id,),
    ).fetchone()[0]
    version = (last_version or 0) + 1

    # Route: scanned → vision model; digital → Docling → PyMuPDF fallback
    if is_scanned_pdf(pdf_path):
        logger.info("Paper %d: scanned PDF detected, using Qwen2.5-VL", paper_id)
        markdown = parse_with_vision(pdf_path)
        parser_used = "qwen2.5vl"
    else:
        logger.info("Paper %d: digital PDF, using Docling", paper_id)
        try:
            markdown = parse_with_docling(pdf_path)
            parser_used = "docling"
        except Exception as exc:
            logger.warning(
                "Paper %d: Docling failed (%s), falling back to PyMuPDF",
                paper_id, exc,
            )
            markdown = parse_with_pymupdf(pdf_path)
            parser_used = "pymupdf"

        # If output is sparse, try PyMuPDF (if not already), then vision model
        if len(markdown.strip()) < _SCANNED_THRESHOLD and parser_used == "docling":
            logger.warning(
                "Paper %d: Docling output sparse (%d chars), falling back to PyMuPDF",
                paper_id, len(markdown.strip()),
            )
            markdown = parse_with_pymupdf(pdf_path)
            parser_used = "pymupdf"

        if len(markdown.strip()) < _SCANNED_THRESHOLD:
            logger.warning(
                "Paper %d: %s output sparse (%d chars), falling back to Qwen2.5-VL",
                paper_id, parser_used, len(markdown.strip()),
            )
            markdown = parse_with_vision(pdf_path)
            parser_used = "qwen2.5vl"

    # Save Markdown file
    review_dir = Path(db.db_path).parent
    md_filename = f"{paper_id}_v{version}.md"
    md_path = review_dir / "parsed_text" / md_filename
    md_path.write_text(markdown)

    # Record in database
    now = datetime.now(timezone.utc).isoformat()
    db._conn.execute(
        """INSERT INTO full_text_assets
           (paper_id, pdf_path, pdf_hash, parsed_text_path, parsed_text_version,
            parser_used, parsed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (paper_id, pdf_path, pdf_hash, str(md_path), version, parser_used, now),
    )
    # Store content hash on the papers row for provenance
    db._conn.execute(
        "UPDATE papers SET pdf_content_hash = ? WHERE id = ?",
        (pdf_hash, paper_id),
    )
    db._conn.commit()

    return ParsedDocument(
        paper_id=paper_id,
        source_pdf_path=pdf_path,
        pdf_hash=pdf_hash,
        parsed_markdown=markdown,
        parser_used=parser_used,
        parsed_at=datetime.now(timezone.utc),
        version=version,
    )


def parse_all_pdfs(db: ReviewDatabase, review_name: str) -> dict:
    """Parse all PDF_ACQUIRED papers. Returns stats dict."""
    papers = db.get_papers_by_status("PDF_ACQUIRED")
    total = len(papers)
    logger.info("Starting PDF parsing for %d papers", total)

    stats = {"parsed": 0, "skipped_existing": 0, "failed": 0, "docling": 0, "pymupdf": 0, "qwen2.5vl": 0}
    review_dir = Path(db.db_path).parent

    for i, paper in enumerate(papers, 1):
        pid = paper["id"]
        pdf_dir = review_dir / "pdfs"

        # Find the PDF file — prefer DB path (works after rename), fall back to glob
        pdf_path = None

        # 1. Check full_text_assets.pdf_path (set by parser/verify_downloads)
        ft_row = db._conn.execute(
            "SELECT pdf_path FROM full_text_assets WHERE paper_id = ? ORDER BY id DESC LIMIT 1",
            (pid,),
        ).fetchone()
        if ft_row and ft_row["pdf_path"]:
            candidate = Path(ft_row["pdf_path"])
            if candidate.exists():
                pdf_path = str(candidate)
            elif not candidate.is_absolute():
                joined = pdf_dir / candidate
                if joined.exists():
                    pdf_path = str(joined)

        # 2. Check papers.pdf_local_path (set by downloader/verify_downloads)
        if not pdf_path:
            lp_row = db._conn.execute(
                "SELECT pdf_local_path FROM papers WHERE id = ?", (pid,),
            ).fetchone()
            if lp_row and lp_row["pdf_local_path"]:
                candidate = Path(lp_row["pdf_local_path"])
                if candidate.exists():
                    pdf_path = str(candidate)
                elif not candidate.is_absolute():
                    joined = pdf_dir / candidate
                    if joined.exists():
                        pdf_path = str(joined)

        # 3. Fall back to filesystem glob (handles bare integer and prefixed names)
        if not pdf_path:
            pdf_candidates = list(pdf_dir.glob(f"{pid}_*.pdf")) + list(
                pdf_dir.glob(f"{pid}.pdf")
            )
            if pdf_candidates:
                pdf_path = str(pdf_candidates[0])

        if not pdf_path:
            logger.warning("Paper %d: no PDF found in %s", pid, pdf_dir)
            stats["failed"] += 1
            continue
        try:
            result = parse_pdf(pdf_path, pid, review_name, db)

            stats[result.parser_used] = stats.get(result.parser_used, 0) + 1

            db.update_status(pid, "PARSED")
            stats["parsed"] += 1
        except Exception as exc:
            logger.error("Paper %d: parsing failed — %s", pid, exc)
            stats["failed"] += 1

        if i % 10 == 0 or i == total:
            logger.info("Parsed %d/%d papers", i, total)

    logger.info(
        "Parsing complete: %d parsed (%d docling, %d pymupdf, %d qwen2.5vl), %d skipped, %d failed",
        stats["parsed"],
        stats["docling"],
        stats["pymupdf"],
        stats["qwen2.5vl"],
        stats["skipped_existing"],
        stats["failed"],
    )
    return stats


# ── Hash Verification ────────────────────────────────────────────────


def verify_hashes(db: ReviewDatabase) -> list[dict]:
    """Check all papers with stored pdf_content_hash against current PDF files.

    Returns a list of mismatch dicts: {paper_id, stored_hash, current_hash, pdf_path}.
    """
    rows = db._conn.execute(
        """SELECT p.id, p.pdf_content_hash, p.pdf_local_path,
                  fta.pdf_path AS fta_pdf_path
           FROM papers p
           LEFT JOIN full_text_assets fta ON fta.paper_id = p.id
           WHERE p.pdf_content_hash IS NOT NULL
           ORDER BY p.id"""
    ).fetchall()

    # Deduplicate by paper_id (take the first/most recent fta path)
    seen: set[int] = set()
    mismatches: list[dict] = []

    for row in rows:
        pid = row["id"]
        if pid in seen:
            continue
        seen.add(pid)

        stored_hash = row["pdf_content_hash"]

        # Resolve PDF path: prefer fta, then papers.pdf_local_path
        pdf_path = row["fta_pdf_path"] or row["pdf_local_path"]
        if not pdf_path:
            mismatches.append({
                "paper_id": pid,
                "stored_hash": stored_hash,
                "current_hash": None,
                "pdf_path": None,
            })
            continue

        current_hash = compute_pdf_hash(pdf_path)
        if current_hash != stored_hash:
            mismatches.append({
                "paper_id": pid,
                "stored_hash": stored_hash,
                "current_hash": current_hash,
                "pdf_path": pdf_path,
            })

    return mismatches


# ── CLI ──────────────────────────────────────────────────────────────


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="PDF parser utilities")
    parser.add_argument("--verify-hashes", action="store_true",
                        help="Check stored PDF hashes against current files")
    parser.add_argument("--review", required=True, help="Review name")
    args = parser.parse_args()

    if args.verify_hashes:
        db = ReviewDatabase(args.review)
        try:
            mismatches = verify_hashes(db)
            if not mismatches:
                print("All PDF content hashes match current files.")
            else:
                print(f"\n{len(mismatches)} PDF hash mismatch(es) found:\n")
                for m in mismatches:
                    status = "MISSING" if m["current_hash"] is None else "CHANGED"
                    print(f"  Paper {m['paper_id']:>5d}: {status}")
                    print(f"    Stored:  {m['stored_hash'][:16]}...")
                    current = m['current_hash'] or 'N/A'
                    print(f"    Current: {current[:16] + '...' if m['current_hash'] else current}")
                    print(f"    Path:    {m['pdf_path'] or 'not found'}")
                sys.exit(1)
        finally:
            db.close()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
