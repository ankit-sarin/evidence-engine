"""PDF-to-Markdown parser: Docling for digital PDFs, MiniCPM-V for scanned."""

import base64
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

import fitz  # PyMuPDF
import ollama
from docling.document_converter import DocumentConverter

from engine.core.database import ReviewDatabase
from engine.parsers.models import ParsedDocument

logger = logging.getLogger(__name__)

_SCANNED_THRESHOLD = 100  # chars per page — below this, assume scanned
_MINICPM_MODEL = "minicpm-v"


# ── Public API ───────────────────────────────────────────────────────


def compute_pdf_hash(pdf_path: str) -> str:
    """SHA-256 hash of the PDF file contents."""
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


def parse_with_minicpm(pdf_path: str) -> str:
    """Parse a scanned PDF by sending page images to MiniCPM-V via Ollama."""
    doc = fitz.open(pdf_path)
    pages_md: list[str] = []

    try:
        for page_num in range(len(doc)):
            page = doc[page_num]
            # Render page to PNG at 200 DPI
            pix = page.get_pixmap(dpi=200)
            img_bytes = pix.tobytes("png")
            img_b64 = base64.b64encode(img_bytes).decode()

            response = ollama.chat(
                model=_MINICPM_MODEL,
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
            logger.info("MiniCPM-V parsed page %d/%d", page_num + 1, len(doc))
    finally:
        doc.close()

    return "\n\n---\n\n".join(pages_md)


def parse_pdf(
    pdf_path: str,
    paper_id: int,
    review_name: str,
    db: ReviewDatabase,
) -> ParsedDocument:
    """Parse a PDF, route between Docling and MiniCPM-V, save and record."""
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

    # Route: try Docling first, fall back to MiniCPM-V for scanned PDFs
    if is_scanned_pdf(pdf_path):
        logger.info("Paper %d: scanned PDF detected, using MiniCPM-V", paper_id)
        markdown = parse_with_minicpm(pdf_path)
        parser_used = "minicpm-v"
    else:
        logger.info("Paper %d: digital PDF, using Docling", paper_id)
        markdown = parse_with_docling(pdf_path)
        parser_used = "docling"

        # Double-check: if Docling output is suspiciously sparse, retry with MiniCPM-V
        if len(markdown.strip()) < _SCANNED_THRESHOLD:
            logger.warning(
                "Paper %d: Docling output sparse (%d chars), falling back to MiniCPM-V",
                paper_id,
                len(markdown.strip()),
            )
            markdown = parse_with_minicpm(pdf_path)
            parser_used = "minicpm-v"

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

    stats = {"parsed": 0, "skipped_existing": 0, "failed": 0, "docling": 0, "minicpm": 0}
    review_dir = Path(db.db_path).parent

    for i, paper in enumerate(papers, 1):
        pid = paper["id"]
        pdf_dir = review_dir / "pdfs"
        # Find the PDF file for this paper
        pdf_candidates = list(pdf_dir.glob(f"{pid}_*.pdf")) + list(
            pdf_dir.glob(f"{pid}.pdf")
        )
        if not pdf_candidates:
            logger.warning("Paper %d: no PDF found in %s", pid, pdf_dir)
            stats["failed"] += 1
            continue

        pdf_path = str(pdf_candidates[0])
        try:
            result = parse_pdf(pdf_path, pid, review_name, db)

            if result.parser_used == "docling":
                stats["docling"] += 1
            else:
                stats["minicpm"] += 1

            db.update_status(pid, "PARSED")
            stats["parsed"] += 1
        except Exception as exc:
            logger.error("Paper %d: parsing failed — %s", pid, exc)
            stats["failed"] += 1

        if i % 10 == 0 or i == total:
            logger.info("Parsed %d/%d papers", i, total)

    logger.info(
        "Parsing complete: %d parsed (%d docling, %d minicpm), %d skipped, %d failed",
        stats["parsed"],
        stats["docling"],
        stats["minicpm"],
        stats["skipped_existing"],
        stats["failed"],
    )
    return stats
