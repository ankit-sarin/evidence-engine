"""Tests for PDF parser with Docling and Qwen2.5-VL routing."""

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from fpdf import FPDF

from engine.core.database import ReviewDatabase
from engine.parsers.models import ParsedDocument
from engine.parsers.pdf_parser import (
    compute_pdf_hash,
    is_scanned_pdf,
    parse_with_docling,
    parse_with_pymupdf,
    parse_pdf,
    verify_hashes,
)
from engine.search.models import Citation


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture()
def digital_pdf(tmp_path) -> Path:
    """Create a minimal digital PDF with extractable text."""
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=12)
    pdf.multi_cell(w=0, text=(
        "Autonomous Robotic Suturing: A Systematic Review. "
        "This study evaluates the performance of autonomous suturing "
        "systems across multiple robotic platforms. Results demonstrate "
        "that Level 3 autonomy achieves comparable accuracy to expert "
        "surgeons in controlled bench-top settings. " * 5
    ))
    path = tmp_path / "digital.pdf"
    pdf.output(str(path))
    return path


@pytest.fixture()
def scanned_pdf(tmp_path) -> Path:
    """Create a PDF with no extractable text (simulating a scanned document)."""
    # An empty-page PDF with an image but no text layer
    pdf = FPDF()
    pdf.add_page()
    # No text added — simulates a scanned image-only page
    path = tmp_path / "scanned.pdf"
    pdf.output(str(path))
    return path


@pytest.fixture()
def db(tmp_path):
    rdb = ReviewDatabase("test_parse", data_root=tmp_path)
    yield rdb
    rdb.close()


def _add_paper(db, pid_hint: str = "1") -> int:
    """Add a paper and return its id."""
    db.add_papers([Citation(title=f"Paper {pid_hint}", source="pubmed", pmid=pid_hint)])
    papers = db.get_papers_by_status("INGESTED")
    return papers[-1]["id"]


# ── Hash ─────────────────────────────────────────────────────────────


def test_compute_pdf_hash_consistent(digital_pdf):
    h1 = compute_pdf_hash(str(digital_pdf))
    h2 = compute_pdf_hash(str(digital_pdf))
    assert h1 == h2
    assert len(h1) == 64  # SHA-256 hex


def test_compute_pdf_hash_different_files(digital_pdf, scanned_pdf):
    h1 = compute_pdf_hash(str(digital_pdf))
    h2 = compute_pdf_hash(str(scanned_pdf))
    assert h1 != h2


# ── Scanned Detection ───────────────────────────────────────────────


def test_digital_pdf_not_scanned(digital_pdf):
    assert is_scanned_pdf(str(digital_pdf)) is False


def test_blank_pdf_detected_as_scanned(scanned_pdf):
    assert is_scanned_pdf(str(scanned_pdf)) is True


# ── Docling Integration ─────────────────────────────────────────────


@pytest.mark.integration
def test_parse_with_docling(digital_pdf):
    md = parse_with_docling(str(digital_pdf))
    assert len(md) > 50
    assert "suturing" in md.lower() or "autonomous" in md.lower()


# ── Routing Logic ────────────────────────────────────────────────────


@pytest.mark.integration
def test_digital_routes_to_docling(digital_pdf, db):
    pid = _add_paper(db)
    # Walk to PDF_ACQUIRED so parse_pdf works
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")

    result = parse_pdf(str(digital_pdf), pid, "test_parse", db)
    assert result.parser_used == "docling"
    assert result.version == 1
    assert len(result.parsed_markdown) > 0


def test_scanned_routes_to_vision_model(scanned_pdf, db):
    pid = _add_paper(db, pid_hint="2")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")

    mock_response = MagicMock()
    mock_response.message.content = "# Extracted Text\n\nSome scanned content here."

    with patch("engine.parsers.pdf_parser.ollama_chat", return_value=mock_response):
        result = parse_pdf(str(scanned_pdf), pid, "test_parse", db)

    assert result.parser_used == "qwen2.5vl"
    assert "Extracted Text" in result.parsed_markdown


# ── Version Incrementing ─────────────────────────────────────────────


@pytest.mark.integration
def test_version_increments_on_reparse(digital_pdf, db):
    pid = _add_paper(db, pid_hint="3")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")

    # First parse
    r1 = parse_pdf(str(digital_pdf), pid, "test_parse", db)
    assert r1.version == 1

    # Modify the PDF hash check so it doesn't skip (simulate changed PDF)
    # We do this by updating the stored hash to something different
    db._conn.execute(
        "UPDATE full_text_assets SET pdf_hash = 'old_hash' WHERE paper_id = ?",
        (pid,),
    )
    db._conn.commit()

    # Second parse — should increment version
    r2 = parse_pdf(str(digital_pdf), pid, "test_parse", db)
    assert r2.version == 2


# ── Skip on Same Hash ───────────────────────────────────────────────


@pytest.mark.integration
def test_skip_if_same_hash(digital_pdf, db):
    pid = _add_paper(db, pid_hint="4")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")

    # First parse
    r1 = parse_pdf(str(digital_pdf), pid, "test_parse", db)

    # Second parse with same file — should return same version
    r2 = parse_pdf(str(digital_pdf), pid, "test_parse", db)
    assert r2.version == r1.version


# ── PyMuPDF Fallback ───────────────────────────────────────────────


def test_pymupdf_extracts_text(digital_pdf):
    md = parse_with_pymupdf(str(digital_pdf))
    assert len(md) > 50
    assert "Page 1" in md


def test_docling_success_uses_docling(digital_pdf, db):
    """Docling success path is unchanged — parser_used == 'docling'."""
    pid = _add_paper(db, pid_hint="10")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")

    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value="# Title\n\nLong enough content " * 10):
        result = parse_pdf(str(digital_pdf), pid, "test_parse", db)

    assert result.parser_used == "docling"
    row = db._conn.execute(
        "SELECT parser_used FROM full_text_assets WHERE paper_id = ?", (pid,)
    ).fetchone()
    assert row["parser_used"] == "docling"


def test_docling_exception_triggers_pymupdf(digital_pdf, db):
    """When Docling raises, PyMuPDF fallback activates and parser_used == 'pymupdf'."""
    pid = _add_paper(db, pid_hint="11")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")

    with patch("engine.parsers.pdf_parser.parse_with_docling", side_effect=RuntimeError("hyperlink validation")):
        result = parse_pdf(str(digital_pdf), pid, "test_parse", db)

    assert result.parser_used == "pymupdf"
    assert len(result.parsed_markdown) > 50
    row = db._conn.execute(
        "SELECT parser_used FROM full_text_assets WHERE paper_id = ?", (pid,)
    ).fetchone()
    assert row["parser_used"] == "pymupdf"


def test_docling_and_pymupdf_sparse_triggers_vision(scanned_pdf, db):
    """When both Docling and PyMuPDF return sparse output, vision model activates."""
    pid = _add_paper(db, pid_hint="12")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")

    mock_response = MagicMock()
    mock_response.message.content = "# OCR extracted content from scanned pages"

    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value="short"):
        with patch("engine.parsers.pdf_parser.ollama_chat", return_value=mock_response):
            result = parse_pdf(str(scanned_pdf), pid, "test_parse", db)

    assert result.parser_used == "qwen2.5vl"
    row = db._conn.execute(
        "SELECT parser_used FROM full_text_assets WHERE paper_id = ?", (pid,)
    ).fetchone()
    assert row["parser_used"] == "qwen2.5vl"


def test_docling_sparse_pymupdf_sufficient(digital_pdf, db):
    """When Docling is sparse but PyMuPDF has enough text, PyMuPDF is used."""
    pid = _add_paper(db, pid_hint="13")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")

    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value="short"):
        result = parse_pdf(str(digital_pdf), pid, "test_parse", db)

    assert result.parser_used == "pymupdf"
    assert len(result.parsed_markdown) > 50


# ── PDF Content Hash Tests ─────────────────────────────────────────


def test_hash_computed_correctly(digital_pdf):
    """Hash is a valid 64-char SHA-256 hex digest."""
    h = compute_pdf_hash(str(digital_pdf))
    assert h is not None
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)


def test_same_file_same_hash(digital_pdf):
    """Same file always produces the same hash."""
    h1 = compute_pdf_hash(str(digital_pdf))
    h2 = compute_pdf_hash(str(digital_pdf))
    assert h1 == h2


def test_different_file_different_hash(digital_pdf, scanned_pdf):
    """Different files produce different hashes."""
    h1 = compute_pdf_hash(str(digital_pdf))
    h2 = compute_pdf_hash(str(scanned_pdf))
    assert h1 != h2


def test_missing_file_returns_none(tmp_path):
    """Non-existent file returns None."""
    result = compute_pdf_hash(str(tmp_path / "does_not_exist.pdf"))
    assert result is None


def test_hash_stored_in_papers_table(digital_pdf, db):
    """After parsing, pdf_content_hash is stored on the papers row."""
    pid = _add_paper(db, pid_hint="20")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")

    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value="# Title\n\nLong enough content " * 10):
        parse_pdf(str(digital_pdf), pid, "test_parse", db)

    row = db._conn.execute(
        "SELECT pdf_content_hash FROM papers WHERE id = ?", (pid,)
    ).fetchone()
    assert row["pdf_content_hash"] is not None
    assert row["pdf_content_hash"] == compute_pdf_hash(str(digital_pdf))


def test_verify_hashes_no_mismatch(digital_pdf, db):
    """verify_hashes returns empty list when hashes match."""
    pid = _add_paper(db, pid_hint="21")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")

    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value="# Title\n\nLong enough content " * 10):
        parse_pdf(str(digital_pdf), pid, "test_parse", db)

    mismatches = verify_hashes(db)
    assert mismatches == []


def test_verify_hashes_reports_mismatch(digital_pdf, db, tmp_path):
    """verify_hashes detects when PDF content changes after parsing."""
    pid = _add_paper(db, pid_hint="22")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")

    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value="# Title\n\nLong enough content " * 10):
        parse_pdf(str(digital_pdf), pid, "test_parse", db)

    # Modify the PDF file to change its hash
    with open(str(digital_pdf), "ab") as f:
        f.write(b"\n%% modified content to change hash")

    mismatches = verify_hashes(db)
    assert len(mismatches) == 1
    assert mismatches[0]["paper_id"] == pid
    assert mismatches[0]["stored_hash"] != mismatches[0]["current_hash"]
