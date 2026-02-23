"""Tests for PDF parser with Docling and MiniCPM-V routing."""

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
    parse_pdf,
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
    db.update_status(pid, "SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")

    result = parse_pdf(str(digital_pdf), pid, "test_parse", db)
    assert result.parser_used == "docling"
    assert result.version == 1
    assert len(result.parsed_markdown) > 0


def test_scanned_routes_to_minicpm(scanned_pdf, db):
    pid = _add_paper(db, pid_hint="2")
    db.update_status(pid, "SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")

    mock_response = MagicMock()
    mock_response.message.content = "# Extracted Text\n\nSome scanned content here."

    with patch("engine.parsers.pdf_parser.ollama.chat", return_value=mock_response):
        result = parse_pdf(str(scanned_pdf), pid, "test_parse", db)

    assert result.parser_used == "minicpm-v"
    assert "Extracted Text" in result.parsed_markdown


# ── Version Incrementing ─────────────────────────────────────────────


@pytest.mark.integration
def test_version_increments_on_reparse(digital_pdf, db):
    pid = _add_paper(db, pid_hint="3")
    db.update_status(pid, "SCREENED_IN")
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
    db.update_status(pid, "SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")

    # First parse
    r1 = parse_pdf(str(digital_pdf), pid, "test_parse", db)

    # Second parse with same file — should return same version
    r2 = parse_pdf(str(digital_pdf), pid, "test_parse", db)
    assert r2.version == r1.version
