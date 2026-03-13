"""Tests for PDF verify/import command — matching, validation, rename, DB update."""

import json
from pathlib import Path

import pytest

from engine.acquisition.verify_downloads import (
    _clean_author_name,
    _first_author_last_name,
    _match_file_to_paper,
    _validate_pdf,
    canonical_filename,
    verify_downloads,
)
from engine.acquisition.manual_list import classify_publisher
from engine.core.database import ReviewDatabase
from engine.search.models import Citation


# ── Fixtures ──────────────────────────────────────────────────────────


def _make_citation(paper_id_suffix, *, title="Test Paper", authors=None,
                   year=2024, doi=None, pmid=None):
    return Citation(
        title=f"{title} {paper_id_suffix}",
        abstract="Abstract text",
        pmid=pmid or str(10000 + paper_id_suffix),
        doi=doi,
        source="pubmed",
        authors=authors or ["Smith J", "Doe A"],
        journal="J Test",
        year=year,
    )


def _setup_review_db(tmp_path, review_name="test_verify"):
    """Create a ReviewDatabase with a few papers for testing."""
    db = ReviewDatabase(review_name, data_root=tmp_path)
    return db


def _add_included_paper(db, idx, *, ee_id=None, authors=None, year=2024,
                        doi=None, title="Test Paper"):
    """Add a paper and advance to ABSTRACT_SCREENED_IN. Returns paper_id."""
    cit = _make_citation(idx, title=title, authors=authors, year=year, doi=doi)
    db.add_papers([cit])
    paper = db._conn.execute(
        "SELECT id FROM papers ORDER BY id DESC LIMIT 1"
    ).fetchone()
    pid = paper["id"]
    db.update_status(pid, "ABSTRACT_SCREENED_IN")

    if ee_id:
        db._conn.execute(
            "UPDATE papers SET ee_identifier = ? WHERE id = ?",
            (ee_id, pid),
        )
        db._conn.commit()

    return pid


def _create_valid_pdf(path: Path, size: int = 20000):
    """Create a minimal valid PDF file."""
    # Real PDF header + padding to reach target size
    content = b"%PDF-1.4\n" + b"0" * (size - 9)
    path.write_bytes(content)


def _create_html_error_page(path: Path):
    """Create an HTML file masquerading as PDF."""
    path.write_text("<!DOCTYPE html><html><body>403 Forbidden</body></html>")


def _create_empty_file(path: Path):
    """Create a 0-byte file."""
    path.write_bytes(b"")


def _create_small_file(path: Path):
    """Create a too-small file with PDF header."""
    path.write_bytes(b"%PDF" + b"0" * 100)


# ── Author Name Cleaning Tests ────────────────────────────────────


class TestAuthorCleaning:

    def test_simple_name(self):
        assert _clean_author_name("Smith") == "Smith"

    def test_full_name_extracts_last(self):
        # "John Smith" — last word is > 2 chars, so it's treated as last name
        assert _clean_author_name("John Smith") == "Smith"

    def test_lastname_initial_format(self):
        # "Lukas A" — last word is 1 char (initial), so first word is last name
        assert _clean_author_name("Lukas A") == "Lukas"

    def test_accented_name(self):
        result = _clean_author_name("Müller")
        assert result == "Muller"

    def test_hyphenated_name(self):
        assert _clean_author_name("Garcia-Lopez") == "Garcia-Lopez"

    def test_empty_name(self):
        assert _clean_author_name("") == "Unknown"

    def test_none_name(self):
        assert _clean_author_name(None) == "Unknown"

    def test_special_chars(self):
        result = _clean_author_name("O'Brien")
        assert "Brien" in result

    def test_first_author_from_json(self):
        authors = json.dumps(["Lukas A", "Chen B"])
        assert _first_author_last_name(authors) == "Lukas"

    def test_first_author_none(self):
        assert _first_author_last_name(None) == "Unknown"

    def test_first_author_empty_list(self):
        assert _first_author_last_name("[]") == "Unknown"


# ── Canonical Filename Tests ─────────────────────────────────────


class TestCanonicalFilename:

    def test_standard(self):
        result = canonical_filename("EE-047", '["Lukas A", "Chen B"]', 2020)
        assert result == "EE-047_Lukas_2020.pdf"

    def test_no_ee_id(self):
        result = canonical_filename(None, '["Smith J"]', 2023)
        assert result == "EE-000_Smith_2023.pdf"  # "Smith J" → last word "J" is 1 char → use "Smith"

    def test_no_year(self):
        result = canonical_filename("EE-001", '["Doe A"]', None)
        assert result == "EE-001_Doe_XXXX.pdf"  # "Doe A" → "A" is 1 char → use "Doe"

    def test_no_authors(self):
        result = canonical_filename("EE-005", None, 2021)
        assert result == "EE-005_Unknown_2021.pdf"


# ── File Matching Tests ──────────────────────────────────────────


class TestFileMatching:

    def setup_method(self):
        self.paper_index = {
            47: {"id": 47, "ee_identifier": "EE-047"},
            605: {"id": 605, "ee_identifier": "EE-605"},
            100: {"id": 100, "ee_identifier": "EE-100"},
        }
        self.ee_index = {
            "EE-047": 47,
            "EE-605": 605,
            "EE-100": 100,
        }

    def test_bare_integer(self):
        pid, match_type = _match_file_to_paper("47.pdf", self.paper_index, self.ee_index)
        assert pid == 47
        assert match_type == "bare_integer"

    def test_ee_prefixed(self):
        pid, match_type = _match_file_to_paper("EE-047.pdf", self.paper_index, self.ee_index)
        assert pid == 47
        assert match_type == "ee_prefixed"

    def test_rich_name(self):
        pid, match_type = _match_file_to_paper(
            "EE-047_Lukas_2020.pdf", self.paper_index, self.ee_index
        )
        assert pid == 47
        assert match_type == "ee_prefixed"

    def test_unmatched_integer(self):
        pid, match_type = _match_file_to_paper("999.pdf", self.paper_index, self.ee_index)
        assert pid is None
        assert match_type == "unmatched"

    def test_unmatched_ee(self):
        pid, match_type = _match_file_to_paper("EE-999.pdf", self.paper_index, self.ee_index)
        assert pid is None
        assert match_type == "unmatched"

    def test_random_filename(self):
        pid, match_type = _match_file_to_paper("notes.pdf", self.paper_index, self.ee_index)
        assert pid is None
        assert match_type == "unmatched"


# ── PDF Validation Tests ────────────────────────────────────────


class TestPDFValidation:

    def test_valid_pdf(self, tmp_path):
        path = tmp_path / "good.pdf"
        _create_valid_pdf(path)
        is_valid, reason = _validate_pdf(path)
        assert is_valid is True
        assert reason == "valid"

    def test_html_error_page(self, tmp_path):
        path = tmp_path / "bad.pdf"
        _create_html_error_page(path)
        is_valid, reason = _validate_pdf(path)
        assert is_valid is False
        assert "HTML" in reason

    def test_empty_file(self, tmp_path):
        path = tmp_path / "empty.pdf"
        _create_empty_file(path)
        is_valid, reason = _validate_pdf(path)
        assert is_valid is False
        assert "empty" in reason or "0 bytes" in reason

    def test_too_small(self, tmp_path):
        path = tmp_path / "small.pdf"
        _create_small_file(path)
        is_valid, reason = _validate_pdf(path)
        assert is_valid is False
        assert "small" in reason.lower() or "minimum" in reason.lower()

    def test_missing_file(self, tmp_path):
        path = tmp_path / "missing.pdf"
        is_valid, reason = _validate_pdf(path)
        assert is_valid is False
        assert "not found" in reason


# ── Publisher Classification Tests ──────────────────────────────


class TestPublisherClassification:

    def test_ieee(self):
        assert classify_publisher("10.1109/TRO.2024.001") == "IEEE"

    def test_elsevier(self):
        assert classify_publisher("10.1016/j.robot.2024.001") == "Elsevier"

    def test_springer(self):
        assert classify_publisher("10.1007/s00464-024-001") == "Springer/Nature"

    def test_no_doi(self):
        assert classify_publisher(None) == "Unknown (no DOI)"

    def test_unknown_prefix(self):
        assert classify_publisher("10.9999/obscure.2024") == "Other"


# ── Integration: verify_downloads Round-Trip ────────────────────


class TestVerifyDownloadsIntegration:

    def test_bare_integer_match_and_rename(self, tmp_path):
        """Bare integer file matched, validated, renamed to canonical, DB updated."""
        db = _setup_review_db(tmp_path)
        pid = _add_included_paper(db, 1, ee_id="EE-001",
                                  authors=["Lukas A"], year=2020)

        pdf_dir = tmp_path / "test_verify" / "pdfs"
        pdf_dir.mkdir(parents=True, exist_ok=True)
        _create_valid_pdf(pdf_dir / f"{pid}.pdf")

        db.close()

        result = verify_downloads("test_verify", pdf_dir=pdf_dir, data_root=tmp_path)

        assert result["valid"] == 1
        assert result["renamed"] == 1
        assert result["invalid"] == 0
        assert result["unmatched"] == 0

        # Canonical file should exist
        assert (pdf_dir / "EE-001_Lukas_2020.pdf").exists()
        # Original should be gone
        assert not (pdf_dir / f"{pid}.pdf").exists()

        # DB should be updated
        db2 = ReviewDatabase("test_verify", data_root=tmp_path)
        row = db2._conn.execute(
            "SELECT download_status, pdf_local_path FROM papers WHERE id = ?",
            (pid,),
        ).fetchone()
        assert row["download_status"] == "success"
        assert "EE-001_Lukas_2020.pdf" in row["pdf_local_path"]
        db2.close()

    def test_ee_prefixed_match(self, tmp_path):
        """EE-prefixed file matched and renamed."""
        db = _setup_review_db(tmp_path)
        pid = _add_included_paper(db, 2, ee_id="EE-002",
                                  authors=["Chen B"], year=2021)

        pdf_dir = tmp_path / "test_verify" / "pdfs"
        pdf_dir.mkdir(parents=True, exist_ok=True)
        _create_valid_pdf(pdf_dir / "EE-002.pdf")

        db.close()

        result = verify_downloads("test_verify", pdf_dir=pdf_dir, data_root=tmp_path)

        assert result["valid"] == 1
        assert (pdf_dir / "EE-002_Chen_2021.pdf").exists()

    def test_already_canonical_name(self, tmp_path):
        """File already in canonical name is not renamed."""
        db = _setup_review_db(tmp_path)
        pid = _add_included_paper(db, 3, ee_id="EE-003",
                                  authors=["Kim C"], year=2022)

        pdf_dir = tmp_path / "test_verify" / "pdfs"
        pdf_dir.mkdir(parents=True, exist_ok=True)
        _create_valid_pdf(pdf_dir / "EE-003_Kim_2022.pdf")

        db.close()

        result = verify_downloads("test_verify", pdf_dir=pdf_dir, data_root=tmp_path)

        assert result["valid"] == 1
        assert result["already_canonical"] == 1
        assert result["renamed"] == 0

    def test_invalid_html_detected(self, tmp_path):
        """HTML error page detected and reported."""
        db = _setup_review_db(tmp_path)
        pid = _add_included_paper(db, 4, ee_id="EE-004",
                                  authors=["Doe D"], year=2023)

        pdf_dir = tmp_path / "test_verify" / "pdfs"
        pdf_dir.mkdir(parents=True, exist_ok=True)
        _create_html_error_page(pdf_dir / f"{pid}.pdf")

        db.close()

        result = verify_downloads("test_verify", pdf_dir=pdf_dir, data_root=tmp_path)

        assert result["valid"] == 0
        assert result["invalid"] == 1
        assert "HTML" in result["invalid_files"][0][2]

    def test_unmatched_file_reported(self, tmp_path):
        """Files with no matching paper record are reported."""
        db = _setup_review_db(tmp_path)
        _add_included_paper(db, 5, ee_id="EE-005")

        pdf_dir = tmp_path / "test_verify" / "pdfs"
        pdf_dir.mkdir(parents=True, exist_ok=True)
        _create_valid_pdf(pdf_dir / "9999.pdf")  # no paper_id 9999

        db.close()

        result = verify_downloads("test_verify", pdf_dir=pdf_dir, data_root=tmp_path)

        assert result["unmatched"] == 1
        assert "9999.pdf" in result["unmatched_files"]

    def test_dry_run_no_changes(self, tmp_path):
        """Dry run produces report but no filesystem or DB changes."""
        db = _setup_review_db(tmp_path)
        pid = _add_included_paper(db, 6, ee_id="EE-006",
                                  authors=["Park E"], year=2024)

        pdf_dir = tmp_path / "test_verify" / "pdfs"
        pdf_dir.mkdir(parents=True, exist_ok=True)
        _create_valid_pdf(pdf_dir / f"{pid}.pdf")

        db.close()

        result = verify_downloads("test_verify", pdf_dir=pdf_dir, dry_run=True, data_root=tmp_path)

        assert result["valid"] == 1
        assert result["renamed"] == 1

        # Original file should still exist (not renamed)
        assert (pdf_dir / f"{pid}.pdf").exists()
        # Canonical file should NOT exist
        assert not (pdf_dir / "EE-006_Park_2024.pdf").exists()

        # DB should NOT be updated
        db2 = ReviewDatabase("test_verify", data_root=tmp_path)
        row = db2._conn.execute(
            "SELECT download_status FROM papers WHERE id = ?", (pid,),
        ).fetchone()
        assert row["download_status"] != "success"
        db2.close()

    def test_mixed_files_integration(self, tmp_path):
        """Integration: 3 valid PDFs (mixed naming) + 1 invalid → correct report."""
        db = _setup_review_db(tmp_path)

        # Paper 1: bare integer
        pid1 = _add_included_paper(db, 1, ee_id="EE-001",
                                   authors=["Alpha A"], year=2020)
        # Paper 2: EE-prefixed
        pid2 = _add_included_paper(db, 2, ee_id="EE-002",
                                   authors=["Beta B"], year=2021)
        # Paper 3: rich name (already canonical)
        pid3 = _add_included_paper(db, 3, ee_id="EE-003",
                                   authors=["Gamma C"], year=2022)
        # Paper 4: invalid HTML
        pid4 = _add_included_paper(db, 4, ee_id="EE-004",
                                   authors=["Delta D"], year=2023)

        pdf_dir = tmp_path / "test_verify" / "pdfs"
        pdf_dir.mkdir(parents=True, exist_ok=True)

        _create_valid_pdf(pdf_dir / f"{pid1}.pdf")           # bare integer
        _create_valid_pdf(pdf_dir / "EE-002.pdf")            # EE-prefixed
        _create_valid_pdf(pdf_dir / "EE-003_Gamma_2022.pdf") # canonical
        _create_html_error_page(pdf_dir / f"{pid4}.pdf")     # invalid

        db.close()

        result = verify_downloads("test_verify", pdf_dir=pdf_dir, data_root=tmp_path)

        assert result["valid"] == 3
        assert result["renamed"] == 2  # pid1 and pid2 renamed
        assert result["already_canonical"] == 1  # pid3
        assert result["invalid"] == 1  # pid4
        assert result["still_missing"] == 1  # pid4 (invalid doesn't count)

        # Verify renames happened
        assert (pdf_dir / "EE-001_Alpha_2020.pdf").exists()
        assert (pdf_dir / "EE-002_Beta_2021.pdf").exists()
        assert (pdf_dir / "EE-003_Gamma_2022.pdf").exists()

        # Verify DB updated for valid files
        db2 = ReviewDatabase("test_verify", data_root=tmp_path)
        for pid in [pid1, pid2, pid3]:
            row = db2._conn.execute(
                "SELECT download_status FROM papers WHERE id = ?", (pid,),
            ).fetchone()
            assert row["download_status"] == "success"

        # Invalid file NOT marked as success
        row4 = db2._conn.execute(
            "SELECT download_status FROM papers WHERE id = ?", (pid4,),
        ).fetchone()
        assert row4["download_status"] != "success"
        db2.close()

    def test_zero_byte_file_invalid(self, tmp_path):
        """0-byte file is flagged as invalid."""
        db = _setup_review_db(tmp_path)
        pid = _add_included_paper(db, 7, ee_id="EE-007")

        pdf_dir = tmp_path / "test_verify" / "pdfs"
        pdf_dir.mkdir(parents=True, exist_ok=True)
        _create_empty_file(pdf_dir / f"{pid}.pdf")

        db.close()

        result = verify_downloads("test_verify", pdf_dir=pdf_dir, data_root=tmp_path)
        assert result["invalid"] == 1

    def test_full_text_assets_updated(self, tmp_path):
        """verify_downloads should update full_text_assets.pdf_path."""
        db = _setup_review_db(tmp_path)
        pid = _add_included_paper(db, 8, ee_id="EE-008",
                                  authors=["Wang F"], year=2024)

        pdf_dir = tmp_path / "test_verify" / "pdfs"
        pdf_dir.mkdir(parents=True, exist_ok=True)
        _create_valid_pdf(pdf_dir / f"{pid}.pdf")

        db.close()

        verify_downloads("test_verify", pdf_dir=pdf_dir, data_root=tmp_path)

        db2 = ReviewDatabase("test_verify", data_root=tmp_path)
        ft = db2._conn.execute(
            "SELECT pdf_path FROM full_text_assets WHERE paper_id = ?", (pid,),
        ).fetchone()
        assert ft is not None
        assert "EE-008_Wang_2024.pdf" in ft["pdf_path"]
        db2.close()
