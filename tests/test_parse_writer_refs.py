"""D8 / R99: parse_pdf records its parsed_text_refs row (INPUT-IDENTITY-01 Step 4).

The parsers are patched; the PDF is a real one-page file so hashing, page counts
and the quality gate run for real. No model and no Docling run here.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from unittest.mock import patch

import pytest
from fpdf import FPDF

from engine.core import parsed_text as pt
from engine.core.database import ReviewDatabase
from engine.parsers.pdf_parser import parse_pdf

TEXT_A = "# Title\n\nLong enough content about autonomous suturing. " * 10
TEXT_B = "# Title\n\nLong enough content about autonomous knot tying. " * 10


@pytest.fixture
def pdf(tmp_path) -> Path:
    doc = FPDF()
    doc.add_page()
    doc.set_font("Helvetica", size=12)
    doc.multi_cell(w=0, text="Autonomous robotic suturing evaluated on bench-top models. " * 8)
    path = tmp_path / "paper.pdf"
    doc.output(str(path))
    return path


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("writer", data_root=tmp_path)
    rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, updated_at) "
                      "VALUES (5, 't', 's', 'PDF_ACQUIRED', 'n', 'n')")
    rdb._conn.commit()
    yield rdb
    rdb.close()


def _refs(db):
    return db._conn.execute(
        "SELECT parsed_text_version, parsed_text_path, parsed_text_sha256, "
        "source_full_text_assets_id FROM parsed_text_refs WHERE paper_id = 5 "
        "ORDER BY parsed_text_version").fetchall()


def _parse(pdf, db, text, force=False):
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=text):
        return parse_pdf(str(pdf), 5, "writer", db, force=force)


def test_two_parses_record_versions_1_and_2_and_the_resolver_picks_2(pdf, db):
    r1 = _parse(pdf, db, TEXT_A)
    r2 = _parse(pdf, db, TEXT_B, force=True)
    assert (r1.version, r2.version) == (1, 2)

    refs = _refs(db)
    assert [r[0] for r in refs] == [1, 2]
    assert refs[0][2] != refs[1][2]
    ref = pt.resolve_parsed_text(db._conn, 5)
    assert ref.version == 2
    assert pt.read_parsed_text(ref) == TEXT_B


def test_the_reference_hashes_the_bytes_written_and_names_its_asset_row(pdf, db):
    r = _parse(pdf, db, TEXT_A)
    (version, stored, sha, asset_id) = _refs(db)[0]
    path = Path(db.db_path).parent / "parsed_text" / f"5_v{r.version}.md"
    assert sha == hashlib.sha256(path.read_bytes()).hexdigest()
    assert stored == pt.canonical_path(os.path.abspath(path))  # R100: absolute here, outside the repo
    assert os.path.isabs(stored)
    fta = db._conn.execute("SELECT id, parsed_text_version FROM full_text_assets "
                           "WHERE paper_id = 5").fetchall()
    assert [(a, v) for a, v in fta] == [(asset_id, version)]


def test_the_version_comes_from_the_references_not_full_text_assets(pdf, db):
    """D13: a D12-shape asset row (version 1, no path) no longer pushes the
    first real parse to version 2."""
    db._conn.execute("INSERT INTO full_text_assets (paper_id, pdf_path, parser_used) "
                     "VALUES (5, 'x.pdf', 'docling')")
    db._conn.commit()
    assert _parse(pdf, db, TEXT_A).version == 1


def test_an_existing_file_of_that_name_is_refused(pdf, db):
    target = Path(db.db_path).parent / "parsed_text" / "5_v1.md"
    target.parent.mkdir(exist_ok=True)
    target.write_text("an unrecorded file")
    with pytest.raises(FileExistsError, match="R99"):
        _parse(pdf, db, TEXT_A)
    assert _refs(db) == []
    assert target.read_text() == "an unrecorded file"


def test_a_failed_parse_writes_no_reference_and_no_asset_row(pdf, db):
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=""), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value=""), \
         patch("engine.parsers.pdf_parser.parse_with_docling_ocr", return_value=""), \
         patch("engine.parsers.pdf_parser.parse_with_vision", return_value=""):
        with pytest.raises(Exception):
            parse_pdf(str(pdf), 5, "writer", db)
    assert _refs(db) == []
    assert db._conn.execute("SELECT COUNT(*) FROM full_text_assets").fetchone()[0] == 0
