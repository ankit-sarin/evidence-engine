"""PARSE-GATE-06a — docling crash retry on a link-stripped copy; exceptions as rows.

The defect this closes: a Docling `ConversionError` was swallowed by a bare
`except` and answered with PyMuPDF, whose naive extraction turned p455 into
8,394 units of 7.1 chars — a SHATTERED verdict caused by the *fallback*, not by
the document. PARSE-GATE-05 measured that the same PDF, with its link
annotations stripped, parses to 52,403 chars and PASSES.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import patch

import fitz
import pytest
from fpdf import FPDF

from engine.core.database import ReviewDatabase
from engine.parsers.pdf_parser import (
    error_reason,
    parse_all_pdfs,
    parse_pdf,
    strip_links_to_temp,
)
from engine.search.models import Citation

CLEAN = (
    "The robotic assistant was evaluated in a porcine model over twelve "
    "procedures. Task completion time fell by nineteen percent against the "
    "manual baseline. No adverse events were recorded during the study. "
) * 12

REPO = Path(__file__).resolve().parents[1]
P455_PDF = REPO / "data" / "surgical_autonomy" / "pdfs" / "EE-303_Bauzano_2013.pdf"


def _boom(msg="1 validation error for PdfHyperlink\nuri\n  Input should be a valid URL"):
    return RuntimeError(msg)


@pytest.fixture()
def linked_pdf(tmp_path):
    """A digital PDF carrying a scheme-less URI link annotation."""
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=12)
    pdf.multi_cell(w=0, text="Autonomous robotic suturing evaluation. " * 40)
    base = tmp_path / "base.pdf"
    pdf.output(str(base))

    d = fitz.open(base)
    d[0].insert_link({"kind": fitz.LINK_URI, "from": fitz.Rect(50, 50, 200, 70),
                      "uri": "dx.doi.org/10.1016/j.cmpb.2013.01.017"})
    out = tmp_path / "linked.pdf"
    d.save(str(out), garbage=3, deflate=True)
    d.close()
    return out


@pytest.fixture()
def db(tmp_path):
    rdb = ReviewDatabase("test_sanitized", data_root=tmp_path)
    yield rdb
    rdb.close()


def _paper(db, hint="1") -> int:
    db.add_papers([Citation(title=f"Paper {hint}", source="pubmed", pmid=hint)])
    pid = db.get_papers_by_status("INGESTED")[-1]["id"]
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    return pid


def _rows(db, pid):
    return db._conn.execute(
        "SELECT * FROM parse_attempts WHERE paper_id = ? ORDER BY attempt_index",
        (pid,)).fetchall()


# ── T1 — crash, then the sanitized retry succeeds ─────────────────────

def test_docling_crash_is_recovered_by_the_sanitized_retry(linked_pdf, db):
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling",
               side_effect=[_boom(), CLEAN]) as doc, \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf") as pymupdf, \
         patch("engine.parsers.pdf_parser.parse_with_vision") as vision:
        result = parse_pdf(str(linked_pdf), pid, "test_sanitized", db)

    assert doc.call_count == 2
    pymupdf.assert_not_called()          # the crash no longer degrades to PyMuPDF
    vision.assert_not_called()

    rows = _rows(db, pid)
    assert [r["parser_used"] for r in rows] == ["docling", "docling_sanitized"]
    assert rows[0]["skipped_reason"].startswith("error: RuntimeError: ")
    assert "PdfHyperlink" in rows[0]["skipped_reason"]
    assert rows[0]["passed"] == 0 and rows[0]["accepted"] == 0
    assert rows[1]["skipped_reason"] is None
    assert rows[1]["passed"] == 1 and rows[1]["accepted"] == 1
    assert result.accepted_parser == "docling_sanitized"


def test_sanitized_success_reaches_parsed_status(linked_pdf, db, tmp_path):
    pid = _paper(db)
    pdfs = Path(db.db_path).parent / "pdfs"
    pdfs.mkdir(parents=True, exist_ok=True)
    (pdfs / f"{pid}.pdf").write_bytes(linked_pdf.read_bytes())

    with patch("engine.parsers.pdf_parser.parse_with_docling",
               side_effect=[_boom(), CLEAN]):
        parse_all_pdfs(db, "test_sanitized")

    assert db._conn.execute(
        "SELECT status FROM papers WHERE id=?", (pid,)).fetchone()[0] == "PARSED"
    assert db._conn.execute(
        "SELECT parser_used FROM full_text_assets WHERE paper_id=?", (pid,)
    ).fetchone()[0] == "docling_sanitized"


# ── T2 — both docling attempts fail → PyMuPDF, as before ──────────────

def test_both_docling_attempts_fail_then_pymupdf(linked_pdf, db):
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling",
               side_effect=[_boom(), _boom("second failure")]), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value=CLEAN):
        result = parse_pdf(str(linked_pdf), pid, "test_sanitized", db)

    rows = _rows(db, pid)
    assert [r["parser_used"] for r in rows] == ["docling", "docling_sanitized", "pymupdf"]
    assert rows[0]["skipped_reason"].startswith("error: RuntimeError")
    assert rows[1]["skipped_reason"].startswith("error: RuntimeError")
    assert "second failure" in rows[1]["skipped_reason"]
    assert rows[2]["skipped_reason"] is None and rows[2]["accepted"] == 1
    assert result.accepted_parser == "pymupdf"


# ── T3 — the temp copy: correctness and cleanup ───────────────────────

def test_strip_links_produces_a_linkless_copy_with_identical_text(linked_pdf):
    tmp, why = strip_links_to_temp(str(linked_pdf))
    assert why is None and tmp is not None
    try:
        src, cpy = fitz.open(linked_pdf), fitz.open(tmp)
        try:
            assert sum(len(p.get_links()) for p in src) > 0
            assert sum(len(p.get_links()) for p in cpy) == 0
            assert [p.get_text("text") for p in src] == [p.get_text("text") for p in cpy]
        finally:
            src.close(); cpy.close()
    finally:
        Path(tmp).unlink(missing_ok=True)


def test_temp_copy_is_removed_after_a_successful_retry(linked_pdf, db):
    pid = _paper(db)
    seen = {}

    def capture(path):
        if "parse_sanitized_" in str(path):
            seen["tmp"] = path
            return CLEAN
        raise _boom()

    with patch("engine.parsers.pdf_parser.parse_with_docling", side_effect=capture):
        parse_pdf(str(linked_pdf), pid, "test_sanitized", db)
    assert "tmp" in seen and not Path(seen["tmp"]).exists()


def test_temp_copy_is_removed_after_a_raising_retry(linked_pdf, db):
    pid = _paper(db)
    seen = {}

    def capture(path):
        if "parse_sanitized_" in str(path):
            seen["tmp"] = path
        raise _boom()

    with patch("engine.parsers.pdf_parser.parse_with_docling", side_effect=capture), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value=CLEAN):
        parse_pdf(str(linked_pdf), pid, "test_sanitized", db)
    assert "tmp" in seen and not Path(seen["tmp"]).exists()


def test_a_pdf_with_no_links_skips_the_sanitized_retry(tmp_path, db):
    pdf = FPDF(); pdf.add_page(); pdf.set_font("Helvetica", size=12)
    pdf.multi_cell(w=0, text="Autonomous robotic suturing evaluation. " * 40)
    plain = tmp_path / "plain.pdf"; pdf.output(str(plain))

    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling", side_effect=_boom()), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value=CLEAN):
        parse_pdf(str(plain), pid, "test_sanitized", db)

    rows = _rows(db, pid)
    assert [r["parser_used"] for r in rows] == ["docling", "docling_sanitized", "pymupdf"]
    assert "no link annotations" in rows[1]["skipped_reason"]


# ── T4 — text-layer inequality refuses the copy ───────────────────────

def test_retry_is_skipped_when_stripping_would_change_the_text(linked_pdf, db):
    """A "fix" that altered the text layer is worse than the crash it works around."""
    real = fitz.Page.get_text

    def drifting(self, *a, **k):
        out = real(self, *a, **k)
        # Drift ONLY when reading the sanitized copy, so the verification sees a
        # genuine before/after difference. Targeting the temp file by name keeps
        # is_scanned_pdf's own get_text call out of it.
        name = getattr(getattr(self, "parent", None), "name", "") or ""
        return out + "DRIFT" if "parse_sanitized_" in name else out

    pid = _paper(db)
    with patch.object(fitz.Page, "get_text", drifting), \
         patch("engine.parsers.pdf_parser.parse_with_docling", side_effect=_boom()), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value=CLEAN):
        parse_pdf(str(linked_pdf), pid, "test_sanitized", db)

    rows = _rows(db, pid)
    assert rows[1]["parser_used"] == "docling_sanitized"
    assert "changed the text layer" in rows[1]["skipped_reason"]
    assert rows[2]["parser_used"] == "pymupdf" and rows[2]["accepted"] == 1


# ── T5 — every raising parser produces a row; message truncated ───────

def test_error_reason_truncates_at_300_chars():
    r = error_reason(ValueError("x" * 5000))
    assert r.startswith("error: ValueError: ")
    assert len(r) - len("error: ValueError: ") == 300


def test_a_raising_pymupdf_is_recorded_then_reraised(linked_pdf, db):
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling", side_effect=_boom()), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf",
               side_effect=OSError("disk gone")):
        with pytest.raises(OSError, match="disk gone"):
            parse_pdf(str(linked_pdf), pid, "test_sanitized", db)
    # The write never happened, so no rows are committed -- but nothing is stored
    # silently either: the exception reaches the caller.
    assert db._conn.execute(
        "SELECT COUNT(*) FROM full_text_assets WHERE paper_id=?", (pid,)).fetchone()[0] == 0


def test_a_raising_vision_reroute_is_recorded_and_ends_the_loop(linked_pdf, db):
    """Gate-driven re-route that raises: recorded, loop ends, least-bad accepted."""
    shattered = "\n".join(list("computerassistedsurgicalnavigation" * 40))
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=shattered), \
         patch("engine.parsers.pdf_parser.parse_with_vision",
               side_effect=RuntimeError("ollama down")):
        parse_pdf(str(linked_pdf), pid, "test_sanitized", db)

    rows = _rows(db, pid)
    assert [r["parser_used"] for r in rows] == ["docling", "qwen2.5vl"]
    assert rows[1]["skipped_reason"].startswith("error: RuntimeError: ollama down")
    assert rows[0]["accepted"] == 1          # least-bad, since vision never returned


# ── T6 — attempt cap ──────────────────────────────────────────────────

def test_attempt_cap_is_four():
    from engine.parsers.pdf_parser import _MAX_ATTEMPTS
    assert _MAX_ATTEMPTS == 4


# ── T7 — the real crash, on the real document (integration) ───────────

@pytest.mark.integration
@pytest.mark.skipif(not P455_PDF.exists(), reason="corpus PDF not present")
def test_real_docling_crash_on_p455_is_recovered_by_the_sanitized_retry(db):
    """The measured case. Reads the corpus PDF; writes only the fixture DB.

    A synthetic PDF carrying a scheme-less URI link does NOT reproduce the crash
    (PARSE-GATE-06a I1) -- docling accepts it -- so the only faithful reproduction
    is the document that actually fails.
    """
    from engine.parsers.parse_quality import assess

    pid = _paper(db)
    result = parse_pdf(str(P455_PDF), pid, "test_sanitized", db)

    rows = _rows(db, pid)
    assert rows[0]["parser_used"] == "docling"
    assert "PdfHyperlink" in rows[0]["skipped_reason"]
    assert rows[1]["parser_used"] == "docling_sanitized"
    assert rows[1]["skipped_reason"] is None
    assert result.accepted_parser == "docling_sanitized"
    assert assess(result.parsed_markdown).passed is True
    assert len(result.parsed_markdown) > 45_000
