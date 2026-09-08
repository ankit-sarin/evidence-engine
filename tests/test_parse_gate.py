"""PARSE-GATE-02 — the quality-gated parse cascade, attempt ledger, re-parse.

Every parser is stubbed. Nothing here calls Docling, renders a PDF page, or
reaches Ollama; the only real PDFs are the two-page fpdf fixtures, used solely
so `is_scanned_pdf` and `page_count` have something to open.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fpdf import FPDF

from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.parsers.models import ParseAttempt
from engine.parsers.pdf_parser import (
    format_exclusion_detail,
    parse_all_pdfs,
    parse_pdf,
    reparse_papers,
    select_attempt,
)
from engine.parsers.parse_quality import GLYPH_DENSITY, SHATTERED
from engine.search.models import Citation

SPEC = "review_specs/surgical_autonomy_v1.yaml"

# ── synthetic parser outputs, each engineered to a known verdict ──────

CLEAN = (
    "The robotic assistant was evaluated in a porcine model over twelve "
    "procedures. Task completion time fell by nineteen percent against the "
    "manual baseline. No adverse events were recorded during the study. "
) * 12

GLYPHY = CLEAN + " " + " ".join(["GLYPH<c=1,font=/AOIIJB+MinionPro>"] * 90)

# Character-shattered: p455's shape, long enough to clear the sparse threshold.
SHATTERED_TEXT = "\n".join(list("computerassistedsurgicalnavigation" * 40))


@pytest.fixture()
def digital_pdf(tmp_path):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=12)
    pdf.multi_cell(w=0, text="Autonomous robotic suturing evaluation. " * 40)
    path = tmp_path / "digital.pdf"
    pdf.output(str(path))
    return path


@pytest.fixture()
def big_pdf(tmp_path):
    """A PDF with more pages than vision_max_pages (default 60)."""
    pdf = FPDF()
    for _ in range(61):
        pdf.add_page()
        pdf.set_font("Helvetica", size=12)
        pdf.multi_cell(w=0, text="Autonomous robotic suturing evaluation. " * 20)
    path = tmp_path / "big.pdf"
    pdf.output(str(path))
    return path


@pytest.fixture()
def db(tmp_path):
    rdb = ReviewDatabase("test_gate", data_root=tmp_path)
    yield rdb
    rdb.close()


def _paper(db, hint="1") -> int:
    db.add_papers([Citation(title=f"Paper {hint}", source="pubmed", pmid=hint)])
    pid = db.get_papers_by_status("INGESTED")[-1]["id"]
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    return pid


def _review_dir(db) -> Path:
    return Path(db.db_path).parent


def _staged(db, pid, src: Path) -> Path:
    """Copy a PDF where parse_all_pdfs / reparse_papers resolve it."""
    pdfs = _review_dir(db) / "pdfs"
    pdfs.mkdir(parents=True, exist_ok=True)
    dest = pdfs / f"{pid}.pdf"
    dest.write_bytes(src.read_bytes())
    return dest


def _attempts(db, pid) -> list[sqlite3.Row]:
    return db._conn.execute(
        "SELECT * FROM parse_attempts WHERE paper_id = ? ORDER BY attempt_index",
        (pid,),
    ).fetchall()


def _ran(db, pid) -> list[sqlite3.Row]:
    """Attempts that produced text. Excludes error and skip rows, which carry
    `skipped_reason` and are never selectable."""
    return [r for r in _attempts(db, pid) if r["skipped_reason"] is None]


def _vision(text: str):
    m = MagicMock()
    m.message.content = text
    return m


# ── T1 — one attempt, passes ──────────────────────────────────────────

def test_clean_docling_parse_is_one_accepted_attempt(digital_pdf, db):
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=CLEAN), \
         patch("engine.parsers.pdf_parser.parse_with_vision") as vision:
        result = parse_pdf(str(digital_pdf), pid, "test_gate", db)

    vision.assert_not_called()
    assert result.accepted_parser == "docling"
    assert result.quality_passed is True
    rows = _attempts(db, pid)
    assert len(rows) == 1
    assert rows[0]["parser_used"] == "docling"
    assert rows[0]["passed"] == 1 and rows[0]["accepted"] == 1
    assert json.loads(rows[0]["failures"]) == []
    assert json.loads(rows[0]["metrics"])["chars"] == len(CLEAN)
    assert rows[0]["elapsed_s"] >= 0.0


def test_clean_parse_reaches_parsed_status(digital_pdf, db):
    pid = _paper(db)
    _staged(db, pid, digital_pdf)
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=CLEAN):
        parse_all_pdfs(db, "test_gate")
    assert db._conn.execute(
        "SELECT status FROM papers WHERE id = ?", (pid,)).fetchone()[0] == "PARSED"


# ── T2 — glyph failure re-routes to PyMuPDF, which passes ─────────────

def test_glyph_failure_reroutes_to_pymupdf_and_stops(digital_pdf, db):
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=GLYPHY), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value=CLEAN), \
         patch("engine.parsers.pdf_parser.parse_with_vision") as vision:
        result = parse_pdf(str(digital_pdf), pid, "test_gate", db)

    vision.assert_not_called()          # docling #3081: another extractor first
    assert result.accepted_parser == "pymupdf"
    rows = _attempts(db, pid)
    assert [r["parser_used"] for r in rows] == ["docling", "pymupdf"]
    assert rows[0]["passed"] == 0 and rows[0]["accepted"] == 0
    assert rows[1]["passed"] == 1 and rows[1]["accepted"] == 1
    assert json.loads(rows[0]["failures"])[0][0] == GLYPH_DENSITY


def test_rerouted_pass_still_reaches_parsed(digital_pdf, db):
    pid = _paper(db)
    _staged(db, pid, digital_pdf)
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=GLYPHY), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value=CLEAN):
        parse_all_pdfs(db, "test_gate")
    assert db._conn.execute(
        "SELECT status FROM papers WHERE id = ?", (pid,)).fetchone()[0] == "PARSED"


# ── T3 — three attempts, vision rescues ───────────────────────────────

def test_glyph_then_shattered_then_vision_passes(digital_pdf, db):
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=GLYPHY), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value=SHATTERED_TEXT), \
         patch("engine.parsers.pdf_parser.parse_with_vision", return_value=CLEAN):
        result = parse_pdf(str(digital_pdf), pid, "test_gate", db)

    rows = _attempts(db, pid)
    assert [r["parser_used"] for r in rows] == ["docling", "pymupdf", "qwen2.5vl"]
    assert [r["accepted"] for r in rows] == [0, 0, 1]
    assert result.accepted_parser == "qwen2.5vl"
    assert json.loads(rows[1]["failures"])[0][0] == SHATTERED


# ── T4 — PyMuPDF-first path: SHATTERED goes straight to vision ────────

def test_pymupdf_first_shattered_goes_to_vision_not_back_to_pymupdf(digital_pdf, db):
    """Docling raises, so PyMuPDF is attempt 1. SHATTERED must not retry it."""
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling",
               side_effect=RuntimeError("hyperlink validation")), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf",
               return_value=SHATTERED_TEXT) as pymupdf, \
         patch("engine.parsers.pdf_parser.parse_with_vision", return_value=CLEAN):
        result = parse_pdf(str(digital_pdf), pid, "test_gate", db)

    assert pymupdf.call_count == 1                  # tried once, never retried
    # PARSE-GATE-06a: the docling raise and the skipped sanitized retry are now
    # recorded in front of these. The parsers that PRODUCED TEXT are unchanged.
    assert [r["parser_used"] for r in _ran(db, pid)] == ["pymupdf", "qwen2.5vl"]
    assert result.accepted_parser == "qwen2.5vl"


# ── T5 — nothing passes: least-bad selection, then PDF_EXCLUDED ───────

def test_select_attempt_prefers_fewest_failures():
    two = ParseAttempt(attempt_index=1, parser_used="docling", failures=[
        (GLYPH_DENSITY, 10.0, 5.0), ("REPLACEMENT_DENSITY", 4.0, 1.0)])
    one = ParseAttempt(attempt_index=2, parser_used="pymupdf", failures=[
        (GLYPH_DENSITY, 500.0, 5.0)])
    assert select_attempt([two, one]) is one


def test_select_attempt_tie_breaks_on_summed_ratio():
    worse = ParseAttempt(attempt_index=1, parser_used="docling",
                         failures=[(GLYPH_DENSITY, 50.0, 5.0)])     # ratio 10
    better = ParseAttempt(attempt_index=2, parser_used="pymupdf",
                          failures=[(GLYPH_DENSITY, 10.0, 5.0)])    # ratio 2
    assert select_attempt([worse, better]) is better
    assert select_attempt([better, worse]) is better                # order-free


def test_shattered_tie_break_uses_the_share_half_only():
    """chars_per_unit shrinks with damage, so mixing it in would invert the order."""
    worse = ParseAttempt(attempt_index=1, parser_used="pymupdf",
                         failures=[(SHATTERED, (99.0, 2.0), (50.0, 20.0))])
    better = ParseAttempt(attempt_index=2, parser_used="docling",
                          failures=[(SHATTERED, (55.0, 19.0), (50.0, 20.0))])
    assert select_attempt([worse, better]) is better


def test_a_passing_attempt_always_beats_a_least_bad_one():
    bad = ParseAttempt(attempt_index=1, parser_used="docling",
                       failures=[(GLYPH_DENSITY, 6.0, 5.0)])
    good = ParseAttempt(attempt_index=2, parser_used="pymupdf", passed=True)
    assert select_attempt([bad, good]) is good


def test_skipped_attempts_are_never_selectable():
    skipped = ParseAttempt(attempt_index=1, parser_used="qwen2.5vl",
                           skipped_reason="too many pages")
    assert select_attempt([skipped]) is None


def test_all_fail_selects_least_bad_and_excludes_with_reason(digital_pdf, db):
    pid = _paper(db)
    _staged(db, pid, digital_pdf)
    worst = CLEAN + " " + " ".join(["GLYPH<c=1,font=/X>"] * 400)   # highest ratio
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=worst), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value=GLYPHY), \
         patch("engine.parsers.pdf_parser.parse_with_vision", return_value=GLYPHY):
        parse_all_pdfs(db, "test_gate")

    rows = _attempts(db, pid)
    assert len(rows) == 3
    accepted = [r for r in rows if r["accepted"]]
    assert len(accepted) == 1
    assert accepted[0]["parser_used"] == "pymupdf"      # lower glyph ratio wins
    assert accepted[0]["passed"] == 0

    paper = db._conn.execute(
        "SELECT status, pdf_exclusion_reason, pdf_exclusion_detail "
        "FROM papers WHERE id = ?", (pid,)).fetchone()
    assert paper["status"] == "PDF_EXCLUDED"
    assert paper["pdf_exclusion_reason"] == "PARSE_QUALITY"
    assert paper["pdf_exclusion_detail"].startswith("parse_quality: ")
    assert GLYPH_DENSITY in paper["pdf_exclusion_detail"]
    assert "(limit 5.0)" in paper["pdf_exclusion_detail"]


def test_excluded_paper_still_has_its_text_stored(digital_pdf, db):
    """Exclusion is a corpus decision; the parse is still evidence."""
    pid = _paper(db)
    _staged(db, pid, digital_pdf)
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=GLYPHY), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value=GLYPHY), \
         patch("engine.parsers.pdf_parser.parse_with_vision", return_value=GLYPHY):
        parse_all_pdfs(db, "test_gate")

    row = db._conn.execute(
        "SELECT parsed_text_path FROM full_text_assets WHERE paper_id = ?",
        (pid,)).fetchone()
    assert row is not None and Path(row["parsed_text_path"]).exists()


def test_exclusion_detail_lists_every_failed_criterion():
    att = ParseAttempt(attempt_index=1, parser_used="docling", failures=[
        (GLYPH_DENSITY, 9.0, 5.0), ("REPLACEMENT_DENSITY", 3.0, 1.0)])
    detail = format_exclusion_detail(att)
    assert detail == ("parse_quality: GLYPH_DENSITY=9.0 (limit 5.0); "
                      "REPLACEMENT_DENSITY=3.0 (limit 1.0)")


# ── T6 — vision page cap ──────────────────────────────────────────────

def test_vision_skipped_above_page_cap_and_never_called(big_pdf, db):
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=SHATTERED_TEXT), \
         patch("engine.parsers.pdf_parser.parse_with_vision") as vision:
        parse_pdf(str(big_pdf), pid, "test_gate", db)

    vision.assert_not_called()
    rows = _attempts(db, pid)
    assert [r["parser_used"] for r in rows] == ["docling", "qwen2.5vl"]
    assert rows[1]["skipped_reason"] is not None
    assert "61 pages" in rows[1]["skipped_reason"]
    assert "vision_max_pages=60" in rows[1]["skipped_reason"]
    assert rows[1]["accepted"] == 0
    assert rows[0]["accepted"] == 1          # the skipped one is not selectable


# ── T7 — ordering pin: length cascade runs before the gate ────────────

def test_sparse_length_fallback_precedes_the_quality_gate(digital_pdf, db):
    """Docling returns 5 chars; PyMuPDF must be reached by LENGTH, not by verdict.

    If the gate ran first it would see EMPTY_TEXT/SHATTERED on "short" and the
    attempt ledger would record a judged docling attempt. It must not: the
    length question is settled before the structure question, so the first
    JUDGED attempt is already PyMuPDF's.
    """
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value="short"), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value=CLEAN), \
         patch("engine.parsers.pdf_parser.parse_with_vision") as vision:
        result = parse_pdf(str(digital_pdf), pid, "test_gate", db)

    vision.assert_not_called()
    rows = _attempts(db, pid)
    assert [r["parser_used"] for r in rows] == ["pymupdf"]
    assert result.accepted_parser == "pymupdf"


# ── T8 — atomicity ────────────────────────────────────────────────────

def test_failure_between_asset_and_attempt_rows_leaves_neither(digital_pdf, db):
    pid = _paper(db)
    real = db._conn

    class _FailsOnAttemptInsert:
        """Delegates everything, but refuses the parse_attempts INSERT.

        `sqlite3.Connection.execute` is a read-only C attribute, so the failure
        is injected by substituting the connection rather than patching it.
        """

        def execute(self, sql, *a, **k):
            if "INSERT INTO parse_attempts" in sql:
                raise sqlite3.OperationalError("injected failure")
            return real.execute(sql, *a, **k)

        def __getattr__(self, name):
            return getattr(real, name)

    db._conn = _FailsOnAttemptInsert()
    try:
        with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=CLEAN):
            with pytest.raises(sqlite3.OperationalError, match="injected failure"):
                parse_pdf(str(digital_pdf), pid, "test_gate", db)
    finally:
        db._conn = real

    assert db._conn.execute(
        "SELECT COUNT(*) FROM full_text_assets WHERE paper_id = ?", (pid,)
    ).fetchone()[0] == 0
    assert db._conn.execute(
        "SELECT COUNT(*) FROM parse_attempts WHERE paper_id = ?", (pid,)
    ).fetchone()[0] == 0
    parsed_dir = _review_dir(db) / "parsed_text"
    assert not list(parsed_dir.glob(f"{pid}_v*.md"))


# ── T9 — targeted re-parse never moves status ─────────────────────────

def test_reparse_writes_new_version_and_leaves_status_alone(digital_pdf, db):
    pid = _paper(db)
    _staged(db, pid, digital_pdf)
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=CLEAN):
        parse_all_pdfs(db, "test_gate")
    db._conn.execute("UPDATE papers SET status='EXTRACTED' WHERE id=?", (pid,))
    db._conn.execute("UPDATE papers SET status='AI_AUDIT_COMPLETE' WHERE id=?", (pid,))
    db._conn.commit()

    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=CLEAN):
        out = reparse_papers(db, [pid])

    assert out[pid]["passed"] is True
    assert out[pid]["version"] == 2                    # short-circuit bypassed
    assert out[pid]["accepted_parser"] == "docling"
    assert db._conn.execute(
        "SELECT status FROM papers WHERE id=?", (pid,)
    ).fetchone()[0] == "AI_AUDIT_COMPLETE"             # untouched
    versions = [r["parsed_text_version"] for r in _attempts(db, pid)]
    assert sorted(set(versions)) == [1, 2]


def test_reparse_reports_failures_without_excluding(digital_pdf, db):
    pid = _paper(db)
    _staged(db, pid, digital_pdf)
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=CLEAN):
        parse_all_pdfs(db, "test_gate")

    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=GLYPHY), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value=GLYPHY), \
         patch("engine.parsers.pdf_parser.parse_with_vision", return_value=GLYPHY):
        out = reparse_papers(db, [pid])

    assert out[pid]["passed"] is False
    assert out[pid]["failures"][0][0] == GLYPH_DENSITY
    assert db._conn.execute(
        "SELECT status FROM papers WHERE id=?", (pid,)).fetchone()[0] == "PARSED"


def test_reparse_reports_a_missing_pdf_without_aborting_the_batch(db):
    pid = _paper(db)
    out = reparse_papers(db, [pid])
    assert "error" in out[pid]


# ── T10 — same-hash short-circuit reports the stored parser ───────────

def test_short_circuit_returns_the_stored_parser_used(digital_pdf, db):
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling",
               side_effect=RuntimeError("boom")), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value=CLEAN):
        first = parse_pdf(str(digital_pdf), pid, "test_gate", db)
    assert first.parser_used == "pymupdf"

    again = parse_pdf(str(digital_pdf), pid, "test_gate", db)   # same hash
    assert again.parser_used == "pymupdf"        # not the literal "docling"
    assert again.accepted_parser == "pymupdf"
    assert again.version == first.version


# ── T11 — spec thresholds ─────────────────────────────────────────────

def test_ondisk_yaml_yields_engine_defaults():
    from engine.parsers.parse_quality import Thresholds
    spec = load_review_spec(SPEC)
    assert Thresholds.from_mapping(spec.pdf_parsing.parse_quality) == Thresholds()
    assert spec.pdf_parsing.vision_max_pages == 60


def test_spec_override_reaches_assess(digital_pdf, db):
    """Relaxing the glyph limit turns a would-be failure into a pass."""
    pid = _paper(db)
    spec = load_review_spec(SPEC)
    spec.pdf_parsing.parse_quality.glyph_density_per_kchar_max = 10_000.0

    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=GLYPHY), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf") as pymupdf:
        result = parse_pdf(str(digital_pdf), pid, "test_gate", db, spec=spec)

    pymupdf.assert_not_called()
    assert result.quality_passed is True
    assert result.accepted_parser == "docling"


def test_spec_vision_cap_override_is_honoured(digital_pdf, db):
    pid = _paper(db)
    spec = load_review_spec(SPEC)
    spec.pdf_parsing.vision_max_pages = 0        # cap below any real page count

    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=SHATTERED_TEXT), \
         patch("engine.parsers.pdf_parser.parse_with_vision") as vision:
        parse_pdf(str(digital_pdf), pid, "test_gate", db, spec=spec)

    vision.assert_not_called()
    rows = _attempts(db, pid)
    assert rows[-1]["skipped_reason"] is not None


# ── attempt-cap pin ───────────────────────────────────────────────────

def test_at_most_three_attempts_are_ever_made(digital_pdf, db):
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=GLYPHY), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value=GLYPHY), \
         patch("engine.parsers.pdf_parser.parse_with_vision", return_value=GLYPHY):
        parse_pdf(str(digital_pdf), pid, "test_gate", db)
    assert len(_attempts(db, pid)) == 3
