"""ParseFailed and the parse-stage event writer (R227/R228, 10a-C6-B).

T1-T7 of the 10a-C6-B brief. Fixture PDFs follow tests/test_pdf_parser.py's
established pattern (fpdf); the database is a scratch ReviewDatabase; a
fixture run comes from tests/_event_store_fixture.fixture_run (raw SQL, no
spec/codebook needed — write_paper_event only checks the run_id names a real
run_manifests row).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from fpdf import FPDF

from engine.core.database import ReviewDatabase
from engine.core import paper_state as PS
from engine.core.effective import effective_state
from engine.exporters.prisma import generate_prisma_flow
from engine.parsers.pdf_parser import ParseFailed, parse_all_pdfs, parse_pdf
from engine.search.models import Citation
from _event_store_fixture import fixture_run, seed_eligibility

from engine.core.review_spec import load_review_spec
PARSE_SPEC = load_review_spec("review_specs/surgical_autonomy.yaml")   # 12c-C53: the parse entry points take the spec


@pytest.fixture(autouse=True)
def _no_unstubbed_ocr():
    """Same guard as test_pdf_parser.py: keep real docling+rapidocr out of
    the gate. Tests that need docling_ocr patch it themselves."""
    with patch("engine.parsers.pdf_parser.parse_with_docling_ocr",
               side_effect=RuntimeError("docling_ocr not stubbed in this test")):
        yield


@pytest.fixture()
def digital_pdf(tmp_path) -> Path:
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=12)
    pdf.multi_cell(w=0, text=(
        "Autonomous Robotic Suturing: A Systematic Review. This study "
        "evaluates the performance of autonomous suturing systems. " * 5
    ))
    path = tmp_path / "digital.pdf"
    pdf.output(str(path))
    return path


@pytest.fixture()
def scanned_pdf(tmp_path) -> Path:
    pdf = FPDF()
    pdf.add_page()
    path = tmp_path / "scanned.pdf"
    pdf.output(str(path))
    return path


@pytest.fixture()
def db(tmp_path):
    rdb = ReviewDatabase("parse_fail", data_root=tmp_path)
    yield rdb
    rdb.close()


def _add_paper(db, pid_hint: str = "1") -> int:
    db.add_papers([Citation(title=f"Paper {pid_hint}", source="pubmed", pmid=pid_hint)])
    papers = db.get_papers_by_status("INGESTED")
    return papers[-1]["id"]


def _attempt_count(db, pid) -> int:
    return db._conn.execute(
        "SELECT COUNT(*) FROM parse_attempts WHERE paper_id = ?", (pid,)
    ).fetchone()[0]


# ── T1: ParseFailed at each ruled branch ───────────────────────────────


def test_T1_branch_2_file_unreadable(digital_pdf, db):
    pid = _add_paper(db, "b2")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    with patch("engine.parsers.pdf_parser.is_scanned_pdf",
               side_effect=RuntimeError("corrupt file")):
        with pytest.raises(ParseFailed) as exc:
            parse_pdf(str(digital_pdf), pid, "test_parse_fail", db, spec=PARSE_SPEC)
    assert exc.value.reason_code == PS.REASON_PARSE_FILE_UNREADABLE
    assert isinstance(exc.value.__cause__, RuntimeError)
    assert _attempt_count(db, pid) == 0, "no tier ran — no ledger row (unique to #2)"


def test_T1_branch_6_vision_exhausted_scanned_route(scanned_pdf, db):
    pid = _add_paper(db, "b6")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    with patch("engine.parsers.pdf_parser.parse_with_docling_ocr",
               side_effect=RuntimeError("ocr off")), \
         patch("engine.parsers.pdf_parser.parse_with_vision",
               side_effect=RuntimeError("vision off")):
        with pytest.raises(ParseFailed) as exc:
            parse_pdf(str(scanned_pdf), pid, "test_parse_fail", db, spec=PARSE_SPEC)
    assert exc.value.reason_code == PS.REASON_PARSE_VISION_EXHAUSTED
    assert isinstance(exc.value.__cause__, RuntimeError)
    assert str(exc.value.__cause__) == "vision off"
    assert _attempt_count(db, pid) >= 1, "the OCR and vision attempts are recorded"


def test_T1_branch_10_pymupdf_exhausted_digital_route(digital_pdf, db):
    pid = _add_paper(db, "b10")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    with patch("engine.parsers.pdf_parser.parse_with_docling",
               side_effect=RuntimeError("docling exploded")), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf",
               side_effect=OSError("disk gone")):
        with pytest.raises(ParseFailed) as exc:
            parse_pdf(str(digital_pdf), pid, "test_parse_fail", db, spec=PARSE_SPEC)
    assert exc.value.reason_code == PS.REASON_PARSE_PYMUPDF_EXHAUSTED
    assert isinstance(exc.value.__cause__, OSError)
    assert _attempt_count(db, pid) >= 1


def test_T1_branch_17_cascade_empty(digital_pdf, db):
    pid = _add_paper(db, "b17")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value="  "), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value="  "), \
         patch("engine.parsers.pdf_parser.parse_with_vision", return_value="  "):
        with pytest.raises(ParseFailed) as exc:
            parse_pdf(str(digital_pdf), pid, "test_parse_fail", db, spec=PARSE_SPEC)
    assert exc.value.reason_code == PS.REASON_PARSE_CASCADE_EMPTY
    assert exc.value.__cause__ is None, "a logic-derived failure, nothing to wrap"
    assert _attempt_count(db, pid) >= 1, "Contract 7: the ledger survives total failure"


# ── T2 / T3: parse_all_pdfs under a run writes the event ──────────────


def _linked_run(db) -> int:
    return fixture_run(db._conn)


def _place_pdf(db, digital_pdf, pid) -> None:
    pdf_dir = Path(db.db_path).parent / "pdfs"
    pdf_dir.mkdir(parents=True, exist_ok=True)
    (pdf_dir / f"{pid}.pdf").write_bytes(digital_pdf.read_bytes())


def test_T2_cascade_empty_under_a_run_writes_one_event_and_continues(digital_pdf, db):
    run_id = _linked_run(db)
    p1 = _add_paper(db, "t2a")
    p2 = _add_paper(db, "t2b")
    for pid in (p1, p2):
        db.update_status(pid, "ABSTRACT_SCREENED_IN")
        db.update_status(pid, "PDF_ACQUIRED")
        _place_pdf(db, digital_pdf, pid)
    status_before = db._conn.execute(
        "SELECT status FROM papers WHERE id = ?", (p1,)).fetchone()[0]

    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value="  "), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value="  "), \
         patch("engine.parsers.pdf_parser.parse_with_vision", return_value="  "):
        stats = parse_all_pdfs(db, "test_parse_fail", run_id=run_id, spec=PARSE_SPEC)

    assert stats["failed"] == 2, "the loop continued to the second paper"
    rows = db._conn.execute(
        "SELECT event_type, to_state, reason_code, actor_kind, actor_role, "
        "stage_name, run_id FROM paper_events WHERE paper_id = ?", (p1,)
    ).fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert tuple(row) == ("parsed", "parse_failed", PS.REASON_PARSE_CASCADE_EMPTY,
                          "engine", "system", "parse", run_id)
    status_after = db._conn.execute(
        "SELECT status FROM papers WHERE id = ?", (p1,)).fetchone()[0]
    assert status_after == status_before, "no papers.status write in the new branch"


def test_T3_unclassified_exception_under_a_run_writes_its_own_code(digital_pdf, db):
    """Not one of the ruled branches — a failure in the write phase, after a
    successful parse, escapes parse_pdf as a plain exception (no ParseFailed
    wraps the write path; only the cascade's own failure-to-parse branches
    do). That is exactly what parse_all_pdfs's generic `except Exception`
    branch, and REASON_PARSE_UNCLASSIFIED_ERROR, exist for."""
    run_id = _linked_run(db)
    pid = _add_paper(db, "t3")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    _place_pdf(db, digital_pdf, pid)

    with patch("engine.parsers.pdf_parser.parse_with_docling",
               return_value="Autonomous robotic suturing evaluation. " * 40), \
         patch("engine.parsers.pdf_parser.record_parsed_text",
               side_effect=KeyError("not a ParseFailed at all")):
        stats = parse_all_pdfs(db, "test_parse_fail", run_id=run_id, spec=PARSE_SPEC)

    assert stats["failed"] == 1
    row = db._conn.execute(
        "SELECT reason_code FROM paper_events WHERE paper_id = ?", (pid,)
    ).fetchone()
    assert row[0] == PS.REASON_PARSE_UNCLASSIFIED_ERROR


# ── T4: no run_id — today's behaviour, pinned ──────────────────────────


def test_T4_no_run_id_writes_no_event(digital_pdf, db):
    pid = _add_paper(db, "t4")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    _place_pdf(db, digital_pdf, pid)

    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value="  "), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value="  "), \
         patch("engine.parsers.pdf_parser.parse_with_vision", return_value="  "):
        stats = parse_all_pdfs(db, "test_parse_fail", spec=PARSE_SPEC)  # run_id defaults to None

    assert stats["failed"] == 1
    assert db._conn.execute(
        "SELECT COUNT(*) FROM paper_events WHERE paper_id = ?", (pid,)
    ).fetchone()[0] == 0


# ── T5: readers see it, generically ────────────────────────────────────


def test_T5_effective_state_and_prisma_carry_the_new_reason(digital_pdf, db):
    run_id = _linked_run(db)
    pid = _add_paper(db, "t5")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    seed_eligibility(db._conn, pid)
    _place_pdf(db, digital_pdf, pid)

    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value="  "), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value="  "), \
         patch("engine.parsers.pdf_parser.parse_with_vision", return_value="  "):
        parse_all_pdfs(db, "test_parse_fail", run_id=run_id, spec=PARSE_SPEC)

    state = effective_state(db._conn, pid)
    assert state.processing == "parse_failed"
    assert state.processing_reason == PS.REASON_PARSE_CASCADE_EMPTY

    flow = generate_prisma_flow(db)
    assert flow["parse_failed"] == 1
    assert flow["failure_reasons"]["parse_failed"] == {PS.REASON_PARSE_CASCADE_EMPTY: 1}


# ── T6: FileExistsError propagates uncaught ────────────────────────────


def test_T6_file_exists_error_propagates_out_of_parse_all_pdfs(digital_pdf, db):
    run_id = _linked_run(db)
    pid = _add_paper(db, "t6")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    _place_pdf(db, digital_pdf, pid)

    target = Path(db.db_path).parent / "parsed_text" / f"{pid}_v1.md"
    target.parent.mkdir(exist_ok=True)
    target.write_text("an unrecorded file")

    with pytest.raises(FileExistsError, match="R99"):
        parse_all_pdfs(db, "test_parse_fail", run_id=run_id, spec=PARSE_SPEC)
    assert db._conn.execute(
        "SELECT COUNT(*) FROM paper_events WHERE paper_id = ?", (pid,)
    ).fetchone()[0] == 0, "an integrity guard is never a paper outcome (R231)"


# ── T7: seed_processing validates against PROCESSING_REASONS ──────────


def test_T7_seed_processing_refuses_an_unknown_code(db):
    from _event_store_fixture import seed_processing
    with pytest.raises(ValueError, match="PROCESSING_REASONS"):
        seed_processing(db._conn, 1, "parse_failed", reason_code="not_a_real_code")
    with pytest.raises(ValueError, match="PROCESSING_REASONS"):
        seed_processing(db._conn, 1, "full_text_not_obtainable",
                        reason_code="also_not_real")


def test_T7_seed_processing_accepts_every_ruled_code(db):
    from _event_store_fixture import seed_processing
    db._conn.execute(
        "INSERT INTO papers (id, title, source, created_at, updated_at) "
        "VALUES (1, 't', 's', 'n', 'n')")
    db._conn.commit()
    for code in PS.PARSE_REASONS:
        seed_processing(db._conn, 1, "parse_failed", reason_code=code)
    for code in PS.ACQUISITION_REASONS:
        seed_processing(db._conn, 1, "full_text_not_obtainable", reason_code=code)
