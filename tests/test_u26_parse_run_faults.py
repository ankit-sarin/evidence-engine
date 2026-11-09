"""U26 (C56's parse half; R790(b), R790(e)): a run fault reaching one of the six parse handlers propagates as itself.

The handlers, by the I2 mapping of 12ae_U26A/12ae_U26A.md (12f_triage.md's numbering; lines at e5d9fed):
  4  parse_pdf, vision on the scanned route            (pdf_parser.py:717)
  5  parse_pdf, vision on the digital-sparse route     (:806)
  6  parse_pdf, the gate-driven re-route               (:901)
  7  parse_all_pdfs, the per-paper except Exception    (:1120)
  12 _commit_attempts, outer                           (:520)
  13 _commit_attempts, inner (the rollback)            (:525)

Seams: 12ae_U26A/fake_probe_u26a.py's. A scratch ReviewDatabase under tmp_path, a fixture run from
tests/_event_store_fixture.fixture_run, fpdf PDFs, every parser tier patched (no server is reached; the conftest
fences stay on). Every case is driven through parse_all_pdfs(..., run_id=...), the probe's entry point; for 4-6 the
exception leaves parse_pdf first and then parse_all_pdfs, and the test asserts the object that leaves
parse_all_pdfs is the one injected (not a ParseFailed wrapping it).
  4  a blank (scanned) PDF; docling_ocr raises RuntimeError (this module's guard), parse_with_vision raises.
  5  a digital PDF; docling and pymupdf return "x" (sparse), parse_with_vision raises.
  6  the vision route forced as the probe forced it: docling returns clean text, assess is patched to fail it on
     replacement density, _next_parser returns "qwen2.5vl" once and then None, parse_with_vision raises.
  7  docling returns clean text; record_parsed_text (inside the asset transaction) raises.
  12 every tier returns whitespace (the cascade-empty path, which calls _commit_attempts with no asset); the
     connection's INSERT INTO parse_attempts raises the exception; the rollback works.
  13 as 12, but the INSERT raises a RuntimeError (handler 12 converts it, as it should) and the rollback raises
     the exception, so that handler 13 is the one that sees it. (The probe raised the exception at the INSERT
     and a fixed sqlite3 error at the rollback; under U26 that form reaches handler 12 first and never 13.)

Run faults: the three run_fault_classes() members the probe drove (sqlite3.OperationalError, ConnectionError,
UndeclaredCall). Each propagates out of parse_all_pdfs with no parse_failed event, no asset row and no PDF_EXCLUDED
write for the paper; at 4-5 the failed vision attempt row is kept (R790(b)). KeyError is not a run fault and keeps
today's outcome (12ae_U26A.md §3, §5): parse_vision_exhausted at 4-5, the exclusion at 6, parse_unclassified_error
at 7, the lost ledger (parse_cascade_empty, no attempt row) at 12-13.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest
from fpdf import FPDF

from engine.core.database import ReviewDatabase
from engine.core.extraction_events import is_run_fault
from engine.core.review_spec import load_review_spec
from engine.core.run_manifest import UndeclaredCall
from engine.parsers import pdf_parser as pp
from engine.parsers.parse_quality import REPLACEMENT_DENSITY, Verdict
from engine.search.models import Citation
from _event_store_fixture import fixture_run

SPEC = load_review_spec("review_specs/surgical_autonomy.yaml")
CLEAN = "Autonomous robotic suturing evaluation. " * 40

HANDLERS = ["4", "5", "6", "7", "12", "13"]
EXCEPTIONS = {
    "sqlite3.OperationalError": lambda: sqlite3.OperationalError("database is locked"),
    "ConnectionError": lambda: ConnectionError("[Errno 111] Connection refused"),
    "UndeclaredCall": lambda: UndeclaredCall("vision_parse", None, "qwen2.5vl:7b"),
    "KeyError": lambda: KeyError("unclassified"),
}


@pytest.fixture(autouse=True)
def _no_unstubbed_ocr():
    """test_parse_failures.py's guard: keep real docling+rapidocr out of the gate."""
    with patch("engine.parsers.pdf_parser.parse_with_docling_ocr",
               side_effect=RuntimeError("docling_ocr not stubbed in this test")):
        yield


class _FailingConn:
    """Delegates everything, but refuses the parse_attempts INSERT and, when told to, the rollback
    (test_parse_gate.py's connection-wrapper form)."""

    def __init__(self, real, insert_exc, rollback_exc=None):
        self.real, self.insert_exc, self.rollback_exc = real, insert_exc, rollback_exc

    def execute(self, sql, *a, **k):
        if "INSERT INTO parse_attempts" in sql:
            raise self.insert_exc
        return self.real.execute(sql, *a, **k)

    def rollback(self):
        if self.rollback_exc is not None:
            raise self.rollback_exc
        return self.real.rollback()

    def __getattr__(self, name):
        return getattr(self.real, name)


def _pdf(path: Path, kind: str) -> Path:
    pdf = FPDF()
    pdf.add_page()
    if kind == "digital":
        pdf.set_font("Helvetica", size=12)
        pdf.multi_cell(w=0, text="Autonomous robotic suturing. " * 20)
    pdf.output(str(path))
    return path


def _patches(handler: str, exc: BaseException):
    if handler == "4":
        return [patch.object(pp, "parse_with_vision", side_effect=exc)]
    if handler == "5":
        return [patch.object(pp, "parse_with_docling", return_value="x"),
                patch.object(pp, "parse_with_pymupdf", return_value="x"),
                patch.object(pp, "parse_with_vision", side_effect=exc)]
    if handler == "6":
        calls = {"n": 0}

        def nxt(failures, tried):
            calls["n"] += 1
            return "qwen2.5vl" if calls["n"] == 1 else None
        return [patch.object(pp, "parse_with_docling", return_value=CLEAN),
                patch.object(pp, "assess", return_value=Verdict(
                    passed=False, failures=((REPLACEMENT_DENSITY, 0.5, 0.1),), metrics={})),
                patch.object(pp, "_next_parser", side_effect=nxt),
                patch.object(pp, "parse_with_vision", side_effect=exc)]
    if handler == "7":
        return [patch.object(pp, "parse_with_docling", return_value=CLEAN),
                patch.object(pp, "record_parsed_text", side_effect=exc)]
    return [patch.object(pp, "parse_with_docling", return_value="  "),
            patch.object(pp, "parse_with_pymupdf", return_value="  "),
            patch.object(pp, "parse_with_vision", return_value="  ")]


@pytest.mark.parametrize("exc_name", list(EXCEPTIONS))
@pytest.mark.parametrize("handler", HANDLERS)
def test_run_faults_leave_the_parse_handlers_as_themselves(tmp_path, handler, exc_name):
    exc = EXCEPTIONS[exc_name]()
    db = ReviewDatabase("u26", data_root=tmp_path)
    real_conn = db._conn
    try:
        run_id = fixture_run(real_conn)
        db.add_papers([Citation(title="P", source="pubmed", pmid="1")])
        pid = db.get_papers_by_status("INGESTED")[-1]["id"]
        db.update_status(pid, "ABSTRACT_SCREENED_IN")
        db.update_status(pid, "PDF_ACQUIRED")
        pdir = Path(db.db_path).parent / "pdfs"
        pdir.mkdir(parents=True, exist_ok=True)
        _pdf(pdir / f"{pid}.pdf", "scanned" if handler == "4" else "digital")

        if handler == "12":
            db._conn = _FailingConn(real_conn, insert_exc=exc)
        elif handler == "13":
            db._conn = _FailingConn(real_conn, insert_exc=RuntimeError("insert refused"), rollback_exc=exc)

        propagated = None
        patches = _patches(handler, exc)
        for p in patches:
            p.start()
        try:
            pp.parse_all_pdfs(db, "u26", spec=SPEC, run_id=run_id)
        except BaseException as e:   # noqa: BLE001 — the test reads what left the entry point
            propagated = e
        finally:
            for p in patches:
                p.stop()
            db._conn = real_conn

        events = real_conn.execute(
            "SELECT to_state, reason_code FROM paper_events WHERE paper_id = ? AND event_type = 'parsed'",
            (pid,)).fetchall()
        events = [tuple(r) for r in events]
        assets = real_conn.execute(
            "SELECT COUNT(*) FROM full_text_assets WHERE paper_id = ?", (pid,)).fetchone()[0]
        vision_rows = real_conn.execute(
            "SELECT COUNT(*) FROM parse_attempts WHERE paper_id = ? AND parser_used = 'qwen2.5vl'",
            (pid,)).fetchone()[0]
        attempts = real_conn.execute(
            "SELECT COUNT(*) FROM parse_attempts WHERE paper_id = ?", (pid,)).fetchone()[0]
        status = tuple(real_conn.execute(
            "SELECT status, pdf_exclusion_reason FROM papers WHERE id = ?", (pid,)).fetchone())

        if is_run_fault(exc):
            assert propagated is exc, f"handler {handler}: {exc_name} did not leave parse_all_pdfs as itself: {propagated!r}"
            assert events == [], f"handler {handler}: a run fault became a paper outcome: {events}"
            assert assets == 0
            assert status[0] != "PDF_EXCLUDED", status
            if handler in ("4", "5"):
                assert vision_rows == 1, "the failed vision attempt row is kept (R790(b))"
        else:
            assert propagated is None, f"handler {handler}: KeyError propagated: {propagated!r}"
            if handler in ("4", "5"):
                assert events == [("parse_failed", "parse_vision_exhausted")]
                assert vision_rows == 1 and assets == 0
            elif handler == "6":
                assert events == [] and assets == 1
                assert status == ("PDF_EXCLUDED", "PARSE_QUALITY")
            elif handler == "7":
                assert events == [("parse_failed", "parse_unclassified_error")] and assets == 0
            else:
                assert events == [("parse_failed", "parse_cascade_empty")]
                assert attempts == 0 and assets == 0
    finally:
        db._conn = real_conn
        db.close()
