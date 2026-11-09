"""12ae-U26A — FAKE probe for U26 (C56's parse handlers). For each U26 handler (I2's mapping: triage 4 → pdf_parser.py:717, 5 → :806,
6 → :901, 7 → :1120, 12 → :520, 13 → :525), drive one exception into it through parse_all_pdfs under a fixture run, on a scratch
ReviewDatabase under a temp directory, and print what the paper and the run record. The seams are tests/test_parse_failures.py's
(scratch ReviewDatabase, tests/_event_store_fixture.fixture_run, fpdf PDFs, patched parser tiers); no server is reached (every model
tier is patched), no live database is opened.
Exceptions driven: three members of run_fault_classes() — sqlite3.OperationalError("database is locked"), ConnectionError (a refused
Ollama connection, R532), UndeclaredCall (C54) — and one unclassified KeyError.
usage, from the repository root: .venv/bin/python fake_probe_u26a.py <tmp dir>   (stdout: one block per handler × exception)"""
import logging, sqlite3, sys, tempfile
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path.cwd())); sys.path.insert(0, str(Path.cwd() / "tests"))
logging.disable(logging.CRITICAL)
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
TMP = Path(sys.argv[1]); TMP.mkdir(parents=True, exist_ok=True)
CLEAN = "Autonomous robotic suturing evaluation. " * 40

def pdf(kind):
    p = FPDF(); p.add_page()
    if kind == "digital":
        p.set_font("Helvetica", size=12); p.multi_cell(w=0, text="Autonomous robotic suturing. " * 20)
    path = Path(tempfile.mkdtemp(dir=TMP)) / f"{kind}.pdf"; p.output(str(path)); return path

def excs():
    return [("sqlite3.OperationalError", sqlite3.OperationalError("database is locked")),
            ("ConnectionError", ConnectionError("[Errno 111] Connection refused")),
            ("UndeclaredCall", UndeclaredCall("vision_parse", None, "qwen2.5vl:7b")),
            ("KeyError (unclassified)", KeyError("unclassified"))]

class FailingConn:
    """Delegates everything, but refuses the parse_attempts INSERT (and, with rollback_fails, the rollback) — test_parse_gate.py's form."""
    def __init__(self, real, exc, rollback_fails=False): self.real, self.exc, self.rb = real, exc, rollback_fails
    def execute(self, sql, *a, **k):
        if "INSERT INTO parse_attempts" in sql: raise self.exc
        return self.real.execute(sql, *a, **k)
    def rollback(self):
        if self.rb: raise sqlite3.OperationalError("rollback failed")
        return self.real.rollback()
    def __getattr__(self, n): return getattr(self.real, n)

def run_case(handler, name, exc):
    d = Path(tempfile.mkdtemp(dir=TMP)); db = ReviewDatabase("probe", data_root=d)
    run_id = fixture_run(db._conn)
    db.add_papers([Citation(title="P", source="pubmed", pmid="1")]); pid = db.get_papers_by_status("INGESTED")[-1]["id"]
    db.update_status(pid, "ABSTRACT_SCREENED_IN"); db.update_status(pid, "PDF_ACQUIRED")
    kind = "scanned" if handler == "4 (:717 scanned vision)" else "digital"
    pdir = Path(db.db_path).parent / "pdfs"; pdir.mkdir(parents=True, exist_ok=True); (pdir / f"{pid}.pdf").write_bytes(pdf(kind).read_bytes())
    patches = [patch.object(pp, "parse_with_docling_ocr", side_effect=RuntimeError("ocr off"))]
    real_conn = db._conn
    if handler == "4 (:717 scanned vision)":
        patches += [patch.object(pp, "parse_with_vision", side_effect=exc)]
    elif handler == "5 (:806 digital-sparse vision)":
        patches += [patch.object(pp, "parse_with_docling", return_value="x"), patch.object(pp, "parse_with_pymupdf", return_value="x"),
                    patch.object(pp, "parse_with_vision", side_effect=exc)]
    elif handler == "6 (:901 gate-driven re-route)":
        calls = {"n": 0}
        def nxt(failures, tried):
            calls["n"] += 1; return "qwen2.5vl" if calls["n"] == 1 else None
        patches += [patch.object(pp, "parse_with_docling", return_value=CLEAN),
                    patch.object(pp, "assess", return_value=Verdict(passed=False, failures=((REPLACEMENT_DENSITY, 0.5, 0.1),), metrics={})),
                    patch.object(pp, "_next_parser", side_effect=nxt), patch.object(pp, "parse_with_vision", side_effect=exc)]
    elif handler == "7 (:1120 per-paper)":
        patches += [patch.object(pp, "parse_with_docling", return_value=CLEAN), patch.object(pp, "record_parsed_text", side_effect=exc)]
    elif handler.startswith("12") or handler.startswith("13"):
        patches += [patch.object(pp, "parse_with_docling", return_value="  "), patch.object(pp, "parse_with_pymupdf", return_value="  "),
                    patch.object(pp, "parse_with_vision", return_value="  ")]
        db._conn = FailingConn(real_conn, exc, rollback_fails=handler.startswith("13"))
    propagated = None
    try:
        for p in patches: p.start()
        stats = pp.parse_all_pdfs(db, "probe", spec=SPEC, run_id=run_id)
    except BaseException as e:
        propagated, stats = f"{type(e).__name__}: {e}"[:90], None
    finally:
        for p in patches: p.stop()
        db._conn = real_conn
    ev = real_conn.execute("SELECT to_state, reason_code FROM paper_events WHERE paper_id = ? AND event_type = 'parsed'", (pid,)).fetchall()
    att = real_conn.execute("SELECT COUNT(*) FROM parse_attempts WHERE paper_id = ?", (pid,)).fetchone()[0]
    asset = real_conn.execute("SELECT COUNT(*) FROM full_text_assets WHERE paper_id = ?", (pid,)).fetchone()[0]
    status = real_conn.execute("SELECT status, pdf_exclusion_reason FROM papers WHERE id = ?", (pid,)).fetchone()
    db.close()
    print(f"  {name:26s} run_fault={is_run_fault(exc)!s:5s} | propagated: {propagated or 'no'} | stats.failed={stats['failed'] if stats else '-'} "
          f"| paper events: {[tuple(r) for r in ev]} | parse_attempts rows: {att} | asset rows: {asset} | papers.status: {tuple(status)}")

for h in ("4 (:717 scanned vision)", "5 (:806 digital-sparse vision)", "6 (:901 gate-driven re-route)", "7 (:1120 per-paper)",
          "12 (:520 _commit_attempts outer)", "13 (:525 _commit_attempts inner, rollback also fails)"):
    print(f"\n=== handler {h}")
    for name, exc in excs():
        run_case(h, name, exc)
