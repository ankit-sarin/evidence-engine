"""L09a reproducer 2 — FT adjudication import/export on a SYNTHETIC temp database.
(a) xlsx without 'PI_decision' header: 'DECISION' fallback picks 'Primary Decision'
(b) xlsx joined on Paper ID alone: row carries another paper's title; EE-ID ignored
(c) the Instructions-sheet import command (no spec=) raises TypeError
(d) export with review_spec=None (the default) raises AttributeError
(e) xlsx row whose Title cell is empty is skipped silently
No git, no model: fixed GitState and a null digest (the test seam).
"""
import io, contextlib, shutil, tempfile
from pathlib import Path
from openpyxl import Workbook
REPO = Path("/home/ankitsarin/projects/evidence-engine")
from engine.core import run_manifest as rm
from engine.core.database import ReviewDatabase
from engine.core.review_paths import load_spec_for
from engine.search.models import Citation
from engine.adjudication.ft_screening_adjudicator import (
    import_ft_adjudication_decisions, export_ft_adjudication_queue)

spec = load_spec_for("surgical_autonomy", REPO / "review_specs/surgical_autonomy.yaml")
CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)

def mkdb(tmp, n=3):
    db = ReviewDatabase("synthetic", data_root=Path(tmp))
    shutil.copy2(REPO / "data/surgical_autonomy/extraction_codebook.yaml",
                 Path(db.db_path).parent / "extraction_codebook.yaml")
    db.add_papers([Citation(title=f"Paper {i}", abstract="a", pmid=f"P{i}", source="pubmed",
                            authors=["A"], journal="J", year=2024) for i in range(1, n + 1)])
    for i in range(1, n + 1):
        for s in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "FT_ELIGIBLE", "FT_FLAGGED"):
            db.update_status(i, s)
    return db

def st(db):
    return {r["id"]: r["status"] for r in db._conn.execute("SELECT id,status FROM papers")}

def imp(db, p):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        r = import_ft_adjudication_decisions(db, p, spec=spec, git=CLEAN, digest_fn=lambda m: "")
    return r, (buf.getvalue().strip().splitlines() or [""])[0]

def xlsx(path, headers, rows):
    wb = Workbook(); ws = wb.active; ws.title = "Review Queue"
    ws.append(headers)
    for r in rows: ws.append(r)
    wb.save(path)

with tempfile.TemporaryDirectory() as t:
    print("== (a) header 'DECISION' (no 'PI_decision'): human wrote FT_SCREENED_OUT x3")
    db = mkdb(t + "/a")
    p = Path(t) / "a.xlsx"
    xlsx(p, ["Row #", "Paper ID", "Title", "Primary Decision", "Verifier Decision", "DECISION"],
         [[i, i, f"Paper {i}", "FT_ELIGIBLE", "FT_FLAGGED", "FT_SCREENED_OUT"] for i in (1, 2, 3)])
    r, line = imp(db, p)
    print("  printed:", line); print("  stats:", r["stats"]); print("  statuses:", st(db))
    print("  events:", [tuple(x) for x in db._conn.execute(
        "SELECT paper_id, event_type, to_state FROM paper_events")])
    db.close()

    print("== (b) row: Paper ID 2, Title 'Paper 1', EE-ID 'EE-001'")
    db = mkdb(t + "/b")
    p = Path(t) / "b.xlsx"
    xlsx(p, ["Row #", "Paper ID", "EE-ID", "Title", "PI_decision (FT_ELIGIBLE/FT_SCREENED_OUT)"],
         [[1, 2, "EE-001", "Paper 1", "FT_SCREENED_OUT"]])
    r, line = imp(db, p)
    print("  printed:", line); print("  statuses:", st(db))
    print("  adjudication rows:", [tuple(x) for x in db._conn.execute(
        "SELECT paper_id, title, adjudication_decision FROM ft_screening_adjudication")])
    db.close()

    print("== (c) the command the Instructions sheet prints: import_ft_adjudication_decisions(db, path)")
    db = mkdb(t + "/c")
    try:
        import_ft_adjudication_decisions(db, p)
    except TypeError as e:
        print("  TypeError:", e)
    print("== (d) export_ft_adjudication_queue(db, out) with the default review_spec=None")
    try:
        with contextlib.redirect_stderr(io.StringIO()):
            export_ft_adjudication_queue(db, Path(t) / "q.xlsx")
    except Exception as e:
        print(f"  {type(e).__name__}: {e}")
    db.close()

    print("== (e) three flagged; row 2's Title cell emptied, its Paper ID and decision intact")
    db = mkdb(t + "/e")
    p = Path(t) / "e.xlsx"
    xlsx(p, ["Row #", "Paper ID", "Title", "PI_decision (FT_ELIGIBLE/FT_SCREENED_OUT)"],
         [[1, 1, "Paper 1", "FT_ELIGIBLE"], [2, 2, None, "FT_SCREENED_OUT"], [3, 3, "Paper 3", "FT_ELIGIBLE"]])
    r, line = imp(db, p)
    print("  printed:", line); print("  stats:", r["stats"], "warnings:", r["warnings"]); print("  statuses:", st(db))
    db.close()
