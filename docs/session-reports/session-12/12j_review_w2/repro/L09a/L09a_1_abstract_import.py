"""L09a reproducer 1 — abstract adjudication import on a SYNTHETIC temp database.
(a) a decision for a paper not at ABSTRACT_SCREEN_FLAGGED / a conflicting duplicate
(b) xlsx header fallback picks 'Primary Decision' (the model's) as the human decision
(c) xlsx row joined on DOI alone: title of one paper, DOI of another
(d) a flagged paper with neither DOI nor PMID cannot be resolved by an xlsx
(e) an empty file re-stamps ABSTRACT_ADJUDICATION_COMPLETE
"""
import json, tempfile, io, contextlib
from pathlib import Path
from openpyxl import Workbook
from engine.core.database import ReviewDatabase
from engine.search.models import Citation
from engine.adjudication.screening_adjudicator import import_adjudication_decisions

def mkdb(tmp, n=3, ids=True):
    db = ReviewDatabase("synthetic", data_root=Path(tmp))
    db.add_papers([Citation(title=f"Paper {i}", abstract="a",
                            pmid=(f"P{i}" if ids else None), doi=(f"10.1/d{i}" if ids else None),
                            source="pubmed", authors=["A"], journal="J", year=2024)
                   for i in range(1, n + 1)])
    return db

def st(db):
    return {r["id"]: r["status"] for r in db._conn.execute("SELECT id,status FROM papers")}

def quiet(fn, *a):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        r = fn(*a)
    return r, buf.getvalue().strip().splitlines()[0] if buf.getvalue().strip() else ""

def xlsx(path, headers, rows):
    wb = Workbook(); ws = wb.active; ws.title = "Review Queue"
    ws.append(headers)
    for r in rows: ws.append(r)
    wb.save(path)

with tempfile.TemporaryDirectory() as t:
    print("== (a) JSON: paper 1 FLAGGED, paper 2 INGESTED (never flagged), paper 1 twice")
    db = mkdb(t + "/a")
    db.update_status(1, "ABSTRACT_SCREEN_FLAGGED")
    p = Path(t) / "a.json"
    p.write_text(json.dumps([
        {"paper_id": 1, "decision": "EXCLUDE"},
        {"paper_id": 1, "decision": "INCLUDE"},      # conflicting duplicate
        {"paper_id": 2, "decision": "INCLUDE"},      # not flagged
        {"paper_id": "3", "decision": "EXCLUDE"},    # text id, not flagged
    ]))
    r, line = quiet(import_adjudication_decisions, db, p)
    print("  printed:", line); print("  stats:", r["stats"], "warnings:", r["warnings"])
    print("  statuses:", st(db))
    print("  adjudication rows:", [tuple(x) for x in db._conn.execute(
        "SELECT paper_id, adjudication_decision FROM abstract_screening_adjudication")])
    db.close()

    print("== (b) xlsx without a 'PI_decision' header but with a 'DECISION' column")
    db = mkdb(t + "/b")
    for i in (1, 2, 3): db.update_status(i, "ABSTRACT_SCREEN_FLAGGED")
    p = Path(t) / "b.xlsx"
    xlsx(p, ["Row #", "Title", "DOI", "PMID", "Primary Decision", "Verifier Decision", "DECISION"],
         [[1, "Paper 1", "10.1/d1", "P1", "include", "exclude", "EXCLUDE"],
          [2, "Paper 2", "10.1/d2", "P2", "include", "exclude", "EXCLUDE"],
          [3, "Paper 3", "10.1/d3", "P3", "include", "exclude", "EXCLUDE"]])
    r, line = quiet(import_adjudication_decisions, db, p)
    print("  human wrote EXCLUDE x3; printed:", line); print("  stats:", r["stats"]); print("  statuses:", st(db))
    db.close()

    print("== (c) xlsx row: Title of paper 1, DOI of paper 2, no PMID -> joined on DOI alone")
    db = mkdb(t + "/c")
    for i in (1, 2): db.update_status(i, "ABSTRACT_SCREEN_FLAGGED")
    p = Path(t) / "c.xlsx"
    xlsx(p, ["Row #", "Title", "DOI", "PMID", "PI_decision (INCLUDE/EXCLUDE)"],
         [[1, "Paper 1", "10.1/d2", None, "EXCLUDE"]])
    r, line = quiet(import_adjudication_decisions, db, p)
    print("  printed:", line); print("  statuses:", st(db))
    print("  adjudication rows:", [tuple(x) for x in db._conn.execute(
        "SELECT paper_id, external_key, title, adjudication_decision FROM abstract_screening_adjudication")])
    db.close()

    print("== (d) flagged paper with no DOI and no PMID, decided in an xlsx")
    db = mkdb(t + "/d", n=1, ids=False)
    db.update_status(1, "ABSTRACT_SCREEN_FLAGGED")
    p = Path(t) / "d.xlsx"
    xlsx(p, ["Row #", "Title", "DOI", "PMID", "PI_decision (INCLUDE/EXCLUDE)"],
         [[1, "Paper 1", "", "", "INCLUDE"]])
    r, line = quiet(import_adjudication_decisions, db, p)
    print("  printed:", line); print("  stats:", r["stats"], "warnings:", r["warnings"]); print("  statuses:", st(db))
    db.close()

    print("== (e) stage stamp: complete by a real import, then an empty file")
    db = mkdb(t + "/e", n=1)
    db.update_status(1, "ABSTRACT_SCREEN_FLAGGED")
    p = Path(t) / "e1.json"; p.write_text(json.dumps([{"paper_id": 1, "decision": "INCLUDE"}]))
    quiet(import_adjudication_decisions, db, p)
    q = "SELECT status, metadata FROM workflow_state WHERE stage_name='ABSTRACT_ADJUDICATION_COMPLETE'"
    print("  after real import :", tuple(db._conn.execute(q).fetchone()))
    p = Path(t) / "e2.json"; p.write_text("[]")
    r, line = quiet(import_adjudication_decisions, db, p)
    print("  empty file printed:", line)
    print("  after empty import:", tuple(db._conn.execute(q).fetchone()))
    db.close()
