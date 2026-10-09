"""12i-REVIEW-W1, W1-d — timelines for papers 4, 23, 168. Read-only (live mode=ro; xlsx read_only).

usage, from the repository root:
    papers_timeline.py data/surgical_autonomy > papers_timeline.json
Per paper: the papers row's status / created_at / updated_at / rejected_reason and acquisition
columns; every decision row with its timestamp (abstract passes, abstract verification, FT primary,
FT verifier, both adjudication tables, full_text_assets, paper_events), merged in time order; the
workflow_state rows; and every row of every .xlsx under <review>/adjudication/ and of the two FT
adjudication JSON files that carries the paper's id, title, PMID or DOI.
Also the population context: counts by (status, date of updated_at) for papers that have FT rows,
and the distribution of rejected_reason over ABSTRACT_SCREENED_OUT papers updated 2026-03-13.
"""
import glob, json, os, sqlite3, sys

review = sys.argv[1]
c = sqlite3.connect("file:%s?mode=ro" % os.path.join(review, "review.db"), uri=True)
c.row_factory = sqlite3.Row
IDS = (4, 23, 168)
out = {"papers": {}, "tables_without_rows": []}
for t in ("abstract_screening_adjudication", "abstract_verification_decisions", "ft_screening_adjudication",
          "paper_events"):
    n = c.execute("SELECT COUNT(*) FROM %s" % t).fetchone()[0]
    k = c.execute("SELECT COUNT(*) FROM %s WHERE paper_id IN (4,23,168)" % t).fetchone()[0]
    out["tables_without_rows"].append({"table": t, "rows_all": n, "rows_these_three": k})
out["workflow_state"] = [dict(r) for r in c.execute("SELECT * FROM workflow_state ORDER BY 1")]

papers = {}
for pid in IDS:
    p = dict(c.execute("SELECT id, pmid, doi, title, source, status, created_at, updated_at, rejected_reason, "
                       "ee_identifier, oa_status, download_status, pdf_local_path, acquisition_date, "
                       "pdf_exclusion_reason, pdf_quality_check_status FROM papers WHERE id=?", (pid,)).fetchone())
    papers[pid] = p
    ev = []
    for r in c.execute("SELECT id, pass_number, decision, model, decided_at, rationale FROM abstract_screening_decisions "
                       "WHERE paper_id=?", (pid,)):
        ev.append({"at": r["decided_at"], "what": "abstract pass %d" % r["pass_number"], "decision": r["decision"],
                   "model": r["model"], "row_id": r["id"], "rationale": (r["rationale"] or "")[:300]})
    for r in c.execute("SELECT id, decision, model, decided_at, rationale FROM abstract_verification_decisions WHERE paper_id=?", (pid,)):
        ev.append({"at": r["decided_at"], "what": "abstract verification", "decision": r["decision"],
                   "model": r["model"], "row_id": r["id"], "rationale": (r["rationale"] or "")[:300]})
    for r in c.execute("SELECT id, model, decision, reason_code, confidence, decided_at, rationale FROM ft_screening_decisions WHERE paper_id=?", (pid,)):
        ev.append({"at": r["decided_at"], "what": "FT primary", "decision": r["decision"], "model": r["model"],
                   "reason_code": r["reason_code"], "row_id": r["id"], "rationale": (r["rationale"] or "")[:300]})
    for r in c.execute("SELECT id, model, decision, confidence, decided_at, rationale FROM ft_verification_decisions WHERE paper_id=?", (pid,)):
        ev.append({"at": r["decided_at"], "what": "FT verifier", "decision": r["decision"], "model": r["model"],
                   "row_id": r["id"], "rationale": (r["rationale"] or "")[:300]})
    for r in c.execute("SELECT id, pdf_path, parsed_text_path, parsed_text_version, parser_used, parsed_at FROM full_text_assets WHERE paper_id=?", (pid,)):
        ev.append({"at": r["parsed_at"], "what": "full_text_assets row (parsed_at)", "parser": r["parser_used"],
                   "version": r["parsed_text_version"], "row_id": r["id"], "parsed_text_path": r["parsed_text_path"]})
    for r in c.execute("SELECT * FROM abstract_screening_adjudication WHERE paper_id=?", (pid,)):
        ev.append({"at": r["adjudication_timestamp"], "what": "abstract adjudication", **dict(r)})
    for r in c.execute("SELECT * FROM ft_screening_adjudication WHERE paper_id=?", (pid,)):
        ev.append({"at": r["adjudication_timestamp"], "what": "FT adjudication", **dict(r)})
    for r in c.execute("SELECT event_id, event_type, to_state, from_state, occurred_at FROM paper_events WHERE paper_id=?", (pid,)):
        ev.append({"at": r["occurred_at"], "what": "paper event", **dict(r)})
    ev.append({"at": p["created_at"], "what": "papers.created_at"})
    ev.append({"at": p["updated_at"], "what": "papers.updated_at (the last UPDATE that stamped the row; status now %s)" % p["status"]})
    if p["acquisition_date"]:
        ev.append({"at": p["acquisition_date"], "what": "papers.acquisition_date", "download_status": p["download_status"]})
    ev.sort(key=lambda e: (e["at"] or ""))
    out["papers"][pid] = {"row": p, "timeline": ev,
                          "parsed_text_file_exists": [os.path.exists(e["parsed_text_path"]) if e.get("parsed_text_path") else None
                                                      for e in ev if e["what"].startswith("full_text_assets")]}

# artifacts
hits = []
try:
    import openpyxl
    for x in sorted(glob.glob(os.path.join(review, "adjudication", "*.xlsx")) + glob.glob(os.path.join(review, "*.xlsx"))
                    + glob.glob(os.path.join(review, "expanded_search", "*.xlsx"))):
        st = os.stat(x)
        wb = openpyxl.load_workbook(x, read_only=True, data_only=True)
        info = {"file": x, "mtime": st.st_mtime, "sheets": {}}
        for ws in wb.worksheets:
            rows = list(ws.iter_rows(values_only=True))
            header = None
            found = []
            for i, row in enumerate(rows):
                if header is None and row and sum(1 for v in row if isinstance(v, str)) >= 3:
                    header = [str(v) if v is not None else "" for v in row]
                    hrow = i
                    continue
                vals = ["" if v is None else str(v) for v in row]
                for pid, p in papers.items():
                    keys = [k for k in (p["title"], p["pmid"], p["doi"], p["ee_identifier"]) if k]
                    idcols = [j for j, h in enumerate(header or []) if h.strip().lower() in ("paper_id", "id", "paper id")]
                    by_id = any(j < len(vals) and vals[j] == str(pid) for j in idcols)
                    by_key = any(str(k).strip().lower() == v.strip().lower() for k in keys for v in vals if v)
                    if by_id or by_key:
                        found.append({"paper": pid, "sheet_row": i + 1, "matched_by": "id column" if by_id else "title/pmid/doi",
                                      "cells": {(header[j] if header and j < len(header) else "col%d" % j): v[:200]
                                                for j, v in enumerate(vals) if v}})
            info["sheets"][ws.title] = {"rows": len(rows), "header": header, "found": found}
        hits.append(info)
except ImportError as e:
    hits.append({"error": str(e)})
out["xlsx_artifacts"] = hits
jhits = []
for jf in sorted(glob.glob(os.path.join(review, "*adjudication*.json"))):
    txt = open(jf, encoding="utf-8").read()
    d = json.loads(txt)
    items = d if isinstance(d, list) else (d.get("papers") or d.get("decisions") or d.get("items") or [])
    found = [it for it in items if isinstance(it, dict) and it.get("paper_id") in IDS]
    jhits.append({"file": jf, "mtime": os.stat(jf).st_mtime, "top_type": type(d).__name__,
                  "top_keys": list(d.keys())[:12] if isinstance(d, dict) else None, "n_items": len(items),
                  "found": found})
out["json_artifacts"] = jhits

# population context
out["context"] = {
 "aso_updated_20260313_by_reason": [list(r) for r in c.execute(
     "SELECT COALESCE(rejected_reason,'(null)'), COUNT(*) FROM papers WHERE status='ABSTRACT_SCREENED_OUT' "
     "AND substr(updated_at,1,10)='2026-03-13' GROUP BY 1 ORDER BY 2 DESC")],
 "ft_primary_rows_by_day": [list(r) for r in c.execute(
     "SELECT substr(decided_at,1,10), COUNT(*), MIN(decided_at), MAX(decided_at) FROM ft_screening_decisions GROUP BY 1 ORDER BY 1")],
 "ft_verifier_rows_by_day": [list(r) for r in c.execute(
     "SELECT substr(decided_at,1,10), COUNT(*), MIN(decided_at), MAX(decided_at) FROM ft_verification_decisions GROUP BY 1 ORDER BY 1")],
 "abstract_rows_20260313": [list(r) for r in c.execute(
     "SELECT model, pass_number, decision, COUNT(*), COUNT(DISTINCT paper_id), MIN(decided_at), MAX(decided_at) FROM "
     "abstract_screening_decisions WHERE substr(decided_at,1,10)='2026-03-13' GROUP BY 1,2,3 ORDER BY 2,3")],
 "status_of_papers_rescreened_20260313": [list(r) for r in c.execute(
     "SELECT p.status, COUNT(*) FROM papers p WHERE p.id IN (SELECT paper_id FROM abstract_screening_decisions "
     "WHERE substr(decided_at,1,10)='2026-03-13') GROUP BY 1 ORDER BY 2 DESC")],
 "rejected_reason_these": {pid: papers[pid]["rejected_reason"] for pid in IDS},
}
json.dump(out, sys.stdout, indent=1, default=str)
