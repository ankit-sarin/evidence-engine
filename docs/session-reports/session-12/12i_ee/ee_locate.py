"""12i-EE-LOCATE — which paper does an "EE-NNN" key name, by source. Read-only (live mode=ro).
usage, from the repository root:  ee_locate.py data/surgical_autonomy > ee_locate.json
Sources: (H) the human workbook Extraction_Workbook_v2_A.xlsx, sheet "Extraction Form", column 1
(Paper ID) and column 4 (Title); (L) live papers.ee_identifier; (I) the key the human importer
validates against, printf('EE-%03d', papers.id); (M) concordance_pdfs/paper_manifest.csv
(ee_id | db_id); (P) the EE number in papers.pdf_local_path file names.
"""
import csv, json, os, re, sqlite3, sys
import openpyxl
review = sys.argv[1]
c = sqlite3.connect("file:%s?mode=ro" % os.path.join(review, "review.db"), uri=True)
papers = {r[0]: {"id": r[0], "ee": r[1], "title": r[2] or "", "status": r[3], "pdf": r[4]}
          for r in c.execute("SELECT id, ee_identifier, title, status, pdf_local_path FROM papers")}
norm = lambda s: re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()
by_title = {}
for p in papers.values():
    by_title.setdefault(norm(p["title"]), []).append(p["id"])
by_ee = {p["ee"]: p["id"] for p in papers.values() if p["ee"]}
out = {"live": {"papers": len(papers), "with_ee_identifier": len(by_ee),
                "ee_identifier_equals_EE_id": sum(1 for p in papers.values() if p["ee"] == "EE-%03d" % p["id"]),
                "max_id_with_ee_identifier": max(p["id"] for p in papers.values() if p["ee"])}}
# how many keys are ambiguous between the two schemes
amb = [(k, pid, by_ee[k]) for pid in papers for k in ["EE-%03d" % pid] if k in by_ee and by_ee[k] != pid]
out["keys_naming_two_different_papers"] = {"count": len(amb), "first": amb[:5]}
# H — the human workbook
wb = openpyxl.load_workbook(os.path.join(review, "Extraction_Workbook_v2_A.xlsx"), read_only=True, data_only=True)
ws = wb["Extraction Form"]
rows = [r for r in ws.iter_rows(values_only=True) if r and isinstance(r[0], str) and re.match(r"^EE-\d{3}$", r[0].strip())]
H = []
for r in rows:
    key, title = r[0].strip(), (r[3] or "")      # columns: Paper ID, First Author, Year, Title
    ids = by_title.get(norm(title), [])
    pid = ids[0] if len(ids) == 1 else None
    H.append({"key": key, "title": str(title)[:90], "live_paper_by_title": pid, "title_matches": len(ids),
              "paper_named_by_EE_id": int(key[3:]) if int(key[3:]) in papers else None,
              "paper_named_by_ee_identifier": by_ee.get(key),
              "live_ee_identifier_of_that_paper": papers[pid]["ee"] if pid else None,
              "status": papers[pid]["status"] if pid else None})
out["human_workbook"] = {
    "file": "Extraction_Workbook_v2_A.xlsx", "sheet": "Extraction Form", "keys": len(H),
    "title_resolved_to_one_live_paper": sum(1 for h in H if h["live_paper_by_title"]),
    "key_is_EE_papers_id": sum(1 for h in H if h["live_paper_by_title"] and h["live_paper_by_title"] == h["paper_named_by_EE_id"]),
    "key_is_ee_identifier": sum(1 for h in H if h["live_paper_by_title"] and h["live_paper_by_title"] == h["paper_named_by_ee_identifier"]),
    "key_also_an_ee_identifier_of_a_DIFFERENT_paper": [
        {"key": h["key"], "human_side_paper": h["live_paper_by_title"], "human_title": h["title"],
         "live_paper_with_that_ee_identifier": h["paper_named_by_ee_identifier"],
         "live_title": papers[h["paper_named_by_ee_identifier"]]["title"][:90]}
        for h in H if h["live_paper_by_title"] and h["paper_named_by_ee_identifier"]
        and h["paper_named_by_ee_identifier"] != h["live_paper_by_title"]],
    "key_not_any_ee_identifier": [h["key"] for h in H if h["paper_named_by_ee_identifier"] is None],
    "unresolved_titles": [h for h in H if not h["live_paper_by_title"]],
    "statuses": {}, "rows": H}
for h in H:
    out["human_workbook"]["statuses"][h["status"] or "?"] = out["human_workbook"]["statuses"].get(h["status"] or "?", 0) + 1
# M — the concordance manifest
M = list(csv.DictReader(open(os.path.join(review, "concordance_pdfs", "paper_manifest.csv"), encoding="utf-8"), delimiter="|"))
out["concordance_manifest"] = {
    "rows": len(M),
    "ee_id_equals_live_ee_identifier_of_db_id": sum(1 for m in M if papers.get(int(m["db_id"]), {}).get("ee") == m["ee_id"]),
    "ee_id_equals_EE_db_id": sum(1 for m in M if m["ee_id"] == "EE-%03d" % int(m["db_id"])),
    "disagree_with_live_ee_identifier": [(m["ee_id"], int(m["db_id"]), papers.get(int(m["db_id"]), {}).get("ee")) for m in M
                                         if papers.get(int(m["db_id"]), {}).get("ee") != m["ee_id"]][:20]}
# P — pdf file names
P = [(p["id"], p["ee"], os.path.basename(p["pdf"])) for p in papers.values() if p["pdf"] and re.match(r"EE-\d{3}_", os.path.basename(p["pdf"]))]
out["pdf_file_names"] = {"papers_with_EE_named_pdf": len(P),
                         "file_EE_equals_ee_identifier": sum(1 for i, e, f in P if f[:6] == e),
                         "file_EE_equals_EE_id": sum(1 for i, e, f in P if f[:6] == "EE-%03d" % i),
                         "neither": [(i, e, f) for i, e, f in P if f[:6] != e and f[:6] != "EE-%03d" % i][:10]}
json.dump(out, sys.stdout, indent=1)
