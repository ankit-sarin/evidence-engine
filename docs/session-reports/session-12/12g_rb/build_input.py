"""12g front-half rehearsal, C1 — build the screening-entry input from live, read-only
(live opened mode=ro). Records and order are rb_sample.csv's. Run from the repo root."""
import csv, json, os, sqlite3, sys
OUT = sys.argv[1]
rows = list(csv.DictReader(open("docs/session-reports/session-12/12g_rb/rb_sample.csv")))
conn = sqlite3.connect("file:data/surgical_autonomy/review.db?mode=ro", uri=True)
conn.row_factory = sqlite3.Row
os.makedirs(OUT, exist_ok=False)
papers, idmap = [], []
for r in rows:
    p = conn.execute("SELECT title, pmid, doi, abstract, authors, journal, year FROM papers WHERE id = ?", (int(r["live_paper_id"]),)).fetchone()
    assert len(p["abstract"] or "") == int(r["abstract_chars"]), r
    papers.append({"title": p["title"], "pmid": p["pmid"], "doi": p["doi"], "abstract": p["abstract"],
                   "authors": json.loads(p["authors"]) if p["authors"] else None, "journal": p["journal"], "year": p["year"]})
    idmap.append({"throwaway_paper_id": int(r["throwaway_paper_id"]), "live_paper_id": int(r["live_paper_id"]),
                  "abstract_chars": int(r["abstract_chars"]), "abstract_is_null": p["abstract"] is None})
with open(os.path.join(OUT, "entry.json"), "x") as f:
    json.dump({"source": "12g front-half rehearsal: 42 records read from surgical_autonomy (live), 2026-10-08", "papers": papers}, f, indent=1)
with open(os.path.join(OUT, "id_map.json"), "x") as f:
    json.dump(idmap, f, indent=1)
print(len(papers), "records;", sum(m["abstract_is_null"] for m in idmap), "null abstracts;", [m["live_paper_id"] for m in idmap if m["abstract_chars"] < 200])
