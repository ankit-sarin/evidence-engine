"""12g Rehearsal A, C1 — build the import input from live, read-only.
Live opened mode=ro; each text read through the engine's resolver (hash-verified) and
written byte-for-byte under <out>/texts/. Run from the repository root."""
import csv, hashlib, json, os, sqlite3, sys
sys.path.insert(0, os.getcwd())
from engine.core.parsed_text import read_parsed_bytes, resolve_parsed_text

ORDER = [121, 415, 11, 498, 607, 748, 368, 455, 699, 431, 783, 604]   # 12g_RA-A.md section 2
OUT = sys.argv[1]
want = {int(r["paper_id"]): r for r in csv.DictReader(
    open("docs/session-reports/session-12/12g_ra/ra_candidates.csv"))}
conn = sqlite3.connect("file:data/surgical_autonomy/review.db?mode=ro", uri=True)
conn.row_factory = sqlite3.Row
os.makedirs(os.path.join(OUT, "texts"), exist_ok=False)
papers, record = [], []
for n, pid in enumerate(ORDER, 1):
    ref = resolve_parsed_text(conn, pid)
    data = read_parsed_bytes(ref)
    sha = hashlib.sha256(data).hexdigest()
    assert sha == ref.sha256 == want[pid]["parsed_text_sha256"], pid
    assert str(ref.version) == want[pid]["parsed_text_version"], pid
    name = f"texts/{n:02d}_live{pid}_v{ref.version}.md"
    with open(os.path.join(OUT, name), "xb") as f:
        f.write(data)
    p = conn.execute("SELECT title, pmid, doi, abstract, authors, journal, year FROM papers WHERE id = ?", (pid,)).fetchone()
    authors = json.loads(p["authors"]) if p["authors"] else None
    papers.append({"title": p["title"], "pmid": p["pmid"], "doi": p["doi"], "abstract": p["abstract"],
                   "authors": authors, "journal": p["journal"], "year": p["year"], "text_path": name})
    record.append({"throwaway_paper_id": n, "live_paper_id": pid, "version": ref.version,
                   "sha256": sha, "bytes": len(data), "text_path": name})
with open(os.path.join(OUT, "entry.json"), "x") as f:
    json.dump({"source": "12g Rehearsal A: twelve parsed texts read from surgical_autonomy (live), 2026-10-08",
               "papers": papers}, f, indent=1)
with open(os.path.join(OUT, "id_map.json"), "x") as f:
    json.dump(record, f, indent=1)
for r in record:
    print(r["throwaway_paper_id"], r["live_paper_id"], f"v{r['version']}", r["bytes"], r["sha256"][:16])
