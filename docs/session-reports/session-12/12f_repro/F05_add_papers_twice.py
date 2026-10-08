"""F05 — real ReviewDatabase (data_root = scratch), real add_papers, same DOI-only citation twice."""
import os, sys
from pathlib import Path
root = Path(os.path.expanduser("~/scratch/12f/g2/f05_data"))
assert "evidence-engine" not in str(root)
from engine.core.database import ReviewDatabase
from engine.search.models import Citation
db = ReviewDatabase("f05_synth", data_root=root)
print("db_path:", db.db_path)
cit = Citation(source="openalex", title="A DOI-only record", doi="10.1000/doi-only")
print("first  add_papers ->", db.add_papers([cit]))
print("second add_papers ->", db.add_papers([cit]))
print("same-call pair    ->", db.add_papers([cit, cit]))
for r in db._conn.execute("SELECT id, pmid, doi, title, status FROM papers"): print("   row:", tuple(r))
print("rows:", db._conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0])
p = Citation(source="pubmed", title="A PMID record", pmid="999", doi="10.1000/p")
print("pmid first/second ->", db.add_papers([p]), db.add_papers([p]))
print("schema_migrations:", db._conn.execute("SELECT COUNT(*), MAX(migration_id) FROM schema_migrations").fetchone()[:])
print("indexes on papers:", [r[1] for r in db._conn.execute("PRAGMA index_list(papers)")])
