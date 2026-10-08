"""F02: what ReviewDatabase.__init__ writes before runner.run can raise PendingMigrations.
Synthetic DB built by the engine's own constructor under ~/scratch/12f/g1/f02.
"Receipts through 021, 022 pending" is arranged by DELETING THE 022 RECEIPT ROW ONLY
(022's schema stays applied; the runner decides 'pending' from receipts alone)."""
import gc, logging, shutil, sqlite3
logging.disable(logging.CRITICAL)
from pathlib import Path
ROOT = Path.home() / "scratch/12f/g1/f02"
shutil.rmtree(ROOT, ignore_errors=True); ROOT.mkdir(parents=True)

from engine.core.database import ReviewDatabase
from engine.migrations import runner
from engine.search.models import Citation
from engine.tools.db_fingerprint import fingerprint

def ro(p): return sqlite3.connect(f"file:{p}?mode=ro", uri=True)
def state(p):
    c = ro(p)
    try:
        return {"review_runs": c.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='review_runs'").fetchone()[0],
                "tables": c.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0],
                "journal_mode": c.execute("PRAGMA journal_mode").fetchone()[0],
                "receipts": c.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
                            if c.execute("SELECT 1 FROM sqlite_master WHERE name='schema_migrations'").fetchone() else None}
    finally: c.close()

# ---- build
db = ReviewDatabase("syn", data_root=ROOT)
db.add_papers([Citation(title=f"P{i}", source="pubmed", pmid=str(i)) for i in range(5)])
p = db.db_path; db._conn.close()
def rc(p):
    c = ro(p)
    try: return runner.receipts(c)
    finally: c.close()
print("built; receipts:", sorted(r[:3] for r in rc(p)))

# ---- arrange: 022 pending, review_runs dropped, journal switched to DELETE, a subdir removed
w = sqlite3.connect(str(p))
w.execute("DELETE FROM schema_migrations WHERE migration_id LIKE '022%'")
w.execute("DROP TABLE review_runs"); w.commit()
w.execute("PRAGMA journal_mode=DELETE"); w.close()
(ROOT / "syn" / "vector_store").rmdir()
shutil.copy2(p, ROOT / "b_copy.db")           # scratch-only copy for part (b); no sidecars exist (DELETE mode)
before, fp_before = state(p), fingerprint(p)["overall_sha256"]
print("(a) BEFORE refused open:", before, "| vector_store dir:", (ROOT/"syn"/"vector_store").exists())

try:
    ReviewDatabase("syn", data_root=ROOT)
    print("(a) constructed WITHOUT refusal")
except runner.PendingMigrations as e:
    print("(a) raised PendingMigrations:", str(e)[:112], "...")
    # (c) the finally block's reopened connection, on an object the caller never received
    leaked = [o for o in gc.get_objects() if isinstance(o, ReviewDatabase)]
    alive = []
    for o in leaked:
        try: o._conn.execute("SELECT 1"); alive.append(True)
        except sqlite3.ProgrammingError: alive.append(False)
    print("(c) ReviewDatabase objects alive after the raise:", len(leaked), "| their _conn usable:", alive)
    for o in leaked: o._conn.close()
    del leaked, o
after, fp_after = state(p), fingerprint(p)["overall_sha256"]
print("(a) AFTER  refused open:", after, "| vector_store dir:", (ROOT/"syn"/"vector_store").exists())
print("(a) content fingerprint changed:", fp_before != fp_after,
      "| sidecars now:", sorted(x.name for x in p.parent.glob("review.db-*")))

# ---- (b) populated database, NO receipts: is it 'fresh' to the runner?
root_b = ROOT / "b"; (root_b / "legacy").mkdir(parents=True)
pb = root_b / "legacy" / "review.db"; shutil.move(str(ROOT / "b_copy.db"), pb)
w = sqlite3.connect(str(pb)); w.execute("DELETE FROM schema_migrations"); w.commit()
print("(b) BEFORE:", state(pb), "| papers:", w.execute("SELECT COUNT(*) FROM papers").fetchone()[0]); w.close()
try:
    d = ReviewDatabase("legacy", data_root=root_b); d._conn.close()
    print("(b) constructed without refusal")
except Exception as e:
    print("(b) raised:", type(e).__name__, "-", str(e)[:150])
    for o in [o for o in gc.get_objects() if isinstance(o, ReviewDatabase)]: o._conn.close()
print("(b) AFTER :", state(pb))
print("(b) receipts now:", sorted(k[:3] for k in rc(pb)))
