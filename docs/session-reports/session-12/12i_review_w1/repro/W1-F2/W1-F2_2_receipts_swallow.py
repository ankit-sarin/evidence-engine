"""runner.receipts() turns ANY sqlite3.OperationalError into {} ("no receipts");
check_drift() then reports no drift. Shown with a lock held by another
connection on a rollback-journal scratch database, and with a receipts table of
a different shape. Scratch files only; nothing is migrated."""
import sqlite3, tempfile
from pathlib import Path
from engine.migrations import runner

tmp = Path(tempfile.mkdtemp(dir="."))
real = runner.discover()[0]            # a real id + path, only hashed, never run
db = tmp / "x.db"
c = sqlite3.connect(db); runner.ensure_receipts(c)
c.execute("INSERT INTO schema_migrations VALUES (?,?,?,?,?,?)",
          (real[0], "0" * 64, "t", "executed", 1, None)); c.commit()
print("unlocked: receipts ->", list(runner.receipts(c)), "| check_drift ->", runner.check_drift(c))

holder = sqlite3.connect(db, isolation_level=None)
holder.execute("BEGIN EXCLUSIVE")
r = sqlite3.connect(db, timeout=0.1)
try:
    r.execute("SELECT 1 FROM schema_migrations").fetchall()
except sqlite3.OperationalError as e:
    print("a plain read under the lock raises:", e)
print("locked:   receipts ->", runner.receipts(r), "| check_drift ->", runner.check_drift(r))
holder.execute("ROLLBACK")

db2 = tmp / "y.db"
c2 = sqlite3.connect(db2)
c2.execute("CREATE TABLE schema_migrations (migration_id TEXT PRIMARY KEY, file_sha256 TEXT, applied_at TEXT)")
c2.execute("INSERT INTO schema_migrations VALUES (?,?,?)", (real[0], "0" * 64, "t")); c2.commit()
print("other-shape receipts table (1 row): receipts ->", runner.receipts(c2))
