"""F03 (interval): a WAL connection that opens AFTER `_refuse_if_open` returns and BEFORE
`os.replace`. The real probe runs unmodified; a wrapper opens the second connection the moment
it returns. Scratch files only (~/scratch/12f/g1/f03b)."""
import shutil, sqlite3
from pathlib import Path
from unittest.mock import patch
ROOT = Path.home() / "scratch/12f/g1/f03b"
shutil.rmtree(ROOT, ignore_errors=True); ROOT.mkdir(parents=True)
from engine.utils import db_backup

def make(path, journal, val):
    c = sqlite3.connect(str(path)); c.execute(f"PRAGMA journal_mode={journal}")
    c.execute("CREATE TABLE t (v TEXT)"); c.execute("INSERT INTO t VALUES (?)", (val,)); c.commit(); c.close()
bak, t = ROOT / "backup.db", ROOT / "target.db"
make(bak, "DELETE", "from-backup"); make(t, "WAL", "target-original")
late = {}
real = db_backup._refuse_if_open
def probe_then_late_opener(target):
    real(target)                                    # the real probe: passes, lock already released
    c = sqlite3.connect(str(target)); c.execute("PRAGMA journal_mode=WAL")
    c.execute("INSERT INTO t VALUES ('committed-by-late-opener')"); c.commit()   # lives in -wal
    late["c"] = c
    print("late opener committed a row; sidecars:", sorted(p.name for p in ROOT.glob("target.db-*")))
with patch.object(db_backup, "_refuse_if_open", probe_then_late_opener):
    try:
        db_backup.restore(bak, t); print("restore(): COMPLETED, no refusal")
    except Exception as e: print("restore() raised:", type(e).__name__, str(e)[:120])
print("sidecars after restore:", sorted(p.name for p in ROOT.glob("target.db-*")))
c = late["c"]
for sql in ("SELECT v FROM t", "INSERT INTO t VALUES ('second write')"):
    try: print("late opener:", sql, "->", c.execute(sql).fetchall())
    except Exception as e: print("late opener:", sql, "-> ERR", type(e).__name__, e)
try: c.commit()
except Exception as e: print("late opener commit -> ERR", type(e).__name__, e)
c.close()
r = sqlite3.connect(f"file:{t}?mode=ro", uri=True)
print("file at target path holds:", r.execute("SELECT v FROM t").fetchall(),
      "| integrity:", r.execute("PRAGMA integrity_check").fetchone()[0]); r.close()
print("sidecars at end:", sorted(p.name for p in ROOT.glob("target.db-*")))
