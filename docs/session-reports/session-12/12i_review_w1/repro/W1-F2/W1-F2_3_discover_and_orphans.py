"""discover() only sees files matching ^\\d{3}_[a-z0-9_]+\\.py$; a numbered file
outside that shape is silently not a migration, two files may share a number,
and a receipt whose file is gone is not drift. MIGRATIONS_DIR is pointed at a
scratch directory; nothing is executed."""
import sqlite3, tempfile
from pathlib import Path
from engine.migrations import runner

tmp = Path(tempfile.mkdtemp(dir="."))
for n in ("004_ok.py", "023_AddThing.py", "024-add-thing.py", "0025_four_digits.py",
          "005_a.py", "005_b.py"):
    (tmp / n).write_text("def run_migration(db_path): pass\n")
runner.MIGRATIONS_DIR = tmp
print("files   :", sorted(p.name for p in tmp.glob("*.py")))
print("discover:", [m for m, _ in runner.discover()], "(023/024/0025 have no KINDS entry; no refusal)")

db = tmp / "x.db"; c = sqlite3.connect(db); runner.ensure_receipts(c)
c.execute("INSERT INTO schema_migrations VALUES ('010_deleted_or_renamed','f'||'0','t','executed',1,NULL)")
c.commit()
print("receipt for a file no longer on disk -> check_drift:", runner.check_drift(c))
