"""include_data is all-or-nothing and is honoured on a FRESH database.
Only FAKE migrations are discoverable here (runner.discover is replaced), so no
real migration - in particular 003 - can execute. Scratch files only."""
import sqlite3, sys, types, tempfile
from pathlib import Path
from engine.migrations import runner

tmp = Path(tempfile.mkdtemp(dir="."))
BODY = ("def run_migration(db_path):\n    import sqlite3\n    c = sqlite3.connect(db_path)\n"
        "    c.execute('CREATE TABLE IF NOT EXISTS {t} (id INTEGER PRIMARY KEY)')\n"
        "    c.commit(); c.close()\n")
fakes = []
for mid, kind in (("901_fake_schema", "schema"), ("902_fake_data_old", "data"),
                  ("903_fake_data_wanted", "data")):
    p = tmp / f"{mid}.py"; p.write_text(BODY.format(t="t_" + mid))
    m = types.ModuleType(f"engine.migrations.{mid}"); exec(p.read_text(), m.__dict__)
    sys.modules[m.__name__] = m
    runner.KINDS[mid[:3]] = kind
    fakes.append((mid, p))
runner.discover = lambda: list(fakes)

def tables(path):
    c = sqlite3.connect(path)
    try: return sorted(r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE name LIKE 't_9%'"))
    finally: c.close()

# (a) a FRESH database (file does not exist yet), include_data=True
a = tmp / "fresh.db"
print("(a) fresh db, include_data=True ->", runner.run(a, include_data=True))
print("    tables:", tables(a))

# (b) the ordinary route: fresh build skips data; later the operator wants ONE data migration
b = tmp / "review.db"
print("(b) fresh build, default        ->", runner.run(b))
print("    receipts:", sorted(runner.receipts(sqlite3.connect(b))))
print("(b) --apply-pending --include-data ->", runner.run(b, include_data=True, apply_pending=True))
print("    tables:", tables(b))
