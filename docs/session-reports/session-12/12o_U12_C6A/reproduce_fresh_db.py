"""12o-U12-C6A step 3 — B-F17 (g) reproduced. Run from the repo root with .venv/bin/python.

  reproduce_fresh_db.py blocked <data_root> <review_id>   both SDKs blocked before any engine import
  reproduce_fresh_db.py control <data_root> <review_id>   no blocker
  reproduce_fresh_db.py deps                               both SDKs blocked; import every module
                                                           engine/cloud/base.py imports at module
                                                           level, except engine.cloud.schema (whose
                                                           package __init__ is the defect)
Never touches data/: the database is built under <data_root>/<review_id>/.
"""
import sys, traceback, sqlite3
from pathlib import Path

mode = sys.argv[1]
if mode in ("blocked", "deps"):
    sys.modules["openai"] = None
    sys.modules["anthropic"] = None
sys.path.insert(0, str(Path.cwd()))

if mode == "deps":
    import importlib
    deps = ["engine.agents.extractor", "engine.core.codebook", "engine.agents.models",
            "engine.core.completeness", "engine.core.effective", "engine.core.effective_config",
            "engine.core.extraction_telemetry", "engine.core.parsed_text", "engine.core.review_spec"]
    for d in deps:
        importlib.import_module(d)
        print("imported", d)
    print("cloud/package modules loaded:", sorted(m for m in sys.modules if m.startswith("engine.cloud")))
    print("SDK modules loaded:", [m for m in ("openai", "anthropic") if sys.modules.get(m) is not None])
    sys.exit(0)

data_root, review_id = Path(sys.argv[2]), sys.argv[3]
assert not (data_root / review_id).exists(), "fresh path required"
from engine.core.database import ReviewDatabase
try:
    db = ReviewDatabase(review_id, data_root=data_root)
except BaseException as exc:
    print("RAISED", type(exc).__module__ + "." + type(exc).__name__)
    c = exc.__cause__
    print("__cause__:", None if c is None else f"{type(c).__name__}: {c}")
    traceback.print_exc(file=sys.stdout)
    sys.exit(1)
db_path = data_root / review_id / "review.db"
conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
rows = conn.execute("select migration_id, mode from schema_migrations order by migration_id").fetchall()
print("CONSTRUCTED", db_path)
print("receipts", len(rows))
for r in rows: print("  ", r[0], r[1])
print("cloud tables", [r[0] for r in conn.execute("select name from sqlite_master where type='table' and name like 'cloud_%' order by name")])
print("SDK modules loaded:", [m for m in ("openai", "anthropic") if sys.modules.get(m) is not None])
