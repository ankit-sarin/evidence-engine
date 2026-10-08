"""A3 — a WAL-mode database with a committed, un-checkpointed write:
`mode=ro` sees the row, `immutable=1` does not. Synthetic file in the scratch cwd."""
import sqlite3, tempfile, pathlib
p = pathlib.Path(tempfile.mkdtemp(dir=".")) / "review.db"
w = sqlite3.connect(str(p)); w.execute("PRAGMA journal_mode=WAL")
w.execute("PRAGMA wal_autocheckpoint=0")
w.execute("CREATE TABLE papers (id INTEGER PRIMARY KEY, status TEXT)")
w.execute("INSERT INTO papers VALUES (1, 'INGESTED')"); w.commit()
w.execute("PRAGMA wal_checkpoint(TRUNCATE)")
w.execute("UPDATE papers SET status = 'FT_ELIGIBLE' WHERE id = 1")
w.execute("INSERT INTO papers VALUES (2, 'PARSED')"); w.commit()   # committed, in the WAL only
for mode in ("mode=ro", "immutable=1"):
    c = sqlite3.connect(f"file:{p}?{mode}", uri=True)
    print(f"{mode:12s} ->", c.execute("SELECT id, status FROM papers ORDER BY id").fetchall()); c.close()
w.close()
