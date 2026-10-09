"""update_status: when its own BEGIN IMMEDIATE fails, the handler issues ROLLBACK
with no transaction open, and that error replaces the real one.
ReviewDatabase is pointed at a scratch data_root."""
import sqlite3, tempfile, traceback
from pathlib import Path
from engine.core.database import ReviewDatabase, insert_paper_at_status

root = Path(tempfile.mkdtemp(dir=".")).resolve()
db = ReviewDatabase("synth", data_root=root)
pid = insert_paper_at_status(db._conn, status="INGESTED", title="t", source="s"); db._conn.commit()
db._conn.execute("PRAGMA busy_timeout=200")
holder = sqlite3.connect(db.db_path, isolation_level=None)
holder.execute("BEGIN IMMEDIATE")
try:
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
except Exception as e:
    print("raised:", type(e).__name__, "-", e)
    print("__context__:", type(e.__context__).__name__, "-", e.__context__)
holder.execute("ROLLBACK")
print("status after:", db._conn.execute("SELECT status FROM papers WHERE id=?", (pid,)).fetchone()[0])
db.close()
