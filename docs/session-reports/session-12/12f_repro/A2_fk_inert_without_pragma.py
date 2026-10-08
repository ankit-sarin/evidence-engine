"""A2 — on a synthetic review database built by ReviewDatabase (all migrations),
a writer connection WITHOUT `PRAGMA foreign_keys=ON` accepts rows whose FK parents
do not exist; the same INSERT on ReviewDatabase's own connection is refused.
The append-only triggers fire on both. Synthetic DB under the scratch cwd only."""
import sqlite3, tempfile, pathlib, uuid
from engine.core.database import ReviewDatabase

root = pathlib.Path(tempfile.mkdtemp(dir="."))
db = ReviewDatabase("synthetic_a2", data_root=root)
print("sqlite", sqlite3.sqlite_version)
print("ReviewDatabase._conn  PRAGMA foreign_keys =", db._conn.execute("PRAGMA foreign_keys").fetchone()[0])
raw = sqlite3.connect(str(db.db_path))
print("plain sqlite3.connect PRAGMA foreign_keys =", raw.execute("PRAGMA foreign_keys").fetchone()[0])

SQL = ("INSERT INTO paper_events (event_uid, event_type, occurred_at, recorded_at, actor_kind, "
       "actor_role, actor_name, run_id, run_marker, payload_json, paper_id, to_state, stage_name) "
       "VALUES (?, 'screened', 't', 't', 'model', 'reviewer', 'x', NULL, 'pre-manifest', '{}', ?, 'full_text_out', 's')")
def attempt(conn, label):
    try:
        conn.execute(SQL, (str(uuid.uuid4()), 999999)); conn.commit()
        print(f"{label}: INSERT paper_events(paper_id=999999, no such paper) -> ACCEPTED")
    except sqlite3.Error as e:
        conn.rollback(); print(f"{label}: INSERT -> REFUSED: {type(e).__name__}: {e}")
attempt(db._conn, "FK-on  (ReviewDatabase)")
attempt(raw,      "FK-off (plain connect)")
print("orphans now visible to foreign_key_check(paper_events):",
      raw.execute("PRAGMA foreign_key_check(paper_events)").fetchall())
for label, c in (("FK-off (plain connect)", raw), ("FK-on  (ReviewDatabase)", db._conn)):
    for stmt in ("UPDATE paper_events SET reason='edited'", "DELETE FROM paper_events"):
        try:
            c.execute(stmt); c.commit(); print(f"{label}: {stmt.split()[0]} -> ACCEPTED")
        except sqlite3.Error as e:
            c.rollback(); print(f"{label}: {stmt.split()[0]} -> REFUSED by trigger: {e}")
raw.close(); db.close()
