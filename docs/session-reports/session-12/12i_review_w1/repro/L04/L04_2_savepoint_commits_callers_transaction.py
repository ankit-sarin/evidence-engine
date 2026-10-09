"""L04-2: the open_run tail (`RELEASE open_run` / `if conn.in_transaction: conn.commit()`)
run inside a caller's open transaction. Mechanism only, on an in-memory table:
the statements are the ones open_run issues around its inserts."""
import sqlite3, tempfile, os
d = tempfile.mkdtemp(); p = os.path.join(d, "t.db")
conn = sqlite3.connect(p); conn.execute("CREATE TABLE t(x)"); conn.commit()
conn.execute("INSERT INTO t VALUES ('caller write, uncommitted')")   # caller's open transaction
print("caller in_transaction before:", conn.in_transaction)
conn.execute("SAVEPOINT open_run")
conn.execute("INSERT INTO t VALUES ('manifest')")
conn.execute("RELEASE open_run")
print("in_transaction after RELEASE :", conn.in_transaction)
if conn.in_transaction:
    conn.commit()                                                    # open_run's line
conn.rollback()                                                      # caller now rolls back
print("rows after caller rollback   :", sqlite3.connect(p).execute("SELECT x FROM t").fetchall())
