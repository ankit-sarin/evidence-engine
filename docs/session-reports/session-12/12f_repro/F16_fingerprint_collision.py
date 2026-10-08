"""F16 — B's two-database collision, on synthetic DBs in the cwd (scratch)."""
import os, sqlite3, subprocess, sys
from engine.tools.db_fingerprint import fingerprint, compare, encode_cell

def mk(name, rows, ddl="CREATE TABLE t (x TEXT, y TEXT)"):
    if os.path.exists(name): os.remove(name)
    c = sqlite3.connect(name); c.execute(ddl)
    c.executemany("INSERT INTO t VALUES (%s)" % ",".join("?"*len(rows[0])), rows); c.commit(); c.close()

def pair(label, a, b, ddl="CREATE TABLE t (x TEXT, y TEXT)"):
    mk("A.db", a, ddl); mk("B.db", b, ddl)
    fa, fb = fingerprint("A.db"), fingerprint("B.db")
    print(f"--- {label}")
    print("  A rows:", a, " B rows:", b)
    print("  A overall:", fa["overall_sha256"]); print("  B overall:", fb["overall_sha256"])
    print("  overall equal:", fa["overall_sha256"] == fb["overall_sha256"],
          "| table sha equal:", fa["tables"]["t"]["sha256"] == fb["tables"]["t"]["sha256"],
          "| row_count A/B:", fa["tables"]["t"]["row_count"], fb["tables"]["t"]["row_count"],
          "| compare() diffs:", compare(fa, fb))

# 1. B's exact pair: cell boundary moved
pair("B's exact pair (cell boundary)", [("a\x1fT:b", "c")], [("a", "b\x1fT:c")])
# 2. type confusion: a TEXT cell can impersonate an INTEGER cell and a NULL cell
pair("type confusion (TEXT carrying an encoded INTEGER / NULL; 3 untyped-affinity columns)", [("a\x1fI:5", "z", None)], [("a", 5, "z\x1fN")], "CREATE TABLE t (x, y, z)")
# 3. row boundary: one row whose text holds RS vs two rows -> hashes equal, row_count differs
mk("A.db", [("a\x1eT:b",)], "CREATE TABLE t (x TEXT)"); mk("B.db", [("a",), ("b",)], "CREATE TABLE t (x TEXT)")
fa, fb = fingerprint("A.db"), fingerprint("B.db")
print("--- row boundary (1 row with U+001E vs 2 rows)")
print("  table sha equal:", fa["tables"]["t"]["sha256"] == fb["tables"]["t"]["sha256"],
      "| overall equal:", fa["overall_sha256"] == fb["overall_sha256"],
      "| row_count A/B:", fa["tables"]["t"]["row_count"], fb["tables"]["t"]["row_count"])
print("  compare() diffs:", compare(fa, fb))
# 4. the real CLI on B's pair
mk("A.db", [("a\x1fT:b", "c")]); mk("B.db", [("a", "b\x1fT:c")])
subprocess.run([sys.executable, "-m", "engine.tools.db_fingerprint", "A.db", "--out", "A.fp.json"], stdout=subprocess.DEVNULL)
r = subprocess.run([sys.executable, "-m", "engine.tools.db_fingerprint", "B.db", "--compare", "A.fp.json"], capture_output=True, text=True)
print("--- CLI: fingerprint B.db --compare A.fp.json -> exit", r.returncode); print("  " + r.stdout.strip().splitlines()[-1])
print("stored values differ:", sqlite3.connect("A.db").execute("select * from t").fetchall(), sqlite3.connect("B.db").execute("select * from t").fetchall())
