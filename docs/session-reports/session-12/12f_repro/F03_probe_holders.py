"""F03: does the real `_refuse_if_open` refuse an idle holder? DELETE-journal vs WAL,
holder in a SEPARATE PROCESS. Then `restore` over the held DELETE target (scratch files only),
and what the holder sees afterwards. Synthetic files under ~/scratch/12f/g1/f03."""
import os, shutil, sqlite3, subprocess, sys, time
from pathlib import Path
ROOT = Path.home() / "scratch/12f/g1/f03"
shutil.rmtree(ROOT, ignore_errors=True); ROOT.mkdir(parents=True)
from engine.utils.db_backup import _refuse_if_open, restore, RestoreRefused

HOLDER = r'''
import sqlite3, sys
c = sqlite3.connect(sys.argv[1]); mode = sys.argv[2]
if mode == "idle":   c.execute("SELECT count(*) FROM t").fetchall()          # read, finished: idle
elif mode == "never_read": pass                                              # opened, nothing run
elif mode == "read_txn":  c.execute("BEGIN"); c.execute("SELECT count(*) FROM t").fetchall()
print("ready", flush=True)
for line in sys.stdin:
    if line.strip() == "q": break
    try: print(repr(c.execute(line).fetchall()), flush=True)
    except Exception as e: print("ERR", type(e).__name__, e, flush=True)
c.close()
'''
def make(path, journal, val):
    c = sqlite3.connect(str(path)); c.execute(f"PRAGMA journal_mode={journal}")
    c.execute("CREATE TABLE t (v TEXT)"); c.execute("INSERT INTO t VALUES (?)", (val,)); c.commit(); c.close()
def hold(path, mode):
    p = subprocess.Popen([sys.executable, "-c", HOLDER, str(path), mode], stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, text=True)
    assert p.stdout.readline().strip() == "ready"; return p
def ask(p, sql): p.stdin.write(sql + "\n"); p.stdin.flush(); return p.stdout.readline().strip()
def stop(p): p.stdin.write("q\n"); p.stdin.flush(); p.wait(timeout=10)
def probe(path):
    try: _refuse_if_open(path); return "ALLOWED (no refusal)"
    except RestoreRefused as e: return "REFUSED"

for journal in ("DELETE", "WAL"):
    for mode in ("idle", "never_read", "read_txn"):
        t = ROOT / f"{journal}_{mode}.db"; make(t, journal, "target-original")
        h = hold(t, mode); print(f"{journal:6} holder={mode:10} (pid {h.pid}, other process) -> _refuse_if_open: {probe(t)}"); stop(h)

# restore() over an idle-held DELETE-journal target
bak = ROOT / "backup.db"; make(bak, "DELETE", "from-backup")
t = ROOT / "restore_target.db"; make(t, "DELETE", "target-original")
h = hold(t, "idle"); ino_before = os.stat(t).st_ino
try:
    restore(bak, t); print("restore() over idle-held DELETE target: COMPLETED, no refusal")
except RestoreRefused as e: print("restore() refused:", e)
print("  target inode changed:", os.stat(t).st_ino != ino_before)
print("  holder (still open on the OLD inode) reads:", ask(h, "SELECT v FROM t"))
print("  holder writes:", ask(h, "INSERT INTO t VALUES ('written-after-restore')"), ask(h, "COMMIT") if False else "")
print("  holder re-reads:", ask(h, "SELECT v FROM t"))
stop(h)
c = sqlite3.connect(f"file:{t}?mode=ro", uri=True); print("  file now at target path holds:", c.execute("SELECT v FROM t").fetchall()); c.close()
