"""Pre-commit check of the engine commit. usage: check_engine.py <parent evidence> <tree evidence> <pins> <pin reference>"""
import ast, re, subprocess, sys
PE, TE, PINS, REF = sys.argv[1:5]
run = lambda *a: subprocess.run(a, capture_output=True, text=True, check=True).stdout
ok = True
def chk(n, c):
    global ok; ok &= bool(c); print(f"{n}: {bool(c)}")
new = "tests/test_u21_no_overlap.py"
ids = [n.name for n in ast.parse(open(new).read()).body if isinstance(n, ast.FunctionDef) and n.name.startswith("test_")]
PIN = {"test_other_timeout_members_keep_their_retry"}
parent = open(PE).read(); tree = open(TE).read()
fails = set(re.findall(r"^FAILED tests/test_u21_no_overlap\.py::(\w+)", parent, flags=re.M))
chk(f"every id but the named pin fails on the parent ({len(fails)} of {len(ids)})", fails == set(ids) - PIN)
chk("the pin passes on the parent (R736(c))", not (PIN & fails))
chk("every id passes on the tree", all(re.search(rf"::{i} PASSED", tree) for i in ids) and " failed" not in tree.splitlines()[-1])
chk("pins byte-identical", open(PINS, "rb").read() == open(REF, "rb").read())
st = sorted(run("git", "diff", "--cached", "--name-only").split())
want = sorted(["engine/core/run_manifest.py", "engine/utils/ollama_client.py", "tests/test_ollama_client.py", "tests/test_restart_recovery_guard.py", new, "docs/inventory/entry_points.json", "docs/inventory/entry_points.md"])
chk("staged set exactly the two engine files, the tests the census named and the new one, the inventory", st == want)
chk("no migration", not [p for p in st if p.startswith("engine/migrations/")])
s = open("engine/core/run_manifest.py").read(); chk("the six details declared once", s.count("RUN_CALL_ENDING_DETAILS = (") == 1 and s.count('"worker_alive_at_read_timeout"') == 1)
chk("inventory in sync", subprocess.run([".venv/bin/python", "-m", "engine.tools.inventory", "--check"], capture_output=True).returncode == 0)
chk("worktree == index", run("git", "diff", "--name-only") == ""); chk("no untracked", run("git", "ls-files", "--others", "--exclude-standard") == "")
print("RESULT:", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
