"""12q-U13-BUILD item 0 — R652(a)'s rule, for R660(a)'s closing note, over the staged diff of the plan.
usage, from the repository root: prefix_check_12q.py   (runs `git diff --cached -U0` on the plan; stdout: report)
Each "−" line, minus its closing "|", must be a strict prefix of a "+" line in the same diff that
ends with "|"; there must be exactly one "−" line, the INT-p12-2 row. Exit 0 when all hold, 1 otherwise."""
import subprocess, sys
PLAN = "docs/plan/ENGINE_REFACTOR_PLAN.md"
NAMES = {"INT-p12-2"}
diff = subprocess.run(["git", "diff", "--cached", "-U0", "--", PLAN], capture_output=True, text=True, check=True).stdout
minus, plus = [], []
for line in diff.split("\n"):
    if line.startswith(("---", "+++")): continue
    if line.startswith("-"): minus.append(line[1:])
    elif line.startswith("+"): plus.append(line[1:])
print(f"diff of {PLAN} (staged): {len(minus)} '-' lines, {len(plus)} '+' lines")
ok, seen = True, set()
for m in minus:
    name = m.split("|")[1].strip() if m.startswith("| ") else None
    body = m[:-1] if m.endswith("|") else None
    hits = [p for p in plus if body is not None and p.endswith("|") and p[:-1].startswith(body) and len(p) > len(m)]
    passed = len(hits) == 1 and name in NAMES and name not in seen
    seen.add(name); ok &= passed
    print(f"  {name}: closing '|' {'present' if body is not None else 'MISSING'}; '+' lines extending it: {len(hits)}; "
          f"{'+' if hits else ''}{len(hits[0]) - len(m) if hits else 0} chars inserted before the closing '|'; {'PASS' if passed else 'FAIL'}")
ok &= len(minus) == 1 and seen == NAMES
print(f"exactly one '-' line, the INT-p12-2 row: {len(minus) == 1 and seen == NAMES}")
print("RESULT:", "PASS" if ok else "FAIL (STOP)")
sys.exit(0 if ok else 1)
