"""12r-CLOSE — R652(a)'s rule over the staged diff of the plan, with the expected rows as an argument (R664(c), R668(e)).
usage, from the repository root: prefix_check.py --expected-row <ROW-ID> [<ROW-ID> ...]
  (runs `git diff --cached -U0` on the plan; stdout: report)
Each "−" line, minus its closing "|", must be a strict prefix of exactly one "+" line in the same diff that ends
with "|"; every "−" line must be a line of one of the named rows (its first cell is the row id), and each named
row must contribute exactly one "−" line. Exit 0 when all hold, 1 otherwise.
The change from 12q_close/prefix_check.py: the row set was hard-coded to {"INT-p12-2"} with exactly one "−" line;
here it is the --expected-row list and the "−" count is its length. The per-line rule is unchanged."""
import argparse, subprocess, sys
PLAN = "docs/plan/ENGINE_REFACTOR_PLAN.md"
ap = argparse.ArgumentParser(); ap.add_argument("--expected-row", nargs="+", required=True)
NAMES = ap.parse_args().expected_row
diff = subprocess.run(["git", "diff", "--cached", "-U0", "--", PLAN], capture_output=True, text=True, check=True).stdout
minus, plus = [], []
for line in diff.split("\n"):
    if line.startswith(("---", "+++")): continue
    if line.startswith("-"): minus.append(line[1:])
    elif line.startswith("+"): plus.append(line[1:])
print(f"diff of {PLAN} (staged): {len(minus)} '-' lines, {len(plus)} '+' lines; expected rows: {', '.join(NAMES)}")
ok, seen = True, []
for m in minus:
    name = m.split("|")[1].strip() if m.startswith("| ") else None
    body = m[:-1] if m.endswith("|") else None
    hits = [p for p in plus if body is not None and p.endswith("|") and p[:-1].startswith(body) and len(p) > len(m)]
    passed = len(hits) == 1 and name in NAMES and name not in seen
    seen.append(name); ok &= passed
    print(f"  {name}: closing '|' {'present' if body is not None else 'MISSING'}; '+' lines extending it: {len(hits)}; "
          f"{'+' if hits else ''}{len(hits[0]) - len(m) if hits else 0} chars inserted before the closing '|'; {'PASS' if passed else 'FAIL'}")
rows_ok = len(minus) == len(NAMES) and sorted(seen) == sorted(NAMES)
ok &= rows_ok
print(f"exactly {len(NAMES)} '-' line(s), one per named row: {rows_ok}")
print("RESULT:", "PASS" if ok else "FAIL (STOP)")
sys.exit(0 if ok else 1)
