"""12ag-GATEUNIT-B 4e — C5 and C6 (and C7's extent) asserted from the diff of the three tools against HEAD, hunks listed.
Read-only; run from the repository root. Allowed changed lines (old numbering): launch_gate.sh :8 (the refusal) and :36–39 (the
launch block and the prints); run_gate.sh :2 only; README.md :10, :11 and an addition at the end of the file. No tmux in
launch_gate.sh. Exit 0 only if every assertion holds."""
import re, subprocess, sys
T = "docs/session-reports/tools/"
ALLOW = {"launch_gate.sh": ({8, 36, 37, 38, 39}, False), "run_gate.sh": ({2}, False), "README.md": ({10, 11}, True)}
fails = []
def ok(c, m): print(("PASS " if c else "FAIL ") + m); c or fails.append(m)
d = subprocess.run(["git", "diff", "--name-only", "HEAD"], capture_output=True, text=True, check=True).stdout.split()
ok(sorted(d) == sorted(T + f for f in ALLOW), f"the changed files are exactly the three tools: {d}")
for f, (lines, tail_ok) in ALLOW.items():
    old_n = len(subprocess.run(["git", "show", f"HEAD:{T}{f}"], capture_output=True, text=True, check=True).stdout.split("\n"))
    diff = subprocess.run(["git", "diff", "-U0", "HEAD", "--", T + f], capture_output=True, text=True, check=True).stdout
    hunks = [tuple(int(x or 1) for x in m.groups()) for m in re.finditer(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", diff, re.M)]
    print(f"{f}: hunks (old start, old count, new start, new count) {[(h[0], h[1] if re.search(rf'-{h[0]},', diff) else 1, h[2], h[3]) for h in hunks]}")
    bad = []
    for m in re.finditer(r"^@@ -(\d+)(?:,(\d+))? \+\d+(?:,\d+)? @@", diff, re.M):
        s, c = int(m.group(1)), int(m.group(2)) if m.group(2) is not None else 1
        touched = set(range(s, s + c)) if c else set()
        if c == 0 and not (tail_ok and s >= old_n - 2):
            touched = {s}  # a pure insertion after old line s
        if c == 0 and tail_ok and s >= old_n - 2:
            continue
        if c == 0 and s in lines:
            continue
        bad += sorted(touched - lines)
    ok(not bad, f"{f}: no changed line outside the allowed set {sorted(lines)}{' + the end of the file' if tail_ok else ''} (outside: {bad})")
lg = open(T + "launch_gate.sh").read()
ok(not re.search(r"\btmux\b", lg), "launch_gate.sh: no tmux remains")
print("RESULT:", "PASS" if not fails else f"FAIL ({len(fails)})"); sys.exit(0 if not fails else 1)
