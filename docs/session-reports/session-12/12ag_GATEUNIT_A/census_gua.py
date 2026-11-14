r"""12ag-GATEUNIT-A Q5 — census of what depends on the present tmux launch. Read-only: text scan, no file is imported or run.
usage, from the repository root: python3 -I census_gua.py   (stdout: hits by place, then counts)
Places: every file under docs/session-reports/tools/; docs/plan/STANDING_RULES.md; the plan's block whose first line begins
"Expected values at session-12ag open" (from that line to the next line beginning "## ").
Patterns, each line matched against each (case-sensitive unless marked):
  tmux            r"\btmux\b"
  session-name    r"\$SESSION\b|session_name|<session_name>|\bsession name\b"   (the launcher's second argument and its uses)
  launch_line     r"launch_line\.txt"
  gate_launch     r"gate_launch\.txt"
  run_gate-proc   r"run_gate\.sh"   (every mention; the read-out classes which are a process pattern)
  durations-name  r"durations_\$SESSION|durations_<session>|durations_\{session\}"
A line is listed once per pattern it matches. Declared completeness: these places and patterns only; a dependency written
another way (a session name passed through a variable of another name, a process matched by pid) is not seen."""
import re
from pathlib import Path
PAT = {"tmux": re.compile(r"\btmux\b"), "session-name": re.compile(r"\$SESSION\b|session_name|<session_name>|\bsession name\b"),
       "launch_line": re.compile(r"launch_line\.txt"), "gate_launch": re.compile(r"gate_launch\.txt"),
       "run_gate-proc": re.compile(r"run_gate\.sh"), "durations-name": re.compile(r"durations_\$SESSION|durations_<session>|durations_\{session\}")}
places = [(str(p), p.read_text(encoding="utf-8", errors="replace").split("\n"), 1) for p in sorted(Path("docs/session-reports/tools").rglob("*")) if p.is_file()]
places.append(("docs/plan/STANDING_RULES.md", Path("docs/plan/STANDING_RULES.md").read_text(encoding="utf-8").split("\n"), 1))
L = Path("docs/plan/ENGINE_REFACTOR_PLAN.md").read_text(encoding="utf-8").split("\n")
s = next(i for i, l in enumerate(L) if l.startswith("Expected values at session-12ag open"))
e = next(i for i in range(s + 1, len(L)) if L[i].startswith("## "))
places.append((f"docs/plan/ENGINE_REFACTOR_PLAN.md (12ag expected-values block :{s + 1}–{e})", L[s:e], s + 1))
counts = {k: 0 for k in PAT}; per = {}
for name, lines, off in places:
    for i, l in enumerate(lines):
        for k, rx in PAT.items():
            if rx.search(l):
                counts[k] += 1; per[name] = per.get(name, 0) + 1
                print(f"{name}:{i + off} [{k}] {l.strip()[:200]}")
print("\n== counts by pattern"); [print(f"{k}: {v}") for k, v in counts.items()]
print("== counts by place"); [print(f"{k}: {v}") for k, v in per.items()]
print(f"== places scanned: {len(places)}")
