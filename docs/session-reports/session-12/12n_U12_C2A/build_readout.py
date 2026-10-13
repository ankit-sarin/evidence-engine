"""12n-U12-C2A — assembles 12n_U12_C2A.md: the body below, with the A1 quotes cut from their
sources by content anchor (never retyped). usage, from the repository root: build_readout.py"""
import re
D = "docs/session-reports/session-12"
u12 = open(f"{D}/12m_U12/12m_U12A.md", encoding="utf-8").read()
plan = open("docs/plan/ENGINE_REFACTOR_PLAN.md", encoding="utf-8").read()
p1a = open(f"{D}/12l_P1A/12l_P1A.md", encoding="utf-8").read()
l11 = open(f"{D}/12j_review_w2/readers/L11.md", encoding="utf-8").read()
def between(s, a, b): i = s.index(a); return s[i:s.index(b, i + len(a))].rstrip("\n")
def line(s, start):
    hits = [l for l in s.split("\n") if l.startswith(start)]; assert len(hits) == 1, (start, len(hits)); return hits[0]
def quote(t): return "\n".join("> " + l if l else ">" for l in t.split("\n"))
Q = {
 "S101_HEAD": line(u12, "| # | Commit | Files | Tests | Rows |"),
 "S101_ROW2": line(u12, "| 2 | PROPOSED: every model-calling entry point holds the lock"),
 "S101_TAIL": line(u12, "The ten legacy scripts in §5.4 that call models with no lock"),
 "S102_C2": line(u12, "- **Commit 2 changes what the 07:00 job does during a run:**"),
 "S5": between(u12, "## 5. Entry-point lock census", "## 6. The nightly"),
 "N19": line(p1a, "| N19 | A-1 |"), "N22": line(p1a, "| N22 | INT-g12-7 |"),
 "L11C4": between(l11, "### L11-C4 —", "### L11-C5 —"),
 "A1": line(plan, "| A-1 | A's child-worker scenario"), "G127": line(plan, "| INT-g12-7 | An outer `flock(1)`"),
 "TABLE2": line(plan, "| 2 — every local model-calling entry point holds the lock"),
}
body = open(__file__.replace("build_readout.py", "readout_body.md"), encoding="utf-8").read()
for k, v in Q.items():
    assert "{{" + k + "}}" in body, k
    body = body.replace("{{" + k + "}}", quote(v))
assert "{{" not in body, re.findall(r"\{\{\w+\}\}", body)
open(f"{D}/12n_U12_C2A/12n_U12_C2A.md", "w", encoding="utf-8").write(body)
print("written", len(body))
