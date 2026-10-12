"""12l item 0 (12l-U10 brief) — applies R622's transcription to the plan: Block 20, a dated note on
INT-i12-5, the "L (12l additions, R622)" table (INT-l12-23) and its coverage-map line. Run once
from the repository root; refuses if already applied. --recount <file> adds the recount paragraph."""
import re, subprocess, sys
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
s = open(P, encoding="utf-8").read()
MD = subprocess.run(["git", "show", "7293279:docs/session-reports/session-12/12l_U09/12l_U09.md"],
                    capture_output=True, text=True, check=True).stdout
ANCHOR = "\n### Not engine defects — plan v50 wording"
if "--recount" in sys.argv:
    para = open(sys.argv[sys.argv.index("--recount") + 1], encoding="utf-8").read().strip()
    assert s.count(ANCHOR) == 1 and "(12l item 0, R622), by script" not in s
    open(P, "w", encoding="utf-8").write(s.replace(ANCHOR, "\n" + para + "\n" + ANCHOR)); print("recount paragraph written"); sys.exit(0)
assert "R622" not in s and "INT-l12-23" not in s, "already applied"

RULING = '''R622 — U09's findings (R509), on 12l_U09 (7293279).
(a) F1 (an attempt-1 duplicate leaves no raw response in telemetry) and F3 (its log
    line names an empty arm), with F2's stale DuplicateFieldError docstring: one new
    row, Class 3, P7.
(b) F2: a dated note on INT-i12-5. Attempt 1's duplicate path stands for now. The
    rehearsal's design counts how often Pass 1 names a field twice, on attempt 1 and on
    attempt 2, and attempt 1's path is decided on that evidence.
(c) F4: no row.'''
flat = " ".join(RULING.split())
def clause(l): return re.search(rf"\({l}\) (.*?)(?= \([a-c]\) |$)", flat).group(1).strip()
assert s.endswith("during session 12l |\n")
s = s.rstrip("\n") + f'''

**Block 20.** *Transcribed 2026-10-12 (12l item 0 of the 12l-U10 brief), in its own commit with the dated note and the row it orders, in the R597(e) form:* ruling R622, made by PI on architect recommendation during session 12l on unit U09's report (`docs/session-reports/session-12/12l_U09/12l_U09.md`, `7293279`), carried in the 12l-U10 brief. The ruling as issued, with its line breaks:

```text
RULING (PI, 2026-10-11, session 12l, on architect recommendation; transcribe at item 0 as
R622 in Block 19's form)

{RULING}
```

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-10-11 | {flat} | PI, 2026-10-11, during session 12l |
'''
RO = "`12l_U09.md`"
def note(rid, src, body):
    global s
    lines = s.split("\n"); i = [k for k, l in enumerate(lines) if l.startswith(f"| {rid} |")][0]
    assert lines[i].endswith(" |") and " | " not in body
    lines[i] = lines[i][:-2] + f" *Note 2026-10-12 (12l item 0, {src}):* {body} |"; s = "\n".join(lines)
def has(x): assert x in MD, x[:50]; return x
f1 = has("The outer driver's retry rows take `extra` from `_LAST_PASS1_TELEMETRY[\"elicitation\"]`, which is cleared at the start of each attempt and never set when Pass 1 raises. `exc.telemetry` now carries that call's entry, and the driver (`extractor.py`, not in this unit) does not read it.")
f2 = has("The record is built from `{}`, so since U06's floor a paper whose Pass 1 names a field twice on attempt 1, three times running, fails under the floor rather than storing 19 fields. The docstring is in `completeness.py`.")
f3 = has("`check_response` raises `DuplicateFieldError(arm=\"\")` and the message is built then; the handler sets `exc.arm` afterwards, so the driver logs \"Paper 7 (): incomplete extraction\".")
note("INT-i12-5", "R622(b); U09 F2", f"U09 landed at `e816afa` (an attempt-2 response that names an expected field twice keeps attempt 1's result; both calls stay in the context chain and in telemetry). Its finding F2, as {RO} states it: \"`DuplicateFieldError`'s docstring says \\\"the rest of the paper stores\\\", and on the elicited Pass-1 path nothing else exists to store. {f2}\" R622(b): \"{clause('b')}\"".replace('\\"', '"'))
rows = [("INT-l12-23", "F1 — a duplicate on Pass-1 attempt 1 leaves no raw response in telemetry. " + f1 + " F3 — the refusal's log line names an empty arm. " + f3 + " With F2's docstring: `DuplicateFieldError`'s docstring says \"the rest of the paper stores\", and on the elicited Pass-1 path nothing else exists to store.", "3", "P7", "R622(a)", f"{RO} \"R509 findings\", F1, F3 and F2's docstring (`engine/agents/extractor.py::extract_paper_with_completeness`; `engine/elicitation/contracts.py::check_response`; `engine/core/completeness.py::DuplicateFieldError`). R622(a): \"{clause('a')}\"")]
for r in rows: assert all(" | " not in x for x in r)
table = "\n".join("| " + " | ".join(r) + " |" for r in rows)
add = f'''**L (12l additions, R622).** *Opened 2026-10-12 (12l item 0 of the 12l-U10 brief) under R509 and R622.* One row from unit U09's report ({RO}, `7293279`): `INT-l12-23` for its findings F1 and F3, with the stale docstring F2 names (R622(a)). The text cell is each finding's heading and the report's own sentences, quoted unchanged; class and package are R622's. Not rows, as ruled: F2's question about attempt 1's path is a dated note of this date on INT-i12-5 (R622(b)); F4 opens nothing (R622(c)).

| # | On disk (the unit report, quoted) | Class | Owning package | Governing ruling | Evidence |
| --- | --- | --- | --- | --- | --- |
{table}
'''
assert s.count(ANCHOR) == 1; s = s.replace(ANCHOR, "\n" + add + ANCHOR)
cov = "| INT-l12-22 | P7 — R621(a) |\n"; assert s.count(cov) == 1
s = s.replace(cov, cov + "| INT-l12-23 | P7 — R622(a) |\n")
open(P, "w", encoding="utf-8").write(s); print("applied")
