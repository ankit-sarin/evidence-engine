"""12l item 0 (12l-U05 brief) — applies R617's transcription to the plan: Block 15 and three dated
notes (INT-h12-2, INT-l12-17, INT-i12-4). R617 opens no row. Run once from the repository root;
refuses if already applied. --recount <file> adds the recount paragraph."""
import re, subprocess, sys
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
s = open(P, encoding="utf-8").read()
MD = subprocess.run(["git", "show", "dccd679:docs/session-reports/session-12/12l_U04/12l_U04b_addendum.md"],
                    capture_output=True, text=True, check=True).stdout
ANCHOR = "\n### Not engine defects — plan v50 wording"
if "--recount" in sys.argv:
    para = open(sys.argv[sys.argv.index("--recount") + 1], encoding="utf-8").read().strip()
    assert s.count(ANCHOR) == 1 and "(12l item 0, R617), by script" not in s
    open(P, "w", encoding="utf-8").write(s.replace(ANCHOR, "\n" + para + "\n" + ANCHOR)); print("recount paragraph written"); sys.exit(0)
assert "R617" not in s, "already applied"

RULING = '''R617 — U04b's findings (R509), on 12l_U04b_addendum (dccd679).
(a) F1 (the worked example shows only the field-level citation shape): a dated note on
    INT-h12-2. No prompt change before the rehearsal. The rehearsal's design counts
    JUDGMENT first-attempt VALUE_WITHOUT_CITATION separately, split by whether the
    field carried a field-level citation, because the check tightened at dccd679 and
    12g_RA-B was scored under the earlier check.
(b) F2: a dated note on INT-l12-17.
(c) F3: the session-close expected-values record computes pins with
    12l_U04/pin_tuples.py; a dated note on INT-i12-4.
(d) F4: no row.'''
flat = " ".join(RULING.split())
def clause(l): return re.search(rf"\({l}\) (.*?)(?= \([a-d]\) |$)", flat).group(1).strip()
assert s.endswith("during session 12l |\n")
s = s.rstrip("\n") + f'''

**Block 15.** *Transcribed 2026-10-12 (12l item 0 of the 12l-U05 brief), in its own commit with the dated notes it orders, in the R597(e) form:* ruling R617, made by PI on architect recommendation during session 12l on unit U04's addendum (`docs/session-reports/session-12/12l_U04/12l_U04b_addendum.md`, `dccd679`), carried in the 12l-U05 brief. The ruling as issued, with its line breaks:

```text
RULING (PI, 2026-10-11, session 12l, on architect recommendation; transcribe at item 0 as
R617 in Block 14's form)

{RULING}
```

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-10-11 | {flat} | PI, 2026-10-11, during session 12l |
'''
RO = "`12l_U04b_addendum.md`"
def note(rid, src, body):
    global s
    lines = s.split("\n"); i = [k for k, l in enumerate(lines) if l.startswith(f"| {rid} |")][0]
    assert lines[i].endswith(" |") and " | " not in body
    lines[i] = lines[i][:-2] + f" *Note 2026-10-12 (12l item 0, {src}):* {body} |"; s = "\n".join(lines)
def has(x): assert x in MD, x[:50]; return x
f1 = has("Its cases 2 and 3 cite at field level with no steps, on a field with no class. A JUDGMENT field answered in that shape failed before this unit (`STEPS_MISSING`) and fails now. The prompt shows no JUDGMENT field reporting an absence, where the citing step would be the one that cites the absence sentence.")
f2 = has("\"a value was asserted with nothing cited behind it\" is sent to a field that cited at field level; the sentence that names the defect is T-b, two sentences on. No new text was written (I1).")
f3 = has("`~/scratch/12i-scope/pins.py` hard-codes three extraction stages for both arms, so its elicited figure now includes a stage no elicited run declares. `12l_U04/pin_tuples.py` takes the stages from `extraction_stages` and prints the tuples.")
note("INT-h12-2", "R617(a); U04b F1", f"U04's commits 2 and 4 landed at `fe3ffe3` and `dccd679`; commit 4 is R616(c)'s check (a JUDGMENT field with no citing step is `VALUE_WITHOUT_CITATION`). Finding F1, as {RO} states it: \"the worked example shows only the field-level citation shape. {f1}\" R617(a): \"{clause('a')}\"")
note("INT-l12-17", "R617(b); U04b F2", f"Finding F2, as {RO} states it: \"the feedback's opening clause is inexact for the new case. {f2}\" R617(b): \"{clause('b')}\"")
note("INT-i12-4", "R617(c); U04b F3", f"U04's commit 2 landed at `fe3ffe3` (an elicited arm's pin records `segmentation`: pysbd, `min_unit_tokens`, `units_version`). Finding F3, as {RO} states it: \"the session-open pin script no longer computes the elicited pin a run would write. {f3}\" R617(c): \"{clause('c')}\"")
open(P, "w", encoding="utf-8").write(s); print("applied")
