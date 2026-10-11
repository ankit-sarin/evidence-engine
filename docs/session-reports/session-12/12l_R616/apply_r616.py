"""12l item 0 (12l-U04b brief) — applies R616's transcription to the plan: Block 14, a dated note on
INT-h12-2 and one on INT-i12-1, the "L (12l additions, R616)" table and its coverage-map line. Run
once from the repository root; refuses if already applied. --recount <file> adds the recount paragraph."""
import re, subprocess, sys
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
s = open(P, encoding="utf-8").read()
MD = subprocess.run(["git", "show", "a0b818e:docs/session-reports/session-12/12l_U04/12l_U04.md"],
                    capture_output=True, text=True, check=True).stdout
ANCHOR = "\n### Not engine defects — plan v50 wording"
if "--recount" in sys.argv:
    para = open(sys.argv[sys.argv.index("--recount") + 1], encoding="utf-8").read().strip()
    assert s.count(ANCHOR) == 1 and "recounted 2026-10-11 (12l item 0, R616" not in s
    open(P, "w", encoding="utf-8").write(s.replace(ANCHOR, "\n" + para + "\n" + ANCHOR)); print("recount paragraph written"); sys.exit(0)
assert "R616" not in s and "INT-l12-17" not in s, "already applied"

RULING = '''R616 — U04's stop and findings (R509), on 12l_U04 (a0b818e).
(a) I2: commit 2 proceeds as U04's brief specified. The one stored comparison found
    (concordance.check_schema_parity over extractions and cloud_extractions) is a
    non-blocking warning over legacy tables whose stored values on live are all NULL;
    no human-extractor path stores or compares the semantic hash. F5: no row.
(b) F1: the marker sentence stands as written: "A marker written into an index list is
    not valid JSON, and the field it is written in fails."
(c) F2: the check follows R571(b). A JUDGMENT field meets the citation requirement only
    when at least one reasoning step cites a unit; a field-level citation alone no
    longer meets it. Lands in U04 as commit 4, before the rehearsal. A dated note on
    INT-h12-2; no row.
(d) F3 (a text-less step fails as STEPS_MISSING, whose feedback does not name the
    defect): new row, Class 3, P7.
(e) F4: a dated note on INT-i12-1.'''
flat = " ".join(RULING.split())
def clause(l): return re.search(rf"\({l}\) (.*?)(?= \([a-e]\) |$)", flat).group(1).strip()
assert s.endswith("during session 12l |\n")
s = s.rstrip("\n") + f'''

**Block 14.** *Transcribed 2026-10-11 (12l item 0 of the 12l-U04b brief), in its own commit with the dated notes and the row it orders, in the R597(e) form:* ruling R616, made by PI on architect recommendation during session 12l on unit U04's report and stop (`docs/session-reports/session-12/12l_U04/12l_U04.md`, `a0b818e`), carried in the 12l-U04b brief. The ruling as issued, with its line breaks:

```text
RULING (PI, 2026-10-11, session 12l, on architect recommendation; transcribe at item 0 as
R616 in Block 13's form)

{RULING}
```

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-10-11 | {flat} | PI, 2026-10-11, during session 12l |
'''
RO = "`12l_U04.md`"
def note(rid, src, body):
    global s
    lines = s.split("\n"); i = [k for k, l in enumerate(lines) if l.startswith(f"| {rid} |")][0]
    assert lines[i].endswith(" |") and " | " not in body
    lines[i] = lines[i][:-2] + f" *Note 2026-10-11 (12l item 0, {src}):* {body} |"; s = "\n".join(lines)
def has(x): assert x in MD, x[:50]; return x
f2 = has("A JUDGMENT field with a field-level citation and steps that are all criteria applications passes: `VALUE_WITHOUT_CITATION` needs one citation anywhere, not one in a step. R571(b) rules the sentence, not a check.")
f4 = has("The step has a basis (its citation), and the marker reads as not set. The brief's sentence can be read as failing any non-boolean; Phase A's test names the no-indices case, which is what is built.")
f3 = has("The line says a JUDGMENT field \"needs `reasoning_steps`, not a bare value with a citation\". A dedicated code would need a new feedback sentence inside the pin.")
note("INT-h12-2", "R616(c); U04 F2", f"U04's commits 1 and 3 landed at `36de472` and `a0b818e` (items (a), (b) and (e) of this row). Its finding F2, as {RO} states it: \"T-b states more than the checker enforces. {f2}\" R616(c): \"{clause('c')}\"")
note("INT-i12-1", "R616(e); U04 F4", f"U04's commit 1 landed at `36de472` (a step that is not an object or whose `criteria_application` is not JSON `true` has no basis; a step with no text is `STEPS_MISSING`). Its finding F4, as {RO} states it: \"a non-boolean `criteria_application` on a step that also cites units does not fail the field. {f4}\" R616(e): \"{clause('e')}\"")
rows = [("INT-l12-17", "a text-less step fails as `STEPS_MISSING`, whose feedback line does not name the defect. " + f3, "3", "P7", "R616(d)", f"{RO} \"R509 findings\", F3 (`engine/elicitation/contracts.py::check_entry`; `engine/elicitation/prompts.py::_requirement`). R616(d): \"{clause('d')}\"")]
for r in rows: assert all(" | " not in x for x in r)
table = "\n".join("| " + " | ".join(r) + " |" for r in rows)
add = f'''**L (12l additions, R616).** *Opened 2026-10-11 (12l item 0 of the 12l-U04b brief) under R509 and R616.* One row from unit U04's report ({RO}, `a0b818e`): `INT-l12-17` for its finding F3 (R616(d)). The text cell is the finding's heading and the report's own sentences, quoted unchanged; class and package are R616's. Not rows, as ruled: F1 stands as written (R616(b)); F2 is a dated note of this date on INT-h12-2 (R616(c)); F4 is a dated note on INT-i12-1 (R616(e)); F5 opens nothing (R616(a)).

| # | On disk (the unit report, quoted) | Class | Owning package | Governing ruling | Evidence |
| --- | --- | --- | --- | --- | --- |
{table}
'''
assert s.count(ANCHOR) == 1; s = s.replace(ANCHOR, "\n" + add + ANCHOR)
cov = "| INT-l12-16 | P7 — R614(e) |\n"; assert s.count(cov) == 1
s = s.replace(cov, cov + "| INT-l12-17 | P7 — R616(d) |\n")
open(P, "w", encoding="utf-8").write(s); print("applied")
