"""12l item 0 (12l-U04 brief) — applies R615's transcription to the plan: Block 13 and three dated
notes on INT-g12-1. R615 opens no row. Run once from the repository root; refuses if already
applied. --recount <file> adds the recount paragraph."""
import re, subprocess, sys
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
s = open(P, encoding="utf-8").read()
MD = subprocess.run(["git", "show", "bc93976:docs/session-reports/session-12/12l_U03/12l_U03.md"],
                    capture_output=True, text=True, check=True).stdout
ANCHOR = "\n### Not engine defects — plan v50 wording"
if "--recount" in sys.argv:
    para = open(sys.argv[sys.argv.index("--recount") + 1], encoding="utf-8").read().strip()
    assert s.count(ANCHOR) == 1 and "recounted 2026-10-11 (12l item 0, R615" not in s
    open(P, "w", encoding="utf-8").write(s.replace(ANCHOR, "\n" + para + "\n" + ANCHOR)); print("recount paragraph written"); sys.exit(0)
assert "R615" not in s, "already applied"

RULING = '''R615 — U03's findings (R509), on 12l_U03 (bc93976).
(a) F1 (cited drops repeated indices; a JUDGMENT field's field-level list is not
    separable from its step indices): a dated note on INT-g12-1; no row.
(b) F2 (Pass1Result.telemetry's docstring says the citation record must live outside
    the database): corrected in U04, which edits contracts.py; a dated note on
    INT-g12-1.
(c) F4 (CLAUDE.md's line on where the full citation set lives): corrected in U05's
    CLAUDE.md edit with R613(d)'s; a dated note on INT-g12-1.
(d) F3, F5: no row.'''
flat = " ".join(RULING.split())
def clause(l): return re.search(rf"\({l}\) (.*?)(?= \([a-d]\) |$)", flat).group(1).strip()
assert s.endswith("during session 12l |\n")
s = s.rstrip("\n") + f'''

**Block 13.** *Transcribed 2026-10-11 (12l item 0 of the 12l-U04 brief), in its own commit with the dated notes it orders, in the R597(e) form:* ruling R615, made by PI on architect recommendation during session 12l on unit U03's report (`docs/session-reports/session-12/12l_U03/12l_U03.md`, `bc93976`), carried in the 12l-U04 brief. The ruling as issued, with its line breaks:

```text
RULING (PI, 2026-10-11, session 12l, on architect recommendation; transcribe at item 0 as
R615 in Block 12's form)

{RULING}
```

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-10-11 | {flat} | PI, 2026-10-11, during session 12l |
'''
RO = "`12l_U03.md`"
def note(rid, src, body):
    global s
    lines = s.split("\n"); i = [k for k, l in enumerate(lines) if l.startswith(f"| {rid} |")][0]
    assert lines[i].endswith(" |") and " | " not in body
    lines[i] = lines[i][:-2] + f" *Note 2026-10-11 (12l item 0, {src}):* {body} |"; s = "\n".join(lines)
def has(x): assert x in MD, x[:50]; return x
f1 = has("`contracts._resolve_indices` drops a repeated index before `FieldRecord` is built (the repeat is the advisory `DUPLICATE_INDICES`, in telemetry only), and `check_entry` merges field-level and step indices into one list. D-c(1) asks for the indices \"exactly as the model returned them\"; the record gives them in the model's order, each once.")
f2 = has("`contracts.py::Pass1Result.telemetry`'s docstring still says the per-field citation record \"must live outside the database\".")
f4 = has("`CLAUDE.md` still says \"Full citation set lives in `record_call(extra=…)` and the per-run unit-map file (no migration).\"")
note("INT-g12-1", "R615(a); U03 F1", f"U03 landed at `bc93976` (payload key `citation_record` on every elicited `asserted` claim; the unit-map directory named by the manifest run). Its finding F1, as {RO} states it: \"{f1}\" R615(a): \"{clause('a')}\"")
note("INT-g12-1", "R615(b); U03 F2", f"U03's finding F2, as {RO} states it: \"{f2}\" R615(b): \"{clause('b')}\"")
note("INT-g12-1", "R615(c); U03 F4", f"U03's finding F4, as {RO} states it: \"{f4}\" R615(c): \"{clause('c')}\"")
open(P, "w", encoding="utf-8").write(s); print("applied")
