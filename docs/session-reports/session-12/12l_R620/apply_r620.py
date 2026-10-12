"""12l item 0 (12l-U08 brief) — applies R620's transcription to the plan: Block 18, a dated note on
B9 and one on A-11, the "L (12l additions, R620)" table (INT-l12-21) and its coverage-map line. Run
once from the repository root; refuses if already applied. --recount <file> adds the recount
paragraph."""
import re, subprocess, sys
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
s = open(P, encoding="utf-8").read()
MD = subprocess.run(["git", "show", "c4f1369:docs/session-reports/session-12/12l_U07/12l_U07.md"],
                    capture_output=True, text=True, check=True).stdout
ANCHOR = "\n### Not engine defects — plan v50 wording"
if "--recount" in sys.argv:
    para = open(sys.argv[sys.argv.index("--recount") + 1], encoding="utf-8").read().strip()
    assert s.count(ANCHOR) == 1 and "(12l item 0, R620), by script" not in s
    open(P, "w", encoding="utf-8").write(s.replace(ANCHOR, "\n" + para + "\n" + ANCHOR)); print("recount paragraph written"); sys.exit(0)
assert "R620" not in s and "INT-l12-21" not in s, "already applied"

RULING = '''R620 — U07's findings (R509), on 12l_U07 (c4f1369).
(a) F1 (the cloud distribution check skips on any failed paper or under 10 papers
    extracted in the run): a dated note on B9; closes in U28 with the cloud gate.
(b) F3: no row.
(c) F2: on a stored claim, attempts means the attempts spent on the paper; the attempt
    whose record was kept is recorded separately. Lands in U08. A dated note on A-11.
(d) F4 (UncitedValueError's name and first line are narrower than the offenders it
    carries): new row, Class 3, P7.'''
flat = " ".join(RULING.split())
def clause(l): return re.search(rf"\({l}\) (.*?)(?= \([a-d]\) |$)", flat).group(1).strip()
assert s.endswith("during session 12l |\n")
s = s.rstrip("\n") + f'''

**Block 18.** *Transcribed 2026-10-12 (12l item 0 of the 12l-U08 brief), in its own commit with the dated notes and the row it orders, in the R597(e) form:* ruling R620, made by PI on architect recommendation during session 12l on unit U07's report (`docs/session-reports/session-12/12l_U07/12l_U07.md`, `c4f1369`), carried in the 12l-U08 brief. The ruling as issued, with its line breaks:

```text
RULING (PI, 2026-10-11, session 12l, on architect recommendation; transcribe at item 0 as
R620 in Block 17's form)

{RULING}
```

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-10-11 | {flat} | PI, 2026-10-11, during session 12l |
'''
RO = "`12l_U07.md`"
def note(rid, src, body):
    global s
    lines = s.split("\n"); i = [k for k, l in enumerate(lines) if l.startswith(f"| {rid} |")][0]
    assert lines[i].endswith(" |") and " | " not in body
    lines[i] = lines[i][:-2] + f" *Note 2026-10-12 (12l item 0, {src}):* {body} |"; s = "\n".join(lines)
def has(x): assert x in MD, x[:50]; return x
f1 = has("`CloudExtractorBase.run_distribution_check` takes `run_post_extraction_check`'s defaults, and raises on a collapse. The local path does neither. Unreachable while R71 keeps `enabled_arms` empty.")
f2 = has("Since U06's attempt rule, three attempts refused the same way keep attempt 1's record, so the `contract_unmet` claim reads `attempts: 1` after a budget of three. R139's \"with `violation_codes` and `attempts`\" does not say which is meant.")
f4 = has("Its message lists the codes, so nothing is hidden; the class name and its first line (\"a value with no evidence behind it\") are narrower than what it carries.")
note("B9", "R620(a); U07 F1", f"U07's finding F1, as {RO} states it: \"the cloud path's distribution check skips on any failed paper and under 10 papers extracted in the run (I3). {f1}\" The same report's I3: on the local path `run_pipeline._distribution_check` passes `skip_on_failures=False, min_population=\"arm\"`, so a failed paper is recorded and is not a veto. R620(a): \"{clause('a')}\"")
note("A-11", "R620(c); U07 F2", f"U07 landed at `ab240d9` (a stored exhausted record is counted extracted, INT-l12-20). Its finding F2, as {RO} states it: \"`attempts` on a stored claim is the attempt its record was made at, not the attempts spent. {f2}\" R620(c): \"{clause('c')}\"")
rows = [("INT-l12-21", "`UncitedValueError` now also carries offenders that are not uncited values. " + f4, "3", "P7", "R620(d)", f"{RO} \"R509 findings\", F4 (`engine/core/citation_guard.py::UncitedValueError`, `BLANK_VALUE`, `NON_VALUE_TOKEN_AS_VALUE`). R620(d): \"{clause('d')}\"")]
for r in rows: assert all(" | " not in x for x in r)
table = "\n".join("| " + " | ".join(r) + " |" for r in rows)
add = f'''**L (12l additions, R620).** *Opened 2026-10-12 (12l item 0 of the 12l-U08 brief) under R509 and R620.* One row from unit U07's report ({RO}, `c4f1369`): `INT-l12-21` for its finding F4 (R620(d)). The text cell is the finding's heading and the report's own sentences, quoted unchanged; class and package are R620's. Not rows, as ruled: F1 is a dated note of this date on B9 (R620(a)); F2 is a dated note on A-11 and lands in U08 (R620(c)); F3 opens nothing (R620(b)).

| # | On disk (the unit report, quoted) | Class | Owning package | Governing ruling | Evidence |
| --- | --- | --- | --- | --- | --- |
{table}
'''
assert s.count(ANCHOR) == 1; s = s.replace(ANCHOR, "\n" + add + ANCHOR)
cov = "| INT-l12-20 | P1, landing in U07 — R619(c) |\n"; assert s.count(cov) == 1
s = s.replace(cov, cov + "| INT-l12-21 | P7 — R620(d) |\n")
open(P, "w", encoding="utf-8").write(s); print("applied")
