"""12l item 0 (12l-U09 brief) — applies R621's transcription to the plan: Block 19, the
"L (12l additions, R621)" table (INT-l12-22) and its coverage-map line. Run once from the repository
root; refuses if already applied. --recount <file> adds the recount paragraph."""
import re, subprocess, sys
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
s = open(P, encoding="utf-8").read()
MD = subprocess.run(["git", "show", "8058399:docs/session-reports/session-12/12l_U08/12l_U08.md"],
                    capture_output=True, text=True, check=True).stdout
ANCHOR = "\n### Not engine defects — plan v50 wording"
if "--recount" in sys.argv:
    para = open(sys.argv[sys.argv.index("--recount") + 1], encoding="utf-8").read().strip()
    assert s.count(ANCHOR) == 1 and "(12l item 0, R621), by script" not in s
    open(P, "w", encoding="utf-8").write(s.replace(ANCHOR, "\n" + para + "\n" + ANCHOR)); print("recount paragraph written"); sys.exit(0)
assert "R621" not in s and "INT-l12-22" not in s, "already applied"

RULING = '''R621 — U08's findings (R509), on 12l_U08 (8058399).
(a) F1 (two readerless _LAST_PASS2_TELEMETRY writes in pipeline.py) and F2
    (FieldOutcome.attempts no longer reaches anything stored): one new row, Class 3,
    P7.
(b) F3: no row.'''
flat = " ".join(RULING.split())
def clause(l): return re.search(rf"\({l}\) (.*?)(?= \([a-b]\) |$)", flat).group(1).strip()
assert s.endswith("during session 12l |\n")
s = s.rstrip("\n") + f'''

**Block 19.** *Transcribed 2026-10-12 (12l item 0 of the 12l-U09 brief), in its own commit with the row it orders, in the R597(e) form:* ruling R621, made by PI on architect recommendation during session 12l on unit U08's report (`docs/session-reports/session-12/12l_U08/12l_U08.md`, `8058399`), carried in the 12l-U09 brief. The ruling as issued, with its line breaks:

```text
RULING (PI, 2026-10-11, session 12l, on architect recommendation; transcribe at item 0 as
R621 in Block 18's form)

{RULING}
```

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-10-11 | {flat} | PI, 2026-10-11, during session 12l |
'''
RO = "`12l_U08.md`"
def has(x): assert x in MD, x[:50]; return x
f1 = has("`_LAST_PASS2_TELEMETRY[\"n_contract_unmet\"]` and `[\"accepted_pass1_attempt\"]` are written beside the two removed here and read by nothing; both figures already reach the telemetry row through the `elicitation` extra.")
f2 = has("The record builders still set it; `plan_extraction_events` now writes the record's `spent` and `attempts`.")
rows = [("INT-l12-22", "F1 — two more readerless telemetry writes remain in `pipeline.py`. " + f1 + " F2 — `FieldOutcome.attempts` no longer reaches anything stored. " + f2, "3", "P7", "R621(a)", f"{RO} \"R509 findings\", F1 and F2 (`engine/elicitation/pipeline.py::extract_paper_elicited`; `engine/core/extraction_events.py::FieldOutcome`, `plan_extraction_events`). R621(a): \"{clause('a')}\"")]
for r in rows: assert all(" | " not in x for x in r)
table = "\n".join("| " + " | ".join(r) + " |" for r in rows)
add = f'''**L (12l additions, R621).** *Opened 2026-10-12 (12l item 0 of the 12l-U09 brief) under R509 and R621.* One row from unit U08's report ({RO}, `8058399`): `INT-l12-22` for its findings F1 and F2 together (R621(a)). The text cell is each finding's heading and the report's own sentences, quoted unchanged; class and package are R621's. Not a row, as ruled: F3 opens nothing (R621(b)).

| # | On disk (the unit report, quoted) | Class | Owning package | Governing ruling | Evidence |
| --- | --- | --- | --- | --- | --- |
{table}
'''
assert s.count(ANCHOR) == 1; s = s.replace(ANCHOR, "\n" + add + ANCHOR)
cov = "| INT-l12-21 | P7 — R620(d) |\n"; assert s.count(cov) == 1
s = s.replace(cov, cov + "| INT-l12-22 | P7 — R621(a) |\n")
open(P, "w", encoding="utf-8").write(s); print("applied")
