"""12l item 0 (12l-CLOSE brief) — applies R624's transcription to the plan: Block 22, two dated
notes on INT-i12-69, the "L (12l additions, R624)" table (INT-l12-25) and its coverage-map line. Run
once from the repository root; refuses if already applied. --recount <file> adds the recount
paragraph."""
import re, subprocess, sys
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
s = open(P, encoding="utf-8").read()
MD = subprocess.run(["git", "show", "65327f6:docs/session-reports/session-12/12l_U11/12l_U11.md"],
                    capture_output=True, text=True, check=True).stdout
ANCHOR = "\n### Not engine defects — plan v50 wording"
if "--recount" in sys.argv:
    para = open(sys.argv[sys.argv.index("--recount") + 1], encoding="utf-8").read().strip()
    assert s.count(ANCHOR) == 1 and "(12l item 0, R624), by script" not in s
    open(P, "w", encoding="utf-8").write(s.replace(ANCHOR, "\n" + para + "\n" + ANCHOR)); print("recount paragraph written"); sys.exit(0)
assert "R624" not in s and "INT-l12-25" not in s, "already applied"

RULING = '''R624 — U11's findings (R509), on 12l_U11 (65327f6).
(a) F1 (run_pipeline's extract stage returns, on an empty selection, stats without
    claimed_text_unreadable): new row, Class 3, P1, landing in U12 (R543 form).
(b) F2 (the in-loop text refusal writes an event each time) and F3 (a held paper whose
    text changed is not re-extracted; only the report says so): dated notes on
    INT-i12-69; F3's note puts the re-extraction question to the PI before U16.
(c) F4: no row.'''
flat = " ".join(RULING.split())
def clause(l): return re.search(rf"\({l}\) (.*?)(?= \([a-c]\) |$)", flat).group(1).strip()
assert s.endswith("during session 12l |\n")
s = s.rstrip("\n") + f'''

**Block 22.** *Transcribed 2026-10-12 (12l item 0 of the 12l-CLOSE brief), in its own commit with the dated notes and the row it orders, in the R597(e) form:* ruling R624, made by PI on architect recommendation during session 12l on unit U11's report (`docs/session-reports/session-12/12l_U11/12l_U11.md`, `65327f6`), carried in the 12l-CLOSE brief. The ruling as issued, with its line breaks:

```text
RULING (PI, 2026-10-11, session 12l, on architect recommendation; transcribe at item 0 as
R624 in Block 21's form)

{RULING}
```

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-10-11 | {flat} | PI, 2026-10-11, during session 12l |
'''
RO = "`12l_U11.md`"
def note(rid, src, body):
    global s
    lines = s.split("\n"); i = [k for k, l in enumerate(lines) if l.startswith(f"| {rid} |")][0]
    assert lines[i].endswith(" |") and " | " not in body
    lines[i] = lines[i][:-2] + f" *Note 2026-10-12 (12l item 0, {src}):* {body} |"; s = "\n".join(lines)
def has(x): assert x in MD, x[:50]; return x
f1 = has("`scripts/run_pipeline.py::_stage_extract`, on `if not selection.to_extract:`, builds `{\"extracted\": 0, \"skipped_asserted\": …, \"skipped_refused\": …}` and returns before `run_extraction`, so `run_extraction_summary` is not reached. That is the usual shape of the case this unit is about (every paper already extracted). The condition is still logged twice and written to the run's telemetry on that path; only the returned stats lack it.")
f2 = has("`_extract_selected` re-reads the file (`read_parsed_text(ref)`, R95) and, if it changed between selection and the read, calls `record_failure(pid, exc, \"extract_pass1\")` unconditionally. The window is one run, and the next run's selection applies the once-only rule, so a repeat needs the file to change in that window on successive runs.")
f3 = has("This is the contract (\"re-extracting a paper whose text changed\" is out of scope). The paper stays `audited_ai`; the audit's own refusal of the same file is `audit_events.py`'s.")
note("INT-i12-69", "R624(b); U11 F2", f"U11 landed at `e2d76a2` (selection decides \"already held under this key\" from the recorded reference before it reads the file; a held paper whose file is missing or altered is reported, not refused; a refusal event is written once per reason). Its finding F2, as {RO} states it: \"the in-loop text refusal still writes an event each time. {f2}\" R624(b): \"{clause('b')}\"")
note("INT-i12-69", "R624(b); U11 F3", f"a question for the PI before unit U16 — whether a paper the arm already holds, whose recorded text file has changed, is to be re-extracted. U11's finding F3, as {RO} states it: \"a held paper whose text changed is not re-extracted, and nothing but the report says its claims rest on bytes that are no longer on disk. {f3}\" R624(b): \"{clause('b')}\"")
rows = [("INT-l12-25", "when nothing is selected, `run_pipeline`'s extract stage returns its own stats and they do not carry the new entry. " + f1, "3", "P1, landing in unit U12 (R543 form)", "R624(a)", f"{RO} \"R509 findings\", F1 (`scripts/run_pipeline.py::_stage_extract`; `engine/core/extraction_events.py::run_extraction_summary`). R624(a): \"{clause('a')}\"")]
for r in rows: assert all(" | " not in x for x in r)
table = "\n".join("| " + " | ".join(r) + " |" for r in rows)
add = f'''**L (12l additions, R624).** *Opened 2026-10-12 (12l item 0 of the 12l-CLOSE brief) under R509 and R624.* One row from unit U11's report ({RO}, `65327f6`): `INT-l12-25` for its finding F1 (R624(a)). The text cell is the finding's heading and the report's own sentences, quoted unchanged; class and package are R624's. Not rows, as ruled: F2 and F3 are dated notes of this date on INT-i12-69 (R624(b)); F4 opens nothing (R624(c)).

| # | On disk (the unit report, quoted) | Class | Owning package | Governing ruling | Evidence |
| --- | --- | --- | --- | --- | --- |
{table}
'''
assert s.count(ANCHOR) == 1; s = s.replace(ANCHOR, "\n" + add + ANCHOR)
cov = "| INT-l12-24 | P7 — R623(a) |\n"; assert s.count(cov) == 1
s = s.replace(cov, cov + "| INT-l12-25 | P1, landing in U12 — R624(a) |\n")
open(P, "w", encoding="utf-8").write(s); print("applied")
