"""12l item 0 (12l-U07 brief) — applies R619's transcription to the plan: Block 17, a dated note on
INT-l12-15 and two on A-11, the "L (12l additions, R619)" table (INT-l12-20) and its coverage-map
line. Run once from the repository root; refuses if already applied. --recount <file> adds the
recount paragraph."""
import re, subprocess, sys
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
s = open(P, encoding="utf-8").read()
MD = subprocess.run(["git", "show", "2fc5a0b:docs/session-reports/session-12/12l_U06/12l_U06.md"],
                    capture_output=True, text=True, check=True).stdout
ANCHOR = "\n### Not engine defects — plan v50 wording"
if "--recount" in sys.argv:
    para = open(sys.argv[sys.argv.index("--recount") + 1], encoding="utf-8").read().strip()
    assert s.count(ANCHOR) == 1 and "(12l item 0, R619), by script" not in s
    open(P, "w", encoding="utf-8").write(s.replace(ANCHOR, "\n" + para + "\n" + ANCHOR)); print("recount paragraph written"); sys.exit(0)
assert "R619" not in s and "INT-l12-20" not in s, "already applied"

RULING = '''R619 — U06's findings (R509), on 12l_U06 (2fc5a0b).
(a) T-d2's replacement is accepted as made: the old string differed only by a hard
    line wrap, and the comparability warning and the MIN_UNIT_TOKENS clause were
    verified unchanged. CLAUDE.md text rulings from now match verbatim apart from line
    wraps.
(b) F1: a dated note on INT-l12-15: migration 019's CHECK requires a reason for a
    failure state and enumerates no code.
(c) F2 (a stored exhausted record is counted failed in the run's stats, progress line,
    log and the distribution check's counts): new row, Class 2, P1, landing in U07.
(d) F3: the reason code held_fields_below_floor is confirmed.
(e) F4: the outer retry budget does not apply to a record refused at the write
    boundary; the two-attempt Pass-1 loop is its retry, and the paper is selected again
    on a later run. A dated note on A-11.
(f) F6: a dated note on A-11 for U16. F5, F7: no row.
(g) The gate's wall-time band is re-derived in the session-close expected values from
    this session's measurements; until then a gate above 843 s is reported, not a
    stop.'''
flat = " ".join(RULING.split())
def clause(l): return re.search(rf"\({l}\) (.*?)(?= \([a-g]\) |$)", flat).group(1).strip()
assert s.endswith("during session 12l |\n")
s = s.rstrip("\n") + f'''

**Block 17.** *Transcribed 2026-10-12 (12l item 0 of the 12l-U07 brief), in its own commit with the dated notes and the row it orders, in the R597(e) form:* ruling R619, made by PI on architect recommendation during session 12l on unit U06's report (`docs/session-reports/session-12/12l_U06/12l_U06.md`, `2fc5a0b`), carried in the 12l-U07 brief. The ruling as issued, with its line breaks:

```text
RULING (PI, 2026-10-11, session 12l, on architect recommendation; transcribe at item 0 as
R619 in Block 16's form)

{RULING}
```

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-10-11 | {flat} | PI, 2026-10-11, during session 12l |
'''
RO = "`12l_U06.md`"
def note(rid, src, body):
    global s
    lines = s.split("\n"); i = [k for k, l in enumerate(lines) if l.startswith(f"| {rid} |")][0]
    assert lines[i].endswith(" |") and " | " not in body
    lines[i] = lines[i][:-2] + f" *Note 2026-10-12 (12l item 0, {src}):* {body} |"; s = "\n".join(lines)
def has(x): assert x in MD, x[:50]; return x
f1 = has("The CHECK requires a reason for a failure state and none otherwise; it enumerates no code. `context_ceiling_unavailable` is declared in `paper_state.py` only.")
f2 = has("`_extract_selected` adds it to `stats[\"failed\"]`, reports it `FAILED` on the progress line and logs \"extraction failed\"; the store has it `extracted`. The real-state run's case C reads `extracted: 0, failed: 3, stored_incomplete: 3`. `failed` also feeds the distribution check's `failed_count`.")
f4 = has("The elicited path's all-unmet record fails on the first outer attempt (two Pass-1 calls), where a refused record gets three. R612(f) rules the floor \"at the write boundary, exhausted or not\" and does not say whether the retry budget applies.")
f6 = has("so INT-i12-73's \"all-unmet\" half of Wave 1 adjacent item 4 can no longer arise from a new extraction; an all-declined paper still can, and is now counted.")
note("INT-l12-15", "R619(b); U06 F1", f"this row's text cell quotes `12l_U02.md`, whose clause \"and migration 019's CHECK carries it\" is wrong. U06's finding F1, as {RO} states it: \"my 12l-U02 statement about migration 019's CHECK is wrong, and row INT-l12-15 quotes it. {f1}\" R619(b): \"{clause('b')}\"")
note("A-11", "R619(d), R619(e); U06 F3, F4", f"U06 landed at `e52c17e` (the floor of R535 as amended by R612(f): a record holding fewer than half of the expected fields is `extraction_failed` / `held_fields_below_floor`, exhausted or not; the attempt rule; `incomplete_fields` read by the run summary and PRISMA; the real-state run, `2fc5a0b`). R619(d): \"{clause('d')}\" U06's finding F4, as {RO} states it: \"a record under the floor that was not refused is not retried. {f4}\" R619(e): \"{clause('e')}\"")
note("A-11", "R619(f); U06 F6", f"for unit U16 — U06's finding F6, as {RO} states it: \"with the floor, a paper whose claims are all `contract_unmet` is no longer stored, {f6}\" R619(f): \"{clause('f')}\"")
rows = [("INT-l12-20", "an exhausted record that is stored is still counted as failed. " + f2, "2", "P1, landing in unit U07", "R619(c)", f"{RO} \"R509 findings\", F2 (`engine/agents/extractor.py::_extract_selected`; `real_state_run.json`, case C). R619(c): \"{clause('c')}\"")]
for r in rows: assert all(" | " not in x for x in r)
table = "\n".join("| " + " | ".join(r) + " |" for r in rows)
add = f'''**L (12l additions, R619).** *Opened 2026-10-12 (12l item 0 of the 12l-U07 brief) under R509 and R619.* One row from unit U06's report ({RO}, `2fc5a0b`): `INT-l12-20` for its finding F2 (R619(c)). The text cell is the finding's heading and the report's own sentences, quoted unchanged; class and package are R619's. Not rows, as ruled: F1 is a dated note of this date on INT-l12-15 (R619(b)); F3 and F4 are a dated note on A-11 (R619(d), R619(e)); F6 is a dated note on A-11 (R619(f)); F5 and F7 open nothing. R619(a) accepts U05's CLAUDE.md replacement as made, and R619(g) concerns the gate's wall-time band.

| # | On disk (the unit report, quoted) | Class | Owning package | Governing ruling | Evidence |
| --- | --- | --- | --- | --- | --- |
{table}
'''
assert s.count(ANCHOR) == 1; s = s.replace(ANCHOR, "\n" + add + ANCHOR)
cov = "| INT-l12-19 | P7 — R618(c) |\n"; assert s.count(cov) == 1
s = s.replace(cov, cov + "| INT-l12-20 | P1, landing in U07 — R619(c) |\n")
open(P, "w", encoding="utf-8").write(s); print("applied")
