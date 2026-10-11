"""12l item 0 (12l-U03 brief) — applies R614's transcription to the plan: Block 12, a dated note
on C56, the "L (12l additions, R614)" table and its coverage-map lines. Run once from the
repository root; refuses if already applied. --recount <file> adds the recount paragraph."""
import re, subprocess, sys
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
s = open(P, encoding="utf-8").read()
MD = subprocess.run(["git", "show", "13822e6:docs/session-reports/session-12/12l_U02/12l_U02.md"],
                    capture_output=True, text=True, check=True).stdout
ANCHOR = "\n### Not engine defects — plan v50 wording"
if "--recount" in sys.argv:
    para = open(sys.argv[sys.argv.index("--recount") + 1], encoding="utf-8").read().strip()
    assert s.count(ANCHOR) == 1 and "recounted 2026-10-11 (12l item 0, R614" not in s
    open(P, "w", encoding="utf-8").write(s.replace(ANCHOR, "\n" + para + "\n" + ANCHOR)); print("recount paragraph written"); sys.exit(0)
assert "R614" not in s and "INT-l12-14" not in s, "already applied"

RULING = '''R614 — U02's findings (R509), on 12l_U02 (13822e6).
(a) F1 (a run fault closes the manifest failed with no end_reason): new row, Class 2,
    P1, landing in U12; the manifest's end_reason names the exception class and
    message.
(b) F2 (context_ceiling_unavailable has no emitter): new row, Class 3, P7.
(c) F3: no row (N39).
(d) F4 (the builtin ConnectionError is broad, failing in the safe direction): a dated
    note on C56; no row.
(e) F5 (the proactive restart counter advances after a failed paper): new row,
    Class 3, P7.'''
flat = " ".join(RULING.split())
def clause(l): return re.search(rf"\({l}\) (.*?)(?= \([a-e]\) |$)", flat).group(1).strip()
assert s.endswith("during session 12l |\n")
s = s.rstrip("\n") + f'''

**Block 12.** *Transcribed 2026-10-11 (12l item 0 of the 12l-U03 brief), in its own commit with the dated note and the rows it orders, in the R597(e) form:* ruling R614, made by PI on architect recommendation during session 12l on unit U02's report (`docs/session-reports/session-12/12l_U02/12l_U02.md`, `13822e6`), carried in the 12l-U03 brief. The ruling as issued, with its line breaks:

```text
RULING (PI, 2026-10-11, session 12l, on architect recommendation; transcribe at item 0 as
R614 in Block 11's form)

{RULING}
```

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-10-11 | {flat} | PI, 2026-10-11, during session 12l |
'''
RO = "`12l_U02.md`"
def note(rid, src, body):
    global s
    lines = s.split("\n"); hits = [k for k, l in enumerate(lines) if l.startswith(f"| {rid} | *")]
    assert len(hits) == 1, hits; i = hits[0]
    assert lines[i].endswith(" |") and " | " not in body
    lines[i] = lines[i][:-2] + f" *Note 2026-10-11 (12l item 0, {src}):* {body} |"; s = "\n".join(lines)
def has(x): assert x in MD, x[:50]; return x
f4 = has("`BrokenPipeError` and `ConnectionResetError` are subclasses, so a broken pipe raised inside `extract_paper_with_completeness` for any reason stops the run. That is the safe direction, and the progress reporter no longer raises one.")
note("C56", "R614(d); U02 F4", f"U02 landed at `13822e6` (one run-fault predicate, re-raised ahead of extract-path handlers 1 and 2). Its finding F4, as {RO} states it: \"a builtin `ConnectionError` is broad. {f4}\" R614(d): \"{clause('d')}\"")
f1 = has("`scripts/run_pipeline.py::run_pipeline`. With this unit more conditions end a run this way, and the manifest does not say which.")
f2 = has("`engine/core/paper_state.py` still declares it and migration 019's CHECK carries it.")
f5 = has("`_extract_selected` proceeds to the proactive restart after a failed paper** whatever the failure was; unchanged, seen while editing.").replace("**", "")
rows = [("INT-l12-14", "a run fault closes the manifest `failed` with no reason (I4). " + f1, "2", "P1, landing in unit U12", "R614(a)", f"{RO} \"R509 findings\", F1, and its \"I1–I4\" (`except Exception as exc:` → `logger.error(\"Pipeline failed: %s\", exc, exc_info=True)` → `_finish_review_run(db, run_id, \"failed\")`). R614(a): \"{clause('a')}\""),
        ("INT-l12-15", "`context_ceiling_unavailable` has no emitter. " + f2, "3", "P7", "R614(b)", f"{RO} \"R509 findings\", F2 (`engine/core/extraction_events.py::outcome_for_exception`; `tests/test_extraction_events.py::test_t12_every_code_the_mapping_emits_is_in_the_closed_set`). R614(b): \"{clause('b')}\""),
        ("INT-l12-16", f5, "3", "P7", "R614(e)", f"{RO} \"R509 findings\", F5 (`engine/agents/extractor.py::_extract_selected`). R614(e): \"{clause('e')}\"")]
for r in rows: assert all(" | " not in x for x in r)
table = "\n".join("| " + " | ".join(r) + " |" for r in rows)
add = f'''**L (12l additions, R614).** *Opened 2026-10-11 (12l item 0 of the 12l-U03 brief) under R509 and R614.* Three rows from unit U02's report ({RO}, `13822e6`): `INT-l12-14` for its finding F1 (R614(a)), `INT-l12-15` for F2 (R614(b)) and `INT-l12-16` for F5 (R614(e)). The text cell is the finding's heading and the report's own sentences, quoted unchanged; class and package are R614's. Not rows, as ruled: F3 opens nothing (R614(c): "{clause('c')}"); F4 is a dated note of this date on C56 (R614(d)).

| # | On disk (the unit report, quoted) | Class | Owning package | Governing ruling | Evidence |
| --- | --- | --- | --- | --- | --- |
{table}
'''
assert s.count(ANCHOR) == 1; s = s.replace(ANCHOR, "\n" + add + ANCHOR)
cov = "| INT-l12-13 | P7 — R613(c) |\n"; assert s.count(cov) == 1
s = s.replace(cov, cov + "| INT-l12-14 | P1, landing in U12 — R614(a) |\n| INT-l12-15 | P7 — R614(b) |\n| INT-l12-16 | P7 — R614(e) |\n")
open(P, "w", encoding="utf-8").write(s); print("applied")
