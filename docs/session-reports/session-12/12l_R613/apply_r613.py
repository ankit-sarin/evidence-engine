"""12l item 0 (12l-U02 brief) — applies R613's transcription to the plan: Block 11, two dated
notes on INT-g6-1, the "L (12l additions, R613)" table and its coverage-map lines. Run once from
the repository root; refuses if already applied. --recount <file> adds the recount paragraph."""
import re, subprocess, sys
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
s = open(P, encoding="utf-8").read()
MD = subprocess.run(["git", "show", "10cc974:docs/session-reports/session-12/12l_U01/12l_U01.md"],
                    capture_output=True, text=True, check=True).stdout
ANCHOR = "\n### Not engine defects — plan v50 wording"
if "--recount" in sys.argv:
    para = open(sys.argv[sys.argv.index("--recount") + 1], encoding="utf-8").read().strip()
    assert s.count(ANCHOR) == 1 and "recounted 2026-10-11 (12l item 0, R613" not in s
    open(P, "w", encoding="utf-8").write(s.replace(ANCHOR, "\n" + para + "\n" + ANCHOR)); print("recount paragraph written"); sys.exit(0)
assert "R613" not in s and "INT-l12-12" not in s, "already applied"

RULING = '''R613 — U01's findings (R509), on 12l_U01 (10cc974).
(a) F1 (the locator reports some raw-exact snippets as fuzzy): new row, Class 3, P1,
    closing with U14.
(b) F2 (212 units under three tokens, isolated between comments): a dated note on
    INT-g6-1; no row.
(c) F3 (pysbd ends a sentence at a hard line break): new row, Class 3, P7.
(d) F4 (CLAUDE.md's unit-construction wording is stale): corrected in U05's commit with
    the R237 sentence, by text quoted in U05's handoff for PI approval. A dated note on
    INT-g6-1 records it.
(e) F5: no row (INT-i12-4, N02).'''
flat = " ".join(RULING.split())
def clause(l): return re.search(rf"\({l}\) (.*?)(?= \([a-e]\) |$)", flat).group(1).strip()
assert s.endswith("during session 12l |\n")
s = s.rstrip("\n") + f'''

**Block 11.** *Transcribed 2026-10-11 (12l item 0 of the 12l-U02 brief), in its own commit with the dated notes and the rows it orders, in the R597(e) form:* ruling R613, made by PI on architect recommendation during session 12l on unit U01's report (`docs/session-reports/session-12/12l_U01/12l_U01.md`, `10cc974`), carried in the 12l-U02 brief. The ruling as issued, with its line breaks:

```text
RULING (PI, 2026-10-11, session 12l, on architect recommendation; transcribe at item 0 as
R613 in Block 10's form)

{RULING}
```

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-10-11 | {flat} | PI, 2026-10-11, during session 12l |
'''
RO = "`12l_U01.md`"
def note(rid, src, body):
    global s
    lines = s.split("\n"); i = [k for k, l in enumerate(lines) if l.startswith(f"| {rid} |")][0]
    assert lines[i].endswith(" |") and " | " not in body
    lines[i] = lines[i][:-2] + f" *Note 2026-10-11 (12l item 0, {src}):* {body} |"; s = "\n".join(lines)
def has(x): assert x in MD, x[:50]; return x
f2 = has("212 units in 114 files are under `MIN_UNIT_TOKENS`, each alone between two comments. It follows from \"no comment text in any unit\"; the module docstring states it.")
f4 = has("`CLAUDE.md` says units are \"ported\" and a materialised quote is \"ANCHORED by construction\"")
note("INT-g6-1", "R613(b); U01 F2", f"U01 landed at `10cc974` (units are slices of the raw text; a run is one slice; R534's EXACT check). Its finding F2, as {RO} states it: \"{f2}\" R613(b): \"{clause('b')}\"")
note("INT-g6-1", "R613(d); U01 F4", f"U01's finding F4, as {RO} states it: \"{f4}\". R613(d): \"{clause('d')}\"")
f1 = has("`locator.normalize` replaces a period glued between two word characters with a space. A snippet that ends or starts inside such a token (`tanner.a.` cut from `tanner.a.watts@…`, where pysbd ended a sentence) normalises differently from the same characters inside the paper. 20 of 11,129 sampled pairs; all still located.")
f3 = has("A hard-wrapped sentence is two units (unchanged behaviour, seen while writing the tests). Whether the parsed texts carry hard wraps inside sentences was not measured.")
rows = [("INT-l12-12", "locator-1 can report a raw-exact snippet as `fuzzy`. " + f1, "3", "P1, closing with unit U14", "R613(a)", f"{RO} \"R509 findings\", F1 (`engine/core/locator.py::normalize`; `12l_U01/corpus_check.json`, `locator_1_on_a_sample`). R613(a): \"{clause('a')}\""),
        ("INT-l12-13", "pysbd ends a sentence at a line break. " + f3, "3", "P7", "R613(c)", f"{RO} \"R509 findings\", F3 (`analysis/provenance/segment.py`, pysbd 0.3.4). R613(c): \"{clause('c')}\"")]
for r in rows: assert all(" | " not in x for x in r)
table = "\n".join("| " + " | ".join(r) + " |" for r in rows)
add = f'''**L (12l additions, R613).** *Opened 2026-10-11 (12l item 0 of the 12l-U02 brief) under R509 and R613.* Two rows from unit U01's report ({RO}, `10cc974`): `INT-l12-12` for its finding F1 (R613(a)) and `INT-l12-13` for F3 (R613(c)). The text cell is the finding's heading and the report's own sentences, quoted unchanged; class and package are R613's. Not rows, as ruled: F2 and F4 are dated notes of this date on INT-g6-1 (R613(b), R613(d)); F5 opens nothing (R613(e): "{clause('e')}").

| # | On disk (the unit report, quoted) | Class | Owning package | Governing ruling | Evidence |
| --- | --- | --- | --- | --- | --- |
{table}
'''
assert s.count(ANCHOR) == 1; s = s.replace(ANCHOR, "\n" + add + ANCHOR)
cov = "| INT-l12-11 | P7 — R612(p) |\n"; assert s.count(cov) == 1
s = s.replace(cov, cov + "| INT-l12-12 | P1, closing with U14 — R613(a) |\n| INT-l12-13 | P7 — R613(c) |\n")
open(P, "w", encoding="utf-8").write(s); print("applied")
