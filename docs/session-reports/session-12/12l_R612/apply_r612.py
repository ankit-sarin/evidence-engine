"""12l-R612 — applies the transcription to docs/plan/ENGINE_REFACTOR_PLAN.md: Block 10 (R612),
the dated notes, the "L (12l additions)" table and its coverage-map lines. Run once from the
repository root; refuses if already applied. The recount paragraph is added by --recount <text-file>."""
import re, sys, subprocess
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
s = open(P, encoding="utf-8").read()
MD = subprocess.run(["git", "show", "a579915:docs/session-reports/session-12/12l_P1A/12l_P1A.md"], capture_output=True, text=True, check=True).stdout

if "--recount" in sys.argv:
    para = open(sys.argv[sys.argv.index("--recount") + 1], encoding="utf-8").read().strip()
    anchor = "\n### Not engine defects — plan v50 wording"
    assert s.count(anchor) == 1 and "recounted 2026-10-11 (12l-R612" not in s
    s = s.replace(anchor, "\n" + para + "\n" + anchor)
    open(P, "w", encoding="utf-8").write(s); print("recount paragraph written"); sys.exit(0)

assert "R612" not in s and "INT-l12-" not in s, "already applied"

RULING = '''R612 — P1 package plan, on 12l_P1A (a579915).
(a) The read-out is P1's package plan: units U01–U12 and U14–U36, the comparison
    rehearsal placed as U13, in §3's order with §3's acceptance tests, except as
    amended below.
(b) D1: Option A. The rehearsal's brief separates attempt-level measures (first-attempt
    failure codes per field) from stored-outcome measures; only the former are compared
    against 12g_RA-B's per-field results as R571's test. Stored outcomes are reported,
    not compared.
(c) D2: Option A. INT-i12-1 is pin-affecting and lands in U04 with R571(b).
(d) D3: Option A. extract_retry_snippet leaves the elicited stage set in U04. New row,
    Class 3, P1, pin-affecting.
(e) D4: amended. The sentence that makes a field's instruction to record a sentinel
    subject to the sentinel rule is placed in the STATED and the INFERABLE class
    contracts, next to their field blocks, not in the shared sentinel rule. The codebook
    is not edited (R26).
(f) D5: floor Option B. A field counts as held when it carries an asserted value, a
    cited sentinel or a declined outcome; contract_unmet does not count. The floor
    applies to every record at the write boundary, exhausted or not. The run summary
    counts papers whose held fields are all declined. Attempt rule: Option A (a later
    attempt replaces an earlier one only if strictly better; a tie keeps the earlier).
(g) D6: as recommended. One declared run-fault tuple, exact names fixed in U02's
    handoff; handlers 12–13 with U26; 14–15 to P7 with handler 3.
(h) D7: Design A. U05 includes the full-tuple comparison at the extract stage. The
    CLAUDE.md R237 sentence is replaced in U05's commit by text quoted in U05's handoff
    for PI approval.
(i) D8: Option A. (j) D9: Option A.
(k) D10: Option A, widened. Every model declaration in the spec carries a family. U17
    enforces the extraction→audit pair. The screening generator→verifier pairs are a
    Phase A item for P4, recorded as a dated note on INT-i12-45.
(l) D11: the four class moves accepted. (m) D12: 35 units accepted.
(n) D13: both as recommended. U21 opens with B-F14's measurement, under the lock.
(o) D14: Option A. reparse_cloud_spans.py and backfill_cloud_spans.py retire under R31.
    New row, Class 1 (conditional), P1, closing with U31.
(p) §5: N04, N10, N12, N16, N18, N32, N34, N35, N37 enter as new rows, Class 3, P7.
    Every other side finding takes the disposition §5 proposes.
(q) Handoff obligations: U01's and U04's handoffs first read A-5, INT-i12-39 and
    INT-i12-40. Any part of them that changes text inside the pin rides in that unit
    (R543 form). U12's handoff reads the citation-mcp nightly harness read-only and
    reports whether it loads Ollama models (N20).'''
flat = " ".join(RULING.split())
def clause(letter):
    m = re.search(rf"\({letter}\) (.*?)(?= \([a-q]\) |$)", flat)
    return m.group(1).strip()

# ---------------- Block 10
block = f'''

**Block 10.** *Transcribed 2026-10-11 (12l-R612), in its own commit with the dated notes and the rows it orders, in the R597(e) form:* ruling R612, made by PI on architect recommendation during session 12l on the P1 Phase A read-out (`docs/session-reports/session-12/12l_P1A/12l_P1A.md`, `a579915`), carried in the 12l-R612 brief. One ruling under R607(b): it is P1's package plan. The ruling as issued, with its line breaks:

```text
RULING (PI, 2026-10-11, session 12l, on architect recommendation; transcribe verbatim as
R612, one ruling under R607(b))

{RULING}
```

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-10-11 | {flat} | PI, 2026-10-11, during session 12l |
'''
assert s.endswith("during session 12k |\n")
s = s.rstrip("\n") + block

# ---------------- dated notes
def note(rid, src, body, nth=0):
    """append a dated note to the row's last cell (the first table row starting '| rid |' in Step 2)"""
    global s
    lines = s.split("\n")
    idx = [i for i, l in enumerate(lines) if l.startswith(f"| {rid} |")]
    assert idx, rid
    i = idx[nth]
    assert lines[i].endswith(" |"), rid
    assert " | " not in body
    lines[i] = lines[i][:-2] + f" *Note 2026-10-11 (12l-R612, {src}):* {body} |"
    s = "\n".join(lines)

Q = lambda l: f'"{clause(l)}"'
RO = "`12l_P1A.md`"
note("INT-i12-6", "R612(l); D11", f"class moved by P1's package ruling, Class 1 (conditional) to Class 3 — R612(l): {Q('l')} The Phase A's evidence ({RO} §2 D11): legacy path only, and the trigger is a legacy-prompt response that returns the escape token, a token the legacy prompt never mentions. The row stays in P1 and rides with INT-i12-2 in unit U07 (R612(m); the R543 form).")
note("INT-i12-8", "R612(l); D11", f"class moved by P1's package ruling, Class 1 to Class 2 — R612(l): {Q('l')} The Phase A's evidence ({RO} §2 D11): the locator half is confirmed, but `located` stays correct and nothing reads `bridged`; the auditor half is off the pipeline. The row stays in P1, in unit U14 with INT-i12-7 (R590; R612(m)).")
note("INT-j12-19", "R612(l); D11", f"class moved by P1's package ruling, Class 1 (conditional) to Class 3 — R612(l): {Q('l')} The Phase A's evidence ({RO} §2 D11): the raw response is kept, the difference is confined to the legacy cloud tables, and a test pins the Anthropic half as intended. The row stays in P1, unit U32.")
note("C24", "R612(l); D11", f"class moved by P1's package ruling, Class 1 (conditional) to Class 2, for the Wave 2 row 28 finding, which is the only part of this row on the pre-tag list — R612(l): {Q('l')} The Phase A's evidence ({RO} §2 D11): what is wrong is the run's record of a call, not a stored value; cloud only. P1, unit U29.")
note("INT-i12-1", "R612(c); D2", f"{Q('c')} This replaces the package cell's \"checker-side, not pin-affecting\": the fix moves no template, but fields that pass today fail, so attempt 2 is sent on more papers ({RO} §2 D2).")
note("E-UNITMAP", "R612(m); D12", f"one fix unit with INT-g12-1, U03 (\"Full citation record on the claim; unit map keyed by run\") — R612(m): {Q('m')}")
note("INT-g12-1", "R612(i), R612(m); D8, D12", f"the form R568 left to P1's Phase A is ruled — R612(i): \"D8: Option A.\" As {RO} §2 D8 states Option A: one key on each asserted claim's payload, an ordered list of the cited runs in cited order, each with its first and last unit index and its start and end character offsets into the parsed text the claim names by sha256, plus the units version; no migration; the stored snippet stays the first run. One fix unit with E-UNITMAP, U03, after U01.")
note("C56", "R612(g); D6", f"{Q('g')} The row's three commits (R542) are units U02 (the predicate with handlers 1 and 2, with A-7), U26 (handlers 4–7 and 12–13) and U27 (handlers 8–11), in `12f_triage.md`'s handler numbering; the row closes when the three pass.")
note("INT-g12-4", "R612(h); D7", f"{Q('h')} Design A, as {RO} §2 D7 states it: `open_run` registers the arm unpinned and compares against any existing pin; the pin is written at the extract stage. Unit U05, pin-affecting by arm-binding.")
note("A-11", "R612(f); D5", f"R535's floor and the attempt rule are set for unit U06 — R612(f): {Q('f')}")
note("INT-h12-2", "R612(b), R612(d), R612(e); D1, D3, D4", f"three rulings on this row's batch (unit U04) and its rehearsal (placed as U13). R612(e), on item (a): {Q('e')} R612(d): {Q('d')} (row INT-l12-1). R612(b), on the comparison rehearsal: {Q('b')}")
note("INT-i12-3", "R612(j); D9", f"the reader R588 asks for is named — R612(j): \"D9: Option A.\" As {RO} §2 D9 states Option A: the field is named on the claim's payload, and the run summary — the same reader R535 gives `incomplete_fields` — prints the count and the fields. Unit U08.")
note("INT-i12-45", "R612(k); D10", f"{Q('k')} Ruling assumption A4 is answered by the Phase A: no model-family attribute exists ({RO} §2 D10). Unit U17. **Phase A item for P4:** the screening generator→verifier pairs.")
note("INT-i12-12", "R612(a)", f"stays in P1 as unit U15; the read-out's alternative in the package cell (P6 with D22's 023 build) is not taken, the guard being code and not schema ({RO} §3 U15).")
note("B-F14", "R612(n); D13", f"{Q('n')} The row's own Phase A measurement (whether closing the client cancels generation) was not made at 12l-P1A, which barred model calls; it opens unit U21.")
note("B-F17", "R612(n); D13", f"item (f) is half true at HEAD, as {RO} §2 D13 found: `scripts/nightly_tests.sh` has `set -euo pipefail`, so a failing run already exits non-zero; what holds is that the nightly runs every tier with no marker expression and nothing reads its status. R527's fix stands. R612(n): {Q('n')} Unit U12.")
note("INT-j12-18", "R612(o); D14", f"row INT-l12-2 (the two cloud-span repair scripts, retired under R31) closes with this row's unit, U31 — R612(o): {Q('o')}")
note("A-1N", "R612(q); N20", f"handoff obligation on unit U12 — R612(q): \"U12's handoff reads the citation-mcp nightly harness read-only and reports whether it loads Ollama models (N20).\"")
for rid in ("A-5", "INT-i12-39", "INT-i12-40"):
    note(rid, "R612(q)", "handoff obligation, with no change of class or package — R612(q): \"U01's and U04's handoffs first read A-5, INT-i12-39 and INT-i12-40. Any part of them that changes text inside the pin rides in that unit (R543 form).\"")

# ---------------- L (12l additions)
def md_has(x): assert x in MD, x[:60]; return x
d3 = md_has("`extraction_stages` gives an elicited run `elicitation_pass1`, `extract_pass2` and `extract_retry_snippet`, and all three enter the pin. Nothing under `engine/elicitation` calls the snippet retry.")
d14 = md_has("`scripts/reparse_cloud_spans.py` and `scripts/backfill_cloud_spans.py --confirm` call the cloud response parser and insert into `cloud_evidence_spans` with no run manifest, no completeness guard and no `enabled_arms` check.")
d14c = md_has("conditional on a zero-span `cloud_extractions` row existing")
side = {}
for l in MD.split("\n"):
    m = re.match(r"\| (N\d\d) \| ([^|]+) \| (.*) \| ([^|]*) \| ([^|]*) \| ([^|]*) \|$", l)
    if m: side[m.group(1)] = (m.group(2).strip(), m.group(3).strip())
rows = [("INT-l12-1", d3, "3", "P1 (pin-affecting; lands in unit U04)", "R612(d)", f"{RO} §2 D3 and §5 N42 (seen at C56; `engine/agents/extractor.py::extraction_stages`). R612(d): {Q('d')}"),
        ("INT-l12-2", d14, f"1 (conditional — R612(o): \"New row, Class 1 (conditional), P1, closing with U31.\" Condition, as the read-out states it: \"{d14c}\", which was not counted)", "P1, closing with unit U31 (INT-j12-18)", "R612(o); R31", f"{RO} §2 D14 and §5 N50 (seen at INT-j12-18; `scripts/reparse_cloud_spans.py::main`, `scripts/backfill_cloud_spans.py::main`). R612(o): {Q('o')}")]
for k, n in enumerate(["N04", "N10", "N12", "N16", "N18", "N32", "N34", "N35", "N37"], start=3):
    seen, txt = side[n]
    assert " | " not in txt
    rows.append((f"INT-l12-{k}", txt, "3", "P7", "R612(p)", f"{RO} §5 {n} (seen at {seen}; the finding cell quoted whole)"))
table = "\n".join(f"| {a} | {b} | {c} | {d} | {e} | {f} |" for a, b, c, d, e, f in rows)
for r in rows: assert all(" | " not in x for x in r), r[0]
add = f'''**L (12l additions).** *Opened 2026-10-11 (12l-R612) under R509, R607(b) and R612.* Eleven rows from session 12l's P1 Phase A read-out ({RO}, `a579915`): `INT-l12-1` for the read-out's decision D3 (R612(d)), `INT-l12-2` for D14 (R612(o)), and `INT-l12-3` … `INT-l12-11` for the side findings N04, N10, N12, N16, N18, N32, N34, N35 and N37 of its §5 (R612(p)). The text cell is the read-out's own: for the two decision rows the finding sentences of §2, quoted unchanged; for the nine side findings the finding cell of §5, quoted whole. Class and package are R612's. The IDs follow the 12g additions' form with the letter of the session that found them.

Not rows, recorded here as ruled (R612(p): "Every other side finding takes the disposition §5 proposes."): the other 44 side findings of {RO} §5 close inside the unit §5 names, are notes for a unit's handoff, are already on the list or already known, or are decided in §2; none opens a row. R612's other clauses are dated notes of this date on the rows they change: the four class moves on INT-i12-6, INT-i12-8, INT-j12-19 and C24 (R612(l)); unit membership or scope on INT-i12-1, E-UNITMAP, INT-g12-1, C56, INT-g12-4, A-11, INT-h12-2, INT-i12-3, INT-i12-45, INT-i12-12, B-F14, B-F17 and INT-j12-18; and R612(q)'s handoff obligations on A-1N, A-5, INT-i12-39 and INT-i12-40. P1's package plan — 35 fix units, U01–U12 and U14–U36, with the comparison rehearsal placed as U13 — is {RO} §3 as amended by R612; it is not copied here.

| # | On disk (the read-out, quoted) | Class | Owning package | Governing ruling | Evidence |
| --- | --- | --- | --- | --- | --- |
{table}
'''
anchor = "\n### Not engine defects — plan v50 wording"
assert s.count(anchor) == 1
s = s.replace(anchor, "\n" + add + anchor)

# ---------------- coverage map
cov_anchor = "| INT-j12-59 | P7 — R602 |\n"
assert s.count(cov_anchor) == 1
cov = "".join(f"| {r[0]} | {'P1, in U04' if r[0]=='INT-l12-1' else 'P1, closing with U31' if r[0]=='INT-l12-2' else 'P7'} — {r[4].split(';')[0]} |\n" for r in rows)
s = s.replace(cov_anchor, cov_anchor + cov)
open(P, "w", encoding="utf-8").write(s)
print("applied:", len(rows), "rows; notes written")
