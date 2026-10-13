"""12m-CLOSE item 0 — applies R627's transcription to the plan: Block 25, the
"L (12m additions, R627)" table (INT-m12-5) and its coverage-map line, a dated note on A-4b
(R627(b)) and a dated note on INT-m12-2 (R627(d)). Run once from the repository root; refuses if
already applied. --recount <file> adds the recount paragraph."""
import re, sys
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
s = open(P, encoding="utf-8").read()
ANCHOR = "\n### Not engine defects — plan v50 wording"
if "--recount" in sys.argv:
    para = open(sys.argv[sys.argv.index("--recount") + 1], encoding="utf-8").read().strip()
    assert s.count(ANCHOR) == 1 and "(12m-R627), by script" not in s
    open(P, "w", encoding="utf-8").write(s.replace(ANCHOR, "\n" + para + "\n" + ANCHOR)); print("recount paragraph written"); sys.exit(0)
assert "R627" not in s and "INT-m12-5" not in s, "already applied"

RULING = '''R627(a) — RET-F2 (12m_U12_RET.md): docs/architecture/modules.md is hand-written and
  nothing checks it against the tree, so its script tables go stale at each retirement
  unless the retiring commit edits them. New row INT-m12-5, Class 3, P7.
R627(b) — C1-F1 (12m_U12_C1.md): ollama_client.py:59 builds the engine client at first
  import, reading OLLAMA_HOST then. A test that sets OLLAMA_HOST before that first
  import leaves the client bound to its value for later tests. A dated note on A-4b;
  no new row. RET-F1 (the dated count in the SPEC_BEARING_ENTRY_POINTS comment) is
  accepted as made; no row.
R627(c) — The CLAUDE.md text proposed in 12m_U12_C1.md §(7) is approved for U12
  commit 5. With it, commit 5 rewords the existing sentence "No tier is exempt,
  including the nightly full-suite run" to match the opt-in. The commit 5 handoff
  quotes the final text.
R627(d) — C1-F3: at the first live-tier run (EVIDENCE_ENGINE_LIVE_TIERS=1), any test
  marked network or integration but not ollama that is refused by the Ollama fence is
  reported and re-marked. Recorded on INT-m12-2 as a dated note; no row.'''
flat = " ".join(RULING.split())
assert " | " not in flat and s.endswith("during session 12m |\n")
def clause(l): return re.search(rf"R627\({l}\) — (.*?)(?= R627\([a-d]\) — |$)", flat).group(1).strip()
s = s.rstrip("\n") + f'''

**Block 25.** *Transcribed 2026-10-13 (item 0 of the 12m-CLOSE brief), in its own commit with the row and the dated notes it orders, in the R597(e) form:* ruling R627, made by PI on architect recommendation on 2026-10-13 UTC, during session 12m, on the unit reports of U12 commit 1 (`docs/session-reports/session-12/12m_U12/12m_U12_C1.md`, `9cf9318`) and of the retirement (`12m_U12_RET.md`, `954796e`). The CLAUDE.md text R627(c) approves is the text under the heading "8. PROPOSED — CLAUDE.md text for the opt-in" of `12m_U12_C1.md`, which the 12m-U12-C1b final report carried as its item (7); it is applied at U12 commit 5, not here. The ruling as issued, with its line breaks:

```text
RULING R627 (PI, 2026-10-13 UTC, session 12m, on architect recommendation):

{RULING}
```

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-10-13 | {flat} | PI, 2026-10-13, during session 12m |
'''
a = clause("a"); finding = a[len("RET-F2 (12m_U12_RET.md): "):a.index(" New row INT-m12-5")]
rows = [("INT-m12-5", finding, "3", "P7", "R627(a)",
         f"`12m_U12_RET.md` §6, RET-F2 (READ; `docs/architecture/modules.md`, nine rows marked retired by hand at `954796e`). R627(a): \"{a}\"")]
for r in rows: assert all(" | " not in x for x in r)
table = "\n".join("| " + " | ".join(r) + " |" for r in rows)
add = f'''**L (12m additions, R627).** *Opened 2026-10-13 (item 0 of the 12m-CLOSE brief) under R509 and R627.* One row from the retirement's unit report (`12m_U12_RET.md`, `954796e`): `INT-m12-5` for its finding RET-F2 (R627(a)). The text cell is R627(a)'s statement of the finding, quoted unchanged; class and package are R627's. Not rows, as ruled: C1-F1 is a dated note of this date on A-4b and RET-F1 is accepted as made (R627(b)); C1-F3 is a dated note of this date on INT-m12-2 (R627(d)); R627(c) approves CLAUDE.md text for U12 commit 5.

| # | On disk (R627(a), quoted) | Class | Owning package | Governing ruling | Evidence |
| --- | --- | --- | --- | --- | --- |
{table}
'''
assert s.count(ANCHOR) == 1; s = s.replace(ANCHOR, "\n" + add + ANCHOR)
cov = "| INT-m12-4 | P1, closing in U12 — R626(a) |\n"; assert s.count(cov) == 1
s = s.replace(cov, cov + "| INT-m12-5 | P7 — R627(a) |\n")
lines = s.split("\n")
def note(rid, text):
    i = [k for k, l in enumerate(lines) if l.startswith(f"| {rid} |") and l.count(" | ") > 3]; assert len(i) == 1 and lines[i[0]].endswith(" |") and " | " not in text
    lines[i[0]] = lines[i[0]][:-2] + " " + text + " |"
b = clause("b")
note("A-4b", f"*Note 2026-10-13 (12m-CLOSE item 0, R627(b); U12 commit 1's finding C1-F1):* the engine client is bound to a host at import, as `12m_U12_C1.md` §9 found: `engine/utils/ollama_client.py:59` builds `_client = ollama.Client(...)` when the module is first imported, reading `OLLAMA_HOST` then; a later test saw `10.1.2.3:4567` as the client's host in a probe run, and `tests/test_suite_live_fence.py` imports the module at its top so its own tests cannot cause it. No new row. R627(b): \"{b}\"")
note("INT-m12-2", f"*Note 2026-10-13 (12m-CLOSE item 0, R627(d); U12 commit 1's finding C1-F3):* the suite's half of this row landed at `9cf9318` (the Ollama fence and the live-tier opt-in). Under the opt-in a test marked only `network` or `integration` is still fenced from Ollama; the six `integration` tests were read in part and the eight `network` tests were not read, and none was run. R627(d): \"{clause('d')}\"")
open(P, "w", encoding="utf-8").write("\n".join(lines)); print("applied")
