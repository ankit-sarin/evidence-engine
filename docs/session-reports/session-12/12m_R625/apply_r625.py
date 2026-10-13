"""12m-R625 — applies R625's transcription to the plan: Block 23, the "L (12m additions, R625)"
table (INT-m12-1 … 3) and its coverage-map lines, a dated note on B-F17, and a dated note under the
12l closure. Run once from the repository root; refuses if already applied. --recount <file> adds
the recount paragraph."""
import re, subprocess, sys
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
s = open(P, encoding="utf-8").read()
MD = subprocess.run(["git", "show", "b2d3dfa:docs/session-reports/session-12/12m_U12/12m_U12A.md"],
                    capture_output=True, text=True, check=True).stdout
ANCHOR = "\n### Not engine defects — plan v50 wording"
if "--recount" in sys.argv:
    para = open(sys.argv[sys.argv.index("--recount") + 1], encoding="utf-8").read().strip()
    assert s.count(ANCHOR) == 1 and "(12m-R625), by script" not in s
    open(P, "w", encoding="utf-8").write(s.replace(ANCHOR, "\n" + para + "\n" + ANCHOR)); print("recount paragraph written"); sys.exit(0)
assert "R625" not in s and "INT-m12-1" not in s, "already applied"

RULING = '''R625(a) — The read-out's decisions, as ruled:
  (1) U12-D1: option C. The nightly runs the standard gate whether or not the lock is
      held, ON CONDITION that commit 1 adds a suite-wide fence: any Ollama server call
      from a test without the `ollama` marker is refused and fails the test.
  (2) U12-D2: `ollama`-, `network`- and `integration`-marked tests run only when an
      explicit opt-in is set. Under the opt-in they take the experiment lock
      non-blocking and skip if it is held. Without it they skip, including when the
      file is named on the command line. No scheduled run. A run of these tiers is a
      pre-tag step.
  (3) U12-D3: cloud-only entry points do not hold the lock. The acceptance test's
      census is of local model calls and Ollama restarts.
  (4) U12-D4: lazy import of the provider SDKs within engine/cloud/. Migration 014's
      text is unchanged. The mechanism is chosen after reading every importer of
      engine.cloud and stated in the commit's handoff.
  (5) U12-D5: non-engine model loaders are an operator matter. The Run 7 runbook stops
      hello-ai, llm-council, operativereports and lists for the run. The run's
      pre-flight check refuses to start if a model other than the run's is resident.
      The pre-flight check is specified with Run 7's brief, not in U12.
  (6) U12-D6: the ten legacy model-calling scripts are retired under R31, with a ledger
      entry, in their own commit within U12. That commit also:
        (i) removes the six entries from tests/test_review_paths.py
            SPEC_BEARING_ENTRY_POINTS, as R47 did for earlier retirements. The collected
            count drops by 18, and the commit's report states the new expected count;
        (ii) removes or repoints the comment at engine/utils/ollama_preflight.py:163;
        (iii) removes the two CLAUDE.md lines at 582-583, with the text quoted in that
              commit's handoff for PI approval;
        (iv) regenerates the inventory, so that inventory --check is in sync.
      tests/e2e_test_log.md is a historical log and is left as it is.
  (7) U12-D7: a holder started under an inherited or outer lock fails fast with a
      message naming the lock file. No adoption.
  (8) U12-D8: one reason form, "<ExceptionClass>: <message>", written by one helper, at
      all six failed-close sites. The three import-path assertions change with it.
  (9) U12-D9: chromadb, sentence-transformers and gradio are removed from
      requirements.txt.
R625(b) — Commit 2's acceptance test discovers entry points by static census, as
  entry_census.py does, and fails on any entry point with a path to a local model call
  or a restart that does not hold the lock. It does not use a hard-coded list.
R625(c) — Rows and notes:
  F-1 → new row INT-m12-1, Class 2, P1, landing in U12 (commit 1).
  F-2 → new row INT-m12-2, Class 3, P1, landing in U12 with (a)(2). Also a dated note
    on the 12l closure: its "No model call" is contradicted by five journal entries,
    quoted from the read-out's §11 F-2, attribution by pattern and timing.
  F-4 → new row INT-m12-3, Class 3, P7.
  F-5 → dated note on B-F17: fpdf2 and scikit-learn are also imported and undeclared.
  F-3 → no row; recorded as a dgx-infra item.
  F-6 → no row; an operator read, pending.
  Each new row's text cell quotes the read-out's finding unchanged, with evidence
  12m_U12A.md §11.
R625(d) — U12 lands as the read-out's eight commits (§10.1), plus the retirement commit
  from (a)(6). Changes from §10.1:
  - Commit 1 carries the fence from (a)(1) and the opt-in from (a)(2).
  - Commit 4 carries (a)(8).
  - Commit 7 carries (a)(9).
  One handoff per commit, in order.'''
flat = " ".join(RULING.split())
assert " | " not in flat and s.endswith("during session 12l |\n")
RO = "`12m_U12A.md`"
s = s.rstrip("\n") + f'''

**Block 23.** *Transcribed 2026-10-13 (the 12m-R625 brief), in its own commit with the rows and the dated notes it orders, in the R597(e) form:* ruling R625, made by PI on architect recommendation on 2026-10-12, during session 12m, on unit U12's read-out (`docs/session-reports/session-12/12m_U12/12m_U12A.md`, `b2d3dfa`). (a)(6) is transcribed as restated by the ruling's correction of the same session, made before transcription (R8), after the brief's assumption I2 stopped the task: six of the ten scripts are named in `tests/test_review_paths.py`'s `SPEC_BEARING_ENTRY_POINTS`. I2 as restated: "No engine path or cron job imports or runs any of the ten scripts. Every remaining reference is textual and is listed above." The ruling as issued and corrected, with its line breaks:

```text
RULING [evidence-engine 12m-R625] — U12 read-out rulings; transcription, rows, recount
STATUS: PI ruling, 2026-10-12, session 12m, on architect recommendation.

{RULING}
```

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-10-12 | {flat} | PI, 2026-10-12, during session 12m |
'''
T = MD[MD.index("## 11. Findings outside U12's rows"):MD.index("## 12. State")]
def finding(n):
    line = [l for l in T.split("\n") if l.startswith(f"| {n} |")]; assert len(line) == 1
    return line[0][len(f"| {n} | "):].split(" | ")[0]          # the Finding cell, unchanged
def clause(tag): return re.search(rf"{tag} → (.*?)(?= F-\d → | Each new row's|$)", flat).group(1).strip()
f1, f2, f4, f5 = finding("F-1"), finding("F-2"), finding("F-4"), finding("F-5")
assert f1.startswith("The suite takes the real experiment lock") and f2.endswith("the journal names no client") and "shutil.copy2" in f4 and f5.startswith("`B-F17` under-counts")
SRC = f"{RO} §11"
rows = [("INT-m12-1", f1, "2", "P1, landing in unit U12 (commit 1)", "R625(c)", f"{SRC}, F-1 (§6; CODE ORDER). R625(c): \"{clause('F-1')}\""),
        ("INT-m12-2", f2, "3", "P1, landing in unit U12 (with R625(a)(2))", "R625(c)", f"{SRC}, F-2 (`ollama_journal_20261012.txt`). R625(c): \"{clause('F-2')}\""),
        ("INT-m12-3", f4, "3", "P7", "R625(c)", f"{SRC}, F-4 (READ). R625(c): \"{clause('F-4')}\"")]
for r in rows: assert all(" | " not in x for x in r)
table = "\n".join("| " + " | ".join(r) + " |" for r in rows)
add = f'''**L (12m additions, R625).** *Opened 2026-10-13 (the 12m-R625 brief) under R509 and R625.* Three rows from unit U12's read-out ({RO}, `b2d3dfa`, §11): `INT-m12-1` for its finding F-1, `INT-m12-2` for F-2 and `INT-m12-3` for F-4 (R625(c)). Each text cell is the read-out's Finding cell, quoted unchanged; class and package are R625's. Not rows, as ruled: F-5 is a dated note of this date on B-F17; F-2 also carries a dated note under the 12l closure; F-3 (the morning digest's failure grep does not match a pytest collection error) opens no row and is recorded as a `dgx-infra` item; F-6 (root's crontab not read) opens no row and is an operator read, pending.

| # | On disk (the read-out, quoted) | Class | Owning package | Governing ruling | Evidence |
| --- | --- | --- | --- | --- | --- |
{table}
'''
assert s.count(ANCHOR) == 1; s = s.replace(ANCHOR, "\n" + add + ANCHOR)
cov = "| INT-l12-25 | P1, landing in U12 — R624(a) |\n"; assert s.count(cov) == 1
s = s.replace(cov, cov + "| INT-m12-1 | P1, landing in U12 — R625(c) |\n| INT-m12-2 | P1, landing in U12 — R625(c) |\n| INT-m12-3 | P7 — R625(c) |\n")
lines = s.split("\n")
i = [k for k, l in enumerate(lines) if l.startswith("| B-F17 |") and l.count(" | ") > 3]; assert len(i) == 1 and lines[i[0]].endswith(" |")
lines[i[0]] = lines[i[0]][:-2] + f" *Note 2026-10-13 (12m-R625, R625(c); U12 F-5):* fpdf2 and scikit-learn are also imported and undeclared. The read-out's finding F-5, as {RO} §11 states it: \"{f5}\" R625(c): \"{clause('F-5')}\" |"
j = [k for k, l in enumerate(lines) if l.startswith("Session 12l closed 2026-10-12 UTC;") and "**No model call and no live write.**" in l]; assert len(j) == 1
lines.insert(j[0] + 1, f"\n*Note 2026-10-13 (12m-R625, R625(c); U12 F-2), on the closure paragraph above:* its \"No model call\" is contradicted by five entries in the Ollama journal. The read-out's finding F-2, as {RO} §11 states it: \"{f2}\" Attribution is by pattern and timing. Row INT-m12-2. R625(c): \"{clause('F-2')}\"")
open(P, "w", encoding="utf-8").write("\n".join(lines)); print("applied")
