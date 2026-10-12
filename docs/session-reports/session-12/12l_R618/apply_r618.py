"""12l item 0 (12l-U06 brief) — applies R618's transcription to the plan: Block 16, the
"L (12l additions, R618)" table (INT-l12-18, INT-l12-19) and its coverage-map lines. Run once from
the repository root; refuses if already applied. --recount <file> adds the recount paragraph."""
import re, subprocess, sys
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"
s = open(P, encoding="utf-8").read()
MD = subprocess.run(["git", "show", "6a9b58c:docs/session-reports/session-12/12l_U05/12l_U05.md"],
                    capture_output=True, text=True, check=True).stdout
ANCHOR = "\n### Not engine defects — plan v50 wording"
if "--recount" in sys.argv:
    para = open(sys.argv[sys.argv.index("--recount") + 1], encoding="utf-8").read().strip()
    assert s.count(ANCHOR) == 1 and "(12l item 0, R618), by script" not in s
    open(P, "w", encoding="utf-8").write(s.replace(ANCHOR, "\n" + para + "\n" + ANCHOR)); print("recount paragraph written"); sys.exit(0)
assert "R618" not in s and "INT-l12-18" not in s, "already applied"

RULING = '''R618 — U05's stop and findings (R509), on 12l_U05 (6a9b58c).
(a) T-d is replaced by T-d1 and T-d2 below. The provisional MIN_UNIT_TOKENS clause and
    the comparability warning stay word for word. The CLAUDE.md commit carries T-c,
    T-d1, T-d2 and T-e.
(b) F1 (the event writer accepts a claim on an arm no run has pinned): new row,
    Class 2, P1, landing in U06: the writer refuses a model arm's claim while that arm
    is unpinned.
(c) F2 and F3 (the open-time tuple is held in memory only; the manifest no longer lists
    the local arm's pin): one new row, Class 3, P7: record the resolved tuple durably.
(d) F4, F5: no row.'''
TEXT = '''CLAUDE.MD TEXT (PI-approved; replace exactly, nothing else in the file):
T-c. "The first run whose manifest declares an extraction stage pins the arm, whether
  or not it extracts; recovery is a new arm name (R10)." → "The first run that reaches
  the extract stage pins the arm; a run that stops before extract leaves it unpinned,
  and any later run is compared against the pin over its whole tuple. Recovery is a
  new arm name (R10)."
T-d1. In the Modules line, "ELICIT-01 index space, ported;" → "exact source slices of
  the raw parsed text, units-2, shown to the model whitespace-collapsed;"
T-d2. "Pass 2's `source_snippet` is overwritten with materialized unit text." → "Pass
  2's `source_snippet` is overwritten with the materialized run: one exact slice of the
  raw parsed text, checked at materialisation (SnippetNotExact stops the run)."
T-e. "Full citation set lives in `record_call(extra=…)` and the per-run unit-map file
  (no migration)." → "Every evidenced claim carries its full citation record in its
  payload (`citation_record`: cited indices, derived runs with offsets, per step for
  JUDGMENT), resolvable from the parsed text alone; unit maps are written under
  `run_<run_id>`."
Each old string must be found verbatim once, or STOP before the CLAUDE.md commit.'''
flat = " ".join(RULING.split())
def clause(l): return re.search(rf"\({l}\) (.*?)(?= \([a-d]\) |$)", flat).group(1).strip()
assert s.endswith("during session 12l |\n")
s = s.rstrip("\n") + f'''

**Block 16.** *Transcribed 2026-10-12 (12l item 0 of the 12l-U06 brief), in its own commit with the rows it orders, in the R597(e) form:* ruling R618, made by PI on architect recommendation during session 12l on unit U05's report and stop (`docs/session-reports/session-12/12l_U05/12l_U05.md`, `6a9b58c`), carried in the 12l-U06 brief. The ruling as issued, with its line breaks, and the CLAUDE.md text its clause (a) refers to ("below"), as the brief carries it:

```text
RULING (PI, 2026-10-11, session 12l, on architect recommendation; transcribe at item 0 as
R618 in Block 15's form)

{RULING}

{TEXT}
```

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-10-11 | {flat} | PI, 2026-10-11, during session 12l |
'''
RO = "`12l_U05.md`"
def has(x): assert x in MD, x[:50]; return x
f1 = has("`events._refuse_claim_on_arm` requires only that the claim's run declared the arm (a `run_stage_configs` row). With pinning moved to the extract stage, a claim written between open and extract would land on an unpinned arm, and the freeze trigger would then refuse the pin for good. No engine path writes one there: the extractor writes after `verify_extraction_run`.")
f2 = has("The whole-tuple comparison at extract needs the tuple `open_run` computed, which is carried in memory. A run opened in another process has none; `extraction_digest` still refuses an unpinned arm and compares digests. No engine path extracts under a run another process opened.")
f3 = has("`manifest_json.pins` listed the arm's pin sha when the manifest pinned it; it is now empty for a local extraction run, and `arms.pinned_run_id` names only the run that pinned. The per-stage hashes are still in `run_stage_configs`.")
rows = [("INT-l12-18", "the event writer accepts a claim on an arm that no run has pinned. " + f1, "2", "P1, landing in unit U06", "R618(b)", f"{RO} \"R509 findings\", F1 (`engine/core/events.py::_refuse_claim_on_arm`; `engine/core/run_manifest.py::bind_extraction_arm`; `tests/test_arm_pinned_at_extract.py::test_an_unpinned_arm_that_already_holds_a_claim_is_refused_by_name`). R618(b): \"{clause('b')}\""),
        ("INT-l12-19", "F2 — a run this process did not open gets the digest comparison only. " + f2 + " F3 — a manifest no longer says which tuple its local arm resolved to. " + f3, "3", "P7", "R618(c)", f"{RO} \"R509 findings\", F2 and F3 (`engine/core/run_manifest.py::_RUN_ARM_TUPLES`, `bind_extraction_arm`, `open_run`). R618(c): \"{clause('c')}\"")]
for r in rows: assert all(" | " not in x for x in r)
table = "\n".join("| " + " | ".join(r) + " |" for r in rows)
add = f'''**L (12l additions, R618).** *Opened 2026-10-12 (12l item 0 of the 12l-U06 brief) under R509 and R618.* Two rows from unit U05's report ({RO}, `6a9b58c`): `INT-l12-18` for its finding F1 (R618(b)) and `INT-l12-19` for F2 and F3 together (R618(c)). The text cell is each finding's heading and the report's own sentences, quoted unchanged; class and package are R618's. Not rows, as ruled: F4 and F5 open nothing (R618(d)). R618(a) is the CLAUDE.md text, applied in its own commit.

| # | On disk (the unit report, quoted) | Class | Owning package | Governing ruling | Evidence |
| --- | --- | --- | --- | --- | --- |
{table}
'''
assert s.count(ANCHOR) == 1; s = s.replace(ANCHOR, "\n" + add + ANCHOR)
cov = "| INT-l12-17 | P7 — R616(d) |\n"; assert s.count(cov) == 1
s = s.replace(cov, cov + "| INT-l12-18 | P1, landing in U06 — R618(b) |\n| INT-l12-19 | P7 — R618(c) |\n")
open(P, "w", encoding="utf-8").write(s); print("applied")
