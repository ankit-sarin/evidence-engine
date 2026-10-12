"""12l-CLOSE — builds the 12l lane line, the closure paragraph and the 12m expected values, from
values measured at this closeout. usage, from the repository root:
    close_12l.py            print the lane line's sha256 and bytes and the block (dry run)
    close_12l.py --apply    write the lane line file and insert the block in the plan
Reads ~/claude-config/primer.md (read only) for the five lines the lane line replaces, and
~/scratch/12l-close/gate.out for the gate's figures."""
import hashlib, os, re, sys
H = os.path.expanduser
S = H("~/scratch/12l-close")
tail = open(f"{S}/gate.out").read().strip().split("\n")[-1]
m = re.search(r"(\d+) passed, (\d+) deselected, (\d+) xfailed.* in ([\d.]+)s", tail); assert m, tail
passed, desel, xfail, wall = int(m[1]), int(m[2]), int(m[3]), float(m[4])
start = open(f"{S}/gate_start").read().strip(); done = open(f"{S}/gate.done").read().split()
assert done[0] == "exit=0"
collected = sum(1 for _ in open("docs/session-reports/session-12/12l_close/collected_ids_12l_close.txt"))
lo, hi = round(wall * 0.95), round(wall * 1.05)
f = lambda n: f"{n:,}"

prim = open(H("~/claude-config/primer.md"), encoding="utf-8").read().split("\n")
i = [k for k, l in enumerate(prim) if l.startswith("> (updated 2026-10-11, session 12k closed, session `22744015`)")]
assert len(i) == 1; old = prim[i[0]:i[0] + 5]
assert old[4].endswith("then Run 7.**") and all(l.startswith("> ") for l in old)
new = [
 "> (updated 2026-10-12, session 12l closed, session `7957016e`): HEAD the 12l closeout commit (child of `ff428a8`), pushed and",
 "> measured at origin. 12l closed: P1's Phase A read-out and package plan (R612); P1 units U01–U11 landed (units as source slices, run-fault predicate, citation record on the claim, prompt and pin batch, arm pinned at extract, the floor and attempt rule, write-boundary guard, value divergence, attempt-2 duplicate, codebook identity at open, text refusal at selection); R612–R624 transcribed, pre-tag list 263 rows (95/49/119, 0 unclassed), P1 54 rows in 35 units;",
 f"> gate {f(passed)} passed / {xfail} xfailed / {desel} deselected, {f(collected)} collected, band {lo}–{hi} s (R328, re-cut at the 12l close); elicited pin `8f0aeecd…3488`, non-elicited `d3672ab6…ff5c`, codebook semantic hash `f261af76…5646` (by `12l_U04/pin_tuples.py`);",
 "> no model call, no live write (36 tables, overall `0d3eedea…cb25` unchanged); decision log ends at R624; one throwaway copy from U06's real-state run kept at `~/scratch/12l-u06/throwaway/`;",
 "> Run 7 est. 35–38 h (the 12g estimate, carried). 12m next: item 0, startup verify, U12 (with INT-l12-14 and INT-l12-25), then U13's comparison-rehearsal brief; then the rest of P1, P2–P7, migrations, freeze, tag freshman; then Run 7.**"]
lane = "\n".join("-" + l for l in old) + "\n" + "\n".join("+" + l for l in new) + "\n"
b = lane.encode(); sha = hashlib.sha256(b).hexdigest()

blk = f"""Session 12l closed 2026-10-12 UTC; opened 2026-10-11 18:31 UTC on the 12k HEAD `82a33d751b3cb22cbad19c34299893af9d7cc884`, CC session `7957016e` (one session, continued across context summaries). **The first session of package P1**: P1's Phase A read-out, the package ruling, and fix units U01 to U11, each under its own brief with its ruling transcribed at the next brief's item 0. **No model call and no live write.** One real-state run (U06) was made on a throwaway copy of the review made by R235. Briefs, in order: 12l-OPEN, 12l-P1A, 12l-R612, 12l-U01, 12l-U02, 12l-U03, 12l-U04, 12l-U04b, 12l-U05, 12l-U06, 12l-U07, 12l-U08, 12l-U09, 12l-U10, 12l-U11 and this closeout.

**Commits, in order.**

- claude-config `1300214913c3cf0b9d94c977f37dad91e3ca2350` — item 0: the 12k lane line applied to the global primer, pushed.
- `a579915` — 12l-P1A: P1's Phase A read-out (`12l_P1A/`), 14 decisions, units U01–U36, 53 side findings.
- `4049600` — R612 transcribed (Block 10): the package plan; INT-l12-1 … 11; recount 249.
- `10cc974` — **U01**: units are exact slices of the parsed text; a cited run is one slice, checked EXACT.
- `f323708` — R613 (Block 11): INT-l12-12, -13; recount 251.
- `13822e6` — **U02**: one run-fault predicate, re-raised ahead of extract-path handlers 1 and 2.
- `e2189c1` — R614 (Block 12): INT-l12-14 … 16; recount 254.
- `bc93976` — **U03**: the citation record on the claim; the unit-map directory named by the manifest run.
- `822f1fb` — R615 (Block 13): three notes on INT-g12-1.
- `36de472`, `a0b818e` — **U04**, commits 1 and 3: the Pass-1 contract sentences, the sentinel rule's sentinel, the JUDGMENT step check; each attempt's raw response in telemetry. Stopped before commit 2 on I2.
- `f3c3535` — R616 (Block 14): INT-l12-17; recount 255.
- `fe3ffe3`, `dccd679` — **U04**, commits 2 and 4: the pin records the segmentation, the canonical sentinel joins the semantic hash, an elicited run declares two stages; a JUDGMENT field needs a step that cites a unit.
- `304b13a` — R617 (Block 15): three notes.
- `6a9b58c` — **U05**: the extraction arm is pinned by the run that reaches the extract stage.
- `ace5388` — R618 (Block 16): INT-l12-18, -19; recount 257. `06edfba` — CLAUDE.md: T-c, T-d1, T-d2, T-e.
- `e52c17e`, `2fc5a0b` — **U06**: the floor, the attempt rule, the claim-on-unpinned-arm refusal; the report and the real-state run's output.
- `b6f7fc1` — R619 (Block 17): INT-l12-20; recount 258.
- `ab240d9`, `c4f1369` — **U07**: blank values and non-value tokens refused at the write boundary; a stored record counted extracted; the report.
- `ed112d1` — R620 (Block 18): INT-l12-21; recount 259.
- `3b4b6a7`, `8058399` — **U08**: value divergence on the claim and in the run summary; `attempts` means attempts spent; the report.
- `736e2c9` — R621 (Block 19): INT-l12-22; recount 260.
- `e816afa`, `7293279` — **U09**: a duplicate on Pass-1 attempt 2 keeps attempt 1; the report.
- `962c57d` — R622 (Block 20): INT-l12-23; recount 261.
- `a332f36`, `459575d` — **U10**: `open_run` refuses a codebook that declares another review; the report.
- `af9a46b` — R623 (Block 21): INT-l12-24; recount 262.
- `e2d76a2`, `65327f6` — **U11**: a missing or changed text file on an extracted paper is reported, not re-failed; the report.
- `e9944d7` — R624 (Block 22): INT-l12-25; recount 263. `ff428a8` — CLAUDE.md: `CodebookReviewMismatch` in the list of `open_run` refusals (R623(b)).
- **This closeout.**

**Rulings R612–R624, all transcribed. The decision log in this file ends at R624.**

- R612 — P1's package plan on the Phase A read-out: units U01–U12 and U14–U36, the comparison rehearsal placed as U13; decisions D1–D14.
- R613 — U01's findings: the locator's fuzzy reports (INT-l12-12, with U14), pysbd's line-break sentence end (INT-l12-13).
- R614 — U02's findings: a run fault closes the manifest with no reason (INT-l12-14, U12); two P7 rows; a note on C56.
- R615 — U03's findings: three notes on INT-g12-1.
- R616 — U04's stop: commit 2 proceeds; the marker sentence stands; the JUDGMENT check follows R571(b) as commit 4; INT-l12-17.
- R617 — U04b's findings: no prompt change before the rehearsal, and how the rehearsal counts JUDGMENT `VALUE_WITHOUT_CITATION`; pins are computed with `12l_U04/pin_tuples.py`.
- R618 — U05's stop: the CLAUDE.md text (T-c, T-d1, T-d2, T-e); INT-l12-18 (U06), INT-l12-19.
- R619 — U06's findings: T-d2 accepted as made; `held_fields_below_floor` confirmed; the outer retry budget does not apply at the write boundary; INT-l12-20 (U07); the gate's band is re-derived at this close.
- R620 — U07's findings: `attempts` means attempts spent (U08); INT-l12-21; a note on B9 (U28).
- R621 — U08's findings: INT-l12-22.
- R622 — U09's findings: INT-l12-23; attempt 1's duplicate path stands, decided on the rehearsal's evidence.
- R623 — U10's findings: INT-l12-24; the CLAUDE.md line for `CodebookReviewMismatch`.
- R624 — U11's findings: INT-l12-25 (U12); two notes on INT-i12-69, one a question for the PI.

**Unit closure table.** R510's rows out stay 0 until P1's close (R607(d)); this table is the record of what each unit closed.

| Unit | Rows | Closing commit(s) | State |
| --- | --- | --- | --- |
| U01 | INT-g6-1, A-10, E-TEST | `10cc974` | closed |
| U02 | C56, A-7 | `13822e6` | A-7 closed; **C56 partly — handlers 1 and 2 of 15; open until U26 and U27** (handler 3 is P7) |
| U03 | INT-g12-1, E-UNITMAP | `bc93976` | closed (R615(a): repeated indices are not kept; a JUDGMENT field's field-level list is not separable) |
| U04 | INT-h12-2, INT-i12-1, INT-i12-38, INT-i12-4, C34 (Wave 1 row 14), INT-l12-1; INT-i12-41's prompt-text item | `36de472`, `fe3ffe3`, `a0b818e`, `dccd679` | INT-i12-1, INT-i12-38, INT-i12-4, C34's reopening and INT-l12-1 closed; INT-i12-41's item landed (the row stays P7); **INT-h12-2 partly — items (a), (b), (e) landed; open until U13**, its comparison rehearsal |
| U05 | INT-g12-4 | `6a9b58c` (CLAUDE.md `06edfba`) | closed |
| U06 | A-11, INT-l12-18 | `e52c17e` (real-state run `2fc5a0b`) | closed; R560's real-state test discharged |
| U07 | INT-i12-2, INT-i12-6, INT-l12-20 | `ab240d9` | closed |
| U08 | INT-i12-3 | `3b4b6a7` | closed, with R620(c) |
| U09 | INT-i12-5 | `e816afa` | closed for attempt 2; attempt 1's duplicate path stands, to be decided on the rehearsal's evidence (R622(b)) |
| U10 | INT-i12-9 | `a332f36` (CLAUDE.md `ff428a8`) | closed |
| U11 | INT-i12-69 | `e2d76a2` | closed; R624(b)'s question is open |

**The comparison rehearsal (U13), design constraints as ruled.**

- R612(b): "D1: Option A. The rehearsal's brief separates attempt-level measures (first-attempt failure codes per field) from stored-outcome measures; only the former are compared against 12g_RA-B's per-field results as R571's test. Stored outcomes are reported, not compared."
- R617(a): "F1 (the worked example shows only the field-level citation shape): a dated note on INT-h12-2. No prompt change before the rehearsal. The rehearsal's design counts JUDGMENT first-attempt VALUE_WITHOUT_CITATION separately, split by whether the field carried a field-level citation, because the check tightened at dccd679 and 12g_RA-B was scored under the earlier check."
- R622(b): "F2: a dated note on INT-i12-5. Attempt 1's duplicate path stands for now. The rehearsal's design counts how often Pass 1 names a field twice, on attempt 1 and on attempt 2, and attempt 1's path is decided on that evidence."

**Pending PI question.** R624(b), noted on INT-i12-69, before U16: whether a paper the arm already holds, whose recorded text file has changed, is to be re-extracted. Today it is reported and left as it is.

**Measured — the recount** (`docs/session-reports/session-12/12l_R624/recount_12l_r624.py` → `recount_12l_r624.json`, the chain back to `recount_12k.py` reproducing each committed JSON). **The pre-tag list in this file is 263 rows — Class 1 / 2 / 3 = 95 / 49 / 119, and 0 without a class.** **R510 for the session: rows in 25 (INT-l12-1 … 25, all opened), rows out 0.** Four rows changed class under R612(l), each recorded at its row: C24 and INT-i12-8, Class 1 → 2; INT-i12-6 and INT-j12-19, Class 1 → 3 (98 / 44 / 96 at the 12k close → 95 / 46 / 108 after R612's eleven rows and four moves). No other ruling of the session moved a class. **P1 is 54 rows (26 / 19 / 9) in 35 fix units**, of which U01–U11 are landed.

**Measured — the gate at this closeout**, on `ff428a8` (code unchanged since `e2d76a2`): **{f(passed)} passed / {xfail} xfailed / {desel} deselected in {wall:.2f} s** (started {start}), **{f(collected)} collected**, `docs/session-reports/session-12/12l_close/collected_ids_12l_close.txt`; against `collected_ids_12e.txt` (3,194): 132 added, 2 removed across the session's units (the two removed are the `test_run_manifest.py` tests renamed in U05). **Band: {lo}–{hi} s** — the measured value ±5%, the rule of R328 ("The band is re-cut to the measured value ±5%"), applied to this closeout's gate under R619(g). The session's unit gates, in order: 796.24, 817.69, 816.99, 821.33, 822.82, 823.41, 838.01, 832.79, 842.49, 858.44, 849.70, 845.02 s, over 3,198 → 3,324 collected tests.

**Live.** Never written. `db_fingerprint --compare` against the migration-02 record was IDENTICAL at every check of the session — at open, after every push, and at this closeout (overall `0d3eedead60c2ce1c6341b973fa5931d69c4b13c14e93bd5b805bc06734fcb25`, 36 tables). U06's real-state run read live once through `auto_backup` (`mode=ro`, 2026-10-12 01:10 UTC) and wrote to the copy only. `inventory --check` in sync at this closeout.

*Notes recorded at this close, no rows:*
- (a) The pins moved three times in the session, each by ruling: at `36de472` (the elicited `elicitation_pass1` prompt hash), at `fe3ffe3` (`codebook_hash` on both arms, the elicited arm's `segmentation` keys and stage set). No binding pin exists: live has no `arms` row for `local_deepseek_r1_32b`, 0 `run_manifests`, 0 `field_events`.
- (b) `~/scratch/12i-scope/pins.py`, the session-open pin script until now, hard-codes three extraction stages and so no longer computes the elicited pin a run writes. It is superseded for the elicited pin by `docs/session-reports/session-12/12l_U04/pin_tuples.py` (R617(c)); its non-elicited figure agrees with `pin_tuples.py`.
- (c) Two of this session's reports carried an error of mine that a later brief inherited: `12l_U02.md` F2 said migration 019's CHECK carries the reason codes (it enumerates none; corrected by R619(b) on INT-l12-15), and `12l_U05.md` quoted a CLAUDE.md sentence with its line wrap removed (R619(a)).
- (d) `tests/conftest.py`'s placeholder codebook declares the scratch review's own name; since U10 the shared fixture `open_extraction_run` gives a scratch review the codebook of the spec it runs under.
- (e) The live `generate_prisma_flow` output has two more keys since U06, `extracted_incomplete` (0 on live) and `incomplete_fields` (empty); every count it had is unchanged.

**Session 12l closed.** Next: session 12m, fresh.

Expected values at session-12m open, as at this closeout (2026-10-12):
- HEAD: this closeout's commit (the child of `ff428a8f8e97fc3836e7c2a9f49b1617162194bb`; its full hash is in the closeout block), clean and **level with origin** (R370). The plan file's sha256 and byte count are measured by 12m at open; they are not recorded here, since this file cannot name its own hash.
- **The decision log in this file ends at R624** (or at the last wrap ruling, if any is transcribed before 12m).
- **The gate** on this tree: **{f(passed)} passed / {xfail} xfailed / {desel} deselected**, collected **{f(collected)}**, 0/0 against `docs/session-reports/session-12/12l_close/collected_ids_12l_close.txt`, band **{lo}–{hi} s** ({wall:.2f} s measured at this closeout; R328's rule); `tests/test_eligibility.py` 91; `inventory --check` in sync; `git diff --stat e2d76a2 HEAD -- engine tests scripts analysis review_specs` empty.
- **The fingerprint of record, unchanged:** `docs/session-reports/migration-02/review_db_fingerprint_20260929T172123Z.json` — 36 tables; overall `0d3eedead60c2ce1c6341b973fa5931d69c4b13c14e93bd5b805bc06734fcb25`; structure `b0dffa3fdd7ff57a4aa6910181c1b02269b9478feee0d33c8d14744ecb1f57aa`; textual `f8d6388751e91a08e5d8580dc2de44a4be965e1dfd628a133950f8ed7d153cee`. No second `data/<dir>/review.db`.
- **Re-read at this closeout by the 12l-open script (`~/scratch/12i-scope/dbchecks.py`), identical to its 12l-open output but for note (e)'s two PRISMA keys:** 21 receipts, last 022; the event store (0 `field_events`, 0 `run_manifests`, the three pre-manifest arms and no `local_deepseek_r1_32b` row); paper counts; live PRISMA, valid; live selection 190 / 0 / 0; the parsed-text hash baseline (194 entries, 0 mismatches); the twelve `workflow_state` rows. R71 in force.
- **Retained throwaway reviews:** the eight under `~/scratch/retained/` (file sha256, each unchanged from the 12l open). **One more, outside that directory:** U06's real-state copy at `~/scratch/12l-u06/throwaway/u06_floor_20261012/` — `review.db` file sha256 `fc7d061dc3256bea1ae1be3f9dbd2984e27d7f5acbfa3d11d9fdb587059b7cf8`, fingerprint overall `000db389fa40fd573dff449c33138471f44a3c10585a492dac9affd80397f6ca`, 36 tables (three runs, 42 claims on papers 491, 679 and 783); it was made from live at fingerprint `0d3eedea…cb25` and is not a restore point.
- **The five restore points** (fingerprints, each unchanged from the 12l open): `bb39ba81…6c40`, `e564f250…5b63`, `ea05912d…ce01`, `f376562e…9e00`, `62f39128…b79a`.
- **Codebook and live spec, unchanged:** codebook sha256 `89dbfa91b5033f2b3dff713e36c7ecd0aa086739b5832059948b6f55b29ead82`; live spec sha256 `917c24de29afdf802b5ddea61c34627a9bb0a857de3f97395af36d44ca664e5f` (`elicitation: false`, `enabled_arms: []`).
- **Pins and hashes, by `docs/session-reports/session-12/12l_U04/pin_tuples.py`** (`~/scratch/12l-close/pin_tuples_close.json`, byte-identical to `12l_U11/pin_tuples_after.json`): **codebook semantic hash `f261af76feadd650f3c58b36498e2d140ef2dc4ffec6a80a16c06741eb2f5646`**; **non-elicited arm pin `d3672ab6fb1d9b627b9fa8dd19112e148e88cf174b1d854bb0b7fff004f7ff5c`** (stages `extract_pass1`, `extract_pass2`, `extract_retry_snippet`); **elicited arm pin `8f0aeecdeda659a13b3c182fabdc2568523f38891cd692f0a8cf9ce629de3488`** (stages `elicitation_pass1`, `extract_pass2`; `segmentation` pysbd 0.3.4, `min_unit_tokens` 3, `units_version` units-2). Prompt hashes: `elicitation_pass1` `18ac4255473a556b83e3aefffdc48de0ca2ae1d6826322deccb39175927f2f1c`; elicited `extract_pass2` `b2a44f28c9d9cade12a5782fdfce3618daca1b95a83b152c448fffb88ac710fe`; `extract_pass1` `c0083f30e0d7a4ff3e8036ac6e6feeb0e79319dab7693ec246aee3809d054f34`; non-elicited `extract_pass2` `a4f3cb35706fcc1974494f0e746e10a6bc21499a661bc9a49bcbf2d51b6e8e78`; `extract_retry_snippet` `05a981814969ec38c5785343a403d02f6c5c8ff2724490cae3a602da069b55aa`; `audit` `d972c46dde944f1381c4f2d5472c6264c3a1053bd23bdd3801bdf0fdc2a5838d`; preflight `de95332f63517c2c6a6558c76892369173dd3a55cf71944cabd0850ed44d322a`. Library versions: ollama 0.6.1, openai 2.24.0, anthropic 0.84.0, pysbd 0.3.4. `~/scratch/12i-scope/pins.py` is superseded for the elicited pin (note (b)).
- **Ollama:** 0.21.0; digests `qwen3:32b` `030ee887…7840`, `gemma3:27b` `a418f583…2203`, `deepseek-r1:32b` `edba8017…960c`; `NRestarts=0`, service start Fri 2026-09-11 21:09:13 UTC. **Residency, recorded and not matched:** `gemma3:27b`, keep_alive Forever, at this closeout; this session loaded nothing and called no model.
- **Read `mode=ro`, a WAL database's `-shm` is touched:** live `review.db-shm` carries an mtime of 2026-10-12 from this session's read-only connections; the main file (mtime 2026-09-29 17:21:15 UTC) and the 0-byte `-wal` are unchanged, and the fingerprint is the check.
- **The pre-tag list in this file is 263 rows — Class 1 / 2 / 3 = 95 / 49 / 119, and 0 without a class**; P1 54 rows (26 / 19 / 9), 35 fix units; section L carries "L (12l additions)" through "L (12l additions, R624)" (`INT-l12-1` … `INT-l12-25`).
- **Scratch:** the sixteen 12l directories — `12l-open/`, `12l-p1a/`, `12l-r612/`, `12l-u01/` … `12l-u11/`, `12l-u04b/` and `12l-close/` — beside the fifteen present at the 12l open (the fourteen the 12k close names and `10b-close/`) and `retained/`; the session scratchpad under `/tmp` may be absent, and this closeout's scripts are in `~/scratch/12l-close/`.
- **claude-config:** `1300214913c3cf0b9d94c977f37dad91e3ca2350` (12l's item 0), level with origin, with a hook-appended `PROJECT_LEDGER.md` change uncommitted at this closeout (36 insertions by `git diff --stat`); the wrap settles the ledger, and the claude-config HEAD expected at the 12m open is the one the wrap reports after it.
- **The 12l lane line is proposed to a file, not applied:** `~/scratch/12l-close/lane_line_12l.txt`, sha256 `{sha}`, {len(b):,} bytes — the five "-" lines now in `primer.md` and the five "+" lines that replace them. Its first "+" line names HEAD as "the 12l closeout commit (child of `ff428a8`)" rather than by hash, because the file's sha256 is recorded in the commit that hash would name.

12m's first items:
- **Item 0** — the 12l lane line, applied to `~/claude-config/primer.md` from `~/scratch/12l-close/lane_line_12l.txt` (sha256 as recorded above), not from a paste.
- **The startup verify** against this block.
- **Then U12** (the experiment lock, the nightly and the requirements: A-1, A-1N, B-F17, INT-g12-7, with INT-l12-14 and INT-l12-25), under its own brief; **then U13's brief**, the comparison rehearsal, under the three constraints quoted above.

"""
p = "docs/plan/ENGINE_REFACTOR_PLAN.md"; s = open(p, encoding="utf-8").read()
a = "\n## Decision log\n"; assert s.count(a) == 1 and "Session 12l closed" not in s
k = s.index(a); assert s[:k].rstrip("\n").endswith("stopping for the package ruling.")
print(H("~/scratch/12l-close/lane_line_12l.txt"), sha, len(b)); print(f"gate {passed}/{xfail}/{desel} {wall}s collected {collected} band {lo}-{hi}")
if "--apply" in sys.argv:
    open(H("~/scratch/12l-close/lane_line_12l.txt"), "w", encoding="utf-8").write(lane)
    open(p, "w", encoding="utf-8").write(s[:k].rstrip("\n") + "\n\n" + blk.rstrip("\n") + "\n" + s[k:]); print("applied")
else:
    print(blk[:300])
