"""12m-CLOSE — builds the 12m lane line, the closure and the 12n expected values, from values
measured at this closeout. usage, from the repository root:
    close_12m.py            print the lane line's sha256 and bytes and the block's head (dry run)
    close_12m.py --apply    write the lane line file and insert the block in the plan
Reads ~/claude-config/primer.md (read only) for the five lines the lane line replaces, and
~/scratch/12m-close/ for the gate's figures and the Ollama residency read."""
import hashlib, os, re, sys
H = os.path.expanduser
S = H("~/scratch/12m-close"); D = "docs/session-reports/session-12/12m_close"
tail = open(f"{S}/gate.out").read().strip().split("\n")[-1]
m = re.search(r"(\d+) passed, (\d+) deselected, (\d+) xfailed.* in ([\d.]+)s", tail); assert m, tail
passed, desel, xfail, wall = int(m[1]), int(m[2]), int(m[3]), float(m[4])
start = open(f"{S}/gate_start").read().strip(); done = open(f"{S}/gate.done").read().split()
assert done[0] == "exit=0"; end = done[1]
collected = sum(1 for _ in open(f"{D}/collected_ids_12m_close.txt"))
assert (passed, xfail, desel, collected) == (3249, 1, 17, 3250)
lo, hi = round(wall * 0.95), round(wall * 1.05)
f = lambda n: f"{n:,}"
resident = open(f"{S}/resident.txt").read().strip()
ledger_stat = open(f"{S}/ledger_stat.txt").read().strip()

prim = open(H("~/claude-config/primer.md"), encoding="utf-8").read().split("\n")
i = [k for k, l in enumerate(prim) if l.startswith("> (updated 2026-10-12, session 12l closed, session `7957016e`)")]
assert len(i) == 1; old = prim[i[0]:i[0] + 5]
assert old[4].endswith("then Run 7.**") and all(l.startswith("> ") for l in old)
new = [
 "> (updated 2026-10-13, session 12m closed, session `9e5a3940`): HEAD the 12m closeout commit (child of `432952e`), pushed and",
 "> measured at origin. 12m closed: unit U12's read-out (lock census, scheduled model loaders, the nightly, requirements) and its rulings (R625); U12 commit 1 landed (no test takes the real experiment lock, an Ollama fence over the suite, live tiers opt-in by `EVIDENCE_ENGINE_LIVE_TIERS=1`) and the ten legacy model-calling scripts retired; U12 commits 2–8 open; R625–R627 transcribed, pre-tag list 268 rows (95/50/123, 0 unclassed), P1 57 rows in 35 units;",
 f"> gate {f(passed)} passed / {xfail} xfailed / {desel} deselected, {f(collected)} collected, band {lo}–{hi} s (R328, re-cut at the 12m close); elicited pin `8f0aeecd…3488`, non-elicited `d3672ab6…ff5c`, codebook semantic hash `f261af76…5646` (by `12l_U04/pin_tuples.py`, unchanged);",
 "> no model call, no live write (36 tables, overall `0d3eedea…cb25` unchanged); decision log ends at R627; one throwaway copy from U06's real-state run kept at `~/scratch/12l-u06/throwaway/`;",
 "> Run 7 est. 35–38 h (the 12g estimate, carried). 12n next: item 0, startup verify, the 2026-10-13 nightly check, U12 commit 2 in a fresh CC session (R51), then commits 3–8, then U13's comparison-rehearsal brief; then the rest of P1, P2–P7, migrations, freeze, tag freshman; then Run 7.**"]
lane = "\n".join("-" + l for l in old) + "\n" + "\n".join("+" + l for l in new) + "\n"
b = lane.encode(); sha = hashlib.sha256(b).hexdigest()
LANE = H("~/scratch/12m-close/lane_line_12m.txt")

blk = f"""Session 12m closed 2026-10-13 UTC; opened 2026-10-12 18:00 UTC on the 12l HEAD `4dd721dec82ae47449cf1125756245bff68590e0`, CC session `9e5a3940` (one session, continued across context summaries). **Unit U12 opened**: its read-out and rulings, commit 1 of its fix commits, and the retirement that commit 2 needs. **No model call and no live write.** Briefs, in order: 12m-OPEN, 12m-U12-READ (with a brief amendment), 12m-R625 (with a ruling correction), 12m-U12-C1, 12m-U12-C1b, 12m-U12-RET (with a correction and approval) and this closeout.

**Commits, in order.**

- claude-config `1ef81a820850c0dd8b2bd512409b117f53beb7fa` — item 0: the 12l lane line applied to the global primer, pushed.
- `b2d3dfa` — 12m-U12-READ: U12's read-out (`12m_U12/12m_U12A.md`) — who loaded `qwen3:8b` (the 09:00 UTC nightly), the scheduled-job census, the entry-point lock census, the nightly, requirements, INT-l12-14 and INT-l12-25; an eight-commit plan and decisions D1–D9, all PROPOSED; six R509 findings.
- `7bed8e0` — R625 transcribed (Block 23): the read-out's decisions as ruled; INT-m12-1 … 3; notes on B-F17 and under the 12l closure; recount 266.
- `74f5317` — R626 (Block 24): INT-m12-4; a correction under R625 and an addendum beside the read-out; a note on A-4b; recount 267.
- `9cf9318` — **U12 commit 1**: every test gets its own lock path; a request to the Ollama server from the suite is refused; the `ollama`, `network` and `integration` tiers skip unless `EVIDENCE_ENGINE_LIVE_TIERS=1`; `test_raises_on_failure` patches `ollama.ps`. The report, `12m_U12/12m_U12_C1.md`.
- `954796e` — **U12, the retirement** (R625(a)(6)): the ten legacy model-calling scripts removed under R31, with their 81 generated test cases, ten ledger rows, the CLAUDE.md q8 block, nine `modules.md` rows marked, the inventory regenerated. The report, `12m_U12/12m_U12_RET.md`.
- `432952e` — R627 (Block 25): INT-m12-5; notes on A-4b and INT-m12-2; the opt-in's CLAUDE.md text approved for commit 5; recount 268.
- **This closeout.**

**Rulings R625–R627, all transcribed. The decision log in this file ends at R627.**

- R625 — the U12 read-out's decisions D1–D9, the census form of commit 2's acceptance test, rows for findings F-1, F-2, F-4, and U12's nine commits. (a)(6) was restated before transcription (R8).
- R626 — the finding of commit 1's stopped gate (INT-m12-4); commit 1's Touches widened to that test; the fence covers port 11435; corrections to R625's MEASURED line and to the read-out's §6.
- R627 — INT-m12-5; two dated notes; the CLAUDE.md text for commit 5; the check at the first live-tier run.

**Stops, and the corrections they exposed.** Each stop was made before any edit, or with the tree restored clean, and reported both values.

| Brief | The assumption | What was on disk | Settled by |
| --- | --- | --- | --- |
| 12m-U12-READ, I1 | P1A §3's U12 row names six rows | it names four (A-1, A-1N, B-F17, INT-g12-7); INT-l12-14 arrives by R614(a) and INT-l12-25 by R624(a) | the brief amendment: I1 corrected, six added read-only items placed |
| 12m-R625, I2 | the ten legacy scripts are "unused by any engine path, test or cron job" | none is imported or run, but six are named in `tests/test_review_paths.py::SPEC_BEARING_ENTRY_POINTS` (18 cases read their source), with four further textual references | the ruling correction: I2 restated, (a)(6) given its four sub-items |
| 12m-U12-C1, R625's MEASURED line | "The standard gate run in session 12m made no Ollama request (journal)" | the 12m-OPEN gate window holds `GET /api/ps` at 18:09:51 UTC, from `test_raises_on_failure`; found when the new fence failed that test (gate 3,328 passed / 1 failed) | R626: INT-m12-4; the correction and the addendum. **The line came from my read-out's §6, where the journal entry was read and not attributed to the gate** |
| 12m-U12-RET, I1 | the retirement removes 18 collected cases | 81: the 18, and 63 in `tests/test_api_parity.py` generated from the scripts' own imports | the correction and approval: I1 restated at 81; the gate matched it exactly |

**Unit closure table.** R510's rows out stay 0 until P1's close (R607(d)); this table is the record of what each landed commit closed. **U12 is in progress.**

| U12 commit (R625(d) order) | Rows | Commit | State |
| --- | --- | --- | --- |
| 1 — the suite's lock redirect, the Ollama fence, the live-tier opt-in | INT-m12-1, INT-m12-2, INT-m12-4 | `9cf9318` | landed. INT-m12-1 and INT-m12-4 closed. INT-m12-2 closed for the suite (R625(a)(2)); R627(d)'s check waits for the first live-tier run. The nightly script is unchanged until commit 5 |
| the retirement (R625(a)(6)) | no pre-tag row; ten retention-ledger rows | `954796e` | landed; the prerequisite of commit 2's census test |
| 2 — every local model-calling entry point holds the lock, by static census (R625(b)); a log line before a wait; fail fast under an inherited holder (R625(a)(3), (a)(7)) | A-1, INT-g12-7; the read-out's N19, N22 and L11-C4 (no rows of their own) | — | open |
| 3 — `restart_ollama` honours the opt-out | the read-out's N17 (no row of its own) | — | open |
| 4 — a failed close records "<ExceptionClass>: <message>" at all six sites (R625(a)(8)) | INT-l12-14 | — | open |
| 5 — the nightly runs the standard gate; the CLAUDE.md text (R627(c)) | A-1N, B-F17 (f) | — | open |
| 6 — a fresh database needs no provider SDK, by lazy import (R625(a)(4)) | B-F17 (g) | — | open |
| 7 — requirements; chromadb, sentence-transformers and gradio removed (R625(a)(9)) | B-F17 (a)–(e) | — | open |
| 8 — the empty-selection stats key | INT-l12-25 | — | open |

**Open operator items, no rows.** F-6: root's crontab is unread (not readable without sudo), so the scheduled-job census is complete for the user crontab and systemd timers only. F-3: the morning digest's failure grep does not match a pytest collection error (a `dgx-infra` item). R625(a)(5): the Run 7 runbook stops `hello-ai`, `llm-council`, `operativereports` and `lists` for the run, and the run's pre-flight refuses a foreign resident model; both are specified with Run 7's brief.

**Pending PI question, carried.** R624(b), noted on INT-i12-69, before U16: whether a paper the arm already holds, whose recorded text file has changed, is to be re-extracted.

**The comparison rehearsal (U13)** is not started; its three design constraints stand as the 12l closure quotes them (R612(b), R617(a), R622(b)).

**Measured — the recount** (`docs/session-reports/session-12/12m_R627/recount_12m_r627.py` → `recount_12m_r627.json`, the chain back through `recount_12m_r626.py`, `recount_12m_r625.py` and `recount_12l_r624.py` reproducing each committed JSON). **The pre-tag list in this file is 268 rows — Class 1 / 2 / 3 = 95 / 50 / 123, and 0 without a class.** **R510 for the session: rows in 5 (INT-m12-1 … 5, all opened), rows out 0.** No ruling of the session moved a class. **P1 is 57 rows (26 / 20 / 11) in 35 fix units**, of which U01–U11 are landed and U12 is in progress; P7 is 110.

**Measured — the gate at this closeout**, on `432952e` (code unchanged since `954796e`): **{f(passed)} passed / {xfail} xfailed / {desel} deselected in {wall:.2f} s** ({start} to {end}), **{f(collected)} collected**, `{D}/collected_ids_12m_close.txt`, 0 added and 0 removed against `12m_U12/ret_evidence/collected_ids_ret.txt`; against `12l_close/collected_ids_12l_close.txt` (3,324): 7 added (`tests/test_suite_live_fence.py`), 81 removed (the retirement). **Band: {lo}–{hi} s** — the measured value ±5%, the rule of R328, re-cut at this closeout; the band carried from the 12l close was 814–900 s, and every gate of the session fell inside it. The session's gates, in order: 851.88 (12m-OPEN), 856.51 (commit 1 before R626, 1 failed), 858.07 (commit 1), 854.90 (the retirement), {wall:.2f} s (this closeout). **The Ollama journal holds no request of any kind in this gate's window** (`{D}/journal_gate_window.txt`, empty), as in the two gates before it.

**Requests this session sent to the Ollama server.** No POST and no `/api/show`: no model was loaded or evicted by this session. GET reads of `/api/ps`, `/api/tags` and `/api/version` by the read-out and the open and close measures; two `GET /api/tags` at 03:13:46 and 03:14:38 UTC on 2026-10-13 from an inner test of commit 1's first draft, replaced before its first gate; and, until commit 1, one `GET /api/ps` per gate from `test_raises_on_failure` (INT-m12-4). The real lock file `~/.ollama_experiment.lock` was never opened; its mtime is 2026-07-27 23:22:08 UTC.

**Live.** Never written. `db_fingerprint --compare` against the migration-02 record was IDENTICAL at every check of the session — at open, after every push, and at this closeout (overall `0d3eedead60c2ce1c6341b973fa5931d69c4b13c14e93bd5b805bc06734fcb25`, 36 tables). `inventory --check` in sync at this closeout.

*Notes recorded at this close, no rows:*
- (a) The 12l closure's "No model call" is contradicted by five journal entries (the note under it, R625(c); row INT-m12-2). This closure's "No model call" rests on the journal: no POST in any window of this session's commands.
- (b) The 18:02:05 UTC group (`GET /api/version`, `/api/tags`, `/api/ps`) inside the 12m-OPEN gate window is untraced (R626(e)). The three fenced gates hold no such requests and no test was refused for them.
- (c) `12l_U04/pin_tuples.py` takes two arguments, the spec and the codebook; with the spec alone it exits 1 on `sys.argv[2]`. The 12n open runs it as `pin_tuples.py review_specs/surgical_autonomy.yaml data/surgical_autonomy/extraction_codebook.yaml`.
- (d) From `9cf9318` the 09:00 UTC nightly, whose script is unchanged, collects the 17 live-tier tests and skips them. The first such run is 2026-10-13; its expected result is below and is a prediction until 12n reads it.

**Session 12m closed.** Next: session 12n, fresh.

Expected values at session-12n open, as at this closeout (2026-10-13):
- HEAD: the 12m closeout commit (child of `432952e6f62929d15bad77f423c1b00680e3c68a`; its full hash is in the closeout block), clean and **level with origin** (R370). The plan file's sha256 and byte count are in the closeout block, not here, since this file cannot name its own hash.
- **The decision log in this file ends at R627** (or at the last wrap ruling, if any is transcribed before 12n).
- **The gate** on this tree: **{f(passed)} passed / {xfail} xfailed / {desel} deselected**, collected **{f(collected)}**, 0/0 against `{D}/collected_ids_12m_close.txt` (identical to `12m_U12/ret_evidence/collected_ids_ret.txt`), band **{lo}–{hi} s** ({wall:.2f} s measured at this closeout; R328's rule); the gate's journal window holds no Ollama request; no-marker collection **3,267** (the 17 live-tier tests skip without `EVIDENCE_ENGINE_LIVE_TIERS=1`); `tests/test_eligibility.py` 91; `inventory --check` in sync; `git diff --stat 954796e HEAD -- engine tests scripts analysis review_specs` empty.
- **The fingerprint of record, unchanged:** `docs/session-reports/migration-02/review_db_fingerprint_20260929T172123Z.json` — 36 tables; overall `0d3eedead60c2ce1c6341b973fa5931d69c4b13c14e93bd5b805bc06734fcb25`; structure `b0dffa3fdd7ff57a4aa6910181c1b02269b9478feee0d33c8d14744ecb1f57aa`; textual `f8d6388751e91a08e5d8580dc2de44a4be965e1dfd628a133950f8ed7d153cee`. No second `data/<dir>/review.db`.
- **Re-read at this closeout by `~/scratch/12i-scope/dbchecks.py`, byte-identical to its 12m-open output** (`{D}/dbchecks_close.txt`): 21 receipts, last 022; the event store (0 `field_events`, 0 `run_manifests`, the three pre-manifest arms and no `local_deepseek_r1_32b` row); paper counts; live PRISMA, valid; live selection 190 / 0 / 0; the parsed-text hash baseline (194 entries, 0 mismatches); the twelve `workflow_state` rows. R71 in force.
- **Retained throwaway reviews:** the eight under `~/scratch/retained/` (file sha256, each unchanged from the 12m open; `{D}/retained_close.txt`). **U06's copy**, outside that directory, at `~/scratch/12l-u06/throwaway/u06_floor_20261012/` — `review.db` file sha256 `fc7d061dc3256bea1ae1be3f9dbd2984e27d7f5acbfa3d11d9fdb587059b7cf8`, fingerprint overall `000db389fa40fd573dff449c33138471f44a3c10585a492dac9affd80397f6ca`, 36 tables, unchanged; not a restore point.
- **The five restore points** (fingerprints, each unchanged from the 12m open; `{D}/restore_close.txt`): `bb39ba81…6c40`, `e564f250…5b63`, `ea05912d…ce01`, `f376562e…9e00`, `62f39128…b79a`.
- **Codebook and live spec, unchanged:** codebook sha256 `89dbfa91b5033f2b3dff713e36c7ecd0aa086739b5832059948b6f55b29ead82`; live spec sha256 `917c24de29afdf802b5ddea61c34627a9bb0a857de3f97395af36d44ca664e5f` (`elicitation: false`, `enabled_arms: []`).
- **Pins and hashes, by `docs/session-reports/session-12/12l_U04/pin_tuples.py`** with both arguments (note (c)); `{D}/pin_tuples_close.json`, byte-identical to `12l_close/pin_tuples_close.json`: **codebook semantic hash `f261af76feadd650f3c58b36498e2d140ef2dc4ffec6a80a16c06741eb2f5646`**; **non-elicited arm pin `d3672ab6fb1d9b627b9fa8dd19112e148e88cf174b1d854bb0b7fff004f7ff5c`**; **elicited arm pin `8f0aeecdeda659a13b3c182fabdc2568523f38891cd692f0a8cf9ce629de3488`**; the stage sets, prompt hashes and library versions as the 12m-open block gives them (ollama 0.6.1, openai 2.24.0, anthropic 0.84.0, pysbd 0.3.4). No commit of this session changed a request, an option, a stage set or a prompt.
- **Ollama:** 0.21.0; digests `qwen3:32b` `030ee887…7840`, `gemma3:27b` `a418f583…2203`, `deepseek-r1:32b` `edba8017…960c`; `NRestarts=0`, service start Fri 2026-09-11 21:09:13 UTC. **Residency, recorded and not matched:** {resident} at this closeout, read after the gate; this session loaded nothing. The 07:00 UTC health job loads every installed model in turn each morning, so the resident model at the 12n open is whichever it loaded last.
- **Read `mode=ro`, a WAL database's `-shm` is touched:** live `review.db-shm` carries an mtime of 2026-10-13 from this session's read-only connections; the main file (mtime 2026-09-29 17:21:15 UTC) and the 0-byte `-wal` are unchanged, and the fingerprint is the check.
- **The pre-tag list in this file is 268 rows — Class 1 / 2 / 3 = 95 / 50 / 123, and 0 without a class**; P1 57 rows (26 / 20 / 11), 35 fix units; P7 110; section L carries "L (12m additions, R625)", "(R626)" and "(R627)" (`INT-m12-1` … `INT-m12-5`).
- **Scratch:** the six 12m directories — `12m-open/`, `12m-U12/`, `12m-R625/`, `12m-U12-C1/`, `12m-U12-RET/` and `12m-close/` — beside the thirty-one present at the 12m open and `retained/`; the session scratchpad under `/tmp` may be absent, and nothing in it is needed.
- **claude-config:** `1ef81a820850c0dd8b2bd512409b117f53beb7fa` (12m's item 0), level with origin, with a hook-appended `PROJECT_LEDGER.md` change uncommitted at this closeout ({ledger_stat} by `git diff --stat`); the wrap settles the ledger, and the claude-config HEAD expected at the 12n open is the one the wrap reports after it.
- **The 12m lane line is proposed to a file, not applied:** `~/scratch/12m-close/lane_line_12m.txt`, sha256 `{sha}`, {len(b):,} bytes — the five "-" lines now in `primer.md` (the lines 12m's item 0 applied) and the five "+" lines that replace them. Its first "+" line names HEAD as "the 12m closeout commit (child of `432952e`)" rather than by hash, because the file's sha256 is recorded in the commit that hash would name.
- **The 2026-10-13 09:00 UTC nightly, checked at the 12n open** (a prediction at this closeout, note (d)): `logs/nightly_test_20261013.log` reads **3,249 passed, 17 skipped, 1 xfailed**, 3,267 collected, exit 0; the Ollama journal for 09:00–09:25 UTC holds **zero POST and zero `/api/show`**; the real lock file's mtime is unchanged (2026-07-27 23:22:08 UTC). The 07:00 UTC health job's model loads are expected and are not a finding. Any other nightly result is reported, not adapted to.

12n's first items, in this order:
- **Item 0** — the 12m lane line, applied to `~/claude-config/primer.md` from `~/scratch/12m-close/lane_line_12m.txt` (sha256 as recorded above), not from a paste.
- **The startup verify** against this block.
- **The nightly check**, as the block's last entry states it.
- **Then U12 commit 2** (every local model-calling entry point holds the experiment lock, by static census), under its own brief, in a fresh CC session (R51); then commits 3–8 in the R625(d) order.

"""
p = "docs/plan/ENGINE_REFACTOR_PLAN.md"; s = open(p, encoding="utf-8").read()
a = "\n## Decision log\n"; assert s.count(a) == 1 and "Session 12m closed" not in s
k = s.index(a); assert s[:k].rstrip("\n").endswith("under the three constraints quoted above.")
print(LANE, sha, len(b)); print(f"gate {passed}/{xfail}/{desel} {wall}s collected {collected} band {lo}-{hi}")
if "--apply" in sys.argv:
    open(LANE, "w", encoding="utf-8").write(lane)
    open(p, "w", encoding="utf-8").write(s[:k].rstrip("\n") + "\n\n" + blk.rstrip("\n") + "\n" + s[k:]); print("applied")
else:
    print(blk[:300])
