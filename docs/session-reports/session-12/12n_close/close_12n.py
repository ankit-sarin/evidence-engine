"""12n-CLOSE — writes Block 34 (ruling R636), the C5B hash in the U12 table's commit 5 note, and the
12n closure with the 12o expected values, from values measured at this closeout
(docs/session-reports/session-12/12n_close/ and ~/scratch/12n-close/).
usage, from the repository root:   close_12n.py [--apply]"""
import sys
P = "docs/plan/ENGINE_REFACTOR_PLAN.md"; D = "docs/session-reports/session-12/12n_close"
s = open(P, encoding="utf-8").read()
assert "R636" not in s and "Block 34" not in s and "Session 12n closed" not in s
assert s.endswith("The decision log in this file ends at R635.\n")
collected = sum(1 for _ in open(f"{D}/collected_ids_12n_close.txt")); assert collected == 3299
last4 = [903.11, 861.36, 886.73, 905.76]; mean = sum(last4) / 4
lo, hi = round(mean * 0.9), round(mean * 1.1)

RULING = '''R636(a) — 12n-U12-C5B is accepted as landed at f4a5586: tests/test_nightly_script.py,
  four cases, static and shim-driven, with the script unedited. The scratch-home
  route (a scratch $HOME with an empty venv activation and a python shim first on
  PATH) is accepted in place of the interpreter override the brief's I3 proposed; no
  override is added to a cron-run script.
R636(b) — The 2026-10-14 09:00 UTC nightly's expected values are
  docs/session-reports/session-12/12n_U12_C5B/expected_nightly_20261014.md, which
  supersedes 12n_U12_C5's file.
R636(c) — Under R633(b), the band for session 12o is cut from the last four standard
  gates of 12n (C3, C4, C5-dry, C5B) and written in "Expected values at session-12o
  open". Three of 12n's five gates over 900 s under the old band is recorded as
  variance, not a trend; a trend is declared only on a durations comparison that
  shows a phase or a file growing across gates.
R636(d) — Session 12n closes with U12 commits 1–5 and 5B landed and commits 6–8 open.
  Commit 6 (lazy import of the provider SDKs, R625(a)(4)) opens 12o after the
  nightly check, in a fresh CC session, with a read of every importer of engine.cloud
  before any edit.'''
flat = " ".join(RULING.split()); assert " | " not in flat
old = "the child of `31cc4add0f2b77aff0700b53e7fc3aa7abd79ce8`; the closure records the hash"
assert s.count(old) == 1
s = s.replace(old, "`f4a5586577beb559aa8ee1f0a7c26dc3302b5523`, recorded 2026-10-13 under R636(a); as written it read \"the child of `31cc4add0f2b77aff0700b53e7fc3aa7abd79ce8`\"")
block = f'''
**Block 34.** *Transcribed 2026-10-13 (item 0 of the 12n-CLOSE brief), in the session's closeout commit with the 12n closure, in the R597(e) form:* ruling R636, made by PI on architect recommendation on 2026-10-13 UTC, during session 12n, on the 12n-U12-C5B report (unit U12, the nightly script's test, landed as `f4a5586577beb559aa8ee1f0a7c26dc3302b5523`; evidence in `docs/session-reports/session-12/12n_U12_C5B/`). The ruling as issued, with its line breaks:

```text
RULING R636 (PI, 2026-10-13 UTC, session 12n, on architect recommendation, on the
12n-U12-C5B report):

{RULING}
```

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-10-13 | {flat} | PI, 2026-10-13, during session 12n |

The decision log in this file ends at R636.
'''

closure = f'''Session 12n closed 2026-10-13 UTC; opened 2026-10-13 15:39 UTC on the 12m HEAD `60c6d3e15d4133b1af07794597dfd5d896acd89a`, CC session `d6bf1f3d` (one session, continued across context summaries). **Unit U12 advanced from commit 1 to commit 5**: commit 2's Phase A read-out and its rulings, the screen2f retirement it needed, and commits 2, 3, 4, 5 and 5B. **No model call and no live write.** Briefs, in order: 12n-OPEN, 12n-U12-C2A, 12n-U12-C2B (with a resume and the resume 12n-U12-C2B-R1), 12n-U12-C2 (with the resume 12n-U12-C2-R1), 12n-U12-C3 (with the resumes R1 and R2), 12n-U12-C4 (with the resume R1), 12n-U12-C5 (with the resume R1), 12n-U12-C5B and this closeout.

**Commits, in order.**

- claude-config `c37fc507a21ccc3c004663998ebe1d5a5abb7a99` — item 0 of the open: the 12m lane line applied to the global primer, pushed.
- `469ebe5` — R628 (Block 26): the 12n-open verify accepted; R626(e) closed as operator reads, on the 12l U10 and U11 gate windows (`12n_R628/`).
- `98b9af6` — 12n-U12-C2A: commit 2's Phase A read-out (`12n_U12_C2A/12n_U12_C2A.md`) — the lock census (86 entry points; 19 with a path to a local model call, 6 locked, 12 not, `run_pipeline` for extract only), the outer-lock deadlock reproduced in four forms on a scratch lock, design options not chosen, five findings.
- `7338c85` — R629 (Block 27): commit 2's design as ruled; INT-n12-1; notes on A-1, on commit 2's row and on HK-RETIRE; recount 269.
- `9390328` — **the screen2f retirement** (R629(d)): `analysis/eval/run_screen2f.py` and `screen2f_worker.py` removed under R31 with five test cases, two ledger rows, the inventory regenerated. Evidence, `12n_U12_C2B/`.
- `0457978` — R630 (Block 28): the retirement's gate accepted at 908.49 s.
- `944efd3` — R631 (Block 29): the untracked `analysis/paper1/runner_smoke_phase2a.py` archived outside the repository; INT-n12-2; recount 270.
- `f6a13a6` — **U12 commit 2**: `hold_experiment_lock()` makes one non-blocking attempt and raises `ExperimentLockHeld`; eleven entry points take the lock in their own entry, `run_pipeline` around the whole run; `tests/test_lock_census.py` is the acceptance test. Evidence, `12n_U12_C2/`.
- `4db7a6b` — R632 (Block 30): commit 2 accepted; its cell records the hash.
- `9a00dc9` — **U12 commit 3**: `restart_ollama` honours `EVIDENCE_ENGINE_NO_OLLAMA_RESTART`; rule 12 of the standing session rules (R632(d)). Evidence, `12n_U12_C3/`.
- `d3dd335` — R633 (Block 31): commit 3's gate accepted at 903.11 s; the band rule amended.
- `f7f7409` — **U12 commit 4**: `run_manifest.failure_reason(exc, context=None)` writes "<ExceptionClass>: <message>" at all six failed-close sites. Evidence, `12n_U12_C4/`.
- `69487eb` — R634 (Block 32): the reason form with its optional context; commit 4 accepted.
- `e240cea` — **U12 commit 5**: `scripts/nightly_tests.sh` runs the standard gate's selection, logs "pytest exit code: N" and exits with it, writes a durations file; CLAUDE.md carries R627(c)'s text. Evidence, `12n_U12_C5/`.
- `31cc4ad` — R635 (Block 33): commit 5 accepted; the script's test ordered.
- `f4a5586` — **U12 commit 5B**: `tests/test_nightly_script.py`, four cases, the script unedited. Evidence, `12n_U12_C5B/`.
- **This closeout** — the commit that carries this paragraph: R636 (Block 34), this closure and the 12o expected values, `{D}/`.

**Rulings R628–R636, all transcribed. The decision log in this file ends at R636.**

- R628 — the 12n-open verify accepted; R626(e) closed; Ollama reads are made before and after a gate, never during one.
- R629 — commit 2's design: no acquisition waits; one declared wrapper, found by static census; scope the three trees with no exemption, the screen2f pair retired; `run_pipeline` takes the lock in its command-line entry.
- R630 — the retirement's gate accepted as reported; the band not re-cut on that run.
- R631 — the untracked entry point archived, not retired; the census reads the working tree as found; INT-n12-2.
- R632 — commit 2 accepted; a commit that changes a script's imports changes the collected count (rule 12).
- R633 — commit 3's gate accepted; the band is ±10% of the mean of the session's last four standard gates; the inventory is regenerated in any commit that moves a line number it records.
- R634 — the failed-close reason form with an optional site-supplied context; commit 4 accepted.
- R635 — commit 5 accepted; the nightly script's test lands in its own commit.
- R636 — commit 5B accepted with the script unedited; the 12o band; 12n closes with commits 6–8 open.

**Stops, and what they exposed.** Each stop was made before any edit, or with the tree restored clean and the work saved as a patch, and reported both values.

| Brief | The assumption | What was on disk | Settled by |
| --- | --- | --- | --- |
| 12n-OPEN | the gate's Ollama journal window is empty | three `GET` reads at 15:41:17 UTC — **my own version, tags and ps reads, run beside the gate** | R628: accepted on the basis of the 09:00 UTC nightly's empty window; reads bracket a gate from then on |
| 12n-U12-C2B | "RULING R629, as pasted above in this session" | the ruling had not been pasted | the resume carried it |
| 12n-U12-C2B, item 1 | the retirement's gate lands in the 814–900 s band | 908.49 s, counts exact | R630 |
| 12n-U12-C2 | C3's twelve entry points are in the repository | `analysis/paper1/runner_smoke_phase2a.py` was ignored by `.gitignore` and never committed; found when the acceptance census on a `git archive` of HEAD gave 83 entry points against 84 on disk. **I had edited that file before finding it; the edit was reversed by hand and its pre-edit bytes are unverified** | R631: archived to `~/scratch/12n-c2/archive/`; eleven entry points |
| 12n-U12-C3 | CLAUDE.md's restart text stays true; the guard tests patch `foreign_lock_held` | the opt-out bullet would have become false; the tests use a real holder process | the resume's A1–A5 |
| 12n-U12-C3, item 1 | the gate lands in the band | 903.11 s, counts exact | R633 |
| 12n-U12-C4 | three import-path assertions pin the failed reason | four, plus one pinning an entry label that the plain form would have dropped | the resume's A1–A8; R634 |
| 12n-U12-C5 | the nightly's no-marker collection is 3,295; R627(c)'s text covers every CLAUDE.md line | 3,312; one sentence's final text was not carried and a further line became false | the resume's A1–A5 |
| 12n-U12-C5 | the brief's tests are complete | the test `12l_P1A.md` named for commit 5 was not in it | R635(c): commit 5B |

**Unit closure table.** R510's rows out stay 0 until P1's close (R607(d)); this table is the record of what each landed commit closed. **U12 is in progress.**

| U12 commit (R625(d) order) | Rows | Commit | State |
| --- | --- | --- | --- |
| 1 | INT-m12-1, INT-m12-2, INT-m12-4 | `9cf9318` | landed in 12m |
| the ten-script retirement | ten ledger rows | `954796e` | landed in 12m |
| the screen2f retirement (R629(d)) | two ledger rows | `9390328` | landed |
| 2 — every local model-calling entry point holds the lock; no acquisition waits | A-1, INT-g12-7; N19, N22, L11-C4 | `f6a13a6` | landed. INT-g12-7 closed; A-1 closed for its own statement (its P2 notes stay P2) |
| 3 — `restart_ollama` honours the opt-out | N17 | `9a00dc9` | landed |
| 4 — a failed close records its exception class and message | INT-l12-14 | `f7f7409` | landed. INT-l12-14 closed |
| 5 — the nightly runs the standard gate; CLAUDE.md's opt-in text | A-1N, B-F17 (f) | `e240cea`, test `f4a5586` | landed. A-1N and B-F17 (f) closed; **the first scheduled run of the edited script is 2026-10-14 09:00 UTC and is unread** |
| 6 — a fresh database needs no provider SDK, by lazy import (R625(a)(4)) | B-F17 (g) | — | open; opens 12o (R636(d)) |
| 7 — requirements (R625(a)(9)) | B-F17 (a)–(e) | — | open |
| 8 — the empty-selection stats key | INT-l12-25 | — | open |

**Not in the repository.** `analysis/paper1/runner_smoke_phase2a.py` is at `~/scratch/12n-c2/archive/analysis/paper1/` (sha256 `6b053f0b19b289b52d32053bea5568ab594dfa42b9000e3bfbda7c60da15b810`, 9,539 bytes, with a README); its stale compiled cache is left in `analysis/paper1/__pycache__/` (R632(e)).

**Pending PI question, carried.** R624(b), noted on INT-i12-69, before U16. **The comparison rehearsal (U13)** is not started.

**Measured — the recount** (`docs/session-reports/session-12/12n_R631/recount_12n_r631.py` → `recount_12n_r631.json`, the chain back through `recount_12n_r629.py` and `recount_12m_r627.py`, re-run at this closeout and reproducing its committed JSON). **The pre-tag list in this file is 270 rows — Class 1 / 2 / 3 = 95 / 50 / 125, and 0 without a class.** **R510 for the session: rows in 2 (INT-n12-1, INT-n12-2, both opened), rows out 0.** No ruling of the session moved a class. P1 is 57 rows (26 / 20 / 11) in 35 fix units; P7 is 112.

**Measured — the session's gates.** No gate is run at this closeout (the brief bars it); the tree's code is unchanged since the 5B gate. In order, standard gate, wall seconds: 853.31 (12n-OPEN), 908.49 (the retirement), 856.52 (commit 2), 903.11 (commit 3), 861.36 (commit 4), 886.73 (commit 5, the script's dry run), 905.76 (commit 5B: **3,298 passed / 1 xfailed / 17 deselected, {collected:,} collected**, 2026-10-13T21:42:42Z to 21:57:50Z). Every gate from commit 2 on has a durations file and, from commit 3 on, a comparison with the gate before it: no phase slower by more than 10 s in any. Collect-only at this closeout: {collected:,} / 17 deselected, byte-identical to the 5B gate's list (`{D}/collected_ids_12n_close.txt`).

**Requests this session sent to the Ollama server.** No POST and no `/api/show`: no model was loaded or evicted by this session. `GET` reads only: `HEAD /` and `GET /api/ps` at 15:39:18 UTC (the session-start context script), `GET /api/version`, `/api/tags`, `/api/ps` at 15:41:17 (the open's reads, inside its gate window: R628(a)), `GET /api/ps` at 15:55:15 (after that gate), and the three reads of this closeout at 22:36:31 (`{D}/ollama_reads.txt`). Every gate window from the retirement on is empty. The real lock file `~/.ollama_experiment.lock` was never opened; its mtime is 2026-07-27 23:22:08 UTC. Every lock experiment and lock test used a scratch or per-test path.

**Live.** Never written. `db_fingerprint --compare` against the migration-02 record was IDENTICAL at every check of the session — at open, after every push, and at this closeout (overall `0d3eedead60c2ce1c6341b973fa5931d69c4b13c14e93bd5b805bc06734fcb25`, 36 tables). Live was opened `mode=ro` only: by the fingerprint, by `dbchecks.py`, and once for the `run_manifests` DDL (12n-U12-C4, about 19:35 UTC).

*Notes recorded at this close, no rows:*
- (a) The 2026-10-14 09:00 UTC nightly is the first run of the edited script. Until it is read, commit 5 is checked by its dry run and by `tests/test_nightly_script.py` only.
- (b) Wall time: three of the session's seven gates ran above 900 s. The durations comparisons show the difference spread across many database-building phases with none over 0.6 s, and host user CPU about twice as high in the slower windows; what else was running was not identified (R636(c): variance, not a trend).
- (c) `12l_U04/pin_tuples.py` made no Ollama request at this closeout: the journal for its window holds only the three timestamped reads.
- (d) The 12n lane line is built and applied after this commit, because it names this commit's hash; `{D}/` therefore does not hold it. It is `~/scratch/12n-close/lane_line_12n.txt`, and its sha256 and byte count are in the closeout block.

**Session 12n closed.** Next: session 12o, fresh.

Expected values at session-12o open, as at this closeout (2026-10-13):
- HEAD: the 12n closeout commit (child of `f4a5586577beb559aa8ee1f0a7c26dc3302b5523`; its full hash is in the closeout block and the lane line), clean and **level with origin** (R370). The plan file's sha256 and byte count are in the closeout block and the lane line, since this file cannot name its own hash.
- **The decision log in this file ends at R636** (or at the last wrap ruling, if any is transcribed before 12o).
- **The gate** on this tree: **3,298 passed / 1 xfailed / 17 deselected**, collected **{collected:,}**, 0/0 against `{D}/collected_ids_12n_close.txt`; **band {lo}–{hi} s** — ±10% of {mean:.2f} s, the mean of the session's last four standard gates (903.11, 861.36, 886.73, 905.76; R633(b), R636(c)); Ollama version, tags and ps reads before the gate is launched and after it exits only, each timestamped (R628(c)); the gate's journal window empty; a durations file written (`--durations=0`) and compared with `12n_U12_C5B/durations.txt`, any phase slower by more than 10 s reported.
- **The 2026-10-14 09:00 UTC nightly**, read against `docs/session-reports/session-12/12n_U12_C5B/expected_nightly_20261014.md`: `logs/nightly_test_20261014.log` reads **3,298 passed, 17 deselected, 1 xfailed**, `collected 3316 items / 17 deselected / 3299 selected`, last line **"pytest exit code: 0"**; `logs/nightly_durations_20261014.txt` present; the Ollama journal for 09:00–09:25 UTC empty; the real lock file's mtime unchanged. The 07:00 UTC health job's model loads are expected. Any other nightly result is reported, not adapted to.
- No-marker collection **3,316** (the 17 live-tier tests skip without `EVIDENCE_ENGINE_LIVE_TIERS=1`); `tests/test_eligibility.py` 91; `inventory --check` in sync; `git diff --stat f4a5586 HEAD -- engine tests scripts analysis review_specs` empty.
- **The fingerprint of record, unchanged:** `docs/session-reports/migration-02/review_db_fingerprint_20260929T172123Z.json` — 36 tables; overall `0d3eedead60c2ce1c6341b973fa5931d69c4b13c14e93bd5b805bc06734fcb25`; structure `b0dffa3fdd7ff57a4aa6910181c1b02269b9478feee0d33c8d14744ecb1f57aa`; textual `f8d6388751e91a08e5d8580dc2de44a4be965e1dfd628a133950f8ed7d153cee`. No second `data/<dir>/review.db`.
- **Re-read at this closeout by `~/scratch/12i-scope/dbchecks.py`, byte-identical to its 12m-close output** (`{D}/dbchecks_close.txt`): 21 receipts, last 022; the event store (0 `field_events`, 0 `run_manifests`, the three pre-manifest arms and no `local_deepseek_r1_32b` row); paper counts; live PRISMA, valid; live selection 190 / 0 / 0; the parsed-text hash baseline (194 entries, 0 mismatches); the twelve `workflow_state` rows. R71 in force.
- **Retained throwaway reviews:** the eight under `~/scratch/retained/` (file sha256, each unchanged from the 12m close; `{D}/retained_close.txt`). **U06's copy** at `~/scratch/12l-u06/throwaway/u06_floor_20261012/` — `review.db` file sha256 `fc7d061dc3256bea1ae1be3f9dbd2984e27d7f5acbfa3d11d9fdb587059b7cf8`, fingerprint overall `000db389fa40fd573dff449c33138471f44a3c10585a492dac9affd80397f6ca`, 36 tables, unchanged; not a restore point.
- **The five restore points** (fingerprints, each unchanged from the 12m close; `{D}/restore_close.txt`): `bb39ba81…6c40`, `e564f250…5b63`, `ea05912d…ce01`, `f376562e…9e00`, `62f39128…b79a`.
- **Codebook and live spec, unchanged:** codebook sha256 `89dbfa91b5033f2b3dff713e36c7ecd0aa086739b5832059948b6f55b29ead82`; live spec sha256 `917c24de29afdf802b5ddea61c34627a9bb0a857de3f97395af36d44ca664e5f` (`elicitation: false`, `enabled_arms: []`).
- **Pins and hashes, by `docs/session-reports/session-12/12l_U04/pin_tuples.py`** with both arguments; `{D}/pin_tuples_close.json`, byte-identical to `12m_close/pin_tuples_close.json`: **codebook semantic hash `f261af76feadd650f3c58b36498e2d140ef2dc4ffec6a80a16c06741eb2f5646`**; **non-elicited arm pin `d3672ab6fb1d9b627b9fa8dd19112e148e88cf174b1d854bb0b7fff004f7ff5c`**; **elicited arm pin `8f0aeecdeda659a13b3c182fabdc2568523f38891cd692f0a8cf9ce629de3488`**. No commit of this session changed a request, an option, a stage set or a prompt.
- **Ollama:** 0.21.0; digests `qwen3:32b` `030ee887…7840`, `gemma3:27b` `a418f583…2203`, `deepseek-r1:32b` `edba8017…960c`; `NRestarts=0`, service start Fri 2026-09-11 21:09:13 UTC. **Residency, recorded and not matched:** `llama3.2:3b`, keep_alive Forever, at 22:36:31 UTC, as at the 12n open; this session loaded nothing. The 07:00 UTC health job loads every installed model in turn each morning.
- **Read `mode=ro`, a WAL database's `-shm` is touched:** live `review.db-shm` carries an mtime of 2026-10-13 from this session's read-only connections; the main file (mtime 2026-09-29 17:21:15 UTC, 134,361,088 bytes) and the 0-byte `-wal` are unchanged, and the fingerprint is the check.
- **The pre-tag list in this file is 270 rows — Class 1 / 2 / 3 = 95 / 50 / 125, and 0 without a class**; P1 57 rows (26 / 20 / 11), 35 fix units; P7 112; section L carries "L (12n additions, R629)" and "(R631)" (`INT-n12-1`, `INT-n12-2`).
- **Scratch:** the nine 12n directories — `12n-open/`, `12n-c2a/`, `12n-c2b/`, `12n-c2/` (with `archive/`), `12n-c3/`, `12n-c4/`, `12n-c5/`, `12n-c5b/` and `12n-close/` — beside the thirty-seven present at the 12n open and `retained/`.
- **claude-config:** at this commit, `c37fc507a21ccc3c004663998ebe1d5a5abb7a99` with a hook-appended `PROJECT_LEDGER.md` change uncommitted (22 insertions). The 12n lane line is applied in the commit after this one, with those rows; the wrap may add a ledger commit. The claude-config HEAD expected at the 12o open is the one the closeout block or the wrap reports last.

12o's first items, in this order:
- **Item 0** — any ledger rows the wrap left uncommitted in claude-config.
- **The startup verify** against this block, after 10:35 UTC on 2026-10-14.
- **The nightly check**, as the block states it.
- **Then U12 commit 6** (lazy import of the provider SDKs, R625(a)(4)), under its own brief, in a fresh CC session, with a read of every importer of `engine.cloud` before any edit (R636(d)); then commits 7 and 8.

'''
a = "\n## Decision log\n"; assert s.count(a) == 1
k = s.index(a); assert s[:k].rstrip("\n").endswith("then commits 3–8 in the R625(d) order.")
print(f"band {lo}-{hi} (mean {mean:.2f}); collected {collected}")
if "--apply" in sys.argv:
    open(P, "w", encoding="utf-8").write(s[:k].rstrip("\n") + "\n\n" + closure.rstrip("\n") + "\n" + s[k:] + block); print("applied")
