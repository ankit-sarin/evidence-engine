# VERIFY-EXIT-01 (parse-gate close) — exit-state verification ledger

**Task:** VERIFY-EXIT-01 (parse-gate close) — verify the PARSE-GATE session's printed
closeout against disk.
**Subject session:** `00d781e5-7b67-47b3-93b3-1ce18e5d3bfc` (PARSE-GATE-00…07).
**Machine:** DGX Spark (`spark-59e4`), TZ **UTC** (`Etc/UTC`, clock NTP-synchronised).
**Date of verification:** 2026-09-08, 19:38–19:44 UTC.
**Mode:** read-only. No commit, no DB write, no Ollama restart, no edit to any file other
than this report.

**Verdict up front: 10 / 10 MATCH. Every claim in the closeout survived contact with disk.**

---

## Step 0 — session archive

Usage read before invoking (`bin/session-archive --help`): *"Archive ONE closed Claude Code
session — its transcript and its job record together — into `~/claude-session-archive/`.
Refuses any session that is LIVE. Moves only; nothing is ever removed."* Assumption **I2**
holds in full, including the already-archived no-op.

Command: `~/projects/dgx-infra/bin/session-archive 00d781e5-7b67-47b3-93b3-1ce18e5d3bfc`

Verbatim stdout:

```
already archived 00d781e5-7b67-47b3-93b3-1ce18e5d3bfc: transcript=00d781e5-7b67-47b3-93b3-1ce18e5d3bfc.jsonl job=none in /home/ankitsarin/claude-session-archive [nothing to do]
```

Exit 0. The transcript was already in the archive — the expected no-op branch, not a
refusal.

---

## Check ledger

| # | expected (Plan v44 §9 item 1) | observed | result | note |
|---|---|---|---|---|
| **C1** | HEAD `5e58020`, tree clean, nothing to fetch, HEAD == origin | HEAD `5e580205197fa30d2f7b4facea8c34bdf38a4027`; `git status --porcelain` **empty**; `git fetch --dry-run` **silent**, exit 0; `origin/main` == HEAD; `git ls-remote origin refs/heads/main` == `5e58020…` | **MATCH** | Sync confirmed against the *remote ref itself*, not only the local tracking ref. `refs/remotes/origin/HEAD` is unset (normal for a clone that never ran `set-head`); `origin/main` used instead. |
| **C2** | 1,851 pass at `-m "not network and not ollama and not integration"` | `1851 passed, 17 deselected in 254.30s (0:04:14)`; collected **1,868**, selected 1,851, **0 skipped, 0 failed, 0 errors**; exit 0; wall clock 301 s incl. interpreter start | **MATCH** | Marker expression used verbatim. Ollama untouched across the run: `NRestarts=0` before and after, `ExecMainStartTimestamp` unmoved, no `Started/Stopping/Stopped` in the unit journal for the window. |
| **C3** | `review.db` mtime 2026-09-08 03:48:48, size 99,770,368 B | mtime **2026-09-08 03:48:48.732577380 +0000**, size **99,770,368** | **MATCH** | Sub-second mtime equals the `parse_attempts` row-8 `created_at` (`03:48:48.723172`) to within 9 ms — the last PARSE-GATE-07 write is demonstrably the last write to the file. |
| **C4** | `parse_attempts` 8, `full_text_assets` 794 | **8** and **794** | **MATCH** | Eight rows = four papers × two attempts, all `parsed_text_version=3`; table detail below. |
| **C5** | 455/586/699/719 all `AI_AUDIT_COMPLETE` | all four `AI_AUDIT_COMPLETE`; `papers.updated_at` still **2026-03-18** on all four | **MATCH** | Column confirmed from `sqlite_master` (`papers.status`), not assumed. The March `updated_at` is independent evidence that `update_status` was never called. |
| **C6** | v2 sha256 455 `1e459a15…`, 586 `dd8935f5…`, 699 `a83f443f…`, 719 `4cbc3557…` | all four match; full hashes below; mtimes all still **2026-03-14** | **MATCH** | v2 files untouched by the re-parse, as claimed. |
| **C7** | v3 present, 52,503 / 40,548 / 27,069 / 28,867 B | **52,503 / 40,548 / 27,069 / 28,867** | **MATCH** | Exact to the byte on all four. Paths resolved from `full_text_assets.parsed_text_path`, not guessed. |
| **C8** | two `review.db` backups present | both present, both sha256-identical to the primer record, distinct inodes from the live DB | **MATCH** | Detail below. |
| **C9** | Ollama 0.21.0, `NRestarts=0` | `ollama version is 0.21.0`; `NRestarts=0`; `ActiveEnterTimestamp` = `ExecMainStartTimestamp` = **Mon 2026-08-31 00:41:59 UTC**; state active/running | **MATCH** | Unit name confirmed as `ollama.service` from `systemctl show`. Uptime 8d 19h — never restarted across the whole PARSE-GATE lane. |
| **C10** | `VERIFY-EXIT-01_report.md` exists, tracked at `377b4ec`; no PARSE-GATE report yet | file present (6,433 B, 2026-09-07 19:50), tracked, `git log` → `377b4ec docs(session-reports): VERIFY-EXIT-01 close-state verification ledger`; **zero** files matching `PARSE-GATE*` in `docs/session-reports/` | **MATCH** | 29 files tracked in that directory. Note a **third** file already uses this stem: `VERIFY-EXIT-01_elicit01_report.md` — so the stem now has three distinct occupants and the disambiguating suffix is load-bearing. |

**Literal pass count: 10 / 10.**
**Effective pass count: 10 / 10.** (No mismatch, therefore no wording-vs-state attribution
to make.)

---

## Supporting detail

### C4 — the eight `parse_attempts` rows

All eight carry `parsed_text_version = 3`; all `skipped_reason` NULL (every parser named was
actually run).

| id | paper | attempt | parser | passed | accepted | elapsed | failures |
|---|---|---|---|---|---|---|---|
| 1 | 455 | 1 | `docling` | 0 | 0 | 2.1 s | `error: ConversionError … 1 validation error for PdfHyperlink / uri … input_value='dx.doi.org/10.1016/j.cmpb.2013.01.017'` |
| 2 | 455 | 2 | `docling_sanitized` | 1 | **1** | 20.3 s | `[]` |
| 3 | 586 | 1 | `docling` | 0 | 0 | 13.0 s | `[["GLYPH_DENSITY", 7.831, 5.0]]` |
| 4 | 586 | 2 | `docling_ocr` | 1 | **1** | 48.7 s | `[]` |
| 5 | 699 | 1 | `docling` | 0 | 0 | 7.6 s | `[["GLYPH_DENSITY", 8.631, 5.0]]` |
| 6 | 699 | 2 | `docling_ocr` | 1 | **1** | 37.6 s | `[]` |
| 7 | 719 | 1 | `docling` | 0 | 0 | 13.5 s | `[["GLYPH_DENSITY", 19.586, 5.0]]` |
| 8 | 719 | 2 | `docling_ocr` | 1 | **1** | 37.1 s | `[]` |

Tier/parser sequence, verdicts and elapsed times agree with the PARSE-GATE-07 table in
`primer.md` row for row. Vision (`qwen2.5vl`) appears nowhere — consistent with the claim
that the vision route was never reached.

### C5 / C6 / C7 — files and identities

| paper | ee_identifier | status | v2 sha256 | v2 size | v3 sha256 | v3 size |
|---|---|---|---|---|---|---|
| 455 | EE-303 | AI_AUDIT_COMPLETE | `1e459a15005549fc89250a34c3d35b417045d56c13893d36276e1836eb616e24` | 60,537 | `71d3448949a0952d4c08dd0b5ada3b6889115b534b5993d16636ce94916c6e51` | **52,503** |
| 586 | EE-434 | AI_AUDIT_COMPLETE | `dd8935f5f746e1febe213223040c77b75e1684e3e40f4f8097efd9bd096856fa` | 69,223 | `29eb250acb1db6c2e5bea63d1fd48fa5873581d3aa5285e2a1934e66dede2866` | **40,548** |
| 699 | EE-547 | AI_AUDIT_COMPLETE | `a83f443fb1e6b9ee2520007058837a75aa3c5ddf8aaff2ee25c4291f51fe9b8b` | 48,568 | `5313b9e201163cf5a79345a7a6a3325ab14c265c2f568501983c39be7ef54de7` | **27,069** |
| 719 | EE-567 | AI_AUDIT_COMPLETE | `4cbc3557ff5ef4b0d562f06eb01c23a7ab4536021f35021d13988056edc7cff3` | 279,426 | `37bfcdf8094609ff9397e8059b73f40588cd51214164c4d4cd40ee4b42f1500f` | **28,867** |

All under `data/surgical_autonomy/parsed_text/{id}_v{n}.md`, each path taken from the
`full_text_assets` row rather than a path convention (assumption **I4** holds: v2 and v3 are
separate files, one row each, `parser_used` `pymupdf`/`docling` for v2 and
`docling_sanitized`/`docling_ocr` for v3). v2 mtimes are all 2026-03-14; v3 mtimes are
2026-09-08 03:45–03:48 and equal their `parse_attempts.created_at` to the millisecond.

**Reading note on the v2/v3 numbers.** The primer's PARSE-GATE-07 table quotes *character*
counts (52,403 / 40,486 / 27,045 / 28,836); the plan quotes *byte* sizes
(52,503 / 40,548 / 27,069 / 28,867). These are the same files — the gap is multi-byte UTF-8.
C7 is a byte check and matches the byte figures exactly.

### C8 — backups

| path | size | sha256 | inode |
|---|---|---|---|
| `data/backups/review_pre_PARSE-GATE-03_20260907T230211Z.db` | 99,753,984 | `71a7d59e4985f3747c86cb0de61c1b92868680c627b56cbc21fe09bf48f3b2fc` | 15470940 |
| `data/backups/review_pre_PARSE-GATE-07_20260908T034445Z.db` | 99,762,176 | `bcf4fa1bcafedeeb0930413c73f41a521ca7adb0b9431cf78ed52510d4452901` | 15473282 |

Live DB inode **15363042** — distinct from both, so neither backup is a hard link to the
live file. Both hashes equal the values recorded in `primer.md`. Located from the primer's
named paths (assumption **I5** holds); no filesystem-wide search was performed.

---

## Gate 5 — the task wrote nothing to the database

`review.db` size and mtime, measured **before** any work (19:38:47) and **after** the suite
run (19:43:44):

```
before: size=99770368 mtime=2026-09-08 03:48:48.732577380 +0000
after:  size=99770368 mtime=2026-09-08 03:48:48.732577380 +0000
```

Identical. All queries were issued over
`sqlite3.connect("file:…/review.db?mode=ro", uri=True)`.

One honest qualification: the sidecar `review.db-shm` now carries mtime **19:39:38**. That is
this session's own read-only connection — SQLite requires write access to the shared-memory
index even for a read-only connection to a WAL database. `review.db-wal` is still 0 bytes and
still carries mtime 10:30:35, and the database file itself is untouched. Gate 5 is met as
written.

---

## Observations outside the check set

1. **The WAL sidecars are back, and the cause is benign and identified.** `primer.md` records
   the WAL as "gone — checkpointed" at the PARSE-GATE close. On disk today
   `review.db-wal` (0 B) and `review.db-shm` (32,768 B) exist again, `-wal` stamped
   **2026-09-08 10:30:35.021**. That is one second after
   `dgx-snapshot-user.service` started (`10:30:34`, `snapshot 20260908T103035Z … verdict=ok`),
   and `dgx-infra/snapshot/user_tier.py:157` captures `review.db` via `capture_sqlite`.
   The nightly user-tier snapshot opened the database, which recreates the sidecars; it did
   not modify it — hence `review.db` mtime unchanged at 03:48:48. **Not a defect**, but the
   primer's "WAL is gone" line is a statement with a one-night shelf life and should not be
   quoted forward as a durable property.
2. **The resident Ollama model has changed.** `primer.md` records `gemma3:27b` resident (21 GB,
   `UNTIL Forever`), left over from the unqualified integration run. `ollama ps` now shows
   **`qwen3:8b`, 11 GB, 100 % GPU, context 40960, `Forever`** — so something loaded qwen3:8b
   after the PARSE-GATE close and evicted gemma3 (`MAX_LOADED_MODELS=1`). Harmless, and
   `NRestarts` is still 0, but the primer's "resident model" line is stale.
3. **Working tree is clean including untracked files.** `git status --porcelain -uall` returned
   **empty** before this report was written — `data/` is gitignored wholesale, so the
   PARSE-GATE-07 run artifacts under
   `data/surgical_autonomy/eval/parse_gate07/20260908T034456Z/` (runner, `run_phase2.sh`,
   four phase logs, `phase2.status`) are on disk but invisible to git. They exist and were
   listed.
4. **The `VERIFY-EXIT-01` report stem now has three occupants** —
   `VERIFY-EXIT-01_report.md` (elicit-design-02 close, `377b4ec`),
   `VERIFY-EXIT-01_elicit01_report.md`, and this file. The stem is no longer distinguishing;
   a future brief that says "the VERIFY-EXIT-01 report" without a suffix is ambiguous.
5. **Nothing was found that contradicts a stated assumption.** I1–I7 all held as written.
6. **Added 2026-09-08, after the fact: the C2 command's venv activation was a silent no-op.**
   The suite was launched with a leading `source venv/bin/activate 2>/dev/null`, and this
   project's virtualenv is `.venv`, not `venv` — so that step did nothing and said nothing.
   **The result is unaffected:** `VIRTUAL_ENV` was already
   `/home/ankitsarin/projects/evidence-engine/.venv` and `python` resolved to
   `.venv/bin/python` (3.12.3, pytest 9.0.2), so C2's 1,851 ran on the correct interpreter.
   Recorded because a no-op that prints nothing is invisible to the next reader.

---

## Verdict

The PARSE-GATE session's printed closeout is **accurate in every particular that was checked**.
The repository is at `5e58020`, clean and level with `origin/main` as verified against the
remote ref itself; the standard gate returns 1,851 green with nothing skipped and without
touching Ollama; the live database is byte-identical to its state at the moment of the last
PARSE-GATE-07 write, with the eight ledger rows, 794 assets and four `AI_AUDIT_COMPLETE`
statuses exactly as reported; the four v2 parse outputs are untouched by hash and the four v3
outputs are present at the exact byte sizes claimed; both pre-run backups are present, intact
and independent of the live file; and Ollama has not restarted since 2026-08-31. Ten checks,
ten matches, no cause-unknown residue. The only movement since the close is a nightly snapshot
that reopened the database read-only and a model eviction in Ollama — neither of which alters
any verified value. **Downstream work (the 586/699 version ruling, the `test_judge_pass2`
live-DB guard, the PARSE-GATE consolidation report) is unblocked by this verification;** none
of it was started here.
