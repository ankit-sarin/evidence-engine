# VERIFY-EXIT-01 — FONT-AUDIT close-state verification

**Task:** VERIFY-EXIT-01/font-audit — verify the FONT-AUDIT-02 closeout (session
`19de242b-…`) against disk before any Run 7 work begins.
**Session:** `2885f251-8a7f-442c-84dd-cd1350683f33`
**Run window:** 2026-09-09 22:33:06 → 22:41:20 UTC
**Machine:** DGX Spark (`spark-59e4`), `~/projects/evidence-engine`, `.venv` (Python 3.12.3)
**Nature:** read-only against repo, DB, Ollama and session state. The only writes are this
file and its commit. No `ReviewDatabase` was instantiated; every DB read used
`sqlite3.connect(…?immutable=1, uri=True)`.

---

## Verdict

**CLOSE STATE HOLDS** — all ten checks PASS on substance. Four items diverge from the
brief's *wording* (not its substance) and are recorded below as W1–W4; one new finding
outside the closeout's scope is recorded as **F1**.

---

## Measurements A / B / C — `data/surgical_autonomy/review.db`

Taken before anything else in this task ran (A), immediately after the standard gate (B),
and after every remaining check (C).

```
---STAT A---   (2026-09-09T22:33:06.962407784Z)
data/surgical_autonomy/review.db | size=99770368 | mtime=2026-09-09 19:48:46.784974707 +0000 | epoch=1788983326.784974707

---STAT B---   (after pytest, 2026-09-09T22:38:33Z)
data/surgical_autonomy/review.db | size=99770368 | mtime=2026-09-09 19:48:46.784974707 +0000 | epoch=1788983326.784974707

---STAT C---   (after all checks, 2026-09-09T22:41:20Z)
data/surgical_autonomy/review.db | size=99770368 | mtime=2026-09-09 19:48:46.784974707 +0000 | epoch=1788983326.784974707
```

**A == B == C, byte-for-byte and nanosecond-for-nanosecond.** The suite did not touch
production. WAL/SHM sidecars also unchanged across the window (`review.db-wal` 0 B @
19:49:04.912009856; `review.db-shm` 32768 B @ 20:00:19.116657749 — both predate A).

---

## The ten checks

| # | check | expected (I1–I9) | measured | verdict |
|---|---|---|---|---|
| 1 | Git | HEAD `cd35560` == `origin/main`; tree clean | HEAD `cd35560253917e286cef7c9a9fc6bfadc3f938e6`; `origin/main` identical after `git fetch`; `git status --porcelain` empty; ahead=0 behind=0 | **PASS** |
| 2 | Standard gate | 1,894 passed / 0 failed / 17 deselected | **1894 passed, 17 deselected in 253.27s (0:04:13)**, exit 0, zero warnings | **PASS** |
| 3 | DB after suite | B identical to A and to I3 | B == A == C exactly (see above) | **PASS** |
| 4 | DB contents | 24 tables / 84,400 rows; `parse_attempts` 8 rows × 14 cols, `font_audit` NULL ×8; `full_text_assets` 794 | **23 user tables / 84,395 rows**, **+ `sqlite_sequence` (1 table, 5 rows) = 24 / 84,400**; `parse_attempts` 8 rows, 14 cols, `font_audit` NULL on all 8; `full_text_assets` 794 | **PASS** (see W1) |
| 5 | Four-paper resolution | 455/586/699/719 → v3 in both resolvers, resolvers agree | all four → `<pid>_v3.md` in both; agree; exist; non-empty. Attribution matches I4 exactly | **PASS** |
| 6 | Backups | three present; FONT-AUDIT-02 sha256 begins `aff12bb9` | three present in `data/backups/`; FONT-AUDIT-02 = `aff12bb91d33f06…fba133f1` | **PASS** (see W2) |
| 7 | Session reports | seven files, all tracked at HEAD | all seven present, all tracked at HEAD, none untracked | **PASS** |
| 8 | Ollama | 0.21.0, NRestarts=0 | `ollama version is 0.21.0`; `/api/version` `0.21.0`; `NRestarts=0`; `ActiveEnterTimestamp` = `ExecMainStartTimestamp` = Mon 2026-08-31 00:41:59 UTC; active/running | **PASS** (see W3) |
| 9 | Live-DB guard self-test | self-test green | `pytest tests/test_live_db_guard.py` → **2 passed in 0.27s** | **PASS** (see W4) |
| 10 | Session hygiene | `291609ef` and `19de242b` archived; no orphaned `claude` process | both archived (neither appears in the auditor's transcript table); **no process named `claude`** other than this session's PID 3487652 | **PASS** — with **F1** |

---

## Check detail

### 1 — Git (measured BEFORE this report's commit existed)

```
HEAD        cd35560253917e286cef7c9a9fc6bfadc3f938e6  (cd35560)
origin/main cd35560253917e286cef7c9a9fc6bfadc3f938e6   (after git fetch origin)
git status --porcelain : (empty)
git rev-list --left-right --count origin/main...HEAD : 0  0
```

### 2 — Standard gate

`.venv/bin/python -m pytest tests/ -q -m "not network and not ollama and not integration"`
START 2026-09-09T22:33:33.746777545Z · END 2026-09-09T22:38:33.098555492Z · EXIT=0
`1894 passed, 17 deselected in 253.27s (0:04:13)`. No failures, no errors, **zero warning
lines** in the log. Matches I2 and the primer's 1,894 exactly; no count drift.

### 4 — DB contents (read-only, `immutable=1`)

| table | rows | | table | rows |
|---|---:|---|---|---:|
| abstract_screening_adjudication | 0 | | judge_pair_ratings | 6828 |
| abstract_screening_decisions | 21374 | | judge_ratings | 2276 |
| abstract_verification_decisions | 1422 | | judge_run_audit | 1 |
| audit_adjudication | 0 | | judge_runs | 7 |
| cloud_evidence_spans | 7257 | | papers | 10039 |
| cloud_extractions | 379 | | parse_attempts | 8 |
| evidence_spans | 3760 | | provenance_census_runs | 2 |
| extractions | 190 | | provenance_classifications | 22034 |
| fabrication_verifications | 7422 | | review_runs | 6 |
| ft_screening_adjudication | 36 | | workflow_state | 12 |
| ft_screening_decisions | 366 | | **subtotal (23 user tables)** | **84395** |
| ft_verification_decisions | 182 | | `sqlite_sequence` (5 rows) | 5 |
| full_text_assets | 794 | | **total (24 tables)** | **84400** |

`parse_attempts` columns (14): `id, paper_id, pdf_hash, parsed_text_version, attempt_index,
parser_used, passed, failures, metrics, elapsed_s, accepted, skipped_reason, created_at,
font_audit`. `font_audit IS NULL` on all 8 rows — NULL means nobody looked, as the primer
states.

### 5 — Four-paper resolution

| paper | census `parsed_text_path` | `full_text_assets` (max version) | agree | exists | bytes | `parse_attempts` accepted | `full_text_assets.parser_used` |
|---|---|---|---|---|---:|---|---|
| 455 | `data/surgical_autonomy/parsed_text/455_v3.md` | `data/surgical_autonomy/parsed_text/455_v3.md` (v3) | ✅ | ✅ | 52503 | `docling_sanitized` v3 | `docling_sanitized` |
| 586 | `data/surgical_autonomy/parsed_text/586_v3.md` | `data/surgical_autonomy/parsed_text/586_v3.md` (v3) | ✅ | ✅ | 40548 | `docling_ocr` v3 | `docling_ocr` |
| 699 | `data/surgical_autonomy/parsed_text/699_v3.md` | `data/surgical_autonomy/parsed_text/699_v3.md` (v3) | ✅ | ✅ | 27069 | `docling_ocr` v3 | `docling_ocr` |
| 719 | `data/surgical_autonomy/parsed_text/719_v3.md` | `data/surgical_autonomy/parsed_text/719_v3.md` (v3) | ✅ | ✅ | 28867 | `docling_ocr` v3 | `docling_ocr` |

Resolver definitions used: `full_text_assets` row of maximum `parsed_text_version` per
paper, and `analysis/provenance/census.py::parsed_text_path` (highest-numbered
`parsed_text/{pid}_v*.md` on disk). They agree for all four.

*Incidental, consistent with the known PARSED-PATH-01 item:* the `v1` `full_text_assets`
rows for 586/699/719 carry `parsed_text_path = NULL` and `parsed_at = NULL`. Those are
superseded rows; the max-version rows the resolver returns are all populated.

### 6 — Backups (`data/backups/`)

| file | size | sha256 |
|---|---:|---|
| `review_pre_PARSE-GATE-03_20260907T230211Z.db` | 99,753,984 | `71a7d59e4985f3747c86cb0de61c1b92868680c627b56cbc21fe09bf48f3b2fc` |
| `review_pre_PARSE-GATE-07_20260908T034445Z.db` | 99,762,176 | `bcf4fa1bcafedeeb0930413c73f41a521ca7adb0b9431cf78ed52510d4452901` |
| `review_pre_FONT-AUDIT-02_20260909T194840Z.db` | 99,770,368 | `aff12bb91d33f0644c8eab809a1ffd79de807f395a47bef8753d61c3fba133f1` |

All three sha256 values match `primer.md` verbatim. The FONT-AUDIT-02 backup is the same
size as the live DB, which is expected: `ALTER TABLE ADD COLUMN` rewrites only the schema
record. Its mtime (2026-09-08 03:48:48) is the source file's preserved mtime at copy time,
not the copy time.

### 7 — Session reports (all seven present, all tracked at HEAD, none untracked)

| stem (I6) | actual filename | size | mtime (UTC) |
|---|---|---:|---|
| PARSE-GATE_report | `PARSE-GATE_report.md` | 46263 | 2026-09-08 21:32:13 |
| VERIFY-EXIT-01_parse-gate_report | `VERIFY-EXIT-01_parse-gate_report.md` | 13043 | 2026-09-08 21:32:25 |
| PARSE-GATE-08_readout | `PARSE-GATE-08_readout.md` | 27787 | 2026-09-08 22:33:40 |
| PARSE-GATE-09_readout | `PARSE-GATE-09_readout.md` | 54989 | 2026-09-08 23:29:23 |
| PARSE-GATE-10_readout | `PARSE-GATE-10_readout.md` | 39192 | 2026-09-09 19:37:37 |
| FONT-AUDIT-01_readout | `FONT-AUDIT-01_readout.md` | 18084 | 2026-09-09 16:53:59 |
| FONT-AUDIT-02_readout | `FONT-AUDIT-02_readout.md` | 15201 | 2026-09-09 19:50:37 |

`git status --porcelain docs/session-reports/` returned empty.

### 8 — Ollama

```
ollama version is 0.21.0            /api/version -> {"version":"0.21.0"}
NRestarts=0
ExecMainStartTimestamp=Mon 2026-08-31 00:41:59 UTC
ActiveEnterTimestamp=Mon 2026-08-31 00:41:59 UTC
ActiveState=active  SubState=running
```

Resident model via `/api/ps` (read-only): **`qwen3:8b`**, 11,454,255,232 B, 100 % VRAM,
context 40960, `expires_at 2318-…` (i.e. `OLLAMA_KEEP_ALIVE=-1`). No restart, no pull, no
generate, no model load was performed by this task.

### 9 — Live-DB guard self-test

`tests/_live_db_guard.py` is a **shared-names module** — `REPO_ROOT`, `LIVE_DATA_ROOT`,
`violations`, `LiveDatabaseBlocked(BaseException)`, `resolved_review_db_path()`,
`is_live_data_path()`, `refuse()`. It has **no `__main__` and contains no self-test.** The
self-test is its companion `tests/test_live_db_guard.py`, invoked as an ordinary pytest run:

```
.venv/bin/python -m pytest tests/test_live_db_guard.py -v
  test_guard_refuses_a_construction_under_the_live_data_root PASSED
  test_guard_allows_a_construction_under_tmp_path            PASSED
  2 passed in 0.27s
```

Both are unmarked with respect to the gate's deselect expression, so both also ran inside
check 2's 1,894.

### 10 — Session hygiene (`~/scripts/session-audit.sh`, verbatim)

```
=== dgx-session-audit 1.0 (64a0e24) at 2026-09-09T22:40:05Z ===

-- Claude Code sessions (6 transcripts, 0 job-only, 1 live) --
STATE     ID             AGE  STATUS   CWD                                      TITLE
AWAITING  b343193c      4.1d  done     /home/ankitsarin/projects/surgical-cv    Surgical-cv FD2-CV1 mask record criterio
AWAITING  db0b5105      4.1d  blocked  /home/ankitsarin/projects/surgical-cv    Surgical-cv FD2-CV1 mask record criterio
MID-TURN  855dcebe      2.0d           /home/ankitsarin/projects/hello-ai-v2    None
MID-TURN  eb5f17ae     47.5h           /home/ankitsarin/projects/hello-ai-v2    hello-ai-v2 P1.1-FOLLOWUP
AWAITING  e0a7d38c      5.6h           /home/ankitsarin/projects/evidence-engin PARSE-GATE-09 font forensics
LIVE      2885f251      0.0h  busy     /home/ankitsarin/projects/evidence-engin Evidence-engine VERIFY-EXIT-01 font-audi
INFO: transcript directory -home-ankitsarin-projects-grocery resolves to no directory on disk — the project was deleted or renamed. Unattributable; no condition raised, and the transcripts are kept.

-- tmux sessions --
none (no server running on /tmp/tmux-1000/default)

-- Orphan process candidates --
none (40 user processes examined; 5 logind sessions account for every reparented one)

-- Repositories (12) --
REPO                     BRANCH                  DIRTY  AHEAD BEHIND  OWNED BY
citation-mcp             main                        0      0      0  -
dgx-infra                main                        0      0      0  -
evidence-engine          main                        0      0      0  2885f251 (evidence-engine-ea)
hello-ai                 main                        0      0      0  -
hello-ai-v2              main                        0      0      0  -
inference-determinism    main                        0      0      0  -
lists                    main                        0      0      0  -
llm-council              main                        0      0      0  -
my-first-project         main                        0      0      0  -
operativereports         main                        0      0      0  -
surgical-cv              main                        6      0      0  -
claude-config            main                        1      0      0  -

WARNING [DIRTY_TREE] /home/ankitsarin/projects/surgical-cv: 6 uncommitted path(s) on main and no live Claude Code session has a cwd inside it | meaning: a repository has uncommitted changes and no live session is working in it, so the edits belong to nobody currently at the keyboard | action: open a session in that repo and commit or stash the changes (git -C <repo> status to see them first), or discard them BY NAME once you have read them -- git -C <repo> checkout -- <path>, one path at a time. Never discard the whole tree to clear this line: the auditor cannot tell an abandoned edit from an unfinished one, which is exactly why it reports rather than resolves.
WARNING [DIRTY_TREE] /home/ankitsarin/claude-config: 1 uncommitted path(s) on main and no live Claude Code session has a cwd inside it | meaning: a repository has uncommitted changes and no live session is working in it, so the edits belong to nobody currently at the keyboard | action: open a session in that repo and commit or stash the changes (git -C <repo> status to see them first), or discard them BY NAME once you have read them -- git -C <repo> checkout -- <path>, one path at a time. Never discard the whole tree to clear this line: the auditor cannot tell an abandoned edit from an unfinished one, which is exactly why it reports rather than resolves.

findings: 0 critical, 2 warning
audit verdict: warn — 2 DIRTY_TREE
EXIT=0
```

**I9 confirmed:** `291609ef` and `19de242b` appear in **no** row of the transcript table —
neither LIVE, AWAITING nor MID-TURN. On disk, `291609ef` survives only as a scratch
directory under `~/.claude/projects/-home-ankitsarin-projects-evidence-engine/`, and
`19de242b` only as residue under `~/.claude/session-env/` and `~/.claude/file-history/`;
neither has a `.jsonl` transcript in the project directory. Both are archived.

**No process named `claude` exists other than this session's:** PID 3487652,
PPID 3487588 (`bash`, an interactive login shell — not systemd), cwd
`/home/ankitsarin/projects/evidence-engine`, started 2026-09-09 22:31:18 UTC.

---

## Divergences from the brief's wording (substance unaffected)

**W1 — `sqlite_sequence` is the 24th table (check 4).** Counting only user tables
(`type='table' AND name NOT LIKE 'sqlite_%'`) gives **23 tables / 84,395 rows**. Adding
`sqlite_sequence` — a real table, 5 rows (`judge_ratings` 2277, `judge_pair_ratings` 6831,
`fabrication_verifications` 7431, `judge_run_audit` 1, `provenance_classifications` 22034)
— gives exactly **24 / 84,400**. I3's figures are therefore correct under the
`sqlite_sequence`-inclusive convention and *only* under it. Recorded so a future check does
not read 23/84,395 as drift, and does not read 24/84,400 as covering a table that is not
there.

**W2 — backup location (check 6).** I5 says the three backups "exist alongside review.db";
they are in `data/backups/`, not `data/surgical_autonomy/`. `primer.md` names the correct
path. `data/surgical_autonomy/` holds five *other*, much older backup files
(`review.db.bak-pre-run6-cleanup-…`, `review.db.bak-pre-sonnet-cleanup-…`,
`review.db.pre_rename_backup`, `review_backup_pre_refactor.db`,
`review_backup_v1_schema.db`) plus their `-shm`/`-wal` sidecars; none of these is one of
the three.

**W3 — resident model (check 8).** `primer.md` records `gemma3:27b` resident; `/api/ps`
now reports **`qwen3:8b`** (11.45 GB). Informational only per I7 — the 09:00 nightly and
other lanes rotate the resident model, `MAX_LOADED_MODELS=1` evicts, and no action was
taken. Worth a primer line at the next close so the recorded value is not quoted stale.

**W4 — where the guard's self-test lives (check 9).** I8 says
"`tests/_live_db_guard.py` has a self-test". It does not — that module defines shared names
only. The self-test is `tests/test_live_db_guard.py`, two tests, both green. `primer.md`
already names it correctly. The brief anticipated this ("the architect has not read the
file"), so it is a wording correction, not a contradiction.

---

## F1 — new finding, outside the closeout's scope

**Two orphaned poll loops from the archived `e0a7d38c` lane are wedged and will never
exit, and the session auditor reports `Orphan process candidates: none`.**

| PID | PPID | started (UTC) | age at 22:40:50 | cwd | loop |
|---|---|---|---|---|---|
| 3218188 | 1 | 2026-09-09 16:40:08 | 06:00:41 | `~/projects/evidence-engine` | `until ! pgrep -f e3_run.py >/dev/null; do sleep 5; done` |
| 3224033 | 1 | 2026-09-09 16:54:05 | 05:46:43 | `~/projects/evidence-engine` | `until ! pgrep -f "pytest tests/" >/dev/null 2>&1; do sleep 5; done` |

Both are `/bin/bash -c` wrappers whose command text names
`/tmp/claude-1000/-home-ankitsarin-projects-evidence-engine/e0a7d38c-a025-416a-933f-0538bbc6e653/tasks/…output`,
so both belong to session `e0a7d38c` (AWAITING, 5.6 h). Both were reparented to PID 1 when
that lane's shell died.

**Why they never exit — `pgrep -f` matches the poller's own command line.** The bash
process's `argv` contains the literal string `pgrep -f e3_run.py` (resp.
`pgrep -f "pytest tests/"`), so `pgrep -f` finds *itself*, returns 0, and the `until`
condition is never satisfied. Demonstrated read-only: `pgrep -a -f 'e3_run.py'` returns
PID 3218188 itself and nothing else; `pgrep -a -f 'pytest tests/'` returns PID 3224033
itself and nothing else. Neither `e3_run.py` nor any pytest was running at the time
(`pgrep -a -x python` lists only the four long-lived Gradio/app services). Each loop wakes
every 5 s, indefinitely.

**Why the auditor missed them.** They are named `bash`, not `claude`, and the orphan
detector attributed all reparented processes to logind sessions
("40 user processes examined; 5 logind sessions account for every reparented one"). The
brief's own predicate — "any user process named `claude` not parented by systemd" — also
does not reach them. Both the auditor's rule and the brief's rule return a clean result
while two wedged pollers from a dead lane keep running. This is the
"a check that cannot fail is not a check" shape, in the process plane.

**Not acted on.** Per the `sessions` skill and this brief, orphan candidates are reported,
never killed. Execution is the PI's.

*Consequence for this task: none.* The loops only `sleep` and would only `tail` a stale
output file; measurements A, B and C are byte-identical, and the gate ran clean.

---

## Other conditions observed, not acted on

* **`e0a7d38c` — a second evidence-engine session, AWAITING, 5.6 h**, titled "PARSE-GATE-09
  font forensics", transcript last written 2026-09-09 17:03:20 UTC. Not archived; it owns
  F1's two orphans. Recorded per the brief; not touched.
* **`claude-config` dirty — 1 path, ` M PROJECT_LEDGER.md`**, HEAD `d303627`. Notable
  because the session-startup protocol pulls this repo; `git pull --ff-only` reported
  *Already up to date* (fast-forward clean), so SYNC.md's gate passed, but the tree carries
  an uncommitted edit with no live owner.
* **`surgical-cv` dirty — 6 untracked paths**, all `app/db/app.db.pre-migrate.*.bak-shm` /
  `-wal`. This is the exact population named by global open item **#9**; owner is the
  Evidence Engine task that owes the `db_backup.py` retirement. Other lane's tree — not
  touched.
* **I10 not testable in this window.** The cron jobs that could touch `review.db` are
  `0 9 * * * scripts/nightly_tests.sh` (evidence-engine) and `0 7 * * *
  ollama_health_check.sh`. Host TZ is UTC, so today's 07:00 and 09:00 runs both fired
  *before* the 19:48:46 mtime — they cannot be evidence either way. I10's prediction
  ("mtime survives the 07:00 and 09:00 jobs") is a claim about tomorrow's runs and remains
  **unverified**, not falsified. Re-stat after 2026-09-10 09:05 UTC to settle it.

---

## Scope statement

Nothing was fixed, migrated, reparsed, restarted, killed or configured. No model was loaded
or generated from. No `ollama`-, `network`- or `integration`-marked test was run. No
`ReviewDatabase` was constructed. `review.db` was opened only with `immutable=1`. The
crontab and systemd units were read, never edited. Repository writes: this file and its
commit.

**CLOSE STATE HOLDS**
