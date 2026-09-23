# MANIFEST-01 Phase 3 (ii) — the live write of migration 020

**Task:** MANIFEST-01-P3ii (session 7b, brief 2 of 3) · **Date:** 2026-09-23 · **Machine:** DGX Spark
**Opening HEAD:** `f4fc628fa2cff37a092807369166a1e18326e07e` (P3i docs commit on `b7fff74c`; no
file under `engine/`, `tests/` or `scripts/` changed between them)
**Docs commit:** the commit that adds this file; it carries this report, the new record of
reference and the SUPERSEDED sidecar on the readers-01 record — docs only.

**`data/surgical_autonomy/review.db` WAS written — once, by `runner.run`, applying 020 alone.**
31 → 34 tables. Accepted per table against the P3i rehearsal checkpoint; G3 under R53 met in all
three parts. **The new record of reference is
`docs/session-reports/manifest-01/review_db_fingerprint_20260923T162059Z.json`**, overall
`bb39ba81170c4f11d59954b9e70837afc427c66da710bde30881aad570d16c40`. The readers-01 record is
superseded, not edited (sidecar beside it).

---

## 0. Findings, first

### F1 — I11 as briefed could not be met by any time of day; narrowed by ruling to I11'

I11 required "nothing is due within the next 30 minutes". At 16:05:52 UTC three system timers
were due inside that window — `sysstat-collect.timer` (`OnCalendar=*:00/10`, `sa1 1 1`),
`fwupd-refresh.timer` (hourly, `RandomizedDelaySec=1h`), `anacron.timer` (`07..23:30`) — and
`sysstat-collect` alone makes every 30-minute window non-empty. None references evidence-engine or
`review.db`; `/etc/cron.daily`, `cron.hourly`, `cron.weekly` and `/etc/anacrontab` grep clean. CC
stopped before step 7. **Ruling (MANIFEST-01-P3ii, I11 narrowed):** architect error; replaced by
**I11'** — no scheduled job that reads or writes `review.db` or the evidence-engine tree due during
the write; time windows unchanged. Met: write at 16:10:31 UTC; next evidence-engine-relevant jobs
`service_health_check.sh` 18:00 UTC (crontab `0 */6`), `nightly_tests.sh` 09:00 UTC tomorrow,
`dgx-snapshot-user.timer` 2026-09-24 10:30:12 UTC.

### F2 — report item 11 and the DO NOT TOUCH list disagree about the four older backups

Report item 11 asks for "every review.db backup file under data/surgical_autonomy/ with table
count and overall (read-only)". The brief's DO NOT TOUCH list includes "the four pending-review
older backups", and grants read-only fingerprinting explicitly only to the 6b restore point and
the P3i rehearsal copy. Three of the four carry `-wal`/`-shm` sidecars, and a `mode=ro` open of a
WAL database can rewrite its `-shm`. **Resolved toward not touching:** the three backups this
session may read are fingerprinted (§11); the four older ones are listed by path, size and mtime
only, **not opened**. Their fingerprints are a ruling's to request, by dated addendum.

### F3 — small deviations, gates unaffected

* Write wall time **0.0364 s** against "~0.03 s" (rehearsal 0.0288 s).
* After step 19's `ReviewDatabase` construction (which sets `journal_mode=WAL`), live carries a
  0 B `-wal` and a 32 KiB `-shm` again; immediately after the runner write alone, both were absent.
  `-wal` 0 B at every fingerprint read.
* Schema_migrations acceptance (15) was measured against the rehearsal **record** rather than by
  opening the rehearsal copy, which the brief restricts to fingerprinting: the live table was
  rebuilt in a scratch database with 020's `applied_at` replaced by the rehearsal's, and its content
  hash then equals the record's. That proves every other cell equal without reading the copy.

---

## 1. Pre-flight

| check | expected | measured | match |
|---|---|---|---|
| clock (I11') | outside 07:00–10:00 and 10:28–10:35 UTC | pre-flight 16:05:52; backup 16:10:20; write 16:10:31 UTC | Y |
| scheduled jobs touching review.db / the tree (I11') | none due | next: `service_health_check.sh` 18:00 UTC; `nightly_tests.sh` 09:00 UTC tomorrow; `dgx-snapshot-user.timer` 2026-09-24 10:30:12 UTC | Y |
| nightly / health-check processes | none | none (`/proc` scan, runtime-assembled needles, self and parent excluded) | Y |
| HEAD | `f4fc628…`, clean, level | `f4fc628fa2cff37a092807369166a1e18326e07e`, clean, level with `origin/main` after fetch | Y |
| I10 — fresh database at HEAD, 020 receipt | `b0658f0f…559f`, executed, 1, NULL | `('020_run_manifest', 'b0658f0f53a46786a6bf47fe2b2223b990659815cb264fc9c0fe8e01f1ac559f', '2026-09-23T16:06:15.564537+00:00', 'executed', 1, None)` | Y |
| I10 — fresh tables / structure / textual | 34 / `effd6d51…08fd` / `0ba77776…3305` | 34 / `effd6d519e0618e498d2c44df756b276968c207c27b50b27b826a4c5ca2d08fd` / `0ba77776ffe94f42db8e945897b577cdd385ae594b33a5e76fed6e02981d3305` | Y |
| rehearsal record loaded | 34 / `effd6d51…` / `a4e49d4a…` | 34 / `effd6d519e0618e498d2c44df756b276968c207c27b50b27b826a4c5ca2d08fd` / `a4e49d4a96d171cfe85b919bbc4ee08064ce1733fde7b52a71491ef46876c864` (overall `10325ff5e2db082a29321e007e352df1a28dc3ed66f9cd80b810a1d5431ee2e0`, checkpoint only) | Y |
| lsof / fuser | empty | empty (exit 1 both) | Y |
| live `--compare` readers-01 | exit 0, 31, `-wal` 0 B, `e564f250…` | exit 0, 31, `-wal` 0 B, `e564f250afe40af7eb9a6bc07596a3c795972f7ef3b653c37599e8bc18285b63` | Y |
| event store | 3 · 190 · 194 · 3 · 0 | 3 · 190 · 194 · 3 · 0 | Y |
| I9 — sidecar shape | read | `docs/session-reports/effective-result-02/review_db_fingerprint_20260921T211815Z.json.SUPERSEDED.md` — reproduced in §9 | Y |
| I12 — one writer at a time | read + lsof | `run()` opens one connection, closes it (`conn.close()  # migrations open their own connection`) before `module.run_migration`, which opens and closes its own; the receipt connection closes in `finally`. lsof/fuser empty before the call and after it | Y |

## 2. Pre-write backup (step 7)

* lsof/fuser empty; `auto_backup("data/surgical_autonomy/review.db", reason="manifest-01-phase3-pre-write")`
  → **`data/surgical_autonomy/review.db.bak-manifest-01-phase3-pre-write-20260923-161020`**.
* Verified: **31 tables**, overall `e564f250afe40af7eb9a6bc07596a3c795972f7ef3b653c37599e8bc18285b63`.
* Live `--compare` against readers-01 immediately after: **exit 0**.
* **RETAINED.** Retirement is a future ruling (R31/R55).

## 3. The write (step 8)

* lsof/fuser empty; `runner.run("data/surgical_autonomy/review.db")`, default `include_data`.
* Wall time **0.0364 s**.
* Return value: `{'executed': ['020_run_manifest'], 'skipped': [], 'already': ['002_screening_rename', '003_backfill_expanded_screening', '004_pdf_quality_check', '005_model_digest', '006_not_null_confidence_tier', '007_add_judge_tables', '008_add_fabrication_verifications', '009_add_backfill_audit_log', '010_add_provenance_classifications', '011_add_absence_claim_class', '012_codebook_provenance', '013_drop_schema_hash_not_null', '014_cloud_tables', '015_drop_prerename_adjudication_indices', '016_event_store', '017_seed_event_store', '018_cloud_shape_and_audit_adjudication', '019_paper_state_axes']}`
* Receipt (checkpoint 9), verbatim:
  `('020_run_manifest', 'b0658f0f53a46786a6bf47fe2b2223b990659815cb264fc9c0fe8e01f1ac559f', '2026-09-23T16:10:31.801948+00:00', 'executed', 1, None)`
  — `schema_migrations` **19 rows**.
* Checkpoint 10: **34 tables**; new `run_calls`, `run_manifests`, `run_stage_configs`.

## 4. G3 under R53 (checkpoints 11–13)

* **(i)** `schema_structure_hash(live)` = `effd6d519e0618e498d2c44df756b276968c207c27b50b27b826a4c5ca2d08fd`
  = fresh (step 3); `structure_differences(fresh, live)` = `[]`. **Met.**
* **(ii)** textual `schema_hash_sha256(live)` = `a4e49d4a96d171cfe85b919bbc4ee08064ce1733fde7b52a71491ef46876c864`
  = rehearsal-after. **Met.**
* **(iii)** read back from live `sqlite_master`, compared character for character with the P3i
  §2 / §7 / §9 strings (CHECKs against `RUN_LINK_CHECK` and the digest CHECK in the module; triggers
  against the P3i-quoted texts). **All eight equal.** Old trigger
  `arms_configuration_frozen_once_claimed` absent.

  | object | read back from live (Python repr) | equal |
  |---|---|---|
  | `field_events` CHECK | `"CHECK (\n            (run_id IS NOT NULL AND run_marker IS NULL)\n            OR\n            (run_id IS NULL AND run_marker IS 'pre-manifest')\n        )"` | yes |
  | `paper_events` CHECK | `"CHECK (\n            (run_id IS NOT NULL AND run_marker IS NULL)\n            OR\n            (run_id IS NULL AND run_marker IS 'pre-manifest')\n        )"` | yes |
  | `run_stage_configs` digest CHECK | `"CHECK (provider IS NOT 'ollama' OR (model_digest IS NOT NULL AND length(model_digest) = 64))"` | yes |
  | `field_events_no_update` | `"CREATE TRIGGER field_events_no_update BEFORE UPDATE ON field_events BEGIN SELECT RAISE(ABORT, 'field_events is append-only: correct by appending an event'); END"` | yes |
  | `field_events_no_delete` | `"CREATE TRIGGER field_events_no_delete BEFORE DELETE ON field_events BEGIN SELECT RAISE(ABORT, 'field_events is append-only: correct by appending an event'); END"` | yes |
  | `paper_events_no_update` | `"CREATE TRIGGER paper_events_no_update BEFORE UPDATE ON paper_events BEGIN SELECT RAISE(ABORT, 'paper_events is append-only: correct by appending an event'); END"` | yes |
  | `paper_events_no_delete` | `"CREATE TRIGGER paper_events_no_delete BEFORE DELETE ON paper_events BEGIN SELECT RAISE(ABORT, 'paper_events is append-only: correct by appending an event'); END"` | yes |
  | `arms_configuration_frozen` | `"CREATE TRIGGER arms_configuration_frozen BEFORE UPDATE OF arm_kind, configuration_json, configuration_marker, pinned_run_id, pinned_sha256 ON arms WHEN EXISTS (SELECT 1 FROM field_events WHERE arm = OLD.arm_name) OR OLD.configuration_marker IN ('pinned', 'not recorded (pre-manifest)') BEGIN SELECT RAISE(ABORT, 'arms: configuration is frozen once the arm holds a claim or is pinned by a manifest, and a pre-manifest arm never pins (R21, R59) — a changed configuration is a new arm (R10)'); END"` | yes |

## 5. Per-table acceptance (checkpoints 14–15)

* **14 — 33/33.** Every table except `schema_migrations` has live-after content hash equal to the
  rehearsal record's, including `arms`, `sqlite_sequence`, `field_events` (0 rows), `paper_events`
  (190) and the three new empty tables. No table differs.
* **15 — `schema_migrations`, exactly one cell different.**
  * live-after table hash `a3b51bbbf22d4b22d6e35d4c131c2b8ceb199c4a59fa9ce919a7ffe200c2c05b`;
    rehearsal-after `a69ad8a1bdc35099d2f388c17accd0c72dbda6726849ee40c1c430a640b68d44`.
  * With **only** 020's `applied_at` replaced by the rehearsal's value (scratch rebuild, F3), the
    table hashes to `a69ad8a1bdc35099d2f388c17accd0c72dbda6726849ee40c1c430a640b68d44` — equal to
    the rehearsal record. So all 19 rows agree in `migration_id`, `file_sha256`, `mode`,
    `runner_version`, `note` (and rowid), and the one differing cell is 020's `applied_at`.
  * Rows 1–18 are also cell-for-cell equal to the pre-write backup.
  * **020 `applied_at`:** live `2026-09-23T16:10:31.801948+00:00`; rehearsal
    `2026-09-23T15:42:12.271808+00:00`.
* Live-after overall `bb39ba81170c4f11d59954b9e70837afc427c66da710bde30881aad570d16c40` differs from
  the rehearsal checkpoint's `10325ff5…ee2e0` by that one cell alone, as expected; overall is not
  an acceptance value.

## 6. Data checks (checkpoints 16–18)

* **16 — paper_events:** 190 rows; every `(rowid, *columns)` tuple and the column order identical
  to the pre-write backup (read `mode=ro`); `run_id` NULL 190/190; `run_marker = 'pre-manifest'`
  190/190; `sqlite_sequence` `paper_events` = 190; `('field_events', 0)` present.
* **17 — arms:** 3 rows; `pinned_run_id` and `pinned_sha256` NULL on each; the six original
  columns equal per row to the pre-write backup.
* **18:** `PRAGMA integrity_check` → `ok`; `PRAGMA foreign_key_check` → empty; `-wal` and
  `-shm` absent immediately after the runner's connections closed; `-wal` 0 B after the
  checkpoint's own `mode=ro` reads closed.

## 7. Idempotency and C12 on live (checkpoint 19)

`ReviewDatabase("surgical_autonomy")` constructed on the live path (lsof empty first), `runner.run`
observed by a pass-through wrapper, then `close()`.

* Runner: `executed: []`, `skipped: []`, `already:` 002–020 (19 ids).
* Live fingerprint immediately before and after: `compare()` → `[]`; overall
  `bb39ba81170c4f11d59954b9e70837afc427c66da710bde30881aad570d16c40` both; every table equal;
  textual equal; 34 tables. **C12:** `audit_adjudication` absent after construction.

## 8. Gate after the write (checkpoint 20)

Five chunks (file list byte-equal to the pre-write run's):
581 / 708 / 433 (10 deselected) / 575 (6) / 417 (1) = **2,714 passed / 17 deselected**,
deselects **0/0/10/6/1** — unchanged by the schema change. Live `--compare` against the post-write
checkpoint after the suite: exit 0.

## 9. New record of reference and sidecar (checkpoints 21–22)

* **`docs/session-reports/manifest-01/review_db_fingerprint_20260923T162059Z.json`** —
  **34 tables**; overall `bb39ba81170c4f11d59954b9e70837afc427c66da710bde30881aad570d16c40`;
  structure `effd6d519e0618e498d2c44df756b276968c207c27b50b27b826a4c5ca2d08fd`; textual
  `a4e49d4a96d171cfe85b919bbc4ee08064ce1733fde7b52a71491ef46876c864`; `-wal` 0 B.
  Live `--compare` against it: **exit 0**.
* **Sidecar** `docs/session-reports/readers-01/review_db_fingerprint_20260922T165644Z.json.SUPERSEDED.md`,
  in the effective-result-02 shape: `# SUPERSEDED`; the old record's scope and overall; "superseded
  on 2026-09-23 by the MANIFEST-01 Phase 3 live write of migration 020 … in the commit that adds
  this file"; the current record's path and four values in a code block; "Compare against that one"
  with every intended difference named (31 → 34; `schema_migrations` 18 → 19; `arms` +2 NULL
  columns; `sqlite_sequence` + `('field_events', 0)`; the schema hashes) and the 28 unchanged tables,
  **measured** against the two records (28 equal; differing `arms`, `schema_migrations`,
  `sqlite_sequence`; new `run_calls`, `run_manifests`, `run_stage_configs`); "superseded, not wrong
  … not edited or deleted"; the pre-write backup it matches; the Evidence line. The readers-01
  record itself is untouched.

## 10. Commit

Docs only — this report, the new record, the sidecar. `git diff --cached --stat` shown before
commit and `git show --stat` before push (R52).

## 11. Disk inventory — `review.db` backups under `data/surgical_autonomy/`

| file | opened | tables | overall |
|---|---|---|---|
| `review.db.bak-manifest-01-phase3-pre-write-20260923-161020` | fingerprint, `mode=ro` | 31 | `e564f250afe40af7eb9a6bc07596a3c795972f7ef3b653c37599e8bc18285b63` (= readers-01 record) |
| `review.db.bak-manifest-01-phase3-rehearsal-20260923-154127` | fingerprint, `mode=ro` | 34 | `10325ff5e2db082a29321e007e352df1a28dc3ed66f9cd80b810a1d5431ee2e0` (= rehearsal record) |
| `review.db.bak-readers-01-phase3-pre-write-20260922-165453` (6b restore point, R55) | fingerprint, `mode=ro` | 32 | `62f3912813b6e9efa3a651ee4c4ebcd611adf89c989f240a58e83dcf6759b79a` |
| `review.db.bak-pre-migrations01-20260921-002807` | **not opened** (F2) | — | — (101,978,112 B, mtime 2026-09-21 00:28:07 UTC) |
| `review.db.bak-pre-run6-cleanup-20260315194955` (+ `-shm`, `-wal`) | **not opened** (F2) | — | — (47,759,360 B, mtime 2026-03-15 19:49:55 UTC) |
| `review.db.bak-pre-sonnet-cleanup-20260316-165020` (+ `-shm`, `-wal`) | **not opened** (F2) | — | — (60,252,160 B, mtime 2026-03-16 15:49:56 UTC) |
| `review.db.pre_rename_backup` (+ `-shm`, `-wal`) | **not opened** (F2) | — | — (18,907,136 B, mtime 2026-03-12 17:35:42 UTC) |

The three fingerprinted files carry no sidecars.

## 12. Expectations broken

* **I11** — F1; ruled, I11' met.
* **Item 11 vs DO NOT TOUCH** — F2; resolved toward not opening the four; ruling requested.
* **Wall time, `-wal`/`-shm` after construction, `schema_migrations` method** — F3.

## Not done, deliberately

No `restore()`. No run against live (R19, R71; `run_pipeline` not opened). No VACUUM (R55). No
retirement of any backup. No primer.md, CLAUDE.md, plan, engine, test or spec change. The closeout
is the next brief's.
