# MANIFEST-01 Phase 3 (i) — rehearsal of migration 020 on a verified copy of live

**Task:** MANIFEST-01-P3i (session 7b, brief 1 of 3) · **Date:** 2026-09-23 · **Machine:** DGX Spark
**Opening HEAD:** `b7fff74cc8922377ad28235d64dca6ceac01bb71`
**Docs commit:** the commit that adds this file (see `git log -- <this path>`); it carries this
report and the rehearsal record only.
**Live `review.db` was NOT written.** Every read was `mode=ro`; `auto_backup` read it once
through the online-backup API. `--compare` against
`docs/session-reports/readers-01/review_db_fingerprint_20260922T165644Z.json` exited **0** at
open, immediately after the backup, after the rehearsal (13) and again before commit.
`-wal` **0 B** throughout.

**Rehearsal-after values (P3ii's live acceptance values, per R53 G3 and the per-table rule):**

| value | rehearsal-after |
|---|---|
| tables | **34** |
| `overall_sha256` | `10325ff5e2db082a29321e007e352df1a28dc3ed66f9cd80b810a1d5431ee2e0` (checkpoint only — a live write is accepted per table, never on overall) |
| `schema_structure_hash` | `effd6d519e0618e498d2c44df756b276968c207c27b50b27b826a4c5ca2d08fd` (= fresh) |
| `schema_hash_sha256` (textual) | `a4e49d4a96d171cfe85b919bbc4ee08064ce1733fde7b52a71491ef46876c864` (= 7a's throwaway value) |
| record | `docs/session-reports/manifest-01/rehearsal_db_fingerprint_20260923T155530Z.json` — a **checkpoint, not the record of reference** |

---

## 0. Findings, first

### F1 — brief step 12i could not be met as written; replaced by 12i' under the architect's ruling

12i asked for an UPDATE and a DELETE on `paper_events` and `field_events` "after inserting
nothing", expecting refusal by trigger. On the copy, `field_events` has **0 rows**, and a
`BEFORE UPDATE` / `BEFORE DELETE` trigger fires once per affected row, so on an empty table it
never fires:

| attempt on the rehearsal copy | result |
|---|---|
| `paper_events` UPDATE | refused — `IntegrityError: paper_events is append-only: correct by appending an event` |
| `paper_events` DELETE | refused — same message |
| `field_events` UPDATE | **not refused**, rowcount 0 |
| `field_events` DELETE | **not refused**, rowcount 0 |

Each attempt was rolled back; the copy's overall was unchanged after them. CC stopped and
reported. **Ruling (MANIFEST-01-P3i, 12i resolved):** the expectation was an architect error;
12i is not a G3 gate; it is replaced by **12i'** (§9 below), which is met.

### F2 — the 10:31 UTC `-shm` mtime on live is the user-tier snapshot, which opens `mode=ro`

Live `review.db-shm` carried mtime `2026-09-23 10:31` at open. Identified by reading, per the
ruling's addition:

* **Scheduler:** systemd timer `dgx-snapshot-user.timer` → `dgx-snapshot-user.service`
  (`systemctl list-timers`: LAST `Wed 2026-09-23 10:31:31 UTC`; journal: `Starting
  dgx-snapshot-user.service` at 10:31:31, `Finished` at 10:31:36). It is **not** a crontab entry,
  which is why the plan's cron schedule (07:00, 09:00) does not list it. It belongs to the
  dgx-infra snapshot lane (M7), scheduled 10:30 UTC with `Persistent=true` and a randomised delay.
* **Script:** `ExecStart=/home/ankitsarin/projects/dgx-infra/bin/dgx-snapshot-user` →
  `snapshot/user_tier.py` (`run.capture("review.db", capture_sqlite, …)`, source
  `SA = EE / "data" / "surgical_autonomy"`) → `snapshot/artifacts.py::capture_sqlite`.
* **Read-only, confirmed from code:** `capture_sqlite` opens
  `sqlite3.connect(f"file:{source}?mode=ro", uri=True)` and copies with `VACUUM INTO`. The unit
  grants `ReadWritePaths=/home/ankitsarin/projects/evidence-engine/data/surgical_autonomy`, and its
  own comment states why: "What is granted is the ability for SQLite to manage its own WAL
  shared-memory sidecar, which is a storage-engine detail." A `mode=ro` reader of a WAL database
  touches `-shm`; that is the mtime.

Not evidence of a write, and `--compare` exited 0 throughout. The next scheduled run is
2026-09-24 10:30 UTC — P3ii's live write should not overlap it.

### F3 — minor deviations from the brief's wording, gate unaffected

* claude-config was at **`4625e6f`**, not a ledger commit: it descends from `dfc99c2` via
  `89d1507` (ledger), `56a2bfa` (primer), `4625e6f` (open-items). Accepted in the ruling.
* No existing naming for a rehearsal fingerprint JSON (readers-01 holds only
  `review_db_fingerprint_<ts>.json`); named `rehearsal_db_fingerprint_<ts>Z.json` so it cannot
  be mistaken for the record of reference. Accepted in the ruling.
* 12i' needed no `run_stage_configs` row: `field_events` references `run_manifests(run_id)`,
  `papers(id)` and `arms(arm_name)` only (read from the fresh database's `sqlite_master`).

---

## 1. Startup verify

| check | expected | measured | match |
|---|---|---|---|
| time; nightly / health-check processes | after 10:00 UTC; none | 15:30:26 UTC; `/proc` scan (runtime-assembled needles, self and parent excluded) found none | Y |
| HEAD | `b7fff74c…bb71`, clean, level | `b7fff74cc8922377ad28235d64dca6ceac01bb71`, clean, level with `origin/main` after fetch | Y |
| claude-config | `dfc99c2` or successor | `4625e6f`, descendant of `dfc99c2` (F3) | Y |
| standard gate | 2,714 / 17, deselects 0/0/10/6/1 | 581 / 708 / 433 (10) / 575 (6) / 417 (1) = **2,714 passed / 17 deselected**; per-chunk equal to 7a's too | Y |
| `tests/test_eligibility.py` | 91 | 91 passed | Y |
| lsof / fuser on `review.db` (+`-wal`, `-shm`) | empty | empty (exit 1 both) | Y |
| live `--compare` readers-01 record | exit 0, 31 tables, `-wal` 0 B / absent | exit 0, 31 tables, `-wal` 0 B, overall `e564f250afe40af7eb9a6bc07596a3c795972f7ef3b653c37599e8bc18285b63` | Y |
| event store | 3 arms · 190 paper events · 194 parsed-text refs · 3 identities · 0 field events | 3 · 190 · 194 · 3 · 0 | Y |
| restore point `review.db.bak-readers-01-phase3-pre-write-20260922-165453` | 32 tables, `62f39128…79a` | 32 tables, `62f3912813b6e9efa3a651ee4c4ebcd611adf89c989f240a58e83dcf6759b79a` (fingerprint only) | Y |

## 2. I1–I6

* **I1 — true.** `engine/migrations/runner.py::run(db_path, *, include_data=False)` at `b7fff74c`
  has exactly three branches: `if migration_id in have:` → `already`;
  `if kind_of(migration_id) == "data" and not include_data:` → `skipped`; otherwise import,
  `conn.close()  # migrations open their own connection`, `module.run_migration(str(db_path))`,
  receipt `executed`. No literal naming a review, directory or filename in code (the one
  `data/surgical_autonomy/expanded_search/` string is a comment on `KINDS["003"]`).
  `"020": "schema"` — `include_data` not needed, not passed.
* **I2 — true.** The three CHECKs as the module declares them (`engine/migrations/020_run_manifest.py`):
  * `field_events` and `paper_events` — both render `RUN_LINK_CHECK` (`{RUN_LINK_CHECK}` in
    `field_events_sql` and `paper_events_sql`), text with its exact whitespace:
    ```
    CHECK (
                (run_id IS NOT NULL AND run_marker IS NULL)
                OR
                (run_id IS NULL AND run_marker IS 'pre-manifest')
            )
    ```
    (Python repr: `'CHECK (\n            (run_id IS NOT NULL AND run_marker IS NULL)\n            OR\n            (run_id IS NULL AND run_marker IS \'pre-manifest\')\n        )'`)
  * `run_stage_configs` (in `RUN_STAGE_CONFIGS_SQL`, under the "R78: NULL-safe" comment):
    ```
    CHECK (provider IS NOT 'ollama' OR (model_digest IS NOT NULL AND length(model_digest) = 64))
    ```
* **I3 — true.** `engine/utils/db_backup.py::auto_backup(db_path_or_connection, reason) -> BackupResult`
  (frozen dataclass `path`, `fingerprint`). Reads the source inside `read_snapshot` (`mode=ro`,
  one `BEGIN`), `src.backup(dst)`, then `dst.execute("PRAGMA journal_mode=DELETE")`, removes
  sidecars, fingerprints both and on any difference deletes the copy and raises
  `BackupVerificationError`. Path `db_path.parent / f"{db_path.name}.bak-{reason}-{timestamp}"`,
  timestamp `%Y%m%d-%H%M%S` local (UTC on this host).
* **I4 — true.** CLI `database [--out JSON] [--compare JSON]`; public names include
  `fingerprint`, `structure_differences`, `structure_hash`, `compare`; per-table content hashes
  are `fp["tables"][name]["sha256"]` with `row_count` and `columns`.
* **I5 — true.** `ReviewDatabase.__init__` → `self._run_migrations()` →
  `from engine.migrations import runner` … `result = runner.run(self.db_path)`.
* **I6 — true.** `docs/session-reports/manifest-01/` exists; naming
  `MANIFEST-01_<phase>_<kind>_<ts>.md`. This report follows it.

## 3. Fresh database (step 8)

`ReviewDatabase("fresh_020", data_root=<scratchpad>/fresh)` at `b7fff74c`.

* 020 receipt: `('020_run_manifest', 'b0658f0f53a46786a6bf47fe2b2223b990659815cb264fc9c0fe8e01f1ac559f', '2026-09-23T15:41:19.575487+00:00', 'executed', 1, None)`
  — equal to the file's `sha256sum`.
* Executed 004–016, 018, 019, 020; 002, 003, 017 skipped (data kind).
* **34 tables**; structure `effd6d519e0618e498d2c44df756b276968c207c27b50b27b826a4c5ca2d08fd`;
  textual `0ba77776ffe94f42db8e945897b577cdd385ae594b33a5e76fed6e02981d3305`; overall
  `f7f6019ae65d4ddec07d283d2001fccc62d6ace8988794f6dad65c6f455f3481`.
* Second `runner.run`: `executed: []`, 020 in `already`; overall and textual unchanged, 34 tables.

## 4. Rehearsal copy (step 9)

* lsof / fuser empty immediately before.
* `auto_backup("data/surgical_autonomy/review.db", reason="manifest-01-phase3-rehearsal")` →
  **`data/surgical_autonomy/review.db.bak-manifest-01-phase3-rehearsal-20260923-154127`**.
* Verified fingerprint: **31 tables**, overall
  `e564f250afe40af7eb9a6bc07596a3c795972f7ef3b653c37599e8bc18285b63` (= live's record);
  structure `2b92ac41f333dda269d35d249c822a32b0a768c6fb69fc79030ed85729955769`; textual
  `adc985a2af7cba9c103997c026d9f02d2c02e39304d44c62af2c3bd83572d2b0`. No `-wal`/`-shm` beside it.
* Live `--compare` immediately after: **exit 0**.
* Retained on disk; retention is the closeout's ruling (R31/R55).

## 5. 020 execution (step 11)

`runner.run(copy)` — the same function the live write will call; `include_data` not passed.

* Wall time **0.0288 s**.
* Result: `executed: ['020_run_manifest']`, `skipped: []`, `already:` 002–019 (18 ids).
* Receipt (12a): `('020_run_manifest', 'b0658f0f53a46786a6bf47fe2b2223b990659815cb264fc9c0fe8e01f1ac559f', '2026-09-23T15:42:12.271808+00:00', 'executed', 1, None)`
  — `file_sha256` equal to step 8's re-read; `runner_version` 1; `note` NULL (the runner writes
  no note on an executed receipt).
* 12b: **34 tables**; new: `run_calls`, `run_manifests`, `run_stage_configs`; none removed.
* 12c: structure `effd6d519e0618e498d2c44df756b276968c207c27b50b27b826a4c5ca2d08fd` = fresh;
  `structure_differences(fresh, copy)` = `[]`.
* 12d: textual `a4e49d4a96d171cfe85b919bbc4ee08064ce1733fde7b52a71491ef46876c864` = 7a's throwaway value.

## 6. Per-table content hashes (12e) — rehearsal-before vs rehearsal-after

28 of the 31 shared tables identical, including `field_events` (0 rows) and `paper_events`
(190). Exactly three deltas, each explained:

* **`arms`** — same 3 rows; two columns appended by `ALTER TABLE arms ADD COLUMN`
  (`pinned_run_id INTEGER REFERENCES run_manifests(run_id)`, `pinned_sha256 TEXT`), NULL on every
  row. The column list enters the hash, so the hash changes.
* **`schema_migrations`** — 18 → 19 rows: the 020 receipt.
* **`sqlite_sequence`** — 6 → 7 rows: `('field_events', 0)` added by the AUTOINCREMENT rebuild of
  an empty table. Every pre-existing entry unchanged (`paper_events` 190).

The three new tables are empty.

| table | rows before | rows after | sha256 before | sha256 after | same |
|---|---|---|---|---|---|
| `abstract_screening_adjudication` | 0 | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | yes |
| `abstract_screening_decisions` | 21374 | 21374 | `d961fd298f839a0123cfe58a16ba350e36ddb11f5a9dab3e96af409b5e02b943` | `d961fd298f839a0123cfe58a16ba350e36ddb11f5a9dab3e96af409b5e02b943` | yes |
| `abstract_verification_decisions` | 1422 | 1422 | `3d6b36f5205cd2dc6311ab5e62944dcd7fc0ba55fb50e935870e7543e7b226ce` | `3d6b36f5205cd2dc6311ab5e62944dcd7fc0ba55fb50e935870e7543e7b226ce` | yes |
| `arms` | 3 | 3 | `fcc5dd342ac08ebb262243e57f71fb7a13649258735ce059fb3d6fd9d0a9af11` | `d9a77a0e7744d6111130eda18245d6cf63a1e77226e8d9796d5e8976832e8519` | **no** |
| `cloud_evidence_spans` | 7257 | 7257 | `dc10008d30915bc7f4869959413d08b695348323adf3628aadae00e23abd9148` | `dc10008d30915bc7f4869959413d08b695348323adf3628aadae00e23abd9148` | yes |
| `cloud_extractions` | 379 | 379 | `73cef1a8f86cc6393df025f25f3e484a23cedf32b4805061105b57670d3ee400` | `73cef1a8f86cc6393df025f25f3e484a23cedf32b4805061105b57670d3ee400` | yes |
| `evidence_spans` | 3760 | 3760 | `019e642e3cb14addca435eec1a9135b841597feb3a5ab4c721c8508d9678817c` | `019e642e3cb14addca435eec1a9135b841597feb3a5ab4c721c8508d9678817c` | yes |
| `extractions` | 190 | 190 | `0364cf4ef23310420f61d606e62be2a41c2084cbda0bc2d30b4b0d360d48a60b` | `0364cf4ef23310420f61d606e62be2a41c2084cbda0bc2d30b4b0d360d48a60b` | yes |
| `fabrication_verifications` | 7422 | 7422 | `42925a5f3364d696b66935fe44e6f68b4a7c4f65847cc442fbf19c96f776ba0a` | `42925a5f3364d696b66935fe44e6f68b4a7c4f65847cc442fbf19c96f776ba0a` | yes |
| `field_event_against` | 0 | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | yes |
| `field_event_against_decisions` | 0 | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | yes |
| `field_events` | 0 | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | yes |
| `ft_screening_adjudication` | 36 | 36 | `5d971f9153bc7bcb0934c0d4073f6ae2c7e34eb0a458de657f4d318d7591edc9` | `5d971f9153bc7bcb0934c0d4073f6ae2c7e34eb0a458de657f4d318d7591edc9` | yes |
| `ft_screening_decisions` | 366 | 366 | `70045a6a4332f273b4f8bec5b025e7c3727b4cea03f25b7331d97cb47b8fb6b2` | `70045a6a4332f273b4f8bec5b025e7c3727b4cea03f25b7331d97cb47b8fb6b2` | yes |
| `ft_verification_decisions` | 182 | 182 | `427d5ce62d40c0723251a511499cd553b0bfd99f5ab51285585dca613ef1358e` | `427d5ce62d40c0723251a511499cd553b0bfd99f5ab51285585dca613ef1358e` | yes |
| `full_text_assets` | 794 | 794 | `be9c124d3b860138744140ce6d70f064ed00804123616b28557ce8256cb48876` | `be9c124d3b860138744140ce6d70f064ed00804123616b28557ce8256cb48876` | yes |
| `judge_pair_ratings` | 6828 | 6828 | `e3e2dff4d60ea94c8c8b83845fda5b5a1f7ecca23dd673a59e6a7772a57bb5f6` | `e3e2dff4d60ea94c8c8b83845fda5b5a1f7ecca23dd673a59e6a7772a57bb5f6` | yes |
| `judge_ratings` | 2276 | 2276 | `6318e8d366d3a15979811329858e42666c1536efeba952d66548b8ed5c9d9026` | `6318e8d366d3a15979811329858e42666c1536efeba952d66548b8ed5c9d9026` | yes |
| `judge_run_audit` | 1 | 1 | `483203e723b2509efb5ada8c70bf89e0c535d0cc711f0385794d72bdda6dd7d2` | `483203e723b2509efb5ada8c70bf89e0c535d0cc711f0385794d72bdda6dd7d2` | yes |
| `judge_runs` | 7 | 7 | `ded37890d1d80b1fb3033674b5feac56964b1367c63377aca900caf8660b7812` | `ded37890d1d80b1fb3033674b5feac56964b1367c63377aca900caf8660b7812` | yes |
| `paper_events` | 190 | 190 | `28e0d3df4a7f416cf2f90bdb114e55920911a8ba72ba6d9ef3c3ce18b747b66e` | `28e0d3df4a7f416cf2f90bdb114e55920911a8ba72ba6d9ef3c3ce18b747b66e` | yes |
| `papers` | 10039 | 10039 | `15267385aac0123b11089a2bfccb5eccb121b01ec1789b5089840089ce910baf` | `15267385aac0123b11089a2bfccb5eccb121b01ec1789b5089840089ce910baf` | yes |
| `parse_attempts` | 8 | 8 | `17c8783e6249cd8e662b4f4e680864d56aae70f43c736645c3906f0fad110bff` | `17c8783e6249cd8e662b4f4e680864d56aae70f43c736645c3906f0fad110bff` | yes |
| `parsed_text_refs` | 194 | 194 | `b05b1a4312b2dbf5436dc9a5b68dcde7c26e1bbcbd3fb9ae3ec9556ddb717088` | `b05b1a4312b2dbf5436dc9a5b68dcde7c26e1bbcbd3fb9ae3ec9556ddb717088` | yes |
| `provenance_census_runs` | 2 | 2 | `d4f3d86c40498571a056ac9f5de423ff402294deb6e3ee5d0611d80e1a6b5642` | `d4f3d86c40498571a056ac9f5de423ff402294deb6e3ee5d0611d80e1a6b5642` | yes |
| `provenance_classifications` | 22034 | 22034 | `fe39f2684980ac0a7bb97711e8a3124061a8d39bacd189b40a60c3c0c0b9c4e4` | `fe39f2684980ac0a7bb97711e8a3124061a8d39bacd189b40a60c3c0c0b9c4e4` | yes |
| `review_identities` | 3 | 3 | `4a586901e32c2f7786bee80ae6060065b8729098ef608aae0490edc4efe3941d` | `4a586901e32c2f7786bee80ae6060065b8729098ef608aae0490edc4efe3941d` | yes |
| `review_runs` | 6 | 6 | `7054c7597c68d23ca65e9579a80e176f15a5cf6f03b06bdaf82f497c0136228a` | `7054c7597c68d23ca65e9579a80e176f15a5cf6f03b06bdaf82f497c0136228a` | yes |
| `run_calls` | — | 0 | `—` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | **new** |
| `run_manifests` | — | 0 | `—` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | **new** |
| `run_stage_configs` | — | 0 | `—` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | **new** |
| `schema_migrations` | 18 | 19 | `2ef3302f5fa0633f7d2d1accb2aa2897c2a3f52b27890c12f17ec47112e36fc5` | `a69ad8a1bdc35099d2f388c17accd0c72dbda6726849ee40c1c430a640b68d44` | **no** |
| `sqlite_sequence` | 6 | 7 | `8fbb15bd0efb86331955d9eb03517f19e6d8e3cd7f4323d1b67f8d9578144a77` | `8a34a4493aab8ec413cbab610338ec48f0806d8c3b7250df45924835b9b09f86` | **no** |
| `workflow_state` | 12 | 12 | `e7751792f5e4d688cadf10d2da00f65ade3449df49a46200da64abb1ae68433c` | `e7751792f5e4d688cadf10d2da00f65ade3449df49a46200da64abb1ae68433c` | yes |

## 7. `paper_events` and `arms` (12f, 12g)

* **paper_events:** 190 rows; every tuple `(rowid, *columns)` identical before and after; column
  order identical; `run_id` NULL on 190/190; `run_marker = 'pre-manifest'` on 190/190;
  `sqlite_sequence` `paper_events` 190 → 190.
* **arms:** 3 rows before and after; the six original columns identical per row;
  `pinned_run_id` and `pinned_sha256` NULL on every row. Old trigger
  `arms_configuration_frozen_once_claimed` absent (0). New trigger, read from the copy and equal
  to `arms_trigger()` rendered without `IF NOT EXISTS`:
  ```
  CREATE TRIGGER arms_configuration_frozen BEFORE UPDATE OF arm_kind, configuration_json, configuration_marker, pinned_run_id, pinned_sha256 ON arms WHEN EXISTS (SELECT 1 FROM field_events WHERE arm = OLD.arm_name) OR OLD.configuration_marker IN ('pinned', 'not recorded (pre-manifest)') BEGIN SELECT RAISE(ABORT, 'arms: configuration is frozen once the arm holds a claim or is pinned by a manifest, and a pre-manifest arm never pins (R21, R59) — a changed configuration is a new arm (R10)'); END
  ```

## 8. The three CHECKs read back from the copy (12h)

Extracted from the copy's `sqlite_master.sql` by balanced-parenthesis scan from the CHECK keyword:

| table | read back (repr) | equal to module text (§2) |
|---|---|---|
| `field_events` | `'CHECK (\n            (run_id IS NOT NULL AND run_marker IS NULL)\n            OR\n            (run_id IS NULL AND run_marker IS \'pre-manifest\')\n        )'` | **yes**, character for character |
| `paper_events` | `'CHECK (\n            (run_id IS NOT NULL AND run_marker IS NULL)\n            OR\n            (run_id IS NULL AND run_marker IS \'pre-manifest\')\n        )'` | **yes** |
| `run_stage_configs` | `"CHECK (provider IS NOT 'ollama' OR (model_digest IS NOT NULL AND length(model_digest) = 64))"` | **yes** |

Additionally, beyond the brief: each table's **whole** `CREATE` statement equals the module's
rendered DDL (`field_events_sql` / `paper_events_sql` with the temporary name normalised to the
quoted final name that `ALTER TABLE … RENAME` writes; `RUN_STAGE_CONFIGS_SQL` verbatim).

## 9. Triggers, integrity, idempotency (12i', 12j, 12k)

**12i (as briefed):** see F1.

**12i' (the ruling):**

1. A throwaway copy of the fresh database (online-backup API into
   `<scratchpad>/throwaway_fresh.db`, not the fresh reference and never the rehearsal copy),
   `PRAGMA foreign_keys=ON`. Inserted one `papers` row (id 1), one `run_manifests` row (run_id 1,
   `git_commit` 40 chars, `git_dirty` 0), one `arms` row (`a1`, `model`), then one `field_events`
   row: `event_type 'asserted'`, `actor model/extractor`, **`run_id 1`, `run_marker NULL`**,
   `paper_id 1`, `arm 'a1'`. **Insert succeeded**; `foreign_key_check` empty.
   * `UPDATE field_events SET value='w' WHERE event_id=1` → **refused**:
     `IntegrityError: field_events is append-only: correct by appending an event`
   * `DELETE FROM field_events WHERE event_id=1` → **refused**, same message.
   * Row after: `(1, 'v')` — unchanged.
2. The four event-table triggers, rehearsal copy vs fresh database — **all four equal,
   character for character**:
   ```
   CREATE TRIGGER field_events_no_update BEFORE UPDATE ON field_events BEGIN SELECT RAISE(ABORT, 'field_events is append-only: correct by appending an event'); END
   CREATE TRIGGER field_events_no_delete BEFORE DELETE ON field_events BEGIN SELECT RAISE(ABORT, 'field_events is append-only: correct by appending an event'); END
   CREATE TRIGGER paper_events_no_update BEFORE UPDATE ON paper_events BEGIN SELECT RAISE(ABORT, 'paper_events is append-only: correct by appending an event'); END
   CREATE TRIGGER paper_events_no_delete BEFORE DELETE ON paper_events BEGIN SELECT RAISE(ABORT, 'paper_events is append-only: correct by appending an event'); END
   ```
   (I8 verified.)
3. Throwaway deleted (no file matching `throwaway_fresh*` remains). Rehearsal copy
   re-fingerprinted: overall `10325ff5e2db082a29321e007e352df1a28dc3ed66f9cd80b810a1d5431ee2e0`
   — unchanged.

On the rehearsal copy itself, the `paper_events` UPDATE and DELETE were refused by trigger (F1).

**12j:** `PRAGMA integrity_check` → `ok`. `PRAGMA foreign_key_check` (whole database) → empty,
both before and after 020.

**12k:** second `runner.run(copy)`: `executed: []`, `skipped: []`, `already:` 002–020 (19 ids);
overall and textual unchanged.

## 10. Rehearsal-after record (12l)

`docs/session-reports/manifest-01/rehearsal_db_fingerprint_20260923T155530Z.json` —
34 tables; overall `10325ff5e2db082a29321e007e352df1a28dc3ed66f9cd80b810a1d5431ee2e0`;
structure `effd6d519e0618e498d2c44df756b276968c207c27b50b27b826a4c5ca2d08fd`;
textual `a4e49d4a96d171cfe85b919bbc4ee08064ce1733fde7b52a71491ef46876c864`.

What P3ii's live write will be held to:
* **G3(i)** live-after structure = `effd6d51…` above, and `structure_differences(fresh, live)` = 0.
* **G3(ii)** live-after textual = `a4e49d4a96d171cfe85b919bbc4ee08064ce1733fde7b52a71491ef46876c864`.
* **G3(iii)** the three CHECKs of §8 read back verbatim from live `sqlite_master`.
* **Per table:** the 28 identical tables' hashes equal this record's; `arms`,
  `schema_migrations`, `sqlite_sequence` differ from the readers-01 record exactly as in §6
  (`schema_migrations` will differ from this record in `applied_at` only, so it is compared by
  row, not hash).

## 11. Live at end (step 13)

`--compare` against the readers-01 record: **exit 0** ("IDENTICAL — schema, every table, and the
overall hash all match"), once after step 12l and again immediately before commit.

## 12. Commit

Docs only: this report and the rehearsal record. `git diff --cached --stat` shown before commit
(R52); pushed to `origin/main`.

## 13. Expectations broken, and dispositions

* **12i** — F1; architect error, replaced by 12i', met.
* **Live `-shm` mtime 10:31 UTC** — F2; `dgx-snapshot-user.timer`, `mode=ro` + `VACUUM INTO`.
* **claude-config `4625e6f`** and **rehearsal JSON name** — F3; accepted in the ruling.
* **Receipt `note` is NULL** — the runner writes no note on an executed receipt. Not an expectation
  the brief set; recorded so P3ii's receipt is compared against it.

## Artifacts left on disk

* `data/surgical_autonomy/review.db.bak-manifest-01-phase3-rehearsal-20260923-154127` —
  the rehearsed copy, **migrated** (34 tables, overall `10325ff5…ee2e0`). Retention is the
  closeout's ruling. Not captured by the user-tier snapshot (`user_tier.py` lists its `.bak`
  sources explicitly).
* Scratch only (session scratchpad, not in the repo): the fresh database, the before/after
  fingerprint JSONs, the measurement script and its output.

## Not done, deliberately

No pre-write backup of live, no runner call against live, no new record of reference, no
`.SUPERSEDED` sidecar (all P3ii). No retirement of any backup. No CLAUDE.md, engine, test or
spec change. No `primer.md` update.
