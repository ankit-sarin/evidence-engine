# INPUT-IDENTITY-01 Phase 3 (i) — rehearsal of migration 021 on a verified copy of live

**Task:** INPUT-IDENTITY-01-P3i (session 8b, brief 1 of 3) · **Date:** 2026-09-24 · **Machine:** DGX Spark
**Opening HEAD:** `34f46566bfd4d5928bb74cbeb373fe4ce47136f1` (claude-config `65d21e9`)
**Docs commit:** the commit that adds this file (see `git log -- <this path>`). It carries this
report and the rehearsal record only.
**Live `review.db` was NOT written.** Every read of live was a raw `mode=ro` connection;
`auto_backup` read it once through the online-backup API. No `ReviewDatabase` was constructed on
live (I16). `--compare` against
`docs/session-reports/manifest-01/review_db_fingerprint_20260923T162059Z.json` exited **0** at
open, after the gate, and at close (§11). `-wal` **0 B** throughout.

**Rehearsal-after values (P3ii's live acceptance values, per R53 and the per-table rule):**

| value | rehearsal-after |
|---|---|
| tables | **34** |
| `overall_sha256` | `7f11419797f9d1b0f31b3a3ceee8fd932c96b19c14fd94d3ce93d9db59a23f00` (checkpoint only — a live write is accepted per table, never on overall) |
| `schema_structure_hash` | `96f05996cccc350f0b6697bc31471da0a824657f72ec3eae2fcf28c4ccea74ab` (= fresh, M5) |
| `schema_hash_sha256` (textual) | `cb52026b61394c676f2adf49998df5e1f5d4a13914765a7134ba13a2b644e015` (**≠ fresh** — F1) |
| record | `docs/session-reports/input-identity-01/rehearsal_db_fingerprint_20260924T203420Z.json` — a **checkpoint, not the record of reference** |

---

## 0. Findings, first

### F1 — textual schema hash copy-after ≠ fresh; the brief's expectation was wrong, and the difference is entirely pre-021 lineage

Step 9 expected copy-after textual == fresh `f19274f0…3ea8` "because the rebuild targets the fresh
template", and said to record the value either way. Measured: copy-after textual
`cb52026b…e015` ≠ fresh. Localised object by object from `sqlite_master` (type, name, sql):

| comparison | objects whose SQL text differs |
|---|---|
| live (pre-021) vs copy-after | **exactly one**: `table parsed_text_refs` |
| fresh vs live (pre-021) | 14 |
| fresh vs copy-after | 13 — equal to the previous set minus the `parsed_text_refs` objects |
| `parsed_text_refs` objects among fresh vs copy-after | **none** |

The 13 residual differences are legacy-lineage text that has always differed between a fresh
database and live: tables `abstract_screening_adjudication`, `abstract_screening_decisions`,
`abstract_verification_decisions`, `cloud_evidence_spans`, `cloud_extractions`,
`evidence_spans`, `extractions`, `papers`, `parse_attempts`; indexes
`idx_abstract_adjudication_decision`, `idx_abstract_adjudication_ext_key`,
`idx_abstract_adjudication_paper`, `idx_spans_extraction`. 021 touched none of them. Only one
table's text changed, and that table's text now equals fresh's. This is the 7b shape too: fresh-020 textual `0ba77776…3305` ≠
rehearsal-after `a4e49d4a…c864`, and R53 already took textual equality with fresh out of the live
gate for that reason (G3(ii) is live-after == rehearsal-after). **Not a defect in 021.** Recorded
because the brief's expectation did not hold; the brief's own G4 wording ("textual recorded") is met.

### F2 — I9 resolved to its second branch: 32 of 34, `sqlite_sequence` unchanged

`parsed_text_refs` has a TEXT primary key and no AUTOINCREMENT, so its rebuild adds and removes
nothing in `sqlite_sequence` (7 rows, hash `8a34a449…9f86`, before and after). The only deltas are
`parsed_text_refs` and `schema_migrations`.

### F3 — minor, gate unaffected

* The runner's result carries only `executed/skipped/already`; 021's own return
  (`rows`, `paths_normalized`) is not surfaced through it. I10's "0 paths changed" is therefore
  measured by tuple comparison (§6), not read from the migration's return.
* Chunk 2 of the gate prints 2 sklearn warnings from
  `test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` — the case the
  test exists to exercise. Pre-existing.

---

## 1. Startup verify (pre-flight 1–5)

| check | expected | measured | match |
|---|---|---|---|
| clock (I12) | outside 07:00–10:35 UTC | open 20:22:03 UTC; rehearsal 20:32–20:34 UTC | Y |
| jobs in R85 scope | none due in the brief window | crontab 07:00 health check, 09:00 nightly suite; `dgx-snapshot-user.timer` next Fri 2026-09-25 10:31:44 UTC; nothing else reads/writes `review.db` or the tree | Y |
| HEAD (I1) | `34f46566…f1`, clean, level | `34f46566bfd4d5928bb74cbeb373fe4ce47136f1`, clean, 0/0 with `origin/main` after fetch | Y |
| claude-config | `65d21e9` or successor | `65d21e9`, pull up to date | Y |
| standard gate (I2) | 2,770 / 17, deselects 0/0/10/6/1 | 572 / 741 / 452 (10) / 588 (6) / 417 (1) = **2,770 passed / 17 deselected**; per-chunk equal to M2 | Y |
| live `--compare` manifest-01 record (I3) | exit 0, 34 tables, overall `bb39ba81…6c40` | exit 0, 34 tables, `-wal` 0 B, overall `bb39ba81170c4f11d59954b9e70837afc427c66da710bde30881aad570d16c40` | Y |
| event store | 3 · 190 · 194 · 3 · 0; run tables empty | 3 arms · 190 paper events · 194 parsed-text refs · 3 identities · 0 field events; `run_manifests`/`run_stage_configs`/`run_calls` 0/0/0 | Y |
| last receipt; 021 receipt | `020_run_manifest`; none | `020_run_manifest` (`b0658f0f…559f`, applied 2026-09-23T16:10:31.801948+00:00); 0 rows `LIKE '021%'`; `parsed_text_refs` has 6 columns, no hash | Y |
| restore point manifest-01 pre-write | 31, `e564f250…5b63` | 31, `e564f250afe40af7eb9a6bc07596a3c795972f7ef3b653c37599e8bc18285b63`, no sidecars (fingerprint only) | Y |
| restore point readers-01 pre-write | 32, `62f39128…b79a` | 32, `62f3912813b6e9efa3a651ee4c4ebcd611adf89c989f240a58e83dcf6759b79a`, no sidecars (fingerprint only) | Y |
| hash baseline (M7) | sha256 `67754a47…b6e9`, 194 | `67754a477be575d285654b44cbf025d3ae17db1567b04970c0b6409bab6cb2e7`; `count` 194, 194 entries, 0 unreadable | Y |

The four older backups were not opened (R88).

## 2. I4 and I5 — read from source before any call on live

* **I4 — raw.** `engine/tools/db_fingerprint.py::read_snapshot`, the path branch:
  ```
  db_path = Path(target).resolve()
  uri = f"file:{db_path}?mode=ro"
  conn = sqlite3.connect(uri, uri=True)
  ```
  followed by one `BEGIN` held for the pass. Module imports are stdlib only (`argparse`,
  `binascii`, `hashlib`, `json`, `sqlite3`, `sys`, `time`, `contextlib`, `datetime`, `pathlib`);
  `main()` calls `fingerprint(db)` and nothing else. No `ReviewDatabase`, no runner.
* **I5 — raw.** `engine/utils/db_backup.py::auto_backup(db_path_or_connection, reason) -> BackupResult`:
  ```
  with read_snapshot(db_path_or_connection) as (src, uri, db_path):
      ...
      dst = sqlite3.connect(str(backup_path))
      src.backup(dst)
      dst.execute("PRAGMA journal_mode=DELETE")
  ```
  Its only engine import is `from engine.tools.db_fingerprint import compare, fingerprint, read_snapshot`.
  `engine/__init__.py` and `engine/utils/__init__.py` are empty. The source is opened by
  `read_snapshot` (`mode=ro`); no `ReviewDatabase`, no `runner`.

## 3. Fresh database (pre-flight 6, I6)

`ReviewDatabase("fresh_021", data_root=<scratchpad>/fresh)` at `34f4656`, then its connection closed.

* 021 module, as the test imports it: **`engine.migrations.021_parsed_text_sha256`** (`MID = "021_parsed_text_sha256"`).
* Receipt: `('021_parsed_text_sha256', '1c076888ccd57862ad22f574315aee38b1212397344539f79497780958012024', '2026-09-24T20:23:42.451088+00:00', 'executed', 1, None)`
  — equal to the file's `sha256` and to M4.
* Executed 004–016, 018–021; 002, 003, 017 skipped (data kind).
* **34 tables**; structure `96f05996cccc350f0b6697bc31471da0a824657f72ec3eae2fcf28c4ccea74ab`;
  textual `f19274f0b891e4581b5fdfef0545819f6390b79774833f60cdcf0ab74e4c3ea8` — both = M5.
  Overall `b8568e34be76d666c81588ccd8be352be4faab8fc12697e3cc9868d549144cce`.
* Second `runner.run`: `executed: []`; overall and textual unchanged.

## 4. Rehearsal copy (step 7)

* `lsof` / `fuser` on `review.db`, `-wal`, `-shm` at 20:32:49 UTC: empty (exit 1 both).
* `auto_backup("data/surgical_autonomy/review.db", reason="input-identity-01-rehearsal")` →
  **`data/surgical_autonomy/review.db.bak-input-identity-01-rehearsal-20260924-203254`**.
* Verified `BackupResult` fingerprint: **34 tables**, overall
  `bb39ba81170c4f11d59954b9e70837afc427c66da710bde30881aad570d16c40` (= live, G2); structure
  `effd6d51…08fd`; textual `a4e49d4a…c864`. No `-wal`/`-shm` beside it.
* "Before" record: `--out` to scratch; `parsed_text_refs` `(rowid, *columns)` tuples saved to scratch.
* Retained on disk; retention is the closeout's ruling.

## 5. 021 execution (step 8, I7)

`runner.run(Path(<copy>))`, the same function the live write will call. `include_data` was not passed.

* Wall time **0.0256 s**.
* Result: `executed: ['021_parsed_text_sha256']`, `skipped: []`, `already:` 002–020 (19 ids). **021 alone** (G3).
* Receipt, verbatim: `('021_parsed_text_sha256', '1c076888ccd57862ad22f574315aee38b1212397344539f79497780958012024', '2026-09-24T20:33:07.401400+00:00', 'executed', 1, None)`
  — `file_sha256` = M4 = the fresh re-read; `runner_version` 1; `note` NULL (as for 020).
* No refusal: the backfill recomputed all 194 file hashes and every one agreed with the baseline
  (I8; R101). No file and no baseline was touched.

## 6. Per-table acceptance (step 9, I9, I10) — copy-before vs copy-after

**32 of 34 content-identical.** Deltas: `parsed_text_refs` (rebuilt, + hash column) and
`schema_migrations` (19 → 20, the 021 receipt). `sqlite_sequence` unchanged (F2). Table set unchanged.

| table | rows before | rows after | sha256 before | sha256 after | same |
|---|---|---|---|---|---|
| `abstract_screening_adjudication` | 0 | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | yes |
| `abstract_screening_decisions` | 21374 | 21374 | `d961fd298f839a0123cfe58a16ba350e36ddb11f5a9dab3e96af409b5e02b943` | `d961fd298f839a0123cfe58a16ba350e36ddb11f5a9dab3e96af409b5e02b943` | yes |
| `abstract_verification_decisions` | 1422 | 1422 | `3d6b36f5205cd2dc6311ab5e62944dcd7fc0ba55fb50e935870e7543e7b226ce` | `3d6b36f5205cd2dc6311ab5e62944dcd7fc0ba55fb50e935870e7543e7b226ce` | yes |
| `arms` | 3 | 3 | `d9a77a0e7744d6111130eda18245d6cf63a1e77226e8d9796d5e8976832e8519` | `d9a77a0e7744d6111130eda18245d6cf63a1e77226e8d9796d5e8976832e8519` | yes |
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
| `parsed_text_refs` | 194 | 194 | `b05b1a4312b2dbf5436dc9a5b68dcde7c26e1bbcbd3fb9ae3ec9556ddb717088` | `2c9bf1930de22227ebb34392c0e846c12d7b6254aa59300de21e1b534f89b719` | **no** |
| `provenance_census_runs` | 2 | 2 | `d4f3d86c40498571a056ac9f5de423ff402294deb6e3ee5d0611d80e1a6b5642` | `d4f3d86c40498571a056ac9f5de423ff402294deb6e3ee5d0611d80e1a6b5642` | yes |
| `provenance_classifications` | 22034 | 22034 | `fe39f2684980ac0a7bb97711e8a3124061a8d39bacd189b40a60c3c0c0b9c4e4` | `fe39f2684980ac0a7bb97711e8a3124061a8d39bacd189b40a60c3c0c0b9c4e4` | yes |
| `review_identities` | 3 | 3 | `4a586901e32c2f7786bee80ae6060065b8729098ef608aae0490edc4efe3941d` | `4a586901e32c2f7786bee80ae6060065b8729098ef608aae0490edc4efe3941d` | yes |
| `review_runs` | 6 | 6 | `7054c7597c68d23ca65e9579a80e176f15a5cf6f03b06bdaf82f497c0136228a` | `7054c7597c68d23ca65e9579a80e176f15a5cf6f03b06bdaf82f497c0136228a` | yes |
| `run_calls` | 0 | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | yes |
| `run_manifests` | 0 | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | yes |
| `run_stage_configs` | 0 | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | yes |
| `schema_migrations` | 19 | 20 | `a3b51bbbf22d4b22d6e35d4c131c2b8ceb199c4a59fa9ce919a7ffe200c2c05b` | `db51957defeb54c691898c267c1d5b9046a1d6731d2e307c7fc5ccba7a86f419` | **no** |
| `sqlite_sequence` | 7 | 7 | `8a34a4493aab8ec413cbab610338ec48f0806d8c3b7250df45924835b9b09f86` | `8a34a4493aab8ec413cbab610338ec48f0806d8c3b7250df45924835b9b09f86` | yes |
| `workflow_state` | 12 | 12 | `e7751792f5e4d688cadf10d2da00f65ade3449df49a46200da64abb1ae68433c` | `e7751792f5e4d688cadf10d2da00f65ade3449df49a46200da64abb1ae68433c` | yes |

**`parsed_text_refs` by tuple (I10):**

* 194 rows before, 194 after.
* `(rowid, parsed_text_uid, paper_id, parsed_text_path, parsed_text_version, source_full_text_assets_id, recorded_at)`
  for all 194 rows: **identical, tuple for tuple** (rowids 1–194, order unchanged).
* Columns after: the six originals in their original order, then `parsed_text_sha256`.
* `parsed_text_sha256` populated and 64 characters on **194/194**; equal to the baseline entry's
  `sha256` for its `parsed_text_uid` on **194/194** (0 mismatches). Baseline `paper_id` and
  `parsed_text_version` agree on every row, and the baseline covers exactly the 194 uids.
* Paths changed: **0**. Every stored path was already canonical (R100). Matches M6.

## 7. Structure and textual (step 9, G4)

* Copy-after: **34 tables**.
* Structure `96f05996cccc350f0b6697bc31471da0a824657f72ec3eae2fcf28c4ccea74ab` = fresh (M5);
  `structure_differences(fresh, copy)` = **`[]`**.
* Textual `cb52026b61394c676f2adf49998df5e1f5d4a13914765a7134ba13a2b644e015`, **≠ fresh**; see F1.
  **This value is P3ii's G3(ii) checkpoint:** live-after textual must equal it.

## 8. Read-back (step 10, I11) — copy vs fresh `sqlite_master`, character for character

| object | copy == fresh |
|---|---|
| `CREATE TABLE "parsed_text_refs"` (whole statement) | **equal** |
| hash CHECK (substring of the above; = module `HASH_CHECK`) | **equal** |
| `UNIQUE (paper_id, parsed_text_version)` (substring of the above) | **equal** |
| trigger `parsed_text_refs_no_update` | **equal** |
| trigger `parsed_text_refs_no_delete` | **equal** |
| `sqlite_autoindex_parsed_text_refs_1`, `_2` | equal (both NULL sql) |

Copy's `CREATE TABLE`, repr:
```
'CREATE TABLE "parsed_text_refs" (\n        parsed_text_uid   TEXT PRIMARY KEY,\n        paper_id          INTEGER NOT NULL REFERENCES papers(id),\n        parsed_text_path  TEXT    NOT NULL,\n        parsed_text_version INTEGER NOT NULL,\n        source_full_text_assets_id INTEGER,\n        recorded_at       TEXT    NOT NULL,\n        parsed_text_sha256 TEXT NOT NULL CHECK (parsed_text_sha256 IS NOT NULL AND length(parsed_text_sha256) = 64 AND parsed_text_sha256 NOT GLOB \'*[^0-9a-f]*\'),\n        UNIQUE (paper_id, parsed_text_version)\n    )'
```
Triggers, repr:
```
"CREATE TRIGGER parsed_text_refs_no_update BEFORE UPDATE ON parsed_text_refs\n      BEGIN SELECT RAISE(ABORT, 'parsed_text_refs is append-only: correct by appending an event'); END"
"CREATE TRIGGER parsed_text_refs_no_delete BEFORE DELETE ON parsed_text_refs\n      BEGIN SELECT RAISE(ABORT, 'parsed_text_refs is append-only: correct by appending an event'); END"
```
Beyond the brief: both triggers also equal `trigger_statements()` in the module and the 016 text
read from live's `sqlite_master` (pre-021), so the guard is restored verbatim.

## 9. R87 on a throwaway copy of fresh (step 11)

A copy of the fresh database was made through the online-backup API into
`<scratchpad>/throwaway_fresh.db` (not the fresh reference, not the rehearsal copy, not live), with
`PRAGMA foreign_keys=ON`. One `papers` row was inserted (id 1), then one `parsed_text_refs` row
(`u1`, paper 1, version 1, hash `0123456789abcdef` × 4). **The insert succeeded.**

| attempt | result, verbatim |
|---|---|
| `UPDATE parsed_text_refs SET parsed_text_path='q' WHERE parsed_text_uid='u1'` | `IntegrityError: parsed_text_refs is append-only: correct by appending an event` |
| `DELETE FROM parsed_text_refs WHERE parsed_text_uid='u1'` | `IntegrityError: parsed_text_refs is append-only: correct by appending an event` |
| INSERT, 63-char hash (version 2) | `IntegrityError: CHECK constraint failed: parsed_text_sha256 IS NOT NULL AND length(parsed_text_sha256) = 64 AND parsed_text_sha256 NOT GLOB '*[^0-9a-f]*'` |
| INSERT, `'a'` + 63 × `'z'` (version 3) | `IntegrityError: CHECK constraint failed: parsed_text_sha256 IS NOT NULL AND length(parsed_text_sha256) = 64 AND parsed_text_sha256 NOT GLOB '*[^0-9a-f]*'` |

The two invalid inserts used distinct versions, so the CHECK refused them and not the UNIQUE. The
row after all four attempts was `('u1', 'p/1.md', '0123…cdef')`, unchanged, and `foreign_key_check`
was empty. The throwaway was deleted and no `throwaway*` file remains.

## 10. Idempotency and integrity (steps 12, 13)

* Second `runner.run(copy)`: `executed: []`, `skipped: []`, 021 in `already` (20 ids). Fingerprint
  `compare(after, after2)` = `[]`; overall unchanged.
* `PRAGMA integrity_check` → `ok`. `PRAGMA foreign_key_check` → empty.

## 11. Rehearsal-after record and live at close (steps 14, 15)

`docs/session-reports/input-identity-01/rehearsal_db_fingerprint_20260924T203420Z.json`:
34 tables; overall `7f11419797f9d1b0f31b3a3ceee8fd932c96b19c14fd94d3ce93d9db59a23f00`;
structure `96f05996…74ab`; textual `cb52026b…e015`.

What P3ii's live write will be held to:
* **G3(i)**: live-after structure = `96f05996…74ab`, and `structure_differences(fresh, live)` = `[]`.
* **G3(ii)**: live-after textual = `cb52026b61394c676f2adf49998df5e1f5d4a13914765a7134ba13a2b644e015`.
* **G3(iii)**: the `parsed_text_refs` CREATE TABLE and both triggers read back verbatim from live's
  `sqlite_master`, equal to §8.
* **Per table**: the 32 identical tables' hashes equal this record's. `parsed_text_refs` equals
  this record's hash, since its content is deterministic from the baseline. `schema_migrations`
  differs from this record in the 021 receipt's `applied_at` only, so it is compared by row, not by hash.

Live `--compare` against the manifest-01 record at 20:35:46 UTC, immediately before commit:
**exit 0**, 34 tables, `-wal` 0 B, overall `bb39ba81170c4f11d59954b9e70837afc427c66da710bde30881aad570d16c40`
("IDENTICAL — schema, every table, and the overall hash all match"). It is repeated after the push.

## Artifacts left on disk

* `data/surgical_autonomy/review.db.bak-input-identity-01-rehearsal-20260924-203254`: the
  rehearsed copy, **migrated** (34 tables, overall `7f114197…3f00`). Its retention is the closeout's ruling.
* Scratch only (session scratchpad, not in the repo): the fresh reference database, the
  before/after fingerprints and tuple snapshots, the gate chunk outputs.

## Not done, deliberately

No pre-write backup of live, no runner call against live, no new record of reference (all P3ii).
No backup retired. No engine, test, migration, spec, CLAUDE.md or `primer.md` change.
