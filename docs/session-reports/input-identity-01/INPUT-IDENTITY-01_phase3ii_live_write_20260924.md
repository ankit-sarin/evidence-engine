# INPUT-IDENTITY-01 Phase 3 (ii) — the live write of migration 021

**Task:** INPUT-IDENTITY-01-P3ii (session 8b, brief 2 of 3) · **Date:** 2026-09-24 · **Machine:** DGX Spark
**Opening HEAD:** `00e1a45607d596eb7dff8fcb30ab954a6ca8084c` (P3i docs commit on `34f4656`; no
file under `engine/`, `tests/` or `scripts/` changed between them)
**Docs commit:** the commit that adds this file. It carries this report, the new record of
reference and the SUPERSEDED sidecar on the manifest-01 record, and nothing else.

**`data/surgical_autonomy/review.db` WAS written — once, by `runner.run`, which applied 021 alone.**
34 → 34 tables. The write was accepted per table against the P3i rehearsal checkpoint, and G3 under
R53 was met in all three parts. **The new record of reference is
`docs/session-reports/input-identity-01/review_db_fingerprint_20260924T205225Z.json`**, overall
`ea05912d67f0b841bf003a7a941f6505e2c57140b5c2f62e1ff446ea16b9ce01`. The manifest-01 record is
superseded, not edited; a sidecar sits beside it.

**The I16 interim control is lifted** as of checkpoint 12 (20:42 UTC): 021 is on live, so a
`ReviewDatabase("surgical_autonomy")` construction has no pending migration to apply. That holds
only until the next migration enters the tree. The runner guard remains a later session's work.

---

## 0. Findings, first

### F1 — the P3i rehearsal copy was opened `mode=ro` despite the DO NOT TOUCH list

The brief lists the rehearsal copy under DO NOT TOUCH. Checkpoints 10 and 11 nonetheless opened it
through raw `mode=ro` connections, twice: once to read its `sqlite_master` text for G3(iii), and
once to read its `schema_migrations` rows for the one-cell comparison. It was not written. It is a
rollback-journal database, so no sidecar was created, and after the reads its fingerprint still
equals its record (`compare(record, fingerprint(copy)) == []`, overall `7f114197…3f00`). Both
results are also established **without** the copy:

* G3(iii): live's text equals the **fresh** database's text for every `parsed_text_refs` object,
  and P3i §8 established that rehearsal == fresh for the same objects.
* `schema_migrations`: 7b's record-only method. Live's table was rebuilt in scratch with **only**
  021's `applied_at` replaced by the rehearsal's value, and it hashes to
  `db51957defeb54c691898c267c1d5b9046a1d6731d2e307c7fc5ccba7a86f419`, which is the rehearsal
  record's value (§5).

This is a departure from the brief, and it does not change any gate result.

### F2 — checkpoint 12's "021 'already'" was observed indirectly

`ReviewDatabase._run_migrations` calls `runner.run(self.db_path)` and discards the return value. It
logs only when something executed. 7b wrapped `runner.run` in a pass-through to capture the return
value; this session did not, and it did not open live a second time to do so, because the brief
allows one construction. The evidence for 'already':

* INFO logging was enabled for the construction, and no `Migrations executed` line was emitted.
  The fresh build in P3i emitted that line at INFO.
* `schema_migrations` stayed at 20 rows.
* The live fingerprint was identical before and after (`compare() == []`).

A receipt is written for every executed migration, so an unchanged receipt count with an unchanged
fingerprint means nothing executed.

### F3 — minor, gates unaffected

* Write wall time **0.0428 s** (rehearsal 0.0256 s).
* After the runner write, live carries a 0 B `-wal` and a 32 KiB `-shm`. Live is in WAL mode, and
  the runner's and 021's connections leave them in place. `-wal` was 0 B at every fingerprint read.

---

## 1. Pre-flight

| check | expected | measured | match |
|---|---|---|---|
| clock (I2) | outside 07:00–10:35 UTC | open 20:41:30; backup 20:41:38; write 20:41:46.97; post-write checks to 20:52:25 UTC | Y |
| R85-scope jobs | none due | `dgx-snapshot-user.timer` next 2026-09-25 10:31:44 UTC; crontab 07:00 / 09:00 | Y |
| HEAD | `00e1a45`, clean, level | `00e1a45607d596eb7dff8fcb30ab954a6ca8084c`, clean, 0/0 with `origin/main` after fetch | Y |
| claude-config | untouched | `65d21e9`, `PROJECT_LEDGER.md` modified (hook rows, left for the wrap) | Y |
| live `--compare` manifest-01 (I1) | exit 0, 34, `-wal` 0 B, `bb39ba81…` | exit 0, 34, `-wal` 0 B, `bb39ba81170c4f11d59954b9e70837afc427c66da710bde30881aad570d16c40` | Y |
| lsof / fuser, first pass (I3) | empty | empty (exit 1 both) | Y |

## 2. Pre-write backup (step 5)

* `auto_backup("data/surgical_autonomy/review.db", reason="input-identity-01-phase3-pre-write")`
  → **`data/surgical_autonomy/review.db.bak-input-identity-01-phase3-pre-write-20260924-204138`**.
* Verified `BackupResult` fingerprint: **34 tables**, overall
  `bb39ba81170c4f11d59954b9e70837afc427c66da710bde30881aad570d16c40` (= live, I4); structure
  `effd6d51…08fd`; textual `a4e49d4a…c864`. No sidecars.
* **RETAINED to session 10.** The retention row belongs to the closeout.

## 3. The write (steps 6–7)

* lsof / fuser second pass empty at 20:41:46.932 UTC; the runner started at 20:41:46.966 UTC.
* `runner.run(Path("<repo>/data/surgical_autonomy/review.db"))`, the same call as P3i step 8 with
  the path substituted. `include_data` was not passed.
* Wall time **0.0428 s**.
* Return value: `{"executed": ["021_parsed_text_sha256"], "skipped": [], "already": ["002_screening_rename", "003_backfill_expanded_screening", "004_pdf_quality_check", "005_model_digest", "006_not_null_confidence_tier", "007_add_judge_tables", "008_add_fabrication_verifications", "009_add_backfill_audit_log", "010_add_provenance_classifications", "011_add_absence_claim_class", "012_codebook_provenance", "013_drop_schema_hash_not_null", "014_cloud_tables", "015_drop_prerename_adjudication_indices", "016_event_store", "017_seed_event_store", "018_cloud_shape_and_audit_adjudication", "019_paper_state_axes", "020_run_manifest"]}`
  — **021 alone** (I5, G2).
* Receipt, verbatim (checkpoint 9, the last row of `schema_migrations`):
  `('021_parsed_text_sha256', '1c076888ccd57862ad22f574315aee38b1212397344539f79497780958012024', '2026-09-24T20:41:47.000035+00:00', 'executed', 1, None)`
  — `schema_migrations` **20 rows**.
* The backfill raised no refusal: all 194 recomputed file hashes agreed with the committed baseline (R101).

## 4. Checkpoint 8 and G3 under R53 (checkpoint 10)

* **8:** **34 tables**; `-wal` 0 B; `PRAGMA integrity_check` → `ok`; `PRAGMA foreign_key_check` → empty.
* **(i)** `schema_structure_hash(live)` = `96f05996cccc350f0b6697bc31471da0a824657f72ec3eae2fcf28c4ccea74ab`
  = fresh (the P3i scratch fresh database, built at `34f4656`; `00e1a45` changed docs only).
  `structure_differences(fresh, live)` = `[]`. **Met.**
* **(ii)** textual `schema_hash_sha256(live)` = `cb52026b61394c676f2adf49998df5e1f5d4a13914765a7134ba13a2b644e015`
  = rehearsal-after. **Met.**
* **(iii)** read back from live `sqlite_master`, character for character:

  | object | live == rehearsal | live == fresh |
  |---|---|---|
  | `CREATE TABLE "parsed_text_refs"` (whole statement, incl. hash CHECK and UNIQUE) | yes | yes |
  | hash CHECK `CHECK (parsed_text_sha256 IS NOT NULL AND length(parsed_text_sha256) = 64 AND parsed_text_sha256 NOT GLOB '*[^0-9a-f]*')` present | yes | yes |
  | `UNIQUE (paper_id, parsed_text_version)` present | yes | yes |
  | trigger `parsed_text_refs_no_update` | yes | yes |
  | trigger `parsed_text_refs_no_delete` | yes | yes |
  | `sqlite_autoindex_parsed_text_refs_1`, `_2` | yes (NULL sql) | yes |

  The strings are those quoted in P3i §8. **Met.**

## 5. Per-table acceptance (checkpoint 11) — live-after vs the rehearsal record

* **33 of 33 non-receipt tables equal** by content hash, `parsed_text_refs` included. No table differs.
* **`schema_migrations` differs in exactly one cell.** Both sides have 20 rows with the same
  `migration_id` set. Cell-by-cell against the rehearsal database, the only difference is
  `('021_parsed_text_sha256', 'applied_at')`:
  * rehearsal `2026-09-24T20:33:07.401400+00:00`
  * live `2026-09-24T20:41:47.000035+00:00`

  Independently, by record only (F1): the live table hash is
  `f8b6f5cc33756406b233b8b43eb9505173c76162cb408377a70a6d39159287ec`. With 021's `applied_at`
  substituted it is `db51957defeb54c691898c267c1d5b9046a1d6731d2e307c7fc5ccba7a86f419`, equal to the
  rehearsal record.
* **`parsed_text_refs` against the pre-write backup** (read `mode=ro`):
  * 194 rows before and after.
  * `(rowid, parsed_text_uid, paper_id, parsed_text_path, parsed_text_version, source_full_text_assets_id, recorded_at)`
    is **identical for all 194**.
  * `parsed_text_sha256` is present with 64 characters on 194/194 and equals the baseline on
    194/194 (0 mismatches). The uid set equals the baseline's.
  * **0 paths changed.**
* Against the superseded manifest-01 record, 32 tables are unchanged. The two that differ are
  `parsed_text_refs` (194 → 194 rows, rebuilt) and `schema_migrations` (19 → 20).
* The live-after overall `ea05912d…ce01` differs from the rehearsal checkpoint `7f114197…3f00` by
  that one cell alone. Overall is not an acceptance value.

## 6. I8 on live (checkpoint 12)

`ReviewDatabase("surgical_autonomy")` was constructed once, with INFO logging, and its connection
was closed.

* The live fingerprint immediately before the construction equals checkpoint 11's. Before vs
  after: `compare()` → `[]`; overall `ea05912d67f0b841bf003a7a941f6505e2c57140b5c2f62e1ff446ea16b9ce01` both times.
* 021 'already': observed indirectly, see F2. There was no `Migrations executed` line and
  `schema_migrations` stayed at 20 rows.
* **C12:** `audit_adjudication` is absent after construction (0 rows in `sqlite_master`).
* **The I16 interim control is lifted from this point.**

## 7. Gate after the write (checkpoint 13)

Five chunks, same file list as P3i:
572 / 741 / 452 (10 deselected) / 588 (6) / 417 (1) = **2,770 passed / 17 deselected**, deselects
**0/0/10/6/1**. The totals are unchanged by the write (I9).

## 8. New record of reference and sidecar (checkpoint 14)

* **`docs/session-reports/input-identity-01/review_db_fingerprint_20260924T205225Z.json`**:
  **34 tables**; overall `ea05912d67f0b841bf003a7a941f6505e2c57140b5c2f62e1ff446ea16b9ce01`;
  structure `96f05996cccc350f0b6697bc31471da0a824657f72ec3eae2fcf28c4ccea74ab`; textual
  `cb52026b61394c676f2adf49998df5e1f5d4a13914765a7134ba13a2b644e015`; `-wal` 0 B. Equal to the
  checkpoint-11 fingerprint (taken after the gate). Live `--compare` against it: **exit 0**.
* **Sidecar:** `docs/session-reports/manifest-01/review_db_fingerprint_20260923T162059Z.json.SUPERSEDED.md`,
  in the effective-result-02 shape. The manifest-01 record itself is untouched.

## 9. Disk inventory — backups this lane created, `data/surgical_autonomy/`

| file | tables | overall | status |
|---|---|---|---|
| `review.db.bak-input-identity-01-phase3-pre-write-20260924-204138` | 34 | `bb39ba81…6c40` (= manifest-01 record) | new; retained to session 10 |
| `review.db.bak-input-identity-01-rehearsal-20260924-203254` | 34 | `7f114197…3f00` (= rehearsal record) | retention is the closeout's ruling |
| `review.db.bak-manifest-01-phase3-pre-write-20260923-161020` | 31 | `e564f250…5b63` (P3i, fingerprint) | retained to session 10 |
| `review.db.bak-readers-01-phase3-pre-write-20260922-165453` | 32 | `62f39128…b79a` (P3i, fingerprint) | retained to session 10 |

The four older backups were not opened (R88).

## 10. Expectations broken

* **Rehearsal copy opened `mode=ro`**: F1. It was not written, its fingerprint is unchanged, and
  both results it served are also established without it.
* **'already' observed indirectly**: F2.
* **Wall time and `-wal`/`-shm` presence**: F3.

## Not done, deliberately

No `restore()`. No second `runner.run` on live. No run against live (R19, R71). No backup retired.
No primer.md, CLAUDE.md, plan (F1 of P3i: the textual-hash correction), engine, test or spec
change. The closeout is the next brief's.
