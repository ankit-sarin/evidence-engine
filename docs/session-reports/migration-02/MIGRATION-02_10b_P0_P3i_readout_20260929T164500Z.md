# MIGRATION-02 10b-P0+P3i — read-out (startup verify, backup verification, rehearsal of 022)

Session 10b, 2026-09-29. Opened 16:04:27 UTC; rehearsal 16:41–16:43 UTC (outside 07:00–10:35, R86).
HEAD `2c3fbbd52eb47ce1b798e4ebe5a23bb2c209dce0`, clean, level with origin. **No live write. Uncommitted.**

## Part 0 (G1 met)
| item | expected | measured |
|---|---|---|
| live `--compare` vs `input-identity-01/review_db_fingerprint_20260924T205225Z.json` | exit 0, 34, ea05912d…ce01 / 96f05996…74ab / cb52026b…e015, -wal 0 B | identical, exit 0, -wal 0 B |
| event store | 3 · 190 · 194 (all sha) · 3 · 0; run_* empty | 3 · 190 · 194 (194) · 3 · 0; 0 / 0 / 0 |
| receipts | 20, last 021 | 20, last `021_parsed_text_sha256`; 022 pending, KINDS "schema" |
| codebook / spec | 89dbfa91…ad82; elicitation false; local_deepseek_r1_32b | identical |
| gate | 2,883 / 1 xfail / 17; collected 2,884; id diff 0/0 | 2,883 / 1 xfail / 17; 2,884; 0/0; wall 634.46 s (10m34s) |
| hash baseline | 67754a47…e7 | identical |

## B1 — restore points (G2 met)
input-identity-01 34 `bb39ba81170c4f11d59954b9e70837afc427c66da710bde30881aad570d16c40`; manifest-01 31 `e564f250afe40af7eb9a6bc07596a3c795972f7ef3b653c37599e8bc18285b63`; readers-01 32 `62f3912813b6e9efa3a651ee4c4ebcd611adf89c989f240a58e83dcf6759b79a`.

## B2 — older backups, characterised on copies in `~/scratch/backup-review-10b/` (G3 met)
| original | size | mtime (UTC) | sidecars | tables | overall | schema_migrations | papers |
|---|---|---|---|---|---|---|---|
| review.db.bak-pre-migrations01-20260921-002807 | 101978112 | 2026-09-21 00:28:07 | none | 24 | f376562e095cfbcbf23ca997f31375feba340f74ed7a14ffe26e42cc63839e00 | absent | 10039 |
| review.db.bak-pre-run6-cleanup-20260315194955 | 47759360 | 2026-03-15 19:49:55 | -shm 32768, -wal 0 | 15 | 0b5c0f30fe4b586cac468756a7860ef8d46c516a908fa5a1e047b13cbdfe6e8e | absent | 10039 |
| review.db.bak-pre-sonnet-cleanup-20260316-165020 | 60252160 | 2026-03-16 15:49:56 | -shm 32768, -wal 0 | 15 | 59d9b9876b67cc6b4511dc6928b53e71fbe1ab81b1bc3916ccd7c701fe17668e | absent | 10039 |
| review.db.pre_rename_backup | 18907136 | 2026-03-12 17:35:42 | -shm 32768, -wal 0 | 12 | 5b87b2a9e163dafb3c3fac21f8a1ac78eafc279f9156c1a16dbea9c6f3adb448 | absent | 804 |

Copied sidecars' sha256 unchanged by the open (-shm mtime only). Originals: size, mtime and sha256 unchanged after the review. Nothing deleted.

## Rehearsal
- B3 exclusivity 16:41:22 UTC: `lsof` and `fuser` on review.db / -wal / -shm returned nothing (rc=1).
- B4 `auto_backup(live, "migration-02-rehearsal")` → `data/surgical_autonomy/review.db.bak-migration-02-rehearsal-20260929-164128`, 34 tables, overall ea05912d67f0b841bf003a7a941f6505e2c57140b5c2f62e1ff446ea16b9ce01 (== M1). Record `rehearsal_source_fingerprint_20260929T164140Z.json`. File sha256 4abb5f4d7412576f5d4ebfef68335297117e7f7f1c6aee18ebffdaf103556302. A byte-identical pre-state twin (`~/scratch/rehearsal-10b/source_copy.db`) was taken before B5 to serve as "the source copy" for B9/B10, since B5 migrates the B4 copy in place.
- B5 `python -m engine.migrations <copy> --apply-pending`, exit 0:
```
{'executed': ['022_run_kinds_and_audit_tables'], 'skipped': [], 'already': ['002_screening_rename', '003_backfill_expanded_screening', '004_pdf_quality_check', '005_model_digest', '006_not_null_confidence_tier', '007_add_judge_tables', '008_add_fabrication_verifications', '009_add_backfill_audit_log', '010_add_provenance_classifications', '011_add_absence_claim_class', '012_codebook_provenance', '013_drop_schema_hash_not_null', '014_cloud_tables', '015_drop_prerename_adjudication_indices', '016_event_store', '017_seed_event_store', '018_cloud_shape_and_audit_adjudication', '019_paper_state_axes', '020_run_manifest', '021_parsed_text_sha256']}
```
- B6 same command, exit 0:
```
{'executed': [], 'skipped': [], 'already': [... the 20 above ..., '022_run_kinds_and_audit_tables']}
```
- B7 rehearsal-after: 36 tables; textual 4382dd4fa32e57cefc8ea43a4a9cf0a4b0fd3d320a1fa7cdff8735464750f692; structure b0dffa3fdd7ff57a4aa6910181c1b02269b9478feee0d33c8d14744ecb1f57aa; overall adfa8d04cd944741ce61ffb569c7eb698d9bbb3c2f5fb9f2c59be4dce73137db; no -wal/-shm; journal_mode delete; integrity_check ok. Record `rehearsal_db_fingerprint_20260929T164153Z.json`.
- B8 fresh `ReviewDatabase` in `~/scratch/rehearsal-10b/fresh/`: runner return `executed` 004…016, 018…022 (022 included), `skipped` 002, 003, 017, `already` []. Fresh: 36 tables, structure b0dffa3f…57aa (== rehearsal-after), textual eeebedf6186e831fb18f0e507cedada92ddf5a86816ff1e872fab08af312bef4, overall 71b725551396a2da2d44e04a156b906da90b2541a5b06370fe285e47586c6f41. structure_differences(fresh, rehearsal-after) = 0.
- B9 structure_differences(source copy, rehearsal-after), 5 lines:
```
table 'audit_verdicts': absent in source
table 'claim_inputs': absent in source
run_calls.columns: only in rehearsal-after: ['outcome', 'TEXT', 1, None, 0]
run_calls.columns: only in rehearsal-after: ['outcome_detail', 'TEXT', 0, None, 0]
run_manifests.columns: only in rehearsal-after: ['end_reason', 'TEXT', 0, None, 0]
```
  Tables named = {run_manifests, run_calls, claim_inputs, audit_verdicts}; paper_events and run_stage_configs absent.
- Single transaction (ruling 5): no -wal; no `*_new_022` object in sqlite_master; every 022 object present; row counts paper_events/run_manifests/run_stage_configs/run_calls/claim_inputs/audit_verdicts = 190/0/0/0/0/0; foreign_key_check over those six empty; fresh reaches the same structure.
- Out-of-scope tables: 28 user tables (outside schema_migrations, paper_events, run_manifests, run_stage_configs, run_calls, claim_inputs, audit_verdicts and sqlite_sequence) compared by per-table content hash: 28/28 equal. paper_events, run_stage_configs, run_manifests and run_calls content hashes are also equal to the source.

## J2 probe (throwaway copy of the fresh DB, deleted after)
open run, set end_reason only → ACCEPTED (row: end_status NULL, ended_at NULL, end_reason 'x'); close aborted+reason → ACCEPTED; closed run, change end_reason only → refused by end_once; open run, change a body column → refused by end_once.

## A3 — 022 DDL, rendered verbatim from the module (temporary names as the module emits them)
```sql

    CREATE TABLE paper_events_new_022 (
    event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    event_uid   TEXT    NOT NULL UNIQUE,
    event_type  TEXT    NOT NULL,
    occurred_at TEXT    NOT NULL,
    recorded_at TEXT    NOT NULL,
    actor_kind  TEXT    NOT NULL CHECK (actor_kind IN ('model', 'human', 'engine')),
    actor_role  TEXT    NOT NULL CHECK (actor_role IN ('reviewer', 'extractor', 'system')),
    actor_name  TEXT    NOT NULL,
    actor_digest TEXT,
    run_id      INTEGER REFERENCES run_manifests(run_id),
    run_marker  TEXT,
    prior_event_id INTEGER REFERENCES paper_events_new_022(event_id),
    presented_context_sha256 TEXT,
    reason      TEXT,
    payload_json TEXT   NOT NULL DEFAULT '{}',
        paper_id    INTEGER NOT NULL REFERENCES papers(id),
        to_state    TEXT    NOT NULL CHECK (to_state IN ('eligible', 'abstract_out', 'full_text_out', 'parsed', 'extracted', 'extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context', 'audited_ai')),
        from_state  TEXT,
        reason_code TEXT,
        stage_name  TEXT,
        CHECK (actor_role <> 'system' OR actor_kind = 'engine'),
        CHECK (event_type IN ('screened', 'verified', 'adjudicated', 'acquired', 'not_obtainable', 'parsed', 'extracted', 'extraction_failed', 'audited', 'manual_advance', 'bypass', 'state_at_migration')),
        -- R39: an event's type and its to_state must name the same axis.
        CHECK (
            (event_type IN ('screened', 'verified', 'adjudicated') AND to_state IN ('eligible', 'abstract_out', 'full_text_out'))
         OR (event_type IN ('acquired', 'not_obtainable', 'parsed', 'extracted', 'extraction_failed', 'audited') AND to_state IN ('parsed', 'extracted', 'extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context', 'audited_ai'))
         OR (event_type IN ('manual_advance', 'bypass', 'state_at_migration'))
        ),
        -- R39 / S3h: a reason for exactly the failure tokens, and for no other.
        CHECK (
            (to_state IN ('extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context') AND reason_code IS NOT NULL)
         OR (to_state NOT IN ('extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context') AND reason_code IS NULL)
        ),
        -- R68: a row with no run is a seeded, pre-manifest row and nothing else.
        CHECK (
            (run_id IS NOT NULL AND run_marker IS NULL)
            OR
            (run_id IS NULL AND run_marker IS 'pre-manifest')
        )
    )
    
CREATE TRIGGER IF NOT EXISTS paper_events_no_update BEFORE UPDATE ON paper_events BEGIN SELECT RAISE(ABORT, 'paper_events is append-only: correct by appending an event'); END
CREATE TRIGGER IF NOT EXISTS paper_events_no_delete BEFORE DELETE ON paper_events BEGIN SELECT RAISE(ABORT, 'paper_events is append-only: correct by appending an event'); END

    CREATE TABLE run_manifests_new_022 (
        run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
        run_uid         TEXT    NOT NULL UNIQUE,
        review_id       TEXT    NOT NULL,
        run_kind        TEXT    NOT NULL CHECK (run_kind IN ('extraction', 'screening', 'judge', 'review_session', 'import')),
        git_commit      TEXT    NOT NULL CHECK (length(git_commit) = 40),
        git_dirty       INTEGER NOT NULL CHECK (git_dirty = 0),
        git_tag         TEXT,
        engine_state    TEXT,
        spec_hash       TEXT    NOT NULL,
        codebook_hash   TEXT    NOT NULL,
        codebook_sha256 TEXT    NOT NULL,
        library_versions_json TEXT NOT NULL,
        host            TEXT    NOT NULL,
        started_at      TEXT    NOT NULL,
        ended_at        TEXT,
        end_status      TEXT    CHECK (end_status IS NULL OR end_status IN ('completed', 'failed', 'interrupted', 'aborted')),
        end_reason      TEXT,
        cloud_arms_json TEXT    NOT NULL DEFAULT '[]',
        payload_description TEXT,
        manifest_json   TEXT    NOT NULL,
        manifest_sha256 TEXT    NOT NULL,
        CHECK ((ended_at IS NULL) = (end_status IS NULL)),
        CHECK (cloud_arms_json = '[]' OR payload_description IS NOT NULL),
        -- R215/C26: a completed run carries no reason; an aborted one must.
        CHECK (end_status IS NOT 'completed' OR end_reason IS NULL),
        CHECK (end_status IS NOT 'aborted' OR end_reason IS NOT NULL)
    )
    
CREATE TRIGGER IF NOT EXISTS run_manifests_end_once BEFORE UPDATE ON run_manifests WHEN OLD.ended_at IS NOT NULL OR NEW.run_id IS NOT OLD.run_id OR NEW.run_uid IS NOT OLD.run_uid OR NEW.review_id IS NOT OLD.review_id OR NEW.run_kind IS NOT OLD.run_kind OR NEW.git_commit IS NOT OLD.git_commit OR NEW.git_dirty IS NOT OLD.git_dirty OR NEW.git_tag IS NOT OLD.git_tag OR NEW.engine_state IS NOT OLD.engine_state OR NEW.spec_hash IS NOT OLD.spec_hash OR NEW.codebook_hash IS NOT OLD.codebook_hash OR NEW.codebook_sha256 IS NOT OLD.codebook_sha256 OR NEW.library_versions_json IS NOT OLD.library_versions_json OR NEW.host IS NOT OLD.host OR NEW.started_at IS NOT OLD.started_at OR NEW.cloud_arms_json IS NOT OLD.cloud_arms_json OR NEW.payload_description IS NOT OLD.payload_description OR NEW.manifest_json IS NOT OLD.manifest_json OR NEW.manifest_sha256 IS NOT OLD.manifest_sha256 BEGIN SELECT RAISE(ABORT, 'run_manifests: a manifest is written before the first call and never edited; only its end is recorded, once'); END
CREATE TRIGGER IF NOT EXISTS run_manifests_no_delete BEFORE DELETE ON run_manifests BEGIN SELECT RAISE(ABORT, 'run_manifests: a run record is never deleted'); END

    CREATE TABLE run_calls_new_022 (
        call_id         INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id          INTEGER NOT NULL REFERENCES run_manifests(run_id),
        stage           TEXT    NOT NULL,
        paper_id        INTEGER REFERENCES papers(id),
        request_hash    TEXT    NOT NULL,
        response_digest TEXT,
        started_at      TEXT    NOT NULL,
        ended_at        TEXT    NOT NULL,
        outcome         TEXT    NOT NULL CHECK (outcome IN ('completed', 'refused_input_overflow', 'refused_ceiling_unavailable', 'refused_input_truncated', 'refused_input_dropped', 'error')),
        outcome_detail  TEXT,
        FOREIGN KEY (run_id, stage) REFERENCES run_stage_configs(run_id, stage)
    )
    
CREATE TRIGGER IF NOT EXISTS run_calls_no_update BEFORE UPDATE ON run_calls BEGIN SELECT RAISE(ABORT, 'run_calls is a record: one row per call, never edited'); END
CREATE TRIGGER IF NOT EXISTS run_calls_no_delete BEFORE DELETE ON run_calls BEGIN SELECT RAISE(ABORT, 'run_calls is a record: one row per call, never edited'); END

CREATE TABLE claim_inputs (
    extraction_uid     TEXT    PRIMARY KEY,
    arm                TEXT    NOT NULL REFERENCES arms(arm_name),
    paper_id           INTEGER NOT NULL REFERENCES papers(id),
    reuse_key          TEXT    NOT NULL,
    parsed_text_sha256 TEXT    NOT NULL,
    parsed_text_uid    TEXT    NOT NULL,
    run_id             INTEGER NOT NULL REFERENCES run_manifests(run_id),
    recorded_at        TEXT    NOT NULL
)

CREATE TRIGGER IF NOT EXISTS claim_inputs_no_update BEFORE UPDATE ON claim_inputs BEGIN SELECT RAISE(ABORT, 'claim_inputs is a record: one row per extraction call, never edited'); END
CREATE TRIGGER IF NOT EXISTS claim_inputs_no_delete BEFORE DELETE ON claim_inputs BEGIN SELECT RAISE(ABORT, 'claim_inputs is a record: one row per extraction call, never edited'); END

    CREATE TABLE audit_verdicts (
        verdict_id      INTEGER PRIMARY KEY AUTOINCREMENT,
        schema          TEXT    NOT NULL,
        run_id          INTEGER NOT NULL REFERENCES run_manifests(run_id),
        paper_id        INTEGER NOT NULL REFERENCES papers(id),
        claim_id        TEXT    NOT NULL,
        field_name      TEXT    NOT NULL,
        arm             TEXT    NOT NULL,
        auditor_model   TEXT    NOT NULL,
        auditor_digest  TEXT    NOT NULL,
        verdict         TEXT    NOT NULL CHECK (verdict IN ('verified', 'flagged')),
        rationale       TEXT,
        occurred_at     TEXT    NOT NULL
    )
    
CREATE TRIGGER IF NOT EXISTS audit_verdicts_no_update BEFORE UPDATE ON audit_verdicts BEGIN SELECT RAISE(ABORT, 'audit_verdicts is a record: one row per verdict, never edited'); END
CREATE TRIGGER IF NOT EXISTS audit_verdicts_no_delete BEFORE DELETE ON audit_verdicts BEGIN SELECT RAISE(ABORT, 'audit_verdicts is a record: one row per verdict, never edited'); END
CREATE INDEX IF NOT EXISTS idx_paper_events_paper ON paper_events(paper_id, event_id)
CREATE INDEX IF NOT EXISTS idx_run_calls_run ON run_calls(run_id, stage)
CREATE INDEX IF NOT EXISTS idx_claim_inputs_reuse ON claim_inputs(arm, paper_id, reuse_key)
CREATE INDEX IF NOT EXISTS idx_audit_verdicts_run_paper ON audit_verdicts(run_id, paper_id)
```

## B10 — per-table read-back, verbatim (rehearsal-after vs source copy)
```
### schema_migrations
count 21
cols ['migration_id', 'file_sha256', 'applied_at', 'mode', 'runner_version', 'note']
022 row [('022_run_kinds_and_audit_tables', 'e0913e33d309bd6c9941b702e3d440802a1461af0c3433037e47c9060c037e78', '2026-09-29T16:41:46.524825+00:00', 'executed', 1, None)]
source count 20
other 20 receipts equal to source: True
022 file sha256 e0913e33d309bd6c9941b702e3d440802a1461af0c3433037e47c9060c037e78

### paper_events
rows after/source 190 190
all rows equal: True cols equal: True
CREATE after:
CREATE TABLE "paper_events" (
    event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    event_uid   TEXT    NOT NULL UNIQUE,
    event_type  TEXT    NOT NULL,
    occurred_at TEXT    NOT NULL,
    recorded_at TEXT    NOT NULL,
    actor_kind  TEXT    NOT NULL CHECK (actor_kind IN ('model', 'human', 'engine')),
    actor_role  TEXT    NOT NULL CHECK (actor_role IN ('reviewer', 'extractor', 'system')),
    actor_name  TEXT    NOT NULL,
    actor_digest TEXT,
    run_id      INTEGER REFERENCES run_manifests(run_id),
    run_marker  TEXT,
    prior_event_id INTEGER REFERENCES "paper_events"(event_id),
    presented_context_sha256 TEXT,
    reason      TEXT,
    payload_json TEXT   NOT NULL DEFAULT '{}',
        paper_id    INTEGER NOT NULL REFERENCES papers(id),
        to_state    TEXT    NOT NULL CHECK (to_state IN ('eligible', 'abstract_out', 'full_text_out', 'parsed', 'extracted', 'extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context', 'audited_ai')),
        from_state  TEXT,
        reason_code TEXT,
        stage_name  TEXT,
        CHECK (actor_role <> 'system' OR actor_kind = 'engine'),
        CHECK (event_type IN ('screened', 'verified', 'adjudicated', 'acquired', 'not_obtainable', 'parsed', 'extracted', 'extraction_failed', 'audited', 'manual_advance', 'bypass', 'state_at_migration')),
        -- R39: an event's type and its to_state must name the same axis.
        CHECK (
            (event_type IN ('screened', 'verified', 'adjudicated') AND to_state IN ('eligible', 'abstract_out', 'full_text_out'))
         OR (event_type IN ('acquired', 'not_obtainable', 'parsed', 'extracted', 'extraction_failed', 'audited') AND to_state IN ('parsed', 'extracted', 'extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context', 'audited_ai'))
         OR (event_type IN ('manual_advance', 'bypass', 'state_at_migration'))
        ),
        -- R39 / S3h: a reason for exactly the failure tokens, and for no other.
        CHECK (
            (to_state IN ('extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context') AND reason_code IS NOT NULL)
         OR (to_state NOT IN ('extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context') AND reason_code IS NULL)
        ),
        -- R68: a row with no run is a seeded, pre-manifest row and nothing else.
        CHECK (
            (run_id IS NOT NULL AND run_marker IS NULL)
            OR
            (run_id IS NULL AND run_marker IS 'pre-manifest')
        )
    )
CREATE source:
CREATE TABLE "paper_events" (
    event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    event_uid   TEXT    NOT NULL UNIQUE,
    event_type  TEXT    NOT NULL,
    occurred_at TEXT    NOT NULL,
    recorded_at TEXT    NOT NULL,
    actor_kind  TEXT    NOT NULL CHECK (actor_kind IN ('model', 'human', 'engine')),
    actor_role  TEXT    NOT NULL CHECK (actor_role IN ('reviewer', 'extractor', 'system')),
    actor_name  TEXT    NOT NULL,
    actor_digest TEXT,
    run_id      INTEGER REFERENCES run_manifests(run_id),
    run_marker  TEXT,
    prior_event_id INTEGER REFERENCES "paper_events"(event_id),
    presented_context_sha256 TEXT,
    reason      TEXT,
    payload_json TEXT   NOT NULL DEFAULT '{}',
        paper_id    INTEGER NOT NULL REFERENCES papers(id),
        to_state    TEXT    NOT NULL CHECK (to_state IN ('eligible', 'abstract_out', 'full_text_out', 'parsed', 'extracted', 'extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context', 'audited_ai')),
        from_state  TEXT,
        reason_code TEXT,
        stage_name  TEXT,
        CHECK (actor_role <> 'system' OR actor_kind = 'engine'),
        CHECK (event_type IN ('identified', 'duplicate_of', 'screened', 'verified', 'adjudicated', 'acquired', 'not_obtainable', 'parsed', 'extracted', 'extraction_failed', 'audited', 'manual_advance', 'bypass', 'state_at_migration')),
        -- R39: an event's type and its to_state must name the same axis.
        CHECK (
            (event_type IN ('identified', 'duplicate_of', 'screened', 'verified', 'adjudicated') AND to_state IN ('eligible', 'abstract_out', 'full_text_out'))
         OR (event_type IN ('acquired', 'not_obtainable', 'parsed', 'extracted', 'extraction_failed', 'audited') AND to_state IN ('parsed', 'extracted', 'extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context', 'audited_ai'))
         OR (event_type IN ('manual_advance', 'bypass', 'state_at_migration'))
        ),
        -- R39 / S3h: a reason for exactly the failure tokens, and for no other.
        CHECK (
            (to_state IN ('extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context') AND reason_code IS NOT NULL)
         OR (to_state NOT IN ('extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context') AND reason_code IS NULL)
        ),
        -- R68: a row with no run is a seeded, pre-manifest row and nothing else.
        CHECK (
            (run_id IS NOT NULL AND run_marker IS NULL)
            OR
            (run_id IS NULL AND run_marker IS 'pre-manifest')
        )
    )
'identified' in after: False  'duplicate_of' in after: False
CHECK count after/source 8 8
 equal | CHECK (actor_kind IN ('model', 'human', 'engine'))
 equal | CHECK (actor_role IN ('reviewer', 'extractor', 'system'))
 equal | CHECK (to_state IN ('eligible', 'abstract_out', 'full_text_out', 'parsed', 'extracted', 'extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context', 'audited_ai'))
 equal | CHECK (actor_role <> 'system' OR actor_kind = 'engine')
 DIFF | after=CHECK (event_type IN ('screened', 'verified', 'adjudicated', 'acquired', 'not_obtainable', 'parsed', 'extracted', 'extraction_failed', 'audited', 'manual_advance', 'bypass', 'state_at_migration'))
        source=CHECK (event_type IN ('identified', 'duplicate_of', 'screened', 'verified', 'adjudicated', 'acquired', 'not_obtainable', 'parsed', 'extracted', 'extraction_failed', 'audited', 'manual_advance', 'bypass', 'state_at_migration'))
 DIFF | after=CHECK ( (event_type IN ('screened', 'verified', 'adjudicated') AND to_state IN ('eligible', 'abstract_out', 'full_text_out')) OR (event_type IN ('acquired', 'not_obtainable', 'parsed', 'extracted', 'extraction_failed', 'audited') AND to_state IN ('parsed', 'extracted', 'extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context', 'audited_ai')) OR (event_type IN ('manual_advance', 'bypass', 'state_at_migration')) )
        source=CHECK ( (event_type IN ('identified', 'duplicate_of', 'screened', 'verified', 'adjudicated') AND to_state IN ('eligible', 'abstract_out', 'full_text_out')) OR (event_type IN ('acquired', 'not_obtainable', 'parsed', 'extracted', 'extraction_failed', 'audited') AND to_state IN ('parsed', 'extracted', 'extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context', 'audited_ai')) OR (event_type IN ('manual_advance', 'bypass', 'state_at_migration')) )
 equal | CHECK ( (to_state IN ('extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context') AND reason_code IS NOT NULL) OR (to_state NOT IN ('extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context') AND reason_code IS NULL) )
 equal | CHECK ( (run_id IS NOT NULL AND run_marker IS NULL) OR (run_id IS NULL AND run_marker IS 'pre-manifest') )
prior_event_id FK (pragma): [(0, 0, 'papers', 'paper_id', 'id', 'NO ACTION', 'NO ACTION', 'NONE'), (1, 0, 'paper_events', 'prior_event_id', 'event_id', 'NO ACTION', 'NO ACTION', 'NONE'), (2, 0, 'run_manifests', 'run_id', 'run_id', 'NO ACTION', 'NO ACTION', 'NONE')]
prior_event_id stored text: ['prior_event_id INTEGER REFERENCES "paper_events"(event_id),']
source prior_event_id text: ['prior_event_id INTEGER REFERENCES "paper_events"(event_id),']
triggers/indexes byte-equal to source: True
  ('index', 'idx_paper_events_paper', 'CREATE INDEX idx_paper_events_paper ON paper_events(paper_id, event_id)')
  ('index', 'sqlite_autoindex_paper_events_1', None)
  ('trigger', 'paper_events_no_delete', "CREATE TRIGGER paper_events_no_delete BEFORE DELETE ON paper_events BEGIN SELECT RAISE(ABORT, 'paper_events is append-only: correct by appending an event'); END")
  ('trigger', 'paper_events_no_update', "CREATE TRIGGER paper_events_no_update BEFORE UPDATE ON paper_events BEGIN SELECT RAISE(ABORT, 'paper_events is append-only: correct by appending an event'); END")
sqlite_sequence paper_events after/source [(190,)] [(190,)]

### run_manifests
rows 0
CREATE after:
CREATE TABLE "run_manifests" (
        run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
        run_uid         TEXT    NOT NULL UNIQUE,
        review_id       TEXT    NOT NULL,
        run_kind        TEXT    NOT NULL CHECK (run_kind IN ('extraction', 'screening', 'judge', 'review_session', 'import')),
        git_commit      TEXT    NOT NULL CHECK (length(git_commit) = 40),
        git_dirty       INTEGER NOT NULL CHECK (git_dirty = 0),
        git_tag         TEXT,
        engine_state    TEXT,
        spec_hash       TEXT    NOT NULL,
        codebook_hash   TEXT    NOT NULL,
        codebook_sha256 TEXT    NOT NULL,
        library_versions_json TEXT NOT NULL,
        host            TEXT    NOT NULL,
        started_at      TEXT    NOT NULL,
        ended_at        TEXT,
        end_status      TEXT    CHECK (end_status IS NULL OR end_status IN ('completed', 'failed', 'interrupted', 'aborted')),
        end_reason      TEXT,
        cloud_arms_json TEXT    NOT NULL DEFAULT '[]',
        payload_description TEXT,
        manifest_json   TEXT    NOT NULL,
        manifest_sha256 TEXT    NOT NULL,
        CHECK ((ended_at IS NULL) = (end_status IS NULL)),
        CHECK (cloud_arms_json = '[]' OR payload_description IS NOT NULL),
        -- R215/C26: a completed run carries no reason; an aborted one must.
        CHECK (end_status IS NOT 'completed' OR end_reason IS NULL),
        CHECK (end_status IS NOT 'aborted' OR end_reason IS NOT NULL)
    )
CREATE byte-equal to source: False
CREATE source:
CREATE TABLE run_manifests (
    run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_uid         TEXT    NOT NULL UNIQUE,
    review_id       TEXT    NOT NULL,
    run_kind        TEXT    NOT NULL CHECK (run_kind IN ('extraction', 'screening', 'judge', 'review_session')),
    git_commit      TEXT    NOT NULL CHECK (length(git_commit) = 40),
    git_dirty       INTEGER NOT NULL CHECK (git_dirty = 0),
    git_tag         TEXT,
    engine_state    TEXT,
    spec_hash       TEXT    NOT NULL,
    codebook_hash   TEXT    NOT NULL,
    codebook_sha256 TEXT    NOT NULL,
    library_versions_json TEXT NOT NULL,
    host            TEXT    NOT NULL,
    started_at      TEXT    NOT NULL,
    ended_at        TEXT,
    end_status      TEXT    CHECK (end_status IS NULL OR end_status IN ('completed', 'failed', 'interrupted')),
    cloud_arms_json TEXT    NOT NULL DEFAULT '[]',
    payload_description TEXT,
    manifest_json   TEXT    NOT NULL,
    manifest_sha256 TEXT    NOT NULL,
    CHECK ((ended_at IS NULL) = (end_status IS NULL)),
    CHECK (cloud_arms_json = '[]' OR payload_description IS NOT NULL)
)
columns: [('run_id', 'INTEGER', 0), ('run_uid', 'TEXT', 1), ('review_id', 'TEXT', 1), ('run_kind', 'TEXT', 1), ('git_commit', 'TEXT', 1), ('git_dirty', 'INTEGER', 1), ('git_tag', 'TEXT', 0), ('engine_state', 'TEXT', 0), ('spec_hash', 'TEXT', 1), ('codebook_hash', 'TEXT', 1), ('codebook_sha256', 'TEXT', 1), ('library_versions_json', 'TEXT', 1), ('host', 'TEXT', 1), ('started_at', 'TEXT', 1), ('ended_at', 'TEXT', 0), ('end_status', 'TEXT', 0), ('end_reason', 'TEXT', 0), ('cloud_arms_json', 'TEXT', 1), ('payload_description', 'TEXT', 0), ('manifest_json', 'TEXT', 1), ('manifest_sha256', 'TEXT', 1)]
triggers/indexes:
  ('index', 'sqlite_autoindex_run_manifests_1', None)
  ('trigger', 'run_manifests_end_once', "CREATE TRIGGER run_manifests_end_once BEFORE UPDATE ON run_manifests WHEN OLD.ended_at IS NOT NULL OR NEW.run_id IS NOT OLD.run_id OR NEW.run_uid IS NOT OLD.run_uid OR NEW.review_id IS NOT OLD.review_id OR NEW.run_kind IS NOT OLD.run_kind OR NEW.git_commit IS NOT OLD.git_commit OR NEW.git_dirty IS NOT OLD.git_dirty OR NEW.git_tag IS NOT OLD.git_tag OR NEW.engine_state IS NOT OLD.engine_state OR NEW.spec_hash IS NOT OLD.spec_hash OR NEW.codebook_hash IS NOT OLD.codebook_hash OR NEW.codebook_sha256 IS NOT OLD.codebook_sha256 OR NEW.library_versions_json IS NOT OLD.library_versions_json OR NEW.host IS NOT OLD.host OR NEW.started_at IS NOT OLD.started_at OR NEW.cloud_arms_json IS NOT OLD.cloud_arms_json OR NEW.payload_description IS NOT OLD.payload_description OR NEW.manifest_json IS NOT OLD.manifest_json OR NEW.manifest_sha256 IS NOT OLD.manifest_sha256 BEGIN SELECT RAISE(ABORT, 'run_manifests: a manifest is written before the first call and never edited; only its end is recorded, once'); END")
  ('trigger', 'run_manifests_no_delete', "CREATE TRIGGER run_manifests_no_delete BEFORE DELETE ON run_manifests BEGIN SELECT RAISE(ABORT, 'run_manifests: a run record is never deleted'); END")
triggers/indexes byte-equal to source: True
fk: []

### run_stage_configs
rows 0
CREATE after:
CREATE TABLE run_stage_configs (
    run_id          INTEGER NOT NULL REFERENCES run_manifests(run_id),
    stage           TEXT    NOT NULL,
    stage_kind      TEXT    NOT NULL CHECK (stage_kind IN ('abstract_screen_primary', 'abstract_screen_verifier', 'ft_screen_primary', 'ft_screen_verifier', 'audit', 'extract_pass1', 'extract_pass2', 'extract_retry_snippet', 'elicitation_pass1', 'vision_parse', 'pdf_quality', 'preflight', 'cloud')),
    arm_name        TEXT    REFERENCES arms(arm_name),
    provider        TEXT    NOT NULL CHECK (provider IN ('ollama', 'openai', 'anthropic')),
    model_name      TEXT    NOT NULL,
    model_digest    TEXT,
    options_json    TEXT    NOT NULL,
    options_hash    TEXT    NOT NULL,
    sent_keys_json  TEXT    NOT NULL,
    sources_json    TEXT    NOT NULL,
    keep_alive      TEXT    NOT NULL,
    format_schema_hash TEXT NOT NULL,
    prompt_hash     TEXT    NOT NULL,
    PRIMARY KEY (run_id, stage),
    -- R78: NULL-safe. `length(NULL) = 64` is NULL, which a CHECK passes, so the
    -- digest's presence is tested with IS NOT NULL before its length.
    CHECK (provider IS NOT 'ollama' OR (model_digest IS NOT NULL AND length(model_digest) = 64))
)
CREATE byte-equal to source: True
columns: [('run_id', 'INTEGER', 1), ('stage', 'TEXT', 1), ('stage_kind', 'TEXT', 1), ('arm_name', 'TEXT', 0), ('provider', 'TEXT', 1), ('model_name', 'TEXT', 1), ('model_digest', 'TEXT', 0), ('options_json', 'TEXT', 1), ('options_hash', 'TEXT', 1), ('sent_keys_json', 'TEXT', 1), ('sources_json', 'TEXT', 1), ('keep_alive', 'TEXT', 1), ('format_schema_hash', 'TEXT', 1), ('prompt_hash', 'TEXT', 1)]
triggers/indexes:
  ('index', 'sqlite_autoindex_run_stage_configs_1', None)
  ('trigger', 'run_stage_configs_no_delete', "CREATE TRIGGER run_stage_configs_no_delete BEFORE DELETE ON run_stage_configs BEGIN SELECT RAISE(ABORT, 'run_stage_configs is a record: the configuration of a stage is written with its manifest'); END")
  ('trigger', 'run_stage_configs_no_update', "CREATE TRIGGER run_stage_configs_no_update BEFORE UPDATE ON run_stage_configs BEGIN SELECT RAISE(ABORT, 'run_stage_configs is a record: the configuration of a stage is written with its manifest'); END")
triggers/indexes byte-equal to source: True
fk: [(0, 0, 'arms', 'arm_name', 'arm_name', 'NO ACTION', 'NO ACTION', 'NONE'), (1, 0, 'run_manifests', 'run_id', 'run_id', 'NO ACTION', 'NO ACTION', 'NONE')]

### run_calls
rows 0
CREATE after:
CREATE TABLE "run_calls" (
        call_id         INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id          INTEGER NOT NULL REFERENCES run_manifests(run_id),
        stage           TEXT    NOT NULL,
        paper_id        INTEGER REFERENCES papers(id),
        request_hash    TEXT    NOT NULL,
        response_digest TEXT,
        started_at      TEXT    NOT NULL,
        ended_at        TEXT    NOT NULL,
        outcome         TEXT    NOT NULL CHECK (outcome IN ('completed', 'refused_input_overflow', 'refused_ceiling_unavailable', 'refused_input_truncated', 'refused_input_dropped', 'error')),
        outcome_detail  TEXT,
        FOREIGN KEY (run_id, stage) REFERENCES run_stage_configs(run_id, stage)
    )
CREATE byte-equal to source: False
CREATE source:
CREATE TABLE run_calls (
    call_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          INTEGER NOT NULL REFERENCES run_manifests(run_id),
    stage           TEXT    NOT NULL,
    paper_id        INTEGER REFERENCES papers(id),
    request_hash    TEXT    NOT NULL,
    response_digest TEXT,
    started_at      TEXT    NOT NULL,
    ended_at        TEXT    NOT NULL,
    FOREIGN KEY (run_id, stage) REFERENCES run_stage_configs(run_id, stage)
)
columns: [('call_id', 'INTEGER', 0), ('run_id', 'INTEGER', 1), ('stage', 'TEXT', 1), ('paper_id', 'INTEGER', 0), ('request_hash', 'TEXT', 1), ('response_digest', 'TEXT', 0), ('started_at', 'TEXT', 1), ('ended_at', 'TEXT', 1), ('outcome', 'TEXT', 1), ('outcome_detail', 'TEXT', 0)]
triggers/indexes:
  ('index', 'idx_run_calls_run', 'CREATE INDEX idx_run_calls_run ON run_calls(run_id, stage)')
  ('trigger', 'run_calls_no_delete', "CREATE TRIGGER run_calls_no_delete BEFORE DELETE ON run_calls BEGIN SELECT RAISE(ABORT, 'run_calls is a record: one row per call, never edited'); END")
  ('trigger', 'run_calls_no_update', "CREATE TRIGGER run_calls_no_update BEFORE UPDATE ON run_calls BEGIN SELECT RAISE(ABORT, 'run_calls is a record: one row per call, never edited'); END")
triggers/indexes byte-equal to source: True
fk: [(0, 0, 'run_stage_configs', 'run_id', 'run_id', 'NO ACTION', 'NO ACTION', 'NONE'), (0, 1, 'run_stage_configs', 'stage', 'stage', 'NO ACTION', 'NO ACTION', 'NONE'), (1, 0, 'papers', 'paper_id', 'id', 'NO ACTION', 'NO ACTION', 'NONE'), (2, 0, 'run_manifests', 'run_id', 'run_id', 'NO ACTION', 'NO ACTION', 'NONE')]

### claim_inputs
rows 0
CREATE after:
CREATE TABLE claim_inputs (
    extraction_uid     TEXT    PRIMARY KEY,
    arm                TEXT    NOT NULL REFERENCES arms(arm_name),
    paper_id           INTEGER NOT NULL REFERENCES papers(id),
    reuse_key          TEXT    NOT NULL,
    parsed_text_sha256 TEXT    NOT NULL,
    parsed_text_uid    TEXT    NOT NULL,
    run_id             INTEGER NOT NULL REFERENCES run_manifests(run_id),
    recorded_at        TEXT    NOT NULL
)
CREATE byte-equal to source: absent in source
columns: [('extraction_uid', 'TEXT', 0), ('arm', 'TEXT', 1), ('paper_id', 'INTEGER', 1), ('reuse_key', 'TEXT', 1), ('parsed_text_sha256', 'TEXT', 1), ('parsed_text_uid', 'TEXT', 1), ('run_id', 'INTEGER', 1), ('recorded_at', 'TEXT', 1)]
triggers/indexes:
  ('index', 'idx_claim_inputs_reuse', 'CREATE INDEX idx_claim_inputs_reuse ON claim_inputs(arm, paper_id, reuse_key)')
  ('index', 'sqlite_autoindex_claim_inputs_1', None)
  ('trigger', 'claim_inputs_no_delete', "CREATE TRIGGER claim_inputs_no_delete BEFORE DELETE ON claim_inputs BEGIN SELECT RAISE(ABORT, 'claim_inputs is a record: one row per extraction call, never edited'); END")
  ('trigger', 'claim_inputs_no_update', "CREATE TRIGGER claim_inputs_no_update BEFORE UPDATE ON claim_inputs BEGIN SELECT RAISE(ABORT, 'claim_inputs is a record: one row per extraction call, never edited'); END")
fk: [(0, 0, 'run_manifests', 'run_id', 'run_id', 'NO ACTION', 'NO ACTION', 'NONE'), (1, 0, 'papers', 'paper_id', 'id', 'NO ACTION', 'NO ACTION', 'NONE'), (2, 0, 'arms', 'arm', 'arm_name', 'NO ACTION', 'NO ACTION', 'NONE')]

### audit_verdicts
rows 0
CREATE after:
CREATE TABLE audit_verdicts (
        verdict_id      INTEGER PRIMARY KEY AUTOINCREMENT,
        schema          TEXT    NOT NULL,
        run_id          INTEGER NOT NULL REFERENCES run_manifests(run_id),
        paper_id        INTEGER NOT NULL REFERENCES papers(id),
        claim_id        TEXT    NOT NULL,
        field_name      TEXT    NOT NULL,
        arm             TEXT    NOT NULL,
        auditor_model   TEXT    NOT NULL,
        auditor_digest  TEXT    NOT NULL,
        verdict         TEXT    NOT NULL CHECK (verdict IN ('verified', 'flagged')),
        rationale       TEXT,
        occurred_at     TEXT    NOT NULL
    )
CREATE byte-equal to source: absent in source
columns: [('verdict_id', 'INTEGER', 0), ('schema', 'TEXT', 1), ('run_id', 'INTEGER', 1), ('paper_id', 'INTEGER', 1), ('claim_id', 'TEXT', 1), ('field_name', 'TEXT', 1), ('arm', 'TEXT', 1), ('auditor_model', 'TEXT', 1), ('auditor_digest', 'TEXT', 1), ('verdict', 'TEXT', 1), ('rationale', 'TEXT', 0), ('occurred_at', 'TEXT', 1)]
triggers/indexes:
  ('index', 'idx_audit_verdicts_run_paper', 'CREATE INDEX idx_audit_verdicts_run_paper ON audit_verdicts(run_id, paper_id)')
  ('trigger', 'audit_verdicts_no_delete', "CREATE TRIGGER audit_verdicts_no_delete BEFORE DELETE ON audit_verdicts BEGIN SELECT RAISE(ABORT, 'audit_verdicts is a record: one row per verdict, never edited'); END")
  ('trigger', 'audit_verdicts_no_update', "CREATE TRIGGER audit_verdicts_no_update BEFORE UPDATE ON audit_verdicts BEGIN SELECT RAISE(ABORT, 'audit_verdicts is a record: one row per verdict, never edited'); END")
fk: [(0, 0, 'papers', 'paper_id', 'id', 'NO ACTION', 'NO ACTION', 'NONE'), (1, 0, 'run_manifests', 'run_id', 'run_id', 'NO ACTION', 'NO ACTION', 'NONE')]

audit_verdicts cols 12 ['verdict_id', 'schema', 'run_id', 'paper_id', 'claim_id', 'field_name', 'arm', 'auditor_model', 'auditor_digest', 'verdict', 'rationale', 'occurred_at']
cols[1:] == FIELDS (name+order): True FIELDS len 11

### sqlite_sequence
after: [('fabrication_verifications', 7431), ('field_events', 0), ('judge_pair_ratings', 6831), ('judge_ratings', 2277), ('judge_run_audit', 1), ('paper_events', 190), ('provenance_classifications', 22034), ('run_calls', 0), ('run_manifests', 0)]
source: [('fabrication_verifications', 7431), ('field_events', 0), ('judge_pair_ratings', 6831), ('judge_ratings', 2277), ('judge_run_audit', 1), ('paper_events', 190), ('provenance_classifications', 22034)]

### out-of-scope tables
fp tables key type dict
```

## Findings (no stop condition met)
1. `sqlite_sequence` gains rows `('run_calls', 0)` and `('run_manifests', 0)`, created by 022's zero-row `INSERT … SELECT` into AUTOINCREMENT tables. The fresh build carries the same two rows, and 020 left `('field_events', 0)` by the same mechanism. Every other row is unchanged, including `paper_events` 190. Both rows name 022-scope tables, and live-after should expect them.
2. J2: the stated mechanism is imprecise. `run_manifests_end_once` fires when `OLD.ended_at IS NOT NULL` (so **any** UPDATE to a closed run is refused, including one touching only `end_reason`) or when any `_MANIFEST_BODY` column changes. `end_reason` passes on the one permitted close because it is absent from `_MANIFEST_BODY`, alongside `end_status` and `ended_at`. The T5 behaviour holds.
3. CHECK gap (R79 family, architect-ruling candidate): nothing ties `end_reason` to a closed run. An OPEN run (end_status NULL, ended_at NULL) accepts `end_reason = 'x'`, because `end_status IS NOT 'completed'` and `end_status IS NOT 'aborted'` are both TRUE when `end_status` is NULL. Measured on a throwaway copy of the fresh DB. No writer does this today (`close_run` sets all three together).
4. Out-of-scope table count is 28, not the brief's expected 27 (architect-count candidate). 36 tables in all − 7 named in B10 − sqlite_sequence = 28. All 28 are equal.
5. J1: the stored text reads `prior_event_id INTEGER REFERENCES "paper_events"(event_id),`, byte-equal to the source copy's (020 left the same quoted form). Not a textual difference.
6. `audit_verdicts.arm` carries no FK to `arms` (ruling 3: a Step 2 row candidate, no action).
7. Ruling 1 candidate row for the 10b closeout: the five 022 postconditions no test pins (the single transaction; run_stage_configs' CREATE text; paper_events' other CHECKs and trigger text; audit_verdicts' columns against FIELDS; the 36-table count). Owner: session 12.
