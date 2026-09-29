# MIGRATION-02 10b-P3ii — the live write of migration 022

2026-09-29. HEAD `3ce826582b5cc4e6b705349fea61254aadb3cfcd` at the write. Live: `data/surgical_autonomy/review.db`.

## Pre-flight
- P1 clock 17:20:43 UTC. I1: crontab — 09:00 nightly_tests, 13:30 morning_digest, 00/06/12/18:00 service_health_check, 07:00 ollama_health_check, 09:30 citation-mcp; timers — dgx-snapshot-* 10:30/10:45/11:16, backup-monitor 12:30; anacron 17:34 runs /etc/cron.{daily,weekly,monthly} (0anacron apport apt-compat dpkg etckeeper logrotate man-db quota sysstat), none referencing review.db or the tree. Write at 17:21:15 UTC: outside 07:00–10:35 and 39 min before 18:00.
- P2 HEAD 3ce8265, level with origin; untracked only the two rehearsal2_* records (committed here).
- P3 live --compare vs input-identity-01/review_db_fingerprint_20260924T205225Z.json: IDENTICAL, 34 tables, overall ea05912d…ce01, -wal 0 B; 20 receipts, last 021_parsed_text_sha256.
- P4/P6 lsof and fuser on review.db, -wal, -shm: empty (rc=1) at 17:21:04 and immediately before W1.
- P5 pre-write backup (FOURTH RETAINED RESTORE POINT, to the freshman freeze, session 12): `data/surgical_autonomy/review.db.bak-migration-02-phase3-pre-write-20260929-172104`, 34 tables, overall ea05912d67f0b841bf003a7a941f6505e2c57140b5c2f62e1ff446ea16b9ce01, file sha256 4abb5f4d7412576f5d4ebfef68335297117e7f7f1c6aee18ebffdaf103556302. Record `prewrite_db_fingerprint_20260929T172105Z.json`.

## W1 — the write
`python -m engine.migrations data/surgical_autonomy/review.db --apply-pending` — start 2026-09-29T17:21:15.277654053Z, end 17:21:15.339125965Z, exit 0.
stdout:
```
{'executed': ['022_run_kinds_and_audit_tables'], 'skipped': [], 'already': ['002_screening_rename', '003_backfill_expanded_screening', '004_pdf_quality_check', '005_model_digest', '006_not_null_confidence_tier', '007_add_judge_tables', '008_add_fabrication_verifications', '009_add_backfill_audit_log', '010_add_provenance_classifications', '011_add_absence_claim_class', '012_codebook_provenance', '013_drop_schema_hash_not_null', '014_cloud_tables', '015_drop_prerename_adjudication_indices', '016_event_store', '017_seed_event_store', '018_cloud_shape_and_audit_adjudication', '019_paper_state_axes', '020_run_manifest', '021_parsed_text_sha256']}
```
stderr: (empty)

## W2 — idempotence
Same command — start 17:21:15.342349460Z, end 17:21:15.379819303Z, exit 0.
stdout:
```
{'executed': [], 'skipped': [], 'already': ['002_screening_rename', '003_backfill_expanded_screening', '004_pdf_quality_check', '005_model_digest', '006_not_null_confidence_tier', '007_add_judge_tables', '008_add_fabrication_verifications', '009_add_backfill_audit_log', '010_add_provenance_classifications', '011_add_absence_claim_class', '012_codebook_provenance', '013_drop_schema_hash_not_null', '014_cloud_tables', '015_drop_prerename_adjudication_indices', '016_event_store', '017_seed_event_store', '018_cloud_shape_and_audit_adjudication', '019_paper_state_axes', '020_run_manifest', '021_parsed_text_sha256', '022_run_kinds_and_audit_tables']}
```
stderr: (empty)

## W3 — new record of reference `review_db_fingerprint_20260929T172123Z.json`
36 tables; structure b0dffa3fdd7ff57a4aa6910181c1b02269b9478feee0d33c8d14744ecb1f57aa (== rehearsal 2); textual f8d6388751e91a08e5d8580dc2de44a4be965e1dfd628a133950f8ed7d153cee (== rehearsal 2, G3 ii); overall **0d3eedead60c2ce1c6341b973fa5931d69c4b13c14e93bd5b805bc06734fcb25** (≠ rehearsal 2's 3c2f9be4…0a71 — the 022 receipt's applied_at is the only cause, W5); -wal/-shm absent after W1 (the CLI's connection closed last and removed them).

## W4 — G3 (i)
Fresh ReviewDatabase in ~/scratch/p3ii-fresh: structure b0dffa3f…57aa == live; structure_differences(fresh, live) = 0.

## W5 — per-table acceptance, live vs rehearsal 2 (mode=ro)
```
sqlite_master entries live/reh2: 114 114 all byte-equal: True
tables live/reh2: 36 36

| table | rows live | rows reh2 | content equal to reh2 |
| abstract_screening_adjudication | 0 | 0 | True |
| abstract_screening_decisions | 21374 | 21374 | True |
| abstract_verification_decisions | 1422 | 1422 | True |
| arms | 3 | 3 | True |
| audit_verdicts | 0 | 0 | True |
| claim_inputs | 0 | 0 | True |
| cloud_evidence_spans | 7257 | 7257 | True |
| cloud_extractions | 379 | 379 | True |
| evidence_spans | 3760 | 3760 | True |
| extractions | 190 | 190 | True |
| fabrication_verifications | 7422 | 7422 | True |
| field_event_against | 0 | 0 | True |
| field_event_against_decisions | 0 | 0 | True |
| field_events | 0 | 0 | True |
| ft_screening_adjudication | 36 | 36 | True |
| ft_screening_decisions | 366 | 366 | True |
| ft_verification_decisions | 182 | 182 | True |
| full_text_assets | 794 | 794 | True |
| judge_pair_ratings | 6828 | 6828 | True |
| judge_ratings | 2276 | 2276 | True |
| judge_run_audit | 1 | 1 | True |
| judge_runs | 7 | 7 | True |
| paper_events | 190 | 190 | True |
| papers | 10039 | 10039 | True |
| parse_attempts | 8 | 8 | True |
| parsed_text_refs | 194 | 194 | True |
| provenance_census_runs | 2 | 2 | True |
| provenance_classifications | 22034 | 22034 | True |
| review_identities | 3 | 3 | True |
| review_runs | 6 | 6 | True |
| run_calls | 0 | 0 | True |
| run_manifests | 0 | 0 | True |
| run_stage_configs | 0 | 0 | True |
| schema_migrations | 21 | 21 | False |
| sqlite_sequence | 9 | 9 | True |
| workflow_state | 12 | 12 | True |

equal to reh2: 35 of 36 ; differing: ['schema_migrations']
schema_migrations rows live/reh2: 21 21
live 022 row: ('022_run_kinds_and_audit_tables', 'da1f31c82db6426efd634845f5573f39c75530677037c408a539b010f9ada67b', '2026-09-29T17:21:15.324656+00:00', 'executed', 1, None)
reh2 022 row: ('022_run_kinds_and_audit_tables', 'da1f31c82db6426efd634845f5573f39c75530677037c408a539b010f9ada67b', '2026-09-29T17:12:48.602748+00:00', 'executed', 1, None)
first 20 equal: True
022 row columns differing: ['applied_at']
sqlite_sequence live: [('fabrication_verifications', 7431), ('field_events', 0), ('judge_pair_ratings', 6831), ('judge_ratings', 2277), ('judge_run_audit', 1), ('paper_events', 190), ('provenance_classifications', 22034), ('run_calls', 0), ('run_manifests', 0)]
sqlite_sequence equal to reh2: True
out-of-scope vs pre-write: 28 compared; differing: []
paper_events live rows equal to pre-write by PK: True 190
residue: []
integrity: ('ok',) journal_mode: ('wal',)
fk_check: [('paper_events', []), ('run_manifests', []), ('run_calls', []), ('run_stage_configs', []), ('claim_inputs', []), ('audit_verdicts', [])]
```

## W6 — G3 (iii), verbatim on live, each byte-equal to rehearsal 2
```
--- paper_events R39 axis CHECK byte-equal to reh2: True
CHECK (
            (event_type IN ('screened', 'verified', 'adjudicated') AND to_state IN ('eligible', 'abstract_out', 'full_text_out'))
         OR (event_type IN ('acquired', 'not_obtainable', 'parsed', 'extracted', 'extraction_failed', 'audited') AND to_state IN ('parsed', 'extracted', 'extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context', 'audited_ai'))
         OR (event_type IN ('manual_advance', 'bypass', 'state_at_migration'))
        )

--- paper_events CHECK [event_type IN] byte-equal to reh2: True
CHECK (event_type IN ('screened', 'verified', 'adjudicated', 'acquired', 'not_obtainable', 'parsed', 'extracted', 'extraction_failed', 'audited', 'manual_advance', 'bypass', 'state_at_migration'))

--- run_manifests CHECK [run_kind IN] byte-equal to reh2: True
CHECK (run_kind IN ('extraction', 'screening', 'judge', 'review_session', 'import'))

--- run_manifests CHECK [end_status IS NULL OR] byte-equal to reh2: True
CHECK (end_status IS NULL OR end_status IN ('completed', 'failed', 'interrupted', 'aborted'))

--- run_manifests CHECK [(end_status IS NULL AND end_reason] byte-equal to reh2: True
CHECK (
            (end_status IS NULL AND end_reason IS NULL)
         OR (end_status IS 'completed' AND end_reason IS NULL)
         OR (end_status IS 'aborted' AND end_reason IS NOT NULL)
         OR (end_status IS 'failed')
         OR (end_status IS 'interrupted')
        )

--- run_calls CHECK [outcome IN] byte-equal to reh2: True
CHECK (outcome IN ('completed', 'refused_input_overflow', 'refused_ceiling_unavailable', 'refused_input_truncated', 'refused_input_dropped', 'error'))

--- audit_verdicts CHECK [verdict IN] byte-equal to reh2: True
CHECK (verdict IN ('verified', 'flagged'))

--- trigger run_manifests_end_once byte-equal to reh2: True
CREATE TRIGGER run_manifests_end_once BEFORE UPDATE ON run_manifests WHEN OLD.ended_at IS NOT NULL OR NEW.run_id IS NOT OLD.run_id OR NEW.run_uid IS NOT OLD.run_uid OR NEW.review_id IS NOT OLD.review_id OR NEW.run_kind IS NOT OLD.run_kind OR NEW.git_commit IS NOT OLD.git_commit OR NEW.git_dirty IS NOT OLD.git_dirty OR NEW.git_tag IS NOT OLD.git_tag OR NEW.engine_state IS NOT OLD.engine_state OR NEW.spec_hash IS NOT OLD.spec_hash OR NEW.codebook_hash IS NOT OLD.codebook_hash OR NEW.codebook_sha256 IS NOT OLD.codebook_sha256 OR NEW.library_versions_json IS NOT OLD.library_versions_json OR NEW.host IS NOT OLD.host OR NEW.started_at IS NOT OLD.started_at OR NEW.cloud_arms_json IS NOT OLD.cloud_arms_json OR NEW.payload_description IS NOT OLD.payload_description OR NEW.manifest_json IS NOT OLD.manifest_json OR NEW.manifest_sha256 IS NOT OLD.manifest_sha256 BEGIN SELECT RAISE(ABORT, 'run_manifests: a manifest is written before the first call and never edited; only its end is recorded, once'); END

--- trigger claim_inputs_no_update byte-equal to reh2: True
CREATE TRIGGER claim_inputs_no_update BEFORE UPDATE ON claim_inputs BEGIN SELECT RAISE(ABORT, 'claim_inputs is a record: one row per extraction call, never edited'); END

--- trigger claim_inputs_no_delete byte-equal to reh2: True
CREATE TRIGGER claim_inputs_no_delete BEFORE DELETE ON claim_inputs BEGIN SELECT RAISE(ABORT, 'claim_inputs is a record: one row per extraction call, never edited'); END

--- trigger audit_verdicts_no_update byte-equal to reh2: True
CREATE TRIGGER audit_verdicts_no_update BEFORE UPDATE ON audit_verdicts BEGIN SELECT RAISE(ABORT, 'audit_verdicts is a record: one row per verdict, never edited'); END

--- trigger audit_verdicts_no_delete byte-equal to reh2: True
CREATE TRIGGER audit_verdicts_no_delete BEFORE DELETE ON audit_verdicts BEGIN SELECT RAISE(ABORT, 'audit_verdicts is a record: one row per verdict, never edited'); END
```

## W7 — C12 re-measure
ReviewDatabase("surgical_autonomy") constructed and closed at 17:22:11 UTC (no PendingMigrations — I3 holds). --compare against the W3 record: IDENTICAL, 36 tables, overall 0d3eedea…cb25, -wal 0 B. audit_adjudication absent before and after.
