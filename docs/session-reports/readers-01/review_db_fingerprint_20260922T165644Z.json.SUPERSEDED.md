# SUPERSEDED

`review_db_fingerprint_20260922T165644Z.json` — the record of
`data/surgical_autonomy/review.db` as the READERS-01 Phase 3 write of 018 and 019 left it
(31 tables, overall `e564f250afe40af7eb9a6bc07596a3c795972f7ef3b653c37599e8bc18285b63`) —
was **superseded on 2026-09-23** by the MANIFEST-01 Phase 3 live write of migration 020
(session 7b), in the commit that adds this file.

**The current record is**

```
docs/session-reports/manifest-01/review_db_fingerprint_20260923T162059Z.json
overall_sha256  bb39ba81170c4f11d59954b9e70837afc427c66da710bde30881aad570d16c40
schema (textual) a4e49d4a96d171cfe85b919bbc4ee08064ce1733fde7b52a71491ef46876c864
structure        effd6d519e0618e498d2c44df756b276968c207c27b50b27b826a4c5ca2d08fd
tables           34
```

**Compare against that one, not this one.** A `--compare` of live against the file this sidecar
sits beside now reports differences, and every one of them is the intended write: 31 → 34 tables
(`run_manifests`, `run_stage_configs`, `run_calls`, all empty), `schema_migrations` 18 → 19 rows
(the 020 receipt), `arms` gaining `pinned_run_id` and `pinned_sha256` (NULL on all three rows),
`sqlite_sequence` gaining `('field_events', 0)`, and the schema hashes moved with the two rebuilt
event tables. The other twenty-eight tables' content hashes are unchanged — including
`paper_events` (190 rows) and `field_events` (0 rows) through their rebuilds — which is the
evidence that no value moved.

**This record is superseded, not wrong, and it is not edited or deleted.** It remains the true
record of the database between the READERS-01 Phase 3 write and the MANIFEST-01 Phase 3 write, and
it is what the pre-write backup
`data/surgical_autonomy/review.db.bak-manifest-01-phase3-pre-write-20260923-161020` matches.

Evidence: `docs/session-reports/manifest-01/MANIFEST-01_phase3i_rehearsal_20260923.md` (the
rehearsal and its checkpoint,
`docs/session-reports/manifest-01/rehearsal_db_fingerprint_20260923T155530Z.json`) and
`docs/session-reports/manifest-01/MANIFEST-01_phase3ii_live_write_20260923.md`.
