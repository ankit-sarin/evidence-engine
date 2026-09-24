# SUPERSEDED

`review_db_fingerprint_20260923T162059Z.json` — the record of
`data/surgical_autonomy/review.db` as the MANIFEST-01 Phase 3 write of 020 left it
(34 tables, overall `bb39ba81170c4f11d59954b9e70837afc427c66da710bde30881aad570d16c40`) —
was **superseded on 2026-09-24** by the INPUT-IDENTITY-01 Phase 3 live write of migration 021
(session 8b), in the commit that adds this file.

**The current record is**

```
docs/session-reports/input-identity-01/review_db_fingerprint_20260924T205225Z.json
overall_sha256  ea05912d67f0b841bf003a7a941f6505e2c57140b5c2f62e1ff446ea16b9ce01
schema (textual) cb52026b61394c676f2adf49998df5e1f5d4a13914765a7134ba13a2b644e015
structure        96f05996cccc350f0b6697bc31471da0a824657f72ec3eae2fcf28c4ccea74ab
tables           34
```

**Compare against that one, not this one.** A `--compare` of live against the file this sidecar
sits beside now reports differences, and every one of them is the intended write:
`parsed_text_refs` rebuilt with `parsed_text_sha256` (194 rows, every hash equal to the committed
baseline, rowids and the six original columns unchanged, 0 paths changed) and
`UNIQUE (paper_id, parsed_text_version)`; `schema_migrations` 19 → 20 rows (the 021 receipt); and
the schema hashes moved with the one rebuilt table. Table count is unchanged at 34. The other
thirty-two tables' content hashes are unchanged, measured against the two records, and that is the
evidence that no other value moved.

**This record is superseded, not wrong, and it is not edited or deleted.** It remains the true
record of the database between the MANIFEST-01 Phase 3 write and the INPUT-IDENTITY-01 Phase 3
write, and it is what the pre-write backup
`data/surgical_autonomy/review.db.bak-input-identity-01-phase3-pre-write-20260924-204138` matches.

Evidence: `docs/session-reports/input-identity-01/INPUT-IDENTITY-01_phase3i_rehearsal_20260924.md`
(the rehearsal and its checkpoint,
`docs/session-reports/input-identity-01/rehearsal_db_fingerprint_20260924T203420Z.json`) and
`docs/session-reports/input-identity-01/INPUT-IDENTITY-01_phase3ii_live_write_20260924.md`.
