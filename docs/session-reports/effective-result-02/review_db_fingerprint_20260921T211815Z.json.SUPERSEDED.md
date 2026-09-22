# SUPERSEDED

`review_db_fingerprint_20260921T211815Z.json` — the record of
`data/surgical_autonomy/review.db` as the session-5 seed left it
(32 tables, overall `62f3912813b6e9efa3a651ee4c4ebcd611adf89c989f240a58e83dcf6759b79a`) —
was **superseded on 2026-09-22** by the READERS-01 Phase 3 live write of migrations 018 and 019.

**The current record is**

```
docs/session-reports/readers-01/review_db_fingerprint_20260922T165644Z.json
overall_sha256  e564f250afe40af7eb9a6bc07596a3c795972f7ef3b653c37599e8bc18285b63
schema (textual) adc985a2af7cba9c103997c026d9f02d2c02e39304d44c62af2c3bd83572d2b0
structure        2b92ac41f333dda269d35d249c822a32b0a768c6fb69fc79030ed85729955769
tables           31
```

**Compare against that one, not this one.** A `--compare` of live against the file this sidecar
sits beside now reports differences, and every one of them is the intended write: 32 → 31 tables
(`audit_adjudication` dropped, R32/A11), `schema_migrations` 16 → 18 rows (the 018 and 019
receipts), and the schema hashes moved with the three rebuilt tables. The thirty other tables'
content hashes are unchanged, which is the evidence that no value moved.

**This record is superseded, not wrong, and it is not edited or deleted.** It remains the true
record of the database between the session-5 seed and the Phase 3 write, and it is what the
pre-write restore point
`data/surgical_autonomy/review.db.bak-readers-01-phase3-pre-write-20260922-165453` matches.

Evidence: `docs/session-reports/readers-01/READERS-01_phase3i_rehearsal_20260922.md` (the rehearsal
and its checkpoint) and the Phase 3(ii) report.
