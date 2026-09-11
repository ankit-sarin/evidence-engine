# MIGRATE-013 — lifting NOT NULL from the retired schema-hash columns

**Task:** apply `engine/migrations/013_drop_schema_hash_not_null.py` to
`data/surgical_autonomy/review.db` through the wired path. A table rebuild of
`extractions` and `review_runs`; no row content changes. This restores the
ability to extract: SCHEMA-DERIVE-01 stopped writing
`extraction_schema_hash`, and the column's NOT NULL would have rejected every
future insert.

**Date:** 2026-09-11 · **Repo HEAD at launch:** `a8ede6d`

---

## R6 first — `validation_setting.ordered_values`

Applied and pushed as `a8ede6d` before any database work, per the ruling's
sequencing. `Cadaver` moves from fifth to third:

```
In vivo (human); In vivo (animal); Cadaver; Ex vivo; Phantom/Simulation;
Computational/Virtual; Mixed; NR
```

`Mixed` and `NR` remain the trailing non-scale values. No other key on the
field changed; `valid_values` keeps its own order, which is a presentation
list and not a scale.

| | before | after |
|---|---|---|
| `codebook_hash` | `b551ec4393d8c9ef2a4106c704d41b119edd016b97c63b625111a804444fcbad` | `2154e0d3fa6c053dc2ba3cc12300387ce758586dd4cf80f7f69fddd0f2410162` |
| `codebook_sha256` | `3807335b31852c03e2141acf217553cfa96ce44ed68e440132d98d921f4e6b5f` | `f4bd7b391c5ec687d36db4b4b049f4f11436b860ffb366c194a9b2527e6b128a` |

**The semantic hash does include `ordered_values`** — its projection is
`fields[*]` wholesale, and the key reaches a prompt, just not the extraction
one. The extraction prompt is byte-identical across the edit:
`2f12adc7daa3721935e2494850d7508d3726f100e1c422553836155a86c26762`.

**The hash move is free, measured rather than assumed.** Read-only, before the
edit:

| table | rows | `codebook_hash IS NOT NULL` |
|---|---:|---:|
| `extractions` | 190 | **0** |
| `cloud_extractions` | 379 | **0** |
| `review_runs` | 6 | **0** |

No stored provenance value was invalidated.

---

## Pre-flight

**I1 — no holders.** `lsof` and `fuser` both clean; no extraction running.

**I3 — headroom.** `review.db` 99,770,368 B; a rebuild needs ~2×, i.e.
199,540,736 B; 3,319,106,277,376 B free on `/`. Holds by four orders of
magnitude.

**Sidecars.** `review.db-shm` (32,768 B) present from the read-only
`codebook_hash` count above; `review.db-wal` 0 bytes, so nothing was
uncheckpointed.

## Measurement A — before

```
size  = 99,770,368 bytes
mtime = 2026-09-10 22:19:01.118726508 +0000
```

Matches the MIGRATE-012 baseline exactly.

## Backup

```
path   = data/backups/review_pre_MIGRATE-013_20260911T020046Z.db
size   = 99,770,368 bytes
sha256 = d750fa431ae94c8ab3b1b4ffc3990d90c8883dcf9b4a294376fff03096dd91ea
```

Taken with SQLite's backup API from a read-only source connection, as in
MIGRATE-012: consistent by construction rather than by the absence of writers.
Per-table content verified equal between live and backup before launch.

## Content hashes (P4)

SHA-256 over `SELECT * ORDER BY id` for each table, recorded before the
migration and re-computed after:

| table | rows | content sha256 (first 16) |
|---|---:|---|
| `extractions` | 190 | `67fedd1dbfa9945a…` |
| `cloud_extractions` | 379 | `31cf0e9d2ce9856d…` |
| `review_runs` | 6 | `4320a89e23f15d00…` |

---

## Launch

One construction through the normal entry:

```
INFO engine.migrations.007_add_judge_tables: Migration 007 complete: 0 tables created, 0 indexes created
INFO engine.migrations.008_add_fabrication_verifications: Migration 008 complete: 0 tables created, 0 indexes created
INFO engine.migrations.009_add_backfill_audit_log: Migration 009 complete: 0 tables created, 0 indexes created
INFO engine.migrations.013_drop_schema_hash_not_null: Migration 013 lifted NOT NULL on: extractions.extraction_schema_hash, review_runs.extraction_hash
```

`cloud_extractions.extraction_schema_hash` was already nullable and was not
rebuilt — a table rebuild to remove a constraint a table does not have is risk
with no benefit.

## Checkpoints

### C1 — constraints lifted, everything else preserved

```
extractions.extraction_schema_hash:        present=True  NOT NULL=False
review_runs.extraction_hash:               present=True  NOT NULL=False
cloud_extractions.extraction_schema_hash:  present=True  NOT NULL=False   (untouched)

extractions        12 columns
cloud_extractions  15 columns
review_runs        10 columns
```

**The columns stay.** They are the historical record of the 190 local and 379
cloud extractions made while the spec hash was the authority; only the
constraint goes.

`sqlite_master` comparison:

```
objects outside the rebuilt tables:  before=53  after=53  IDENTICAL=True
objects ON the rebuilt tables:       before={extractions, review_runs} (tables only)
                                     after ={extractions, review_runs} (tables only)
                                     same set = True
PRAGMA foreign_key_check:  0 violations
PRAGMA integrity_check:    ok
```

Neither rebuilt table carried an index, so there was none to recreate; the
migration captures and replays them regardless.

### C2 — counts and content identical

| table | rows before → after | content |
|---|---|---|
| `extractions` | 190 → 190 | **IDENTICAL** |
| `cloud_extractions` | 379 → 379 | **IDENTICAL** |
| `review_runs` | 6 → 6 | **IDENTICAL** |

### C3 — idempotent

A second construction produced no `Migration 013 lifted` line.

### C4 — Measurement B, the new baseline

```
size  = 101,978,112 bytes     (delta: +2,207,744)
mtime = 2026-09-11 02:00:52.636956943 +0000
WAL/SHM: absent — checkpointed away on clean close
```

**Unlike 012, the file grows.** 012 added columns, which SQLite records in the
schema alone; 013 rebuilds two tables, writing fresh pages and leaving the old
ones as free space. +2.1 MB on a 99.8 MB database. A `VACUUM` would reclaim it
and is deliberately NOT run here — it rewrites the whole file, which is a
larger operation than the one this task was authorised to perform.

**This is the baseline every later brief should quote.**

### C5 — standard gate

Green. Run in four chunks because the harness kills long background tasks:

| chunk | result |
|---|---|
| `test_[a-e]*` | 855 passed |
| `test_[f-l]*` + `test_[m-p]*` (less `test_ollama_client`) | 483 passed, 14 deselected |
| `test_[q-z]*` + `tests/analysis` | 787 passed, 3 deselected |
| `test_ollama_client` | 26 passed (~87 s of deliberate backoff) |

One test needed updating, and it is the same defect MIGRATE-012 surfaced:
`test_013_lifts_not_null_preserving_every_row` asserted the migration's
**delta** against a copy of the live database, so it passed only until the
migration it describes was applied. It now asserts the outcome
(`rebuilt | already_nullable` is the full target set), with a new test that
builds a database **with** the constraint so the "it really does lift two"
claim stays under test.

---

## What this unblocks

Extraction against the live database. Before this migration the writers no
longer supplied `extraction_schema_hash` and the column rejected NULL, so any
insert would have failed. Nothing was silent — it would have raised on the
column — but nothing could run either.

**Still open before Run 7:** all 190 extractions carry a NULL `codebook_hash`,
which R3 defines as stale. A re-run will re-extract every paper rather than
skip it. That is the intended semantics, not a defect, but it is a 190-paper
consequence and belongs in the Run 7 plan.

## Rollback

Verify `sha256sum data/backups/review_pre_MIGRATE-013_20260911T020046Z.db`
equals `d750fa43…dd91ea`, then copy it over `data/surgical_autonomy/review.db`
with no process holding the database. The backup predates the migration, so
restoring re-imposes the NOT NULL — and with the current writers, extraction
would fail again.
