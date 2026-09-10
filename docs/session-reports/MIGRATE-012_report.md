# MIGRATE-012 — live application of the codebook-provenance migration

**Task:** apply `engine/migrations/012_codebook_provenance.py` to
`data/surgical_autonomy/review.db` through the engine's own wired migration
path. One schema change, six added columns, no row content touched.

**Date:** 2026-09-10 · **Repo HEAD at launch:** `13b16a4`

---

## Pre-flight

### I1 — the wired path is the live route

`engine/core/database.py:398-405`, inside `_run_migrations`:

```python
        # Migration 012: codebook provenance columns (CODEBOOK-AUTH-01 R2).
        # Wired here rather than hand-run, unlike 010 and 011: an extraction
        # written into a database that lacks these columns records no codebook
        # at all, and the gap is indistinguishable from an unedited codebook.
        mod_012 = importlib.import_module(
            "engine.migrations.012_codebook_provenance"
        )
        mod_012.run_migration(str(self.db_path))
```

This is the **same function** the Phase 2 temp-copy run exercised
(`MIG.run_migration(str(db_copy))`); the difference is only that the copy
invoked it directly and the live run reached it through
`ReviewDatabase.__init__`. **I1 true.**

### I2 — no process held the database

`lsof` and `fuser` both reported no holders. The running Python processes on
the box belong to other projects (`citation-mcp`, `lists`, `llm-council`,
`operativereports`, `surgical-cv`, `hello-ai-v2`) plus the VS Code server and
`ollama serve`; none touches evidence-engine. **I2 true.**

### I3 — sidecars

`review.db-shm` (32,768 B) carried an mtime of 2026-09-10 10:30 UTC — the
nightly user-tier snapshot at 10:30, as I3 anticipated. `review.db-wal` was
**0 bytes**, so nothing was uncheckpointed. The backup was taken through
SQLite's own backup API, which folds any WAL content into the copy, so the
sidecars are not part of it. **I3 true.**

---

## Measurement A — before

```
size  = 99,770,368 bytes
mtime = 2026-09-09 19:48:46.784974707 +0000
```

Identical to the baseline carried since VERIFY-EXIT-01/FONT-AUDIT-02.

## Backup

```
path   = data/backups/review_pre_MIGRATE-012_20260910T221849Z.db
size   = 99,770,368 bytes
sha256 = 59956f1ea79cb4685cca4a33ebf30e6593161164def181c54508d96f198b9d46
```

Method: **SQLite's backup API from a read-only source connection**
(`sqlite3.connect("file:...?mode=ro", uri=True).backup(dst)`), chosen over a
filesystem copy because it holds a read transaction for the duration and folds
WAL content into the destination — consistent by construction rather than by
the absence of writers.

Counts verified equal between live and backup before launch:

| table | live | backup |
|---|---:|---:|
| `extractions` | 190 | 190 |
| `cloud_extractions` | 379 | 379 |
| `review_runs` | 6 | 6 |
| tables | 24 | 24 |

The live file was re-stat'd after the backup and was unchanged.

---

## Launch

One construction, through the normal entry:

```
ReviewDatabase("surgical_autonomy")
```

Migration log:

```
INFO engine.migrations.007_add_judge_tables: Migration 007 complete: 0 tables created, 0 indexes created
INFO engine.migrations.008_add_fabrication_verifications: Migration 008 complete: 0 tables created, 0 indexes created
INFO engine.migrations.009_add_backfill_audit_log: Migration 009 complete: 0 tables created, 0 indexes created
INFO engine.migrations.012_codebook_provenance: Migration 012 added: extractions.codebook_hash, extractions.codebook_sha256, cloud_extractions.codebook_hash, cloud_extractions.codebook_sha256, review_runs.codebook_hash, review_runs.codebook_sha256
```

007–009 were no-ops, as expected on a database that already carries them.

---

## Checkpoints

### C1 — columns present, existing rows NULL

```
extractions  (12 columns total)
  cid=10  name=codebook_hash    type=TEXT  notnull=0 default=None pk=0
  cid=11  name=codebook_sha256  type=TEXT  notnull=0 default=None pk=0
  rows=190  codebook_hash NULL=190  codebook_sha256 NULL=190

cloud_extractions  (15 columns total)
  cid=13  name=codebook_hash    type=TEXT  notnull=0 default=None pk=0
  cid=14  name=codebook_sha256  type=TEXT  notnull=0 default=None pk=0
  rows=379  codebook_hash NULL=379  codebook_sha256 NULL=379

review_runs  (10 columns total)
  cid=8   name=codebook_hash    type=TEXT  notnull=0 default=None pk=0
  cid=9   name=codebook_sha256  type=TEXT  notnull=0 default=None pk=0
  rows=6  codebook_hash NULL=6  codebook_sha256 NULL=6
```

All six present, all nullable TEXT, **every existing row NULL in both** —
nothing was backfilled. NULL here means *nobody recorded it*, and must never be
read as *unchanged*: the codebook's content at the time of those 190 + 379
extractions is not recoverable from the database.

### C2 — counts and table count

Row counts unchanged: **190 / 379 / 6**.

**Table count is 24, the same as before — not 25.** Migration 012 adds
*columns*, not a table, and this codebase keeps no migration-tracking table:
every migration is idempotent and re-runs on every `ReviewDatabase`
construction, which is what C3 exercises. The 24 include `sqlite_sequence`;
user tables alone are 23.

### C3 — idempotent on the live database

A second construction produced **no** `Migration 012 added` line. Idempotence
is now demonstrated on the live database, not only on a copy.

### C4 — Measurement B, the new baseline

```
size  = 99,770,368 bytes        (delta: 0)
mtime = 2026-09-10 22:19:01.118726508 +0000
```

**The size is unchanged.** `ALTER TABLE ... ADD COLUMN` in SQLite rewrites the
schema record and leaves every row page alone, so a nullable column with no
default costs nothing until a row carries a value. The WAL and SHM sidecars
were checkpointed away on clean close and no longer exist.

**This is the baseline every later brief should quote.**

### C5 — standard gate

Green after the live migration. Two failures appeared on the first run, both
caused by *executing this brief* rather than by the migration touching data,
and both fixed here:

1. **`test_codebook_provenance.py` — two tests asserted the migration's
   DELTA.** They copied the live database and expected `added` to hold all six
   columns. That was true only while the live database was unmigrated, so the
   tests passed right up until the migration they describe was applied. They
   now assert the OUTCOME (`added | already_present` is the full set) and are
   independent of the starting state, with a new test that builds a database
   without the columns to keep the "it really does add six" claim under test.

2. **`test_inventory.py` — the drift guard fired on the backup file.** The
   inventory's `data_dirs_excluded` reason embedded the first three filenames
   in each excluded directory, so writing one backup into `data/backups/`
   changed the committed artifact. Worse, the drift message printed only the
   dictionary KEYS, which were identical — an alarm reporting that two
   identical lists differ. The reason is now the reason alone
   (`"no review.db"`), and the message names the entry that actually moved.

Neither was a data problem; C1–C4 all passed on the first attempt.

---

## Rollback

Not needed. If it ever is: verify
`sha256sum data/backups/review_pre_MIGRATE-012_20260910T221849Z.db` equals
`59956f1e…8b9d46`, then copy it over `data/surgical_autonomy/review.db` with no
process holding the database. The backup predates the migration, so restoring
it removes the six columns — and any extraction written after 2026-09-10
22:19 UTC.

## Out of scope

No backfill of the new columns. Migrations 010 and 011 remain hand-run
(MIGRATION-WIRE-01). `check_schema_parity` still ignores `codebook_hash`
(folded into SCHEMA-DERIVE-01).
