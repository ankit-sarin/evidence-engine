# Migrations

Numbered files, applied in order by `runner.py`, each with a receipt in the
database's `schema_migrations` table. **The series begins at 002** — there is no
`001` and never was.

## Adding one

1. **Name it** `NNN_short_snake_name.py`, `NNN` the next free three-digit number.
   The filename stem is the migration id and goes in every receipt, so it should
   say what the migration does without anyone looking the number up.
2. **Expose `run_migration(db_path) -> dict`.** It opens its own connection,
   does its work, commits, closes, and returns a small summary the log can
   print. It must be **idempotent**: running it on a database that already has
   its target state must change nothing and must not raise.
3. **Declare its kind** in `runner.KINDS`, with the reason in a comment. The
   runner refuses to start if a numbered file has no declared kind.
   * `schema` — builds structure. Executed on every database, fresh ones
     included.
   * `data` — moves or rewrites rows. **Never executed on a fresh database.**
     `003` would import one review's nine thousand papers into another review's
     database; `002` renames labels a fresh database has no rows to carry.
4. **Verify its target state is detectable.** A migration whose effect cannot be
   seen by inspecting the schema (or, for a data migration, by a documented data
   check) cannot be registered as pre-applied on an existing database, and
   nobody can tell whether it ran.
5. **Add a test** that a fresh `ReviewDatabase` carries the effect and that the
   receipt says `executed`. Assert the **effect**, never that some file contains
   the migration's name — that pins the wiring mechanism, and two tests that did
   exactly that had to be rewritten when the runner replaced it.

## What the runner guarantees

* **Numeric order**, one transaction per migration, committed before the next
  begins — a failure leaves the earlier receipts intact and the failing one
  absent, so the database says how far it got.
* **Fail-fast**, with the migration id in the message.
* **Refusal on drift.** If any receipt's `file_sha256` no longer matches the
  file, the runner refuses to start and names every drifted id. A migration
  whose text changed after it ran is a different migration and the database
  cannot know which one it got. If you must change an applied migration, write
  a new one instead.
* **Idempotence.** A second run executes nothing and writes no receipt.

## What it deliberately does not do

* **No `PRAGMA user_version`.** One integer cannot say which of fourteen
  migrations ran, cannot carry a checksum, and would be a second source of truth
  that drifts the first time something is registered out of order. It stays 0.
* **No rollback.** Only `009` has one, from before this runner existed. Recovery
  is `engine.utils.db_backup.restore` from the backup taken before the run.
* **No schema verification on registration.** `register_preapplied` records an
  assertion that someone made on evidence; it checks nothing about the schema
  and its docstring says so.

## The existing set

| id | kind | note |
|---|---|---|
| 002 | data | screening label rename; its INDEX half is now what `engine/adjudication/schema.py` creates |
| 003 | data | expanded-corpus backfill; **its source directory and default target are literals naming one review** — S9 |
| 004–013 | schema | columns, judge tables, provenance census, codebook provenance, NOT NULL relaxation |
| 014 | schema | cloud tables — calls `engine.cloud.schema.init_cloud_tables` rather than re-declaring the DDL |
| 015 | schema | drops the three pre-rename adjudication index duplicates |
