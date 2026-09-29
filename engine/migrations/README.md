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

* **Numeric order**, one transaction per **receipt**, committed before the next
  migration begins — a failure leaves the earlier receipts intact and the failing
  one absent, so the database says how far it got. The runner does **not** wrap a
  migration in a transaction: it closes its own connection and calls
  `module.run_migration(db_path)`, so **each module owns its transaction** (class
  C row C11; corrected here 2026-09-22 to match `run()`'s docstring, which said
  "one transaction per migration" until R37).
* **Fail-fast**, with the migration id in the message.
* **Refusal on drift.** If any receipt's `file_sha256` no longer matches the
  file, the runner refuses to start and names every drifted id. A migration
  whose text changed after it ran is a different migration and the database
  cannot know which one it got. If you must change an applied migration, write
  a new one instead.
* **Idempotence.** A second run executes nothing and writes no receipt.

## What it deliberately does not do

* **No `PRAGMA user_version`.** One integer cannot say which of the numbered
  migrations ran, cannot carry a checksum, and would be a second source of truth
  that drifts the first time something is registered out of order. It stays 0.
  (It said "fourteen" until 2026-09-22; a count of the set is a measurement of a
  day, and the set grows.)
* **No rollback.** Only `009` has one, from before this runner existed. Recovery
  is `engine.utils.db_backup.restore` from the backup taken before the run.
* **No schema verification on registration.** `register_preapplied` records an
  assertion that someone made on evidence; it checks nothing about the schema
  and its docstring says so.

## Applying to a non-fresh database (R222/R222a)

A **fresh** database (no `schema_migrations` table, or the table exists with
zero rows) always applies every pending migration — that is unchanged, and
covers every test database and every first `ReviewDatabase` construction. A
database that already carries at least one receipt is different: once a
migration module is in the tree, an ordinary `ReviewDatabase` construction
against it (the staleness report, `advance_stage --status`, …) would otherwise
apply that migration with no backup, no embargo check and no rehearsal behind
it. `runner.run()` therefore raises `PendingMigrations` on a non-fresh database
with something pending unless it is called with `apply_pending=True`.

The sanctioned way to apply pending migrations to a non-fresh database
deliberately is:

```
python -m engine.migrations <db_path> --apply-pending [--include-data]
```

`engine/migrations/__main__.py` is the **only** caller in the tree that passes
`apply_pending=True` — `PendingMigrations`'s own message names this exact
invocation, with the resolved path of the database that refused. The module
opens nothing else and performs no backup, embargo or exclusivity check of its
own; those belong to the operator's pre-flight before running it, not to code.

## The existing set

| id | kind | note |
|---|---|---|
| 002 | data | screening label rename; its INDEX half is now what `engine/adjudication/schema.py` creates |
| 003 | data | expanded-corpus backfill; **its source directory and default target are literals naming one review** — S9 |
| 004–013 | schema | columns, judge tables, provenance census, codebook provenance, NOT NULL relaxation |
| 014 | schema | cloud tables — calls `engine.cloud.schema.init_cloud_tables` rather than re-declaring the DDL |
| 015 | schema | drops the three pre-rename adjudication index duplicates |
| 016 | schema | the S2 event store — seven tables, no existing one touched |
| 017 | data | seeds the event store from THIS database's corpus, parsed texts, spec and codebook (R25); imports the frozen `engine/core/corpus.py`, which is row C13 and why R35 forbids it from 018 on |
| 018 | schema | `cloud_evidence_spans` to the fresh NOT NULL shape, `UNIQUE(paper_id, arm)` dropped from `cloud_extractions`, `audit_adjudication` dropped (C10 · R16 · R32) |
| 019 | schema | `paper_events` rebuilt with the two-axis state vocabulary (R29/R39) |
| 020 | schema | `run_manifests`, `run_stage_configs`, `run_calls`; `field_events` and `paper_events` rebuilt with `run_id REFERENCES run_manifests` and R77's run-link CHECK; `arms` pin columns and the widened freeze trigger (S3a/S3b, R59, R68). Every CHECK NULL-safe (R78) |
| 021 | schema | `parsed_text_refs` rebuilt with `parsed_text_sha256 NOT NULL` (64 lowercase hex, NULL-safe CHECK) and `UNIQUE(paper_id, parsed_text_version)`; paths canonicalised (R100); rows backfilled by recomputation and refused on any disagreement with the committed Phase 1 baseline (R93, R101). Empty on a fresh database, so it reads no file there |
| 022 | schema | `paper_events` rebuilt — `identified`/`duplicate_of` retired (R213); `run_manifests` rebuilt — `run_kind` gains `'import'`, `end_status` gains `'aborted'` + new `end_reason` (R214, R215); `run_calls` rebuilt — `outcome` + `outcome_detail` (R216); new `claim_inputs` (R217) and `audit_verdicts` (R218). `run_stage_configs` is untouched — a rebuilt table renamed into the vacated original name never disturbs another table's `REFERENCES` text, measured before drafting this migration (10a-C2 Phase A) |

018 and 019 were applied to the live `surgical_autonomy` database on 2026-09-22
(READERS-01 Phase 3). From 018 onward a migration module is **self-contained**
(R35) and declares its own DDL, token lists and constants; 018 and 019 also
demonstrate the two rules a module-owned transaction must follow — never
`executescript` inside the transaction, and build a rebuilt table under a
temporary name rather than renaming the original away.
