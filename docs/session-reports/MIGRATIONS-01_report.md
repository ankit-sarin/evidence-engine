# MIGRATIONS-01 — numbered migrations with receipts; a fresh database that builds itself

**Date:** 2026-09-20/21. **Task type:** repair + one live write. **No model call.**
**Harness session:** `bd7ea59e-96e8-4b4d-a2a1-6f6adb1ecb93`. **HEAD at start:** `62d3a0b`.

**Tags.** **MEASURED** = produced here by running code. **READ** = quoted at this HEAD.
**INFERRED** = a reading or a cause.

---

## Phase 1 read-out

### 1. Startup verify

| check | EXPECTED | measured | |
|---|---|---|---|
| HEAD / tree / origin | `62d3a0b`, clean, level | identical, `0 0` | ✅ |
| standard gate | 2,397 / 17 — 508/608/390/475/416 | identical; **wall 340.3 s** | ✅ |
| `review.db` | 101,978,112 B @ 2026‑09‑11 02:00:52.636956943 UTC | identical; **`-wal` 0 B** | ✅ |
| fingerprint `--compare` | exit 0 vs `f376562e…39e00` | IDENTICAL, exit 0; 24 tables | ✅ |
| `tests/test_eligibility.py` | 91 passed | 91 passed | ✅ |

### 2. The migration files — 🔴 I1 false

**Twelve files, `002`…`013`. No `001`; `012` and `013` exist.** All expose
`run_migration(db_path)`. Classification and live evidence:

| file | kind | live status, and how it was established |
|---|---|---|
| `002_screening_rename` | **data** | applied — both pre- and post-rename index names present |
| `003_backfill_expanded_screening` | **data** | applied — 21,374 screening rows; source files still on disk |
| `004_pdf_quality_check` | schema | applied — the 6 columns; *also duplicated inline* |
| `005_model_digest` | schema | applied — the 2 columns; *also duplicated inline* |
| `006_not_null_confidence_tier` | schema | applied — `evidence_spans.tier` present |
| `007`–`009` | schema | applied — judge tables, fabrication verifications, audit log present |
| `010_add_provenance_classifications` | schema | applied — both tables + 3 indices; **hand-run** |
| `011_add_absence_claim_class` | schema | applied — columns, widened CHECK, `ABSENCE_CLAIM` carries 150 rows; **hand-run** |
| `012`, `013` | schema | applied — codebook columns; NOT NULL lifted |

**🔴 M1's clause is contradicted**, and the correction is issued as a dated addendum:
`_run_migrations` **imports six** numbered modules (`006`, `007`, `008`, `009`, `012`, `013`)
on every construction. It never runs `002`, `003`, `010`, `011`. See
`DISCOVERY-01_part-B_readout.ADDENDUM_20260921.md`.

**Migration 003's literal** — READ, module level, evaluated at import, no `--review` anywhere:

```python
DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "surgical_autonomy"
EXPANDED_DIR = DATA_DIR / "expanded_search"
...
DB_PATH = DATA_DIR / "review.db"
```

**Run on a fresh database for another review it would import the autonomy review's 9,234 papers
into that review's database** — the sources are `surgical_autonomy`'s regardless of target — and
with `db_path` omitted it writes to `data/surgical_autonomy/review.db` whatever review the caller
meant. **For S9: both the source directory and the default target must derive from a passed
review id.**

### 3. The runner before, and temp-DB cost

READ, in order: `ensure_adjudication_table` → 18 inline `_SIMPLE_MIGRATIONS` ALTERs (swallowing
`already exists` / `duplicate column`) → `_VERIFICATION_TABLE` → the `evidence_spans` CHECK
rebuild → `006` (conditional) → `007`, `008`, `009`, `012`, `013` unconditionally.

**Temp `ReviewDatabase` construction: median 219.5 ms, mean 220.0 ms (n=10).** Every temp
database in the suite goes through it — **I5 confirmed**.

### 4. Fresh vs live — the STOP condition fired

`fresh edb1151a…28ef8` (20 tables) vs `live 1d6af8b9…3a18d` (24). **Zero column differences.**

| live-only object | maps to |
|---|---|
| `provenance_census_runs`, `provenance_classifications`, 3 `idx_prov_class_*` | **010** |
| `idx_prov_class_absence_pattern` | **011** |
| `idx_abstract_adjudication_{paper,ext_key,decision}` | **002**, the rename half |
| **`cloud_extractions`, `cloud_evidence_spans`** | **🔴 no migration** — `engine/cloud/schema.py::init_cloud_tables`, never called from the constructor |

The seven "DDL differs" tables were cosmetic: three carry a quoted name from the 002 rename,
`papers` and `extractions` differ only in **column order** (ALTER appends), `parse_attempts` is
whitespace. Plus two that were not cosmetic:

* **Six adjudication indices where three are meant.** 002 created the new names;
  `ensure_adjudication_table` recreated the old ones on **every** construction.
* **🔴 `audit_adjudication.span_id REFERENCES "_evidence_spans_old"`**, a table that does not
  exist. Measured on a scratch copy: with `PRAGMA foreign_keys=ON`, which the constructor sets,
  an INSERT fails with `no such table: main._evidence_spans_old`. **The human-audit import path
  is unwritable.** Addendum issued against Part A; recorded as **A11**, deferred to session 4.

---

## What was built

### `engine/migrations/runner.py`

READ, the module's own statement:

> **A receipt, not a version number.** `PRAGMA user_version` is one integer: it cannot say which
> of twelve migrations ran, cannot carry a checksum, and would be a second source of truth that
> drifts the first time a migration is registered out of order. It is deliberately left at 0.

Numeric order; **one transaction per migration**, committed before the next begins, so a failure
leaves earlier receipts intact and the failing one absent; fail-fast with the id in the message;
**refusal on drift**, naming *every* drifted id, because a migration whose text changed is a
different migration and the database cannot know which one it got; idempotent by consulting
receipts first.

`KINDS` declares each migration `schema` or `data` **with its reason**, and `discover()` refuses
to run if a numbered file has no declared kind — a new migration cannot be added without a ruling
on what it is. Data migrations are never executed on a fresh database.

**Receipt store** — `schema_migrations(migration_id PK, file_sha256, applied_at, mode CHECK IN
('executed','registered_preapplied'), runner_version, note)`.

### Migrations 014 and 015

**`014_cloud_tables`** closes the one gap that mapped to nothing. It **calls**
`init_cloud_tables` rather than re-declaring the DDL, so the tables have one definition and this
file cannot drift from it. The cloud call sites keep their own calls — they are no-ops once the
tables exist, and removing them would make a cloud run depend on the migration having run.
**Remaining call sites, as asked:** `engine/cloud/base.py`, `engine/cloud/__init__.py`,
`scripts/run_cloud_extraction.py`.

**`015_drop_prerename_adjudication_indices`** drops the three duplicates, and
`engine/adjudication/schema.py` now creates only the post-002 names so they cannot come back.
**Precondition checked before writing it:** the only places naming `idx_adjudication_*` are the
creator and migration 002 itself — no query, no `INDEXED BY`, nothing under `analysis/`. The
migration **refuses to drop a duplicate whose post-rename replacement is absent**: half a rename
must not become no index at all, and a test pins that refusal.

### `engine/tools/db_fingerprint.py` — `schema_structure_hash`

READ:

> Column ORDER, quoting of the table name, comments and whitespace are excluded: they are what
> ALTER TABLE and a rename leave behind, and two schemas that differ only in those are the same
> schema. The textual `schema_hash_sha256` keeps them, and it stays the live integrity check.

Computed from `PRAGMA table_info` (columns sorted **by name**), `index_list`/`index_info`, and
`foreign_key_list`. `compare()` now enumerates structural differences by table and facet, and
**skips any key absent from either record** — a record written before a field existed is not
evidence that the field changed. The 2026‑09‑20 record still compares clean.

### `ReviewDatabase._run_migrations`

The inline base stays — `_SCHEMA`, the adjudication tables, the verification table and the
`evidence_spans` rebuild are the floor several migrations assume. Everything numbered moved to
the runner.

---

## The live write (R3) — exactly once, bracketed

UTC **2026‑09‑21T00:28:07**, outside the 07:00–10:00 no-write window (the script refuses inside
it).

| step | result |
|---|---|
| **(a)** backup | `review.db.bak-pre-migrations01-20260921-002807`, 101,978,112 B; **record vs backup IDENTICAL**; overall `f376562e…39e00` |
| **(b)** before | textual `1d6af8b9…3a18d` · structure `4c570485…e5cb9` · overall `f376562e…39e00` · 24 tables |
| **(c)** write | **13 registered pre-applied** (`002`–`014`); **`015` executed**, dropping `idx_adjudication_paper`, `idx_adjudication_ext_key`, `idx_adjudication_decision`; its receipt written |
| **(d)** after | textual **`c286cde554a27ce514cf201094c06cdd3dbadab3aab5dcd4022717b0a4a790c5`** · structure **`8625b7c506dba77ba00f077eb6689de0cba8e794af618393b11ea08fe5625d37`** · overall **`a9926e626928d1d47f4935e129da698cd2d50c82902f25c0f4f3cd9b1b1eafae`** · 25 tables |
| **(e)** diff | one new table (`schema_migrations`, 14 rows); three indices gone; the three hashes and the table count moved. **Nothing else.** |
| **(f)** backup | kept beside the live database with its label |

**Every one of the 24 pre-existing tables is byte-identical in content** — measured by comparing
each table's row count and row hash in the before and after records. **No data was touched.**

New record: `docs/session-reports/migrations-01/review_db_fingerprint_20260921T002809Z.json`.
Before record kept beside it.

**🔴 One honesty note on receipt 014.** It is registered `registered_preapplied` on live, but its
target state is only **partly** present: the two tables exist, and the NOT NULL tightening on
`cloud_evidence_spans.confidence`/`tier` does not. The receipt's `note` column records exactly
that, in the database, rather than leaving the receipt to imply more than is true.

---

## R4 — fresh vs live, after the write

```
fresh structure: 4ebc8d765adb11809b9ad130ede5758e5ffd73518a8530abb59f849f65329bb7
live  structure: 8625b7c506dba77ba00f077eb6689de0cba8e794af618393b11ea08fe5625d37
fresh tables 25   live tables 25
```

**They are not equal, and I did not adjust the fresh build to make them so.** Six structural
differences remain, from **two causes, both pre-existing, neither introduced here**:

| # | difference | cause |
|---|---|---|
| 1–2 | `audit_adjudication.foreign_keys`: live `span_id → _evidence_spans_old`, fresh `span_id → evidence_spans` | **A11**, deferred to session 4 by ruling. Fixing it means rebuilding a table on the live database. |
| 3–6 | `cloud_evidence_spans.confidence` and `.tier`: live `notnull=0`, fresh `notnull=1` | **New finding.** `init_cloud_tables` carries a rebuild branch that adds NOT NULL to pre-existing databases; it has never run on live, because the function is only called by cloud runs and `014` was *registered*, not executed. Executing `014` on live would rebuild a **7,257-row** table — outside R3's authorised scope. |

**Everything else matches**: same 25 tables, every column name/type/default/PK flag, every index
with its uniqueness and column list, and every other foreign key. The three duplicate indices are
gone from live and cannot return.

**G3 is therefore NOT green**, and is reported as such rather than claimed. It needs one ruling
(execute `014` on live, or record the NOT NULL gap as a second deferred row) and session 4's
decision on A11.

---

## Timings (R5)

| measure | before | after | change |
|---|---:|---:|---|
| standard gate, five chunks | **340.3 s** | **414.7 s** | **+21.9 %** |
| temp `ReviewDatabase` construction (median, n=10) | **219.5 ms** | **397.5 ms** | **+81 %** |

**The temp-DB figure is well over the ±20 % I estimated in Phase 1, and the estimate was wrong
for a reason worth recording:** a fresh database now genuinely builds the *whole* schema. Before,
six migrations ran and `004`/`005` were inline `ALTER`s; now twelve run, including `010`/`011`
(two tables, six indices, a table rebuild) and `014` (two more tables) — work a fresh database
previously never did, which is precisely why its schema was wrong. The cost buys the correctness.
The per-migration connection close/reopen is the other contributor and is deliberate: it is what
makes one-transaction-per-migration true.

---

## Gate table

| gate | requirement | measured | |
|---|---|---|---|
| **G1** | Phase 1 acknowledged before any `engine/` change | delivered → ruling → first `engine/` write | ✅ |
| **G2** | R2 tests pass; runner refuses on checksum mismatch; idempotent | **15 tests**, all pass. Drift raises `MigrationDrift` naming **every** drifted id; re-run executes nothing and changes not one receipt | ✅ |
| **G3** | fresh structure hash == live structure hash | **NOT MET** — `4ebc8d76…29bb7` vs `8625b7c5…625d37`, six differences from two pre-existing causes, enumerated above. Not adjusted by hand, per R4 | ❌ **reported, not claimed** |
| **G4** | exactly one live write, fully bracketed; diff limited to the receipt store; new record; new hash quoted | one write at 00:28:07 UTC; backup verified IDENTICAL to the record first; diff = `schema_migrations` (14 rows) + three dropped indices + the hashes; all 24 pre-existing tables byte-identical; new record committed; overall **`a9926e62…1eafae`** | ✅ |
| **G5** | gate green; new count; deselects 17; timings | **2,412 / 17** (516/607/398/475/416; desel 0/0/10/6/1; **+15**); timings above | ✅ |
| **G6** | every migration classified with evidence; 003's literal with what S9 must change; the two addenda; the A11 row | §2 above; both addenda committed; A11 text below | ✅ |

---

## Inventory rows

**A11** — *`audit_adjudication` is unwritable — FK to a phantom table left by the `evidence_spans`
rebuild; the human-audit import path is broken, not defective.* Measured on a scratch copy:
`INSERT` fails with `no such table: main._evidence_spans_old` under `PRAGMA foreign_keys=ON`,
which `ReviewDatabase.__init__` sets. Explains `audit_adjudication`'s 0 rows and supersedes
DISCOVERY-01 Part A's "exposure entirely prospective". **Input to session 4 (S2 Phase 1).**

**New, from R4** — *`cloud_evidence_spans.confidence` and `.tier` are NOT NULL on a fresh database
and nullable on live.* `init_cloud_tables`'s rebuild branch has never run on the live database.
Closing it means executing `014` there, rebuilding a 7,257-row table. Needs a ruling; it is the
second of the two reasons G3 is not green.

**Carried from Phase 1** — *migration 003's source directory and default target are literals
naming one review* (S9).

---

## Appendix B, updated

> **Fingerprint at session open and close.** The record of reference is now
> `docs/session-reports/migrations-01/review_db_fingerprint_20260921T002809Z.json` — overall
> **`a9926e626928d1d47f4935e129da698cd2d50c82902f25c0f4f3cd9b1b1eafae`**, textual schema
> `c286cde5…a790c5`, structure `8625b7c5…625d37`, 25 tables. The 2026‑09‑20 record
> (`f376562e…39e00`, 24 tables) is **superseded**, not deleted: it is what the database was
> before MIGRATIONS-01's registration write.
>
> ```bash
> python -m engine.tools.db_fingerprint data/<review>/review.db \
>     --compare docs/session-reports/migrations-01/review_db_fingerprint_20260921T002809Z.json
> ```
>
> Exit **0** identical · **1** differences, printed by table · **2** a file is missing. A session
> that intends to change the database writes a new record with `--out`, commits it, and quotes
> the new `overall_sha256` in its closeout; the old record is superseded, never edited.
>
> **Two hashes now.** `schema_hash_sha256` is textual — it keeps column order, quoting and
> whitespace — and remains the integrity check. `schema_structure_hash` is PRAGMA-derived and
> answers "is this the same schema", which is the question a second review asks. A migration
> that rebuilds a table to append a column moves the first and not the second.
>
> **Before any migration**, take a backup with `auto_backup` and record its
> `overall_sha256`; the runner writes a receipt per migration into `schema_migrations`, so
> afterwards the database itself says what ran.

---

## What this does not claim

* **G3 is not met.** Two pre-existing differences remain; both are named, neither was papered
  over, and the fresh build was not adjusted to match.
* **No data migration was executed anywhere** — `002` and `003` were registered on live and
  skipped on fresh databases. `003` has never been run by this session in any form.
* **`014` was not executed on the live database**, so `cloud_evidence_spans` was not rebuilt
  there and its 7,257 rows were not touched.
* **The stale foreign key was not fixed**, by ruling. Nothing writes to `audit_adjudication`
  before session 12.
* `PRAGMA user_version` is still **0**, deliberately.

---

# ADDENDUM — 2026-09-21: the `cloud_evidence_spans` NOT NULL gap, measured

Issued on ruling, after the report above was committed. **Read-only** (`mode=ro`, one read
transaction); no change to the database or to any code.

## The NULL count

| column | rows | `IS NULL` |
|---|---:|---:|
| `cloud_evidence_spans.confidence` | 7,257 | **0** |
| `cloud_evidence_spans.tier` | 7,257 | **0** |
| either | 7,257 | **0** |

`confidence` spans 0.0–1.0 across 37 distinct values; `tier` is 1 (1,832), 2 (3,253), 3 (1,448),
4 (724). For comparison, `evidence_spans` — which *does* carry the constraint — also has zero
NULLs in both.

**The data is already conformant.** The gap is the constraint alone, so executing `014` on the
live database would rebuild the table without changing one value. That is a fact about today's
rows, not a licence: a constraint's job is the row nobody has written yet.

## Readers that treat either column as NOT NULL

**None — no code selects either column from this table at all.** MEASURED across `engine/`,
`scripts/`, `analysis/` and `tests/`: twenty-five modules mention `cloud_evidence_spans`, and
every `SELECT` against it names `value`, `source_snippet`, `field_name`, `arm` or a `COUNT(*)`.
`confidence` and `tier` are written and never read back.

Three things sit near the question and are worth naming precisely:

1. **🔴 `engine/migrations/006_not_null_confidence_tier.py` asserts the constraint exists**, in
   its own docstring — READ:

   > 2. `cloud_evidence_spans` already has NOT NULL on both columns via
   >    `engine/cloud/schema.py` — no changes needed there.

   **That claim is false on the live database.** It is true of a database built by
   `init_cloud_tables`'s `CREATE TABLE` and false of one whose table predates the constraint —
   which is this one. Migration 006 did nothing wrong; it recorded an assumption about a sibling
   module's output and the assumption did not survive. It is the closest thing to a "reader" of
   the constraint in the codebase, and it is a comment, not code.

2. **The three writers would pass `NULL` if given the chance.** `engine/cloud/base.py`
   (`store_result`), `scripts/backfill_cloud_spans.py` and `scripts/reparse_cloud_spans.py` all
   insert `span.get("confidence")` and `span.get("tier")`, and `dict.get` yields `None` for an
   absent key.

3. **But the only producer cannot omit them.** Every span reaching those INSERTs comes from
   `CloudExtractorBase.parse_response_to_spans`, which returns only what
   `ExtractionOutput.model_validate` accepted, and `EvidenceSpan` declares

   ```python
       confidence: float = Field(ge=0.0, le=1.0)
       tier: int = Field(ge=1, le=4)
   ```

   — both **required**, both range-checked. A span missing either is rejected before it can be
   stored. So the `.get()` calls are defensive, not load-bearing, and a NULL cannot arise from
   the supported path. That is why the count above is zero.

## What this means for C10

The gap is **latent, not active**: no reader depends on the constraint, no writer can currently
violate it, and no row does. Its cost is the one already recorded — a fresh database and the live
one are not structurally identical, so **G3 stays NOT MET** until `014` is executed there. Its
risk is the ordinary one: the guarantee is absent, so the next writer added to this table (or a
change that makes `EvidenceSpan` tolerant of a missing field) would be caught on a fresh database
and silently accepted on the live one — the same code, two behaviours, which is exactly the
condition the migration runner exists to end.

Deferred to session 5's migration batch per ruling; inventory row **C10**. The receipt on `014`
carries the gap in its `note` column, in the database, so a reader of the receipts is told.
