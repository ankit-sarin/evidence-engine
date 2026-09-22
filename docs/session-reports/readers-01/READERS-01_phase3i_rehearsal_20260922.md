# READERS-01 Phase 3 (i) — rehearsal of migrations 018 → 019 on a verified copy

**Task:** READERS-01-P3i · **Date:** 2026-09-22 · **Machine:** DGX Spark
**Opening HEAD:** `d0a19c0db388b939bee5dd870b62635e9d610666`
**Docs commit:** `509aa3c4570cbedf3d3bccdcdded0dc77f40e1c7`
**Live `review.db` was NOT written.** It was opened `mode=ro` for every read, and once by
`auto_backup`'s online-backup API, which reads it. `--compare` against the
effective-result-02 record exits **0** at open and at close; `-wal` **0 B** at both.

---

## 0. Findings, first

### F1 — the textual half of G3 (R41) is not met, and 018/019 could not have met it

**S5, computed.** Fresh database and rehearsed copy:

| hash | fresh | rehearsed copy | equal |
|---|---|---|---|
| `schema_structure_hash` (PRAGMA-derived) | `2b92ac41f333dda269d35d249c822a32b0a768c6fb69fc79030ed85729955769` | `2b92ac41f333dda269d35d249c822a32b0a768c6fb69fc79030ed85729955769` | **yes** |
| `schema_hash_sha256` (textual) | `9df858d503dd064423e55d8cd9e105c7edda68126aa4b02667a341bdfe4e5ea5` | `adc985a2af7cba9c103997c026d9f02d2c02e39304d44c62af2c3bd83572d2b0` | **no** |

`structure_differences(fresh, copy)` = **0**. The 019 postcondition read-back passes.
So the structural half of G3 and the read-back half both hold; the textual half does not.

**Thirteen `sqlite_master` objects differ in text; none differs in structure.** Every one of
them differs by *how the object came to exist*, not by what it is. Four families, all computed
by per-object diff:

1. **`ALTER TABLE ADD COLUMN` appendix vs. an inline declaration** — `extractions`, `papers`,
   `parse_attempts`. Live carries `…, low_yield INTEGER NOT NULL DEFAULT 0, model_digest TEXT, …)`
   appended after the closing column; fresh declares the same columns inline, in a different place
   in the list.
2. **Quoted table names** — `"abstract_screening_adjudication"`, `"abstract_screening_decisions"`,
   `"abstract_verification_decisions"` (migration 002's rename) and `"cloud_extractions"`,
   `"cloud_evidence_spans"` (018's own rebuild-then-rename). `ALTER TABLE … RENAME TO` rewrites the
   stored `CREATE` with the new name **quoted**; the fresh side was never renamed, so it is bare.
3. **Indentation** — `evidence_spans` and `idx_spans_extraction` (built on live by
   `_EVIDENCE_SPANS_REBUILD`, indented deeper than `_SCHEMA`'s copy) and the three
   `idx_abstract_adjudication_*` indices (line-wrapped on the fresh side, one line on live).
4. **A dropped comment** — fresh `cloud_extractions` carries the six-line R16 comment from
   `engine/cloud/schema.py::_CLOUD_SCHEMA`; 018 declares its own DDL under R35 and has no comment
   inside the `CREATE`.

Nine of the thirteen predate 018 and 019 entirely (002's rename, the `evidence_spans` rebuild, the
`ALTER TABLE` history). The other four are 018's own rebuild, which **cannot** reproduce
`_CLOUD_SCHEMA`'s text: R35 requires 018 to declare its DDL locally, and rebuild-then-rename
requires the rename, which quotes the name. **A migration that makes the two databases structurally
identical cannot make them textually identical.** The gate as R41 words it — "both hashes, fresh vs
live" — is therefore not satisfiable by this write, and was not satisfiable before it.

**Not adapted.** No gate was redefined and no code was touched. The ruling is the architect's.

### F2 — the structure hash has a SECOND blindness, of the same family as R41's

R41 recorded that `schema_structure_hash` cannot see a CHECK constraint. Measured here: it also
cannot see **physical column order**, because `structure()` *sorts* each table's columns by name
before hashing. Two tables genuinely differ in column order between fresh and live, and both the
structure hash and `structure_differences` report them identical:

```
extractions  fresh: … model, model_digest, auditor_model_digest, low_yield, extracted_at, …
extractions  copy : … model, extracted_at, low_yield, model_digest, auditor_model_digest, …
papers       fresh: … status, rejected_reason, created_at, updated_at, ee_identifier, …
papers       copy : … status, created_at, updated_at, rejected_reason, ee_identifier, …
```

This matters beyond bookkeeping: `db_fingerprint` hashes a row's values **in PRAGMA column order**,
so a per-table content hash is comparable only between databases with the same column order. It is
why the textual hash exists, and it is a second reason a zero structural diff is not on its own a
verification. Reported, not acted on.

### F3 — `engine/migrations/README.md` still carries the C11 wording R37 corrected

`run()`'s docstring was corrected in Phase 2a ("one transaction per **receipt**"). The README's
"What the runner guarantees" still reads *"Numeric order, **one transaction per migration**,
committed before the next begins"* — the same claim, in the file the README's own step 3 sends a
migration author to. Out of scope here (this task's only docs commit is the R52 line); recorded so
it can be fixed where the rest of C11 was.

### F4 — something opened live `review.db` during the session, informationally (P5)

`lsof` and `fuser` both report **no** process holding `review.db`, `-wal` or `-shm` at P5 and at
close. But `review.db-shm` has mtime `2026-09-22 16:32:48 UTC`, minutes into this session, while
`review.db` and `review.db-wal` keep their `2026-09-21 21:17` mtimes. A VS Code server with the
Python extension was running in this workspace (`pet server`, started 16:28). An `-shm` touch with
an unchanged main file and an empty `-wal` is what a **read-only** open does — it takes byte-range
read locks in the shared-memory file — and the fingerprint is byte-identical before and after, so
nothing was written. Recorded because **phase ii wants an exclusivity gate**, and a background
editor process that opens the live database on its own schedule is precisely what such a gate has
to contend with.

---

## 1. Pre-flight

| step | expected | measured | verdict |
|---|---|---|---|
| **P0** | not 07:00–10:00 UTC | `Tue Sep 22 16:32:16 UTC 2026` | outside the embargo |
| **I1** | HEAD `d0a19c0d…`, clean, level with origin | `d0a19c0db388b939bee5dd870b62635e9d610666`; `git status --porcelain` empty; `HEAD…origin/main` = `0 0` | **match** |
| **I2** | gate 2,564 / 17, deselects 0/0/10/6/1 | 549 + 662 + 407 + 529 + 417 = **2,564** passed, 0 failed; deselects 0/0/10/6/1 = **17** | **match** |
| **I2** | `tests/test_eligibility.py` 91 passed, fourteen surfaces green | **91 passed**; `-k frozen` → **14 passed**, 77 deselected; `screening_hash` = `e8fa9719b1028ac16f9af72996bc807c8ad8b1071d2420fc5d98ed826f043596` | **match** |
| **I3** | `--compare` exit 0; overall `62f39128…`; 32 tables; `-wal` 0 B | exit **0**; `62f3912813b6e9efa3a651ee4c4ebcd611adf89c989f240a58e83dcf6759b79a`; 32; 0 B | **match** |
| **I4** | 3 arms · 190 paper events · 194 parsed-text refs · 3 identities · 0 field events | 3 · 190 · 194 · 3 · 0 | **match** |
| **I5** | restore point present, 25 tables, `a9926e62…1eafae` | present; 25; `a9926e626928d1d47f4935e129da698cd2d50c82902f25c0f4f3cd9b1b1eafae` | **match** |
| **I6** | 018 `073307f0…88eee`, 019 `131c7c12…d85bca` on a fresh database | identical, see P4 | **match** |
| **P5** | processes holding live open | `lsof` rc=1, `fuser` rc=1 — none. See **F4** for the `-shm` mtime | reported |

Per-chunk pass counts are informational (chunk boundaries move); the totals and the deselect sum
are the check. The gate ran the five documented chunks —
`find tests -name '*.py' -not -path '*/analysis/*'` split into four, plus `tests/analysis`.

### I7 — the runner is not path-keyed (quoted)

`engine/migrations/runner.py`:

```python
def run(db_path: str | Path, *, include_data: bool = False) -> dict:
    ...
    db_path = Path(db_path)
    conn = sqlite3.connect(str(db_path))
```

Its three branches, in full:

```python
        for migration_id, path in discover():
            if migration_id in have:                 # receipt present
                result["already"].append(migration_id)
                continue
            if kind_of(migration_id) == "data" and not include_data:
                result["skipped"].append(migration_id)
                continue
```

The only guard above them is drift:

```python
        drifted = check_drift(conn)
        if drifted:
            raise MigrationDrift(...)
```

`db_path` is used only to open a connection and to call `module.run_migration(str(db_path))`. There
is no literal naming a review, a directory or a filename anywhere in the module, and no branch reads
one. **A rehearsal on a copy exercises the same path the live write will.** Neither 018 nor 019
contains `data/`, `review.db`, `surgical_autonomy`, a `Path(` call, an environment read or a
`__main__` block; both import only `sqlite3` and `__future__` (R35 holds).

### I8 — both are `schema` kind (quoted)

```python
    "018": "schema",   # C10 + R16 + R32: cloud_evidence_spans to the fresh
                       # NOT NULL shape, UNIQUE(paper_id, arm) dropped, and
                       # audit_adjudication dropped. Structure only.
    "019": "schema",   # R29/R39: paper_events rebuilt with the two-axis state
                       # vocabulary. Rebuilds an existing table; a fresh database
                       # needs the shape as much as the live one does.
```

`include_data` was **not** passed and is not required. The run confirms it: `skipped: []`, because
the two data migrations (002, 003) already carry receipts on this database and land in `already`.

### I12 — the receipt columns (read, not inferred)

`schema_migrations(migration_id, file_sha256, applied_at, mode, runner_version, note)`.
`applied_at` is `datetime.now(timezone.utc).isoformat()`, written at the moment the receipt is
written. It is the **only** value in the whole database that a second execution of the same
migrations cannot reproduce, which is what §5's prediction rests on.

### P4 — the fresh database, and the receipt re-read

Built through the runner at the P2 HEAD by `ReviewDatabase("fresh_p4", data_root=…)`, which runs
`_SCHEMA`, the adjudication tables, the `evidence_spans` rebuild, and then `runner.run`.

```
/tmp/claude-1000/-home-ankitsarin-projects-evidence-engine/
  a3614266-5ffc-46e3-8b5a-023aca84653f/scratchpad/p4fresh/fresh_p4/review.db
```

| migration | receipt `file_sha256` | vs I6 |
|---|---|---|
| `018_cloud_shape_and_audit_adjudication` | `073307f0f29ada4abcfeb75563509e208923b3543cf3f601441c9e2368a88eee` | **equal** |
| `019_paper_state_axes` | `131c7c12d8105c55009b9693553c2f0a37bf5ef0578721f7d033878e44d85bca` | **equal** |

31 tables (30 plus `sqlite_sequence`), textual `9df858d5…e5ea5`, structure `2b92ac41…55769`,
overall `10c04678b78f26e9de42aa9ae4709f73c6c82bc16eb6944a31f8195b83b61482`. The fresh table list is
the live list **minus `audit_adjudication`** and nothing else — computed by set difference, not by
eye.

---

## 2. The docs commit

```
509aa3c docs(claude): R52 — the partial-index guard, as an ops invariant
 CLAUDE.md | 9 +++++++++
 1 file changed, 9 insertions(+)
```

Pushed; `HEAD…origin/main` = `0 0`; tree clean. R52 lands as a third `Ops Invariants` section
beside the Ollama-service and database ones, because its subject is the commit rather than either.

---

## 3. The rehearsal, table by table

**S1.** `auto_backup(live, "readers-01-phase3-rehearsal")` →
`data/surgical_autonomy/review.db.bak-readers-01-phase3-rehearsal-20260922-164331`
32 tables, `62f3912813b6e9efa3a651ee4c4ebcd611adf89c989f240a58e83dcf6759b79a` — the backup's own
verified fingerprint is **equal to live's**, so the rehearsal target is the live content.

**I9.** After the backup, live `--compare` exits **0** and `-wal` is **0 B**. The backup read live
and changed nothing.

**S2.** The rehearsal target `--compare`d against
`docs/session-reports/effective-result-02/review_db_fingerprint_20260921T211815Z.json` → exit **0**,
IDENTICAL.

| table | PRE rows · content sha256 | POST rows · content sha256 | changed |
|---|---|---|---|
| `abstract_screening_adjudication` | 0 · `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | 0 · *(same)* | no |
| `abstract_screening_decisions` | 21374 · `d961fd298f839a0123cfe58a16ba350e36ddb11f5a9dab3e96af409b5e02b943` | 21374 · *(same)* | no |
| `abstract_verification_decisions` | 1422 · `3d6b36f5205cd2dc6311ab5e62944dcd7fc0ba55fb50e935870e7543e7b226ce` | 1422 · *(same)* | no |
| `arms` | 3 · `fcc5dd342ac08ebb262243e57f71fb7a13649258735ce059fb3d6fd9d0a9af11` | 3 · *(same)* | no |
| `audit_adjudication` | 0 · `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | — (dropped) | **yes** |
| `cloud_evidence_spans` | 7257 · `dc10008d30915bc7f4869959413d08b695348323adf3628aadae00e23abd9148` | 7257 · *(same)* | no |
| `cloud_extractions` | 379 · `73cef1a8f86cc6393df025f25f3e484a23cedf32b4805061105b57670d3ee400` | 379 · *(same)* | no |
| `evidence_spans` | 3760 · `019e642e3cb14addca435eec1a9135b841597feb3a5ab4c721c8508d9678817c` | 3760 · *(same)* | no |
| `extractions` | 190 · `0364cf4ef23310420f61d606e62be2a41c2084cbda0bc2d30b4b0d360d48a60b` | 190 · *(same)* | no |
| `fabrication_verifications` | 7422 · `42925a5f3364d696b66935fe44e6f68b4a7c4f65847cc442fbf19c96f776ba0a` | 7422 · *(same)* | no |
| `field_event_against` | 0 · `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | 0 · *(same)* | no |
| `field_event_against_decisions` | 0 · `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | 0 · *(same)* | no |
| `field_events` | 0 · `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | 0 · *(same)* | no |
| `ft_screening_adjudication` | 36 · `5d971f9153bc7bcb0934c0d4073f6ae2c7e34eb0a458de657f4d318d7591edc9` | 36 · *(same)* | no |
| `ft_screening_decisions` | 366 · `70045a6a4332f273b4f8bec5b025e7c3727b4cea03f25b7331d97cb47b8fb6b2` | 366 · *(same)* | no |
| `ft_verification_decisions` | 182 · `427d5ce62d40c0723251a511499cd553b0bfd99f5ab51285585dca613ef1358e` | 182 · *(same)* | no |
| `full_text_assets` | 794 · `be9c124d3b860138744140ce6d70f064ed00804123616b28557ce8256cb48876` | 794 · *(same)* | no |
| `judge_pair_ratings` | 6828 · `e3e2dff4d60ea94c8c8b83845fda5b5a1f7ecca23dd673a59e6a7772a57bb5f6` | 6828 · *(same)* | no |
| `judge_ratings` | 2276 · `6318e8d366d3a15979811329858e42666c1536efeba952d66548b8ed5c9d9026` | 2276 · *(same)* | no |
| `judge_run_audit` | 1 · `483203e723b2509efb5ada8c70bf89e0c535d0cc711f0385794d72bdda6dd7d2` | 1 · *(same)* | no |
| `judge_runs` | 7 · `ded37890d1d80b1fb3033674b5feac56964b1367c63377aca900caf8660b7812` | 7 · *(same)* | no |
| `paper_events` | 190 · `28e0d3df4a7f416cf2f90bdb114e55920911a8ba72ba6d9ef3c3ce18b747b66e` | 190 · *(same)* | no |
| `papers` | 10039 · `15267385aac0123b11089a2bfccb5eccb121b01ec1789b5089840089ce910baf` | 10039 · *(same)* | no |
| `parse_attempts` | 8 · `17c8783e6249cd8e662b4f4e680864d56aae70f43c736645c3906f0fad110bff` | 8 · *(same)* | no |
| `parsed_text_refs` | 194 · `b05b1a4312b2dbf5436dc9a5b68dcde7c26e1bbcbd3fb9ae3ec9556ddb717088` | 194 · *(same)* | no |
| `provenance_census_runs` | 2 · `d4f3d86c40498571a056ac9f5de423ff402294deb6e3ee5d0611d80e1a6b5642` | 2 · *(same)* | no |
| `provenance_classifications` | 22034 · `fe39f2684980ac0a7bb97711e8a3124061a8d39bacd189b40a60c3c0c0b9c4e4` | 22034 · *(same)* | no |
| `review_identities` | 3 · `4a586901e32c2f7786bee80ae6060065b8729098ef608aae0490edc4efe3941d` | 3 · *(same)* | no |
| `review_runs` | 6 · `7054c7597c68d23ca65e9579a80e176f15a5cf6f03b06bdaf82f497c0136228a` | 6 · *(same)* | no |
| `schema_migrations` | 16 · `8b938ac55b440856737b68cd5590d849372f7ade4ac493d73829ddf75c9b4969` | 18 · `45f6ccd1640e11e804fd27087dea00e19e05dade0e1a4f96b47e7e39acf736d5` | **yes** |
| `sqlite_sequence` | 6 · `8fbb15bd0efb86331955d9eb03517f19e6d8e3cd7f4323d1b67f8d9578144a77` | 6 · *(same)* | no |
| `workflow_state` | 12 · `e7751792f5e4d688cadf10d2da00f65ade3449df49a46200da64abb1ae68433c` | 12 · *(same)* | no |
**Two tables changed, and only two.**

* **`audit_adjudication` — dropped** (R32/A11). 0 rows, so nothing was lost with it. Live goes
  **32 → 31 tables**, and the remaining 31 are exactly the fresh database's 31.
* **`schema_migrations` — 16 → 18 rows.** The two new receipts. Expected; it is the table the
  runner writes.

**Everything else is byte-identical, the three rebuilt tables included.**
`cloud_evidence_spans` (7,257), `cloud_extractions` (379) and `paper_events` (190) were each
dropped and rebuilt, and each carries the **same content hash** afterwards. That is by
construction, not luck: `db_fingerprint` hashes a row as its values in PRAGMA column order ordered
by rowid, and both modules pin both. 018 declares `CLOUD_EXTRACTIONS_COLUMNS` /
`CLOUD_EVIDENCE_SPANS_COLUMNS` and copies `INSERT INTO tmp (cols) SELECT cols FROM table ORDER BY
rowid`; 019 declares `_COLUMNS` and copies `ORDER BY event_id`. `id` and `event_id` are both
`INTEGER PRIMARY KEY`, hence rowid aliases, and both are copied explicitly — so rowids survive too.
**The change on these three tables is shape (the column declaration), never values**, and the
content hash is the evidence: a value change would have moved it, and a column reorder would have
moved it as well.

**`sqlite_sequence` did not change**, although it was expected to be at risk. Computed rather than
assumed: POST rowid order is `judge_ratings(1), judge_pair_ratings(2), fabrication_verifications(3),
judge_run_audit(4), provenance_classifications(6), paper_events(8)`. 019 inserts into
`paper_events_new_019` **before** dropping `paper_events`, so the new sequence row is appended after
the old one; the drop then removes the old row and the rename renames the new one. `paper_events`
was already the last row, so the rowid-ordered value sequence is unchanged even though the rowid
moved (6 → 8). `seq` stays **190** — the `INSERT..SELECT` carries explicit `event_id`s, so the
rebuilt AUTOINCREMENT high-water mark already equals `seq_before` and 019's restoring `UPDATE`
matches no row, by design (`WHERE … seq < ?`). Neither cloud table has an AUTOINCREMENT row, and
neither did `audit_adjudication`, so 018 touched this table not at all.

---

## 4. S3–S8

### S3 — wall times and receipts

Entry point: `engine.migrations.runner.run(db_path)`, `include_data` left at its default `False`.

```json
{"executed": ["018_cloud_shape_and_audit_adjudication", "019_paper_state_axes"],
 "skipped": [], "already": [ …16 ids, 002 through 017… ]}
```

| migration | wall (includes its receipt write) |
|---|---|
| `018_cloud_shape_and_audit_adjudication` | **0.308 s** |
| `019_paper_state_axes` | **0.014 s** |
| total `runner.run` | **0.323 s** |

Measured by timestamping the runner's own `migration %s executed` log records against the call
start; no code was modified. 018 carries the cost because it rewrites 7,636 rows across two tables;
019 rewrites 190.

| migration | receipt `file_sha256` | vs P4 | vs I6 |
|---|---|---|---|
| `018_cloud_shape_and_audit_adjudication` | `073307f0f29ada4abcfeb75563509e208923b3543cf3f601441c9e2368a88eee` | equal | equal |
| `019_paper_state_axes` | `131c7c12d8105c55009b9693553c2f0a37bf5ef0578721f7d033878e44d85bca` | equal | equal |

Both `mode = executed`, `runner_version = 1`, `note` NULL.

### S4 — POST

| fact | value |
|---|---|
| table count | **31** |
| textual `schema_hash_sha256` | `adc985a2af7cba9c103997c026d9f02d2c02e39304d44c62af2c3bd83572d2b0` |
| `schema_structure_hash` | `2b92ac41f333dda269d35d249c822a32b0a768c6fb69fc79030ed85729955769` |
| `overall_sha256` | `677f9b836d918a76ead5a1ec4ac3779b4ffddc843f58179941e1a8eab069910a` |
| `paper_events` triggers | `paper_events_no_delete`, `paper_events_no_update` — the same set as PRE |
| `paper_events` indices | `idx_paper_events_paper`, `sqlite_autoindex_paper_events_1` |
| `PRAGMA integrity_check` | `ok` |
| `PRAGMA foreign_key_check` | empty |

### S5 — G3 on the copy (R41)

Structure: **equal**, and `structure_differences(fresh, copy)` = **0**.
Textual: **not equal** — see **F1** for the thirteen objects and why.
Postcondition read-back, quoted from the copy's `sqlite_master`:

```sql
to_state    TEXT    NOT NULL CHECK (to_state IN ('eligible', 'abstract_out', 'full_text_out',
    'parsed', 'extracted', 'extraction_failed', 'full_text_not_obtainable', 'parse_failed',
    'input_exceeds_context', 'audited_ai')),
```

It names `parse_failed`. **019 reached this database**, which a zero structural diff alone could not
have told us — R41's point, confirmed on the copy.

### S6 — idempotency

Second `runner.run` on the same file: `executed: []`, `skipped: []`, all eighteen in `already` — the
receipts, not the postconditions, are what stopped it, which is the cheaper of the two guards and
the one the runner reaches first. Fingerprint after: `overall` `677f9b83…910a`, textual and
structure hashes and table count all unchanged from S4.

### S7 — C13 survival

A copy of the rehearsed file was taken through SQLite's online backup API (not `shutil.copy2`) to
`…/scratchpad/s7/surgical_autonomy/review.db`, verified equal to the S6 overall hash, and then
opened by `ReviewDatabase("surgical_autonomy", data_root=…)` — the production constructor, which
runs `_SCHEMA`, `ensure_adjudication_table`, `_SIMPLE_MIGRATIONS`, the `evidence_spans` CHECK check
and `runner.run`.

* tables after construction: **31**
* `audit_adjudication` present: **0**
* `overall_sha256` after construction: **unchanged** from S6

**C13 is closed on this file.** The 1 → 0 → 1 that R32 measured does not recur, because the same
commit that added the DROP removed `_AUDIT_ADJUDICATION_TABLE` from `ensure_adjudication_table`. A
full `ReviewDatabase` open — the exact operation that used to put the table back — now leaves the
database bit-for-bit as the migration left it.

### S8 — append-only

On a second throwaway copy (deleted afterwards; confirmed absent):

```
UPDATE: refused — IntegrityError: paper_events is append-only: correct by appending an event
DELETE: refused — IntegrityError: paper_events is append-only: correct by appending an event
```

019 drops both triggers and restores them inside its own transaction; they are back, and they bite.

---

## 5. Prediction for phase ii (the live write)

Stated from S3/S4 and I12, before the write.

**Identical to rehearsal-after — all thirty tables except `schema_migrations`.** The rehearsal
target is the live content (S1: the backup's verified fingerprint equals live's), 018 and 019 are
deterministic and change no value, and neither reads a clock into a row. Every content hash in §3's
POST column should appear unchanged on live-after, `audit_adjudication` absent, table count **31**.

**Different — `schema_migrations`, and only in two cells.** Both sides will hold 18 rows with the
same eighteen `migration_id`s, the same eighteen `file_sha256`s, the same `mode`, `runner_version`
and `note`. The sixteen pre-existing receipts are copied from live and identical. The two new ones
will differ in `applied_at` alone — the rehearsal's read `2026-09-22T16:43:57.268345+00:00` and
`…16:43:57.282345+00:00`, and the live write will stamp its own moment. So:

* **`overall_sha256` of live-after will NOT equal `677f9b83…910a`**, because `overall` is a hash
  over the per-table hashes and one of them carries those timestamps. Do not use the rehearsal's
  overall hash as a live acceptance value — use the thirty per-table hashes and read
  `schema_migrations` by its columns.
* **`schema_hash_sha256` of live-after IS expected to equal `adc985a2af7cba9c103997c026d9f02d2c02e39304d44c62af2c3bd83572d2b0`** — schema text carries no timestamp.
* **`schema_structure_hash` of live-after IS expected to equal `2b92ac41f333dda269d35d249c822a32b0a768c6fb69fc79030ed85729955769`**, equal in turn to the fresh database's.
* **The 019 read-back will pass on live**, the `to_state` CHECK naming `parse_failed`.
* **G3's textual half will fail on live exactly as it failed on the copy**, for the thirteen objects
  of F1. That needs a ruling before the write, not during it.

Wall time for the live write should be **well under a second** — the rehearsal ran the full pair in
0.323 s against the same 101,978,112-byte file — but the backup that precedes it is the slow step.

---

## 6. Artifacts left on disk

| what | path | note |
|---|---|---|
| rehearsal target (018 + 019 applied) | `data/surgical_autonomy/review.db.bak-readers-01-phase3-rehearsal-20260922-164331` | gitignored; durable |
| P4 fresh database (G3 reference) | `/tmp/claude-1000/-home-ankitsarin-projects-evidence-engine/a3614266-5ffc-46e3-8b5a-023aca84653f/scratchpad/p4fresh/fresh_p4/review.db` | **scratchpad — survives the session but not a reboot** |
| P4 fresh fingerprint | `…/scratchpad/p4fresh/fresh_fingerprint.json` | same caveat |
| PRE / POST / POST2 fingerprints | `…/scratchpad/{PRE,POST,POST2}.json` | same caveat |
| S7 constructed copy | `…/scratchpad/s7/surgical_autonomy/review.db` | same caveat |
| S8 throwaway copy | — | deleted, confirmed absent |

Both retained databases are deleted under a later ruling, not by this task. The fresh database is
cheap to rebuild (`ReviewDatabase` on an empty directory, ~0.9 s) and its hashes are recorded above,
so the scratchpad's impermanence costs nothing; the rehearsal target is not cheap to rebuild and is
in a durable location.

## 7. Not done, deliberately

The live write; the phase-ii restore-point backup; any new fingerprint record for live; the
refactor-plan closure paragraph; the primer; retirement of the effective-result-02 restore point;
C13's remaining self-provisioned tables. No engine code, test or migration module was edited — F1
and F3 are reported, not repaired.
