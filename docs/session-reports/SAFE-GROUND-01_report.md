# SAFE-GROUND-01 — WAL-safe backup, exercised restore, standing fingerprint

**Date:** 2026-09-20. **Task type:** repair + tooling. No model call. **The live `review.db` was
never opened for write.**
**Harness session:** `bd7ea59e-96e8-4b4d-a2a1-6f6adb1ecb93`.
**HEAD at start:** `b8a83b1` (clean, level with origin). **Python 3.12.3 · SQLite 3.45.1.**

**Tags.** **MEASURED** = produced here by running code. **READ** = quoted from a file at this
HEAD. **INFERRED** = a reading or a cause.

---

## Phase 1 read-out

### 1. Startup verify and the fingerprint of record

| check | EXPECTED | measured | |
|---|---|---|---|
| HEAD / tree / origin | `b8a83b1`, clean, level | `b8a83b1`, porcelain empty, `0 0` | ✅ |
| standard gate, five chunks (`find`, not glob) | 2,329 / 17 | 504+594+377+438+416 = **2,329**; deselects 0/0/10/6/1 = **17** | ✅ |
| `review.db` | 101,978,112 B @ 2026-09-11 02:00:52.636956943 UTC | identical | ✅ |
| `tests/test_eligibility.py` | 91 passed, 14 pinned hashes | 91 passed; 14 unique 64-hex constants | ✅ |

**I1 — CONFIRMED.** No fingerprint script existed in the repository: `git ls-files | grep -i
fingerprint` returned only the committed **JSON record**, and no `.py` under `engine/`,
`scripts/`, `analysis/` or `tests/` contained the word. The tool was therefore written fresh to
the canonical serialization stated in that record, and **reproduces it exactly** —
`schema 1d6af8b9…3a18d`, `overall f376562e…39e00`, 24/24 tables, zero per-table differences.

**G5 baseline** (MEASURED at open; see the close table for the same values at close):

| file | bytes | mtime (UTC) |
|---|---:|---|
| `review.db.bak-pre-run6-cleanup-20260315194955` | 47,759,360 | 2026-03-15 19:49:55.844957475 |
| `review.db.bak-pre-sonnet-cleanup-20260316-165020` | 60,252,160 | 2026-03-16 15:49:56.919871746 |
| `review.db.pre_rename_backup` | 18,907,136 | 2026-03-12 17:35:42.937690802 |

### 2. Versions, API availability, and I3 proven

```
python      : 3.12.3  (.venv)
libsqlite   : 3.45.1
Connection.backup available : True
VACUUM INTO available       : True
```

**I2 — CONFIRMED**, so the CONSTRAINTS STOP did not fire.

**I3 — PROVEN, not assumed.** On temp databases in M2's exact shape (WAL on, source connection
held open, a committed INSERT, no checkpoint), MEASURED:

| mechanism | committed WAL row in the backup? | sidecars written? | checkpointed the source? |
|---|---|---|---|
| `Connection.backup()` from a second `mode=ro` connection | **yes** | none | no — live `-wal` unchanged at 12,392 B |
| `VACUUM INTO` from a second `mode=ro` connection | **yes** | none | no — unchanged |
| **control: today's `shutil.copy2`** | **`ERROR: no such table: t`** | — | — |

`VACUUM INTO` worked from a **read-only** source connection, so neither mechanism needs write
access to the source. The control reproduces M2.

### 3. Do the three callers hold an open write transaction at the backup call?

**No — all three back up before their first write, and none has an open transaction at that
moment.** `ReviewDatabase`'s connection uses the pysqlite default `isolation_level=''`, which
begins a transaction only on DML, so the SELECTs preceding the backup in caller 3 open nothing.

| caller | anchor | position |
|---|---|---|
| `ReviewDatabase.cleanup_orphaned_spans` | `auto_backup(self.db_path, "pre-orphan-cleanup")` | first statement in the method |
| `ReviewDatabase.reset_for_reextraction` | `auto_backup(self.db_path, "pre-reset")` | immediately before `self._conn.execute("BEGIN")` |
| `extraction_cleanup.cleanup_stale_extractions` | `auto_backup(db.db_path, "pre-cleanup")` | after the dry-run return, immediately before `conn.execute("BEGIN")` |

**It did not change the repair; it changed the call sites.** Passing a *path* is correct for all
three **only because of that ordering, which nothing enforces** — and caller 3 calls
`db.admin_reset_status(...)` on the same connection afterwards, so a future caller that backed up
mid-sequence would silently get a stale snapshot. INFERRED: the ordering is a convention, not a
guard. All three now pass their **connection**, which makes "back up what this connection can
see" structural rather than implicit.

### 4. Proposal — as ruled

Mechanism `Connection.backup()`; fingerprint tool at `engine/tools/db_fingerprint.py` with one
hashing implementation shared by the backup's self-check; `restore()` as a function with no CLI,
open-target check by exclusive lock; `temp_db` fixture rewritten to WAL with the connection held
open. Ruled additionally: **the dataclass return type with all three call sites updated**
(decision 2); **no live-DB pin in the test suite** (decision 3); **the session scratchpad after a
`df` check** (decision 4).

**`df` (decision 4):** `/dev/nvme0n1p2` on `/`, **3,320,951,599,104 B available = 3,092.88 GiB**,
far above the 1 GiB floor. The scratch directory is outside the repository and outside any synced
folder.

---

## What was built

### `engine/tools/db_fingerprint.py` — the standing check

READ, the module's own statement of why it exists:

> Size and mtime cannot see a committed-but-uncheckpointed write: under WAL the main file is
> untouched until a checkpoint runs, so a database can gain rows without either changing. This
> module reads the rows.

and of why `immutable=1` is refused:

> `immutable` is a promise to SQLite that the file cannot change while it is open, and
> `review.db` is a live database, so the promise would be a lie and the reader could silently see
> a torn page.

CLI: `python -m engine.tools.db_fingerprint <db> [--out <json>] [--compare <json>]`. `--compare`
exits **1** on any difference in content and prints each one; **2** on a missing file; **0** when
identical. The record's shape matches the DISCOVERY-01 record field for field, with one change
that does not touch a hashed byte: the canonical-serialization statement now names the separators
as **`U+001F`** and **`U+001E`** rather than embedding the raw control characters, which the
committed record did and which made it unreadable when printed. **The hashes are unaffected —
proven by the tool reproducing `f376562e…39e00` on the unchanged live database.**

`compare()` ignores a fixed list of fields that describe the *reading* rather than the content
(path, mtime, wall time, URI, generated timestamp), because a backup legitimately differs from
its source in every one of them and none of that is a difference in what the database holds.

### `engine/utils/db_backup.py` — WAL-aware backup, and a restore

`auto_backup(db_path_or_connection, reason) -> BackupResult`, a frozen dataclass carrying
`.path` and `.fingerprint` (plus `.overall_sha256` and `.table_count`). The backup is taken
inside the same read snapshot the source fingerprint is computed under, so the two describe **one
state of the database rather than two moments**. On any content difference the backup file is
deleted and `BackupVerificationError` is raised naming every table that differed — READ:

> **A backup is not a backup until it has been read back.** … a mismatch deletes the file and
> raises rather than leaving something that looks like a safety net.

`restore(backup_path, target_path, *, expected_fingerprint=None)` writes to a sibling
`.restore-tmp-<ts>` and moves it into place with `os.replace`, so an interrupted restore cannot
leave a half-written database at the target; it then removes any stale `-wal`/`-shm` beside the
target, because a new main file with an old WAL is a corrupt pair. It verifies twice — the staged
file against the backup, and the final target against the backup — and against
`expected_fingerprint` when given. **No CLI**, by ruling: the one operation that overwrites a
database should not be reachable by tab-completing a shell history entry.

The open-target check is an exclusive lock, not a `/proc/*/fd` scan — READ:

> it asks SQLite the question that actually matters ("can I have this database to myself?")
> instead of a proxy for it.

MEASURED on temp databases, it refuses against **an idle open connection, an open read
transaction and an open write transaction alike**, and succeeds only once every connection has
closed. The idle case is the one that matters: it is `ReviewDatabase`'s shape.

### 🔴 One finding, from the new tests, against the first draft

The first implementation claimed the backup needed no `-wal`/`-shm` sidecars. **That was false as
written, and `test_backup_includes_uncheckpointed_wal_rows` caught it.** The backup API copies the
source's header — journal mode included — so a backup of a WAL database **is** a WAL database, and
merely *opening* one creates `-shm` and `-wal` beside it. Since `auto_backup` writes beside the
live database, every backup would have littered `data/<review>/` on first read.

**That is the documented cause of global open item #9** ("Capturing WAL `.bak` files leaves
`-shm`/`-wal` sidecars in a live application directory, untracked"). The fix is in the code, not
the test: the destination is switched to `PRAGMA journal_mode=DELETE` before it is closed, and
`_remove_sidecars` cleans up both on success and on the verification-failure path. A backup is now
one file you can copy anywhere and open. The test asserts the property **after** reading the
backup, which is where the first draft's assertion was too early to see the problem.

This does not close #9 — the three pre-existing `.bak` files and their sidecars are untouched and
out of scope — but **the new code no longer re-creates its cause.**

### Tests (B4) — 14 new, all reproducers, all kept

| test | what it pins |
|---|---|
| `test_backup_includes_uncheckpointed_wal_rows` | M2's shape: committed WAL row present; no sidecars **after reading**; journal mode `delete`; source not checkpointed |
| `test_backup_production_shape` | a real `ReviewDatabase` (WAL, connection held) with one paper, one extraction, one span — all three tables present and populated |
| `test_backup_fingerprint_mismatch_raises` | a mismatched backup raises and **leaves no file**, sidecars included |
| `test_restore_roundtrip` | backup → restore to a new path → three-way fingerprint equality, rows readable |
| `test_restore_refuses_open_target` | refused against an **idle** open connection; target untouched; no staging file left; and the same restore **succeeds once the holder closes** |
| `test_backup_returns_a_verified_result` | the return value carries the proof, and the dataclass is frozen |
| `test_backup_creates_valid_copy`, `test_backup_filename_format` | the two original assertions that still hold |
| `test_fingerprint_reproducible` | unchanged → identical; one INSERT → **exactly that table** differs, schema hash unchanged, `beta` untouched |
| `test_fingerprint_sees_uncheckpointed_wal_rows` | size and mtime unchanged while the overall hash moves — the reason the tool exists |
| `test_fingerprint_is_read_only_and_never_immutable` | `mode=ro`, no `immutable`, one read transaction, `U+001F` in the statement |
| `test_fingerprint_from_a_supplied_connection_sees_that_connection` | a connection's own view, uncommitted work included |
| `test_encode_cell_covers_every_storage_class`, `test_compare_ignores_volatile_fields_and_names_real_ones`, `test_cli_compare_exits_nonzero_on_a_difference`, `test_cli_reports_a_missing_database`, `test_without_rowid_tables_order_by_primary_key` | the encoding, the comparison, the CLI contract, WITHOUT ROWID ordering |

**One original assertion was dropped rather than kept: `test_backup_file_size_matches`.** B4
permitted the three to stay; that one cannot, because it asserted
`backup.stat().st_size == original.stat().st_size`, which was true only because `copy2` copies
bytes. It is not a property of a page-level backup and would have had to be deleted or weakened
the first time the two legitimately differed. (As it happens the drill below produced a
byte-identical size, which is exactly why keeping the assertion would have been dangerous: it
would have passed for the wrong reason.)

### The three call sites (ruling 2)

All three previously discarded the return value. Each now binds it and logs the proof:

```python
        backup = auto_backup(self._conn, "pre-orphan-cleanup")
        logger.info(
            "Pre-cleanup backup verified: %s (%d tables, overall=%s)",
            backup.path.name, backup.table_count, backup.overall_sha256[:16],
        )
```

and likewise `"pre-reset"` in `reset_for_reextraction` and `"pre-cleanup"` in
`cleanup_stale_extractions` (which passes `conn`, the connection its deletions run on). The
filename convention is unchanged: `.bak-<label>-<timestamp>`, `%Y%m%d-%H%M%S`.

### `docs/inventory/entry_points.{md,json}`

Regenerated. `tests/test_inventory.py::test_committed_inventory_is_in_sync_with_the_tree` failed
on the first full-gate run with *"engine/tools/db_fingerprint.py: new file, not in the committed
inventory"* — **the guard working exactly as designed**, and `--write` is its own documented
remediation. `--check` now reports *inventory in sync*.

---

## The drill (B5) — on copies only

The live database was **read**, never written, never restored over.

```
(a) fingerprint the live DB                  0.360 s
    auto_backup(live, 'safeground-drill')    0.863 s
    move the backup out of data/             0.000 s
(b) fingerprint copy_a                       0.349 s
(c) restore(copy_a, copy_b)                  1.145 s
(d) fingerprint copy_b                       0.349 s
```

| comparison | result |
|---|---|
| live vs copy_a (the backup) | **IDENTICAL** |
| fingerprint of record vs copy_a | **IDENTICAL** |
| copy_a vs copy_b (the restore) | **IDENTICAL** |
| fingerprint of record vs copy_b | **IDENTICAL** |

All three carry `overall f376562e095cfbcbf23ca997f31375feba340f74ed7a14ffe26e42cc63839e00`, and
all three are 101,978,112 B. **G3 PASS.**

`auto_backup` names its output beside its source, so step (a) wrote into `data/surgical_autonomy/`
and the drill moved it to scratch immediately; **no sidecar was created there** (the journal-mode
switch), and no `.bak-safeground-*` or `.restore-tmp-*` remains. **(e)** both copies deleted,
**203,956,224 bytes freed (194.5 MiB)**; the scratch drill directory is empty.

Live database after the drill: **101,978,112 B @ mtime_ns 1789092052636956943 — identical to
before**; `-wal` **0 B**.

---

## Appendix B addition — to paste into plan v50

> **Fingerprint at session open and close.** The integrity check on `review.db` is its content
> hash, not its size and mtime: under WAL a committed write leaves the main file untouched until
> a checkpoint runs, so size and mtime can both be unchanged across a write that happened. Run,
> at open and again at close:
>
> ```bash
> python -m engine.tools.db_fingerprint data/<review>/review.db \
>     --compare docs/session-reports/discovery-01/review_db_fingerprint_<UTC>.json
> ```
>
> Exit **0** = identical. Exit **1** = the record and the database differ, and every difference
> is printed by table. Exit **2** = a file is missing. A session that **intends** to change the
> database writes a new record with `--out` as part of the change, commits it, and quotes the new
> `overall_sha256` in its closeout; the old record is superseded, never edited. Report the `-wal`
> size at both readings — it is printed in the summary — because a non-empty `-wal` is the
> condition under which size and mtime are least trustworthy.
>
> **Restore procedure.** There is no CLI, deliberately. To restore:
>
> 1. **Close every connection to the target**, including any `ReviewDatabase` that owns it.
>    `restore` refuses otherwise, by taking an exclusive lock, and names the target in the
>    refusal.
> 2. Run, from the repository root:
>
>    ```python
>    from engine.utils.db_backup import restore
>    restore("<path to .bak>", "data/<review>/review.db")
>    ```
>
>    Add `expected_fingerprint=json.load(open("<record>.json"))` when restoring to a known state;
>    a backup that does not match it is refused before anything is written.
> 3. `restore` writes to a sibling temporary file and `os.replace`s it into position, removes any
>    stale `-wal`/`-shm`, and verifies the result against the backup. It returns the restored
>    database's fingerprint — **record that value in the closeout**, and write it out with
>    `--out` so the next session's `--compare` has something current to compare against.
>
> **Before any migration.** Take a backup with `auto_backup` and record `result.overall_sha256`
> and `result.path`. The backup is verified against its source before `auto_backup` returns, so a
> returned `BackupResult` is a proof, not a filename.

---

## Gate table

| gate | requirement | measured | |
|---|---|---|---|
| **G1** | Phase 1 read-out delivered and acknowledged before any file under `engine/` changed | delivered; ruling received; first `engine/` write followed it | ✅ |
| **G2** | All B4 tests pass; standard gate passes; new count stated; deselect sum still 17 | 504 + 608 + 377 + 438 + 416 = **2,343** (**+14**); deselects 0/0/10/6/1 = **17** | ✅ |
| **G3** | live fingerprint == backup fingerprint == restored fingerprint | all three `f376562e…39e00`, four comparisons IDENTICAL | ✅ |
| **G4** | live `review.db` byte- and mtime-identical at open and close; fingerprint identical; `-wal` reported at both | 101,978,112 B @ 2026-09-11 02:00:52.636956943 UTC at both; `--compare` exit **0**; `-wal` **0 B** at open and **0 B** at close | ✅ |
| **G5** | the three existing `.bak` files untouched | sizes and mtimes identical at open and close (table above; repeated below) | ✅ |
| **G6** | the fingerprint tool reproduces the Part A overall hash on the unchanged live DB | `f376562e095cfbcbf23ca997f31375feba340f74ed7a14ffe26e42cc63839e00`, 24/24 tables, zero differences | ✅ |

**G5 at close** — `…bak-pre-run6-cleanup-20260315194955` 47,759,360 B @ 2026-03-15
19:49:55.844957475 · `…bak-pre-sonnet-cleanup-20260316-165020` 60,252,160 B @ 2026-03-16
15:49:56.919871746 · `…pre_rename_backup` 18,907,136 B @ 2026-03-12 17:35:42.937690802. Unchanged.

`review.db-shm`'s mtime moved (04:48 → 21:56) as every read-only open moves it; that is the
convention's excluded file and the ruling confirmed it needs no action.

---

## What this does not claim

* **Open item #9 is not closed.** The three pre-existing `.bak` files and their `-shm`/`-wal`
  sidecars are untouched, as scoped. What changed is that the new code no longer produces that
  population: a backup taken from here on is a rollback-journal database and creates no sidecar
  when read.
* **No migration was performed and the live database was not written.** This task makes the
  database safe to change; it changes nothing in it.
* `VACUUM INTO` was proven equivalent on the WAL question and then **not used**, because it
  repacks — so a fingerprint mismatch could not distinguish "lost rows" from "rewritten file".
  `Connection.backup()` is page-level, which is why the drill's backup is byte-identical in size
  to its source.
* The timestamp in the backup filename is **local time**, unchanged from the previous
  implementation because B1 pinned the convention. It is ambiguous across a DST transition. Noted,
  not fixed.
