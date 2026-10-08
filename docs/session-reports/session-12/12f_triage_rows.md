# 12f — triage rows in full (the evidence behind `12f_triage.md`)

One section per verified item, as written by the read at HEAD `297667c17979a8aeba1d02330740b24f8f28e885`. Each carries: source, presence at HEAD, evidence level, the evidence itself (code quoted by content anchor, or a reproducer under `12f_repro/` with its output), overlap with existing inventory rows, proposed class, owning state, minimum loud-failure fix, full fix, acceptance test, size and PI-decision flag. IDs here are the assessments' own (`F01`–`F17` from B; `A1`–`A11` as numbered by the 12f brief); `12f_triage.md` prefixes them `B-` and `A-` because `A7`, `A16`, `F15` and `F16` already name other things in the plan. Reproducer paths written as `~/scratch/12f/repro/…` are committed as `12f_repro/…`.

The classes and sizes in these sections are each reader's proposal; where `12f_triage.md` proposes a different class it says so, and the triage table governs.

### F01 — Parsed text can have a committed reference but no final file (commit precedes rename in `parse_pdf`)
- **Source:** outside B
- **Present at HEAD:** yes (main claim) · sub-claims: (a) commit-then-rename order — yes; (b) handler rolls back after the commit and deletes the temp file — yes; (c) `test_atomic_write_no_temp_on_db_failure` covers a pre-commit INSERT failure only — yes; (d) "version allocation unserialized / can overwrite an existing version" — partial (see Differences)
- **Evidence level:** reproduced
- **Evidence:**
  - Order, `engine/parsers/pdf_parser.py::parse_pdf` (the block under `# Atomic write: temp file → DB commit → rename`): `tmp_path.write_text(markdown)` → `INSERT INTO full_text_assets …` → `record_parsed_text(db._conn, … path=md_path, … data=tmp_path.read_bytes(), …)` → `UPDATE papers SET pdf_content_hash` → `_insert_attempts(…)` → `db._conn.commit()` / `attempts_committed = True` → `# DB committed — now rename temp to final (atomic on POSIX)` / `tmp_path.rename(md_path)`.
  - Handler, same block: `except Exception:` / `if tmp_path.exists(): tmp_path.unlink()` / `db._conn.rollback()` / `raise`. It does not distinguish before-commit from after-commit; after the commit the rollback is a no-op and the unlink destroys the only copy of the text. Both rows carry the FINAL path (`# Record in database (with final path, not temp)`), which never comes to exist. No fsync of the file or directory anywhere in the block.
  - Reproducer `~/scratch/12f/repro/F01_post_commit_rename.py` (synthetic DB under `~/scratch/12f/g1/f01`, docling stubbed, OCR/vision stubbed to raise, `Path.rename` made to raise `OSError` for `*.md.tmp`); run: `cd ~/scratch/12f/g1 && PYTHONPATH=/home/ankitsarin/projects/evidence-engine .venv/bin/python ~/scratch/12f/repro/F01_post_commit_rename.py`; output in `F01_post_commit_rename.out`:
    - `after injected rename failure: {'full_text_assets': 1, 'parsed_text_refs': 1, 'parse_attempts': 1} files: []` — rows committed (confirmed from a second `mode=ro` connection), neither `.md` nor `.md.tmp` on disk.
    - `resolver read: ParsedTextMissing`.
    - `retry parse_pdf (no force): version 1 parsed_markdown None attempts 0 | files: []` — the same-hash short-circuit (`"Paper %d already parsed (v%d, same hash) — skipping"`) keys on the committed `full_text_assets` row, so an ordinary retry does not repair it.
    - **Worse than B states:** `parse_all_pdfs` on that state: `stats {'parsed': 1, 'docling': 1}`, `status after parse_all_pdfs: PARSED | files: []`, resolver still `ParsedTextMissing`. The driver's `accepted = next((a for a in result.attempts if a.accepted), None)` is `None` for a short-circuit return, so it falls to `db.update_status(pid, "PARSED")`. The first run counts the paper `failed` (and, under a run, writes a `parse_failed` event via the generic `except Exception` branch); the NEXT run advances it to PARSED with no text and no error.
    - `force re-parse: version 2 | files: ['1_v2.md']` — only `force=True` (i.e. `reparse_papers`) recovers, as a new version; v1's asset, ref and `accepted=1` attempt rows remain, describing a file that never existed.
  - Stated guarantee not honoured: ruling R99 in `docs/plan/ENGINE_REFACTOR_PLAN.md` — "The same number is written to the file name, the full_text_assets row and the parsed_text_refs row in one transaction"; the code comment `# D8: the reference the resolver reads, in the same unit of work as the asset row, hashed from the bytes actually written`; the comment `# Atomic write: temp file → DB commit → rename`; `docs/architecture/pipeline.md` step 8 "Atomic write: temp file → DB commit → rename". The three ROWS are one transaction; the FILE is outside it and published after it. `record_parsed_text`'s own docstring concedes the gap: "text already written (or about to be renamed)".
  - Test: `tests/test_pdf_parser.py::test_atomic_write_no_temp_on_db_failure` installs `CREATE TRIGGER block_fta_insert BEFORE INSERT ON full_text_assets … RAISE(ABORT, 'simulated commit failure')` — the first INSERT fails, nothing is ever committed. Its docstring ("If DB commit fails after temp file is written") describes the pre-commit case only; no test fails the rename or kills the process after the commit.
- **Overlap:** none given. Found: D8 (new parse writes no ref — CLOSED; its fix is the `record_parsed_text` call that created this window's ref half), D15 (short-circuit read unverified — CLOSED by returning `parsed_markdown=None`; that closure is what lets the short-circuit return success for a missing file), D13/R99 (version source). None covers the commit/rename order. No existing row found for this defect.
- **Proposed class:** 1 — committed provenance (asset row, ref with a sha256, `accepted=1` attempt) asserts an artifact that was never published, and the next ordinary parse run moves `papers.status` to PARSED silently. Mitigating: every READ of the text is loud (`ParsedTextMissing`), so no wrong extracted value results; if the architect weighs only that, it is class 2.
- **Owning state:** pre-tag (R512)
- **Minimum loud-failure fix:** in `parse_pdf`'s same-hash short-circuit, refuse (raise, as R99's `FileExistsError` guard does) when the stored version's recorded file is absent — so a retry or the next `parse_all_pdfs` cannot report success/PARSED over a dangling ref. One guarded `exists()` on the ref's path; opens no file content.
- **Full fix:** publish before commit — write tmp, flush+fsync, `os.link`/rename to the final name (still refusing an existing target), fsync the directory, THEN insert asset/ref/attempts and commit; on a DB failure after publication unlink the file just published (a crash leaves an orphan file, which R99's existing `FileExistsError` guard already turns into a loud refusal). Make the handler phase-aware so it never unlinks after a commit. Use a per-call unique tmp name.
- **Acceptance test:** inject a failure (i) before commit, (ii) at the rename/publish step, (iii) between publish and commit; after each, every `parsed_text_refs` row resolves through `read_parsed_bytes` to its recorded hash, no `*.tmp` remains, and a subsequent `parse_all_pdfs` either parses the paper to a readable version or refuses — never `PARSED` with `ParsedTextMissing`.
- **Size:** S — one function, mechanism read and reproduced; reorder + handler + the short-circuit guard + three tests. (M only if directory-fsync/crash-kill simulation is required by the acceptance test.)
- **PI-decision flag:** yes — after reordering, a crash leaves an orphan `{id}_v{n}.md` that R99's guard refuses on the next parse (a run fault, R231): keep that as a manual "record or retire" stop, or auto-adopt an orphan whose bytes hash-match a fresh parse?
- **Differences from the original / the brief's note:**
  - B understates the consequence: it says later reads raise; it misses that the next `parse_all_pdfs` marks the paper PARSED (reproduced) and that a non-forced retry short-circuits instead of repairing.
  - "Never overwrite an existing version" is already largely honoured at HEAD: `parse_pdf` raises `FileExistsError` ("refusing to parse as v{version} — … already exists but no parsed_text_refs row records it (R99)") before any parser runs, and `parsed_text_refs` carries `UNIQUE (paper_id, parsed_text_version)` (migration 021; confirmed in the scratch DB's DDL). What remains is narrower and CONDITIONAL on two concurrent parses of the same paper (not reproduced; no in-tree path runs them concurrently that I established): `next_version` is a bare SELECT with no `BEGIN IMMEDIATE`, both callers compute the same version and the SAME temp name (`md_path.with_suffix(".md.tmp")`); the loser's `IntegrityError` handler runs `tmp_path.unlink()` on the shared temp, which can delete the winner's temp between the winner's commit and rename — the same dangling ref by another route. `Path.rename` also replaces an existing target silently if one appears after the `target.exists()` check.
  - "Crash between commit and rename" (process death) leaves the `.md.tmp` on disk — recoverable by hand, unlike the exception path, where the handler deletes it. B's "the temporary file may have been the only recoverable copy" is true for the exception path only.
  - Adjacent, not chased: `docs/architecture/pipeline.md` step 7 still says the all-empty case "raises `ValueError`"; the code raises `ParseFailed` (class 3).


---

### F02 — The migration refusal (`PendingMigrations`) happens after constructor-side writes; a populated database with no receipts is "fresh"
- **Source:** outside B
- **Present at HEAD:** yes — (a) constructor writes precede the refusal: yes; (b) populated database with no receipts treated as fresh: yes; (c) `_run_migrations`' `finally` reopens a connection after a runner failure: yes
- **Evidence level:** reproduced (all three parts, on a synthetic database)
- **Evidence:**
  - (a) code order, `engine/core/database.py::ReviewDatabase.__init__`: `root.mkdir(parents=True, exist_ok=True)` + three `(root / "…").mkdir(exist_ok=True)` → `sqlite3.connect` → `PRAGMA journal_mode=WAL` → `self._conn.executescript(_SCHEMA)` / `self._conn.commit()` → `self._run_migrations()`. In `_run_migrations`: `ensure_adjudication_table(self._conn)` (two `executescript` of `CREATE TABLE IF NOT EXISTS` + `commit`, `engine/adjudication/schema.py`) → `for sql in _SIMPLE_MIGRATIONS:` `execute` + `commit` (inline `ALTER TABLE … ADD COLUMN`) → `self._conn.executescript(_VERIFICATION_TABLE)` → the `evidence_spans` rebuild `if row and "contested" not in row[0]: self._conn.executescript(_EVIDENCE_SPANS_REBUILD)` → only then `self._conn.close()` / `result = runner.run(self.db_path)`. `PendingMigrations` can only come from that last call.
  - (b) code order, `engine/migrations/runner.py::run`: `have = receipts(conn)` / `fresh = (not existed_before) or (not have)` / `if not fresh and not apply_pending:` … `raise PendingMigrations`. Freshness is decided from the receipts table alone; nothing inspects `sqlite_master` or any row count. Before that, `ensure_receipts(conn)` (`executescript(_RECEIPTS_DDL)`; `commit`) creates `schema_migrations` in a database that lacked it.
  - (c) code order, `_run_migrations`: `self._conn.close()` / `try: result = runner.run(self.db_path)` / `finally: self._conn = sqlite3.connect(str(self.db_path))` … `PRAGMA journal_mode=WAL`. The `finally` runs on the exception path too, on an object `__init__` never returns.
  - Reproducer `~/scratch/12f/repro/F02_refusal_after_writes.py` (+ `F02_recreated_shape.out`); run `cd ~/scratch/12f/g1 && PYTHONPATH=/home/ankitsarin/projects/evidence-engine .venv/bin/python ~/scratch/12f/repro/F02_refusal_after_writes.py`. Arrangement, exactly: database built by `ReviewDatabase("syn", data_root=~/scratch/12f/g1/f02)` (fresh → 18 schema receipts, 004–022 less the three data migrations), 5 papers added; then with plain sqlite3: `DELETE FROM schema_migrations WHERE migration_id LIKE '022%'` (the 022 receipt row ONLY — 022's schema stays applied; the runner decides "pending" from receipts alone, so this is the nearest faithful equivalent), `DROP TABLE review_runs`, `PRAGMA journal_mode=DELETE`, and the `vector_store/` subdirectory removed. Output:
    - `(a) BEFORE refused open: {'review_runs': 0, 'tables': 35, 'journal_mode': 'delete', 'receipts': 17} | vector_store dir: False`
    - `(a) raised PendingMigrations: refusing to apply pending migrations to a database that already carries receipts (R222/I16): 022_run_kinds_and_a ...`
    - `(a) AFTER  refused open: {'review_runs': 1, 'tables': 36, 'journal_mode': 'wal', 'receipts': 17} | vector_store dir: True` · `content fingerprint changed: True` (`engine.tools.db_fingerprint`).
    - B's figure confirmed exactly: `review_runs` before = 0, after = 1. Journal mode before = `delete`, after = `wal` (a persistent header change made by a refused open). A directory was created.
    - The recreated `review_runs` is `_SCHEMA`'s floor shape — `extraction_hash` NOT NULL, no `codebook_hash`/`codebook_sha256` — while receipts 012 and 013 (which add those columns and lift that NOT NULL) are still present: a schema the receipts mis-describe (`F02_recreated_shape.out`).
    - `(c) ReviewDatabase objects alive after the raise: 2 | their _conn usable: [True, False]` — the refused object (kept alive by the exception's traceback) holds a live, usable WAL connection; the `False` is the builder object the script had closed itself.
    - `(b)` same file with `DELETE FROM schema_migrations` (0 receipts, 5 papers, 35 tables): `constructed without refusal`; afterwards `receipts: 18`, every schema migration 004–022 recorded `executed` against a populated database, `journal_mode: wal`.
  - (b) consequence, code order (not exercised with rows): the route so entered includes the table rebuilds of 013/018/019/020/021/022 and `engine/migrations/018_cloud_shape_and_audit_adjudication.py::run_migration`'s unconditional `conn.execute("DROP TABLE IF EXISTS audit_adjudication")` — no row-count guard precedes it (`already_applied` tests only existence), so a receipt-less legacy database holding human audit rows would lose them on an ordinary open.
  - Stated guarantees not honoured on the constructor path: `runner.PendingMigrations` docstring — "This refusal is raised before any transaction opens and before any write."; `runner.run` docstring — "Raised before any transaction opens and before any write; the database is untouched." Both are true of `runner.run` called directly on a receipt-bearing database and false of the only in-tree route by which ordinary commands reach it (`ReviewDatabase.__init__`). Inventory row I16's closure text — "this guard is what refuses any `ReviewDatabase("surgical_autonomy")` construction" — is true of the migration and silent on the writes that precede the refusal. `engine/migrations/README.md` "A **fresh** database (no `schema_migrations` table, or the table exists with zero rows)" states (b) as the intended definition.
- **Overlap:**
  - C12 — CONFIRMED, F02 is broader. C12 is exactly the `ensure_adjudication_table` half of (a) ("`CREATE TABLE IF NOT EXISTS` … on **every** `ReviewDatabase` construction, outside the receipted runner"; its measured `audit_adjudication` 1 → 0 → 1 is the same recreate-after-drop this reproduction shows for `review_runs`). C12 does not name `_SCHEMA`, `_SIMPLE_MIGRATIONS`, `_VERIFICATION_TABLE`, the `evidence_spans` rebuild, the WAL switch, the mkdirs, or the ordering against the refusal.
  - I16 (R222/R222a) — CONFIRMED as the row whose closure F02 qualifies; a different defect. I16 was "a pending migration is applied by any construction"; that IS closed (022 was not applied in the reproduction; receipts stayed 17). F02(a) is the residue: the refusal is not write-free. F02(b) is R222's own definition of fresh ("no receipts"), ruled and documented, not a regression — B is challenging the ruling.
  - C37 — CONFIRMED as family, different site (`ensure_workflow_table` writing inside gate reads; "C12's family: a schema no receipt describes, written by a reader"). Other members seen, not chased: `engine/cloud/schema.py` and `engine/adjudication/import_extraction_entry.py` also carry `CREATE TABLE IF NOT EXISTS`.
- **Proposed class:** 1 — an open that is refused (or an ordinary open of a receipt-less populated database) changes schema, journal mode and receipts, leaving a database whose receipts mis-describe its shape; (b) can run destructive rebuilds/drops with no backup or adoption step. (On a healthy, fully-receipted database the inline DDL is a no-op, which is why this is latent on live.)
- **Owning state:** pre-tag (R512). Flag: the full fix is S3c ("all schema into numbered migrations", the owner already recorded for C12 and C37), which the plan places in a later session — the minimum fix below is the pre-tag part.
- **Minimum loud-failure fix:** in `ReviewDatabase.__init__`, when `review.db` already exists, run a read-only pre-flight (`mode=ro`: receipts + pending set, via a `runner` function that shares `run`'s predicate) BEFORE mkdir/WAL/`_SCHEMA`, and raise `PendingMigrations` from there; and in the runner, refuse (new `UnreceiptedDatabase`) when `have` is empty but `sqlite_master` already holds user tables with rows, unless an explicit adopt flag is passed.
- **Full fix:** split `create_new` / `open_existing` / `migrate`; move `_SCHEMA`, `_SIMPLE_MIGRATIONS`, `_VERIFICATION_TABLE`, the `evidence_spans` rebuild and the adjudication/workflow/cloud DDL into numbered migrations (S3c, with C12 and C37); define fresh as "no user tables", with `register_preapplied` as the explicit adoption route; on a failed construction close the connection instead of reopening it.
- **Acceptance test:** on a populated fixture with a receipt removed and a floor table dropped, `ReviewDatabase(...)` raises `PendingMigrations` and `db_fingerprint` overall hash, `sqlite_master`, `PRAGMA journal_mode` and the directory listing are identical before and after; a populated fixture with zero receipts refuses an ordinary open; after either refusal no `ReviewDatabase` object holds an open connection.
- **Size:** M for the minimum fix (the pre-flight must share the runner's predicate, and TESTS ARE CALLERS: every test database is built through this constructor on the fresh path); L for the full fix (S3c, migration-dependent).
- **PI-decision flag:** yes — should a database that has user tables/rows but zero receipts refuse an ordinary open (requiring explicit adoption), reversing R222's "no receipts = fresh"?
- **Differences from the original / the brief's note:**
  - B's reproduction figures hold exactly at HEAD. B does not report the journal-mode flip (delete → wal), the directory creation, or that the recreated table contradicts receipts 012/013 — all observed here.
  - B's "inline schema maintenance" at HEAD is: adjudication DDL, 18-odd `ALTER TABLE` strings in `_SIMPLE_MIGRATIONS` (errors "already exists"/"duplicate column" swallowed), `_VERIFICATION_TABLE`, and the conditional `evidence_spans` rebuild via `executescript` — the last is itself a non-atomic rebuild-by-rename of a referenced table, the pattern CLAUDE.md's migration rules forbid, reachable only on a pre-"contested" table (not exercised).
  - B's "(c) reopens a connection even after runner failure" is correct; the practical effect is a leaked WAL connection for as long as the exception/traceback lives, not a second write.
  - "An inspection command can change a live-style database": true only when the floor DDL is not already satisfied; on a database whose floor tables and columns all exist, the pre-refusal statements change nothing but the journal mode (already WAL on an engine-created file). The reproduction's trigger (a dropped floor table) is synthetic, as B's was; C12's `audit_adjudication` 1 → 0 → 1 is the recorded real instance.


---

### F03 — Restore's open-database probe is not a complete exclusion mechanism
- **Source:** outside B
- **Present at HEAD:** yes — (a) probe lock released before `os.replace`, nothing held across the interval: yes; (b) probe misses an idle holder of a DELETE-journal database: yes; (c) target `-wal`/`-shm` deleted after replacement: yes; (d) stated guarantee stronger than behaviour: yes
- **Evidence level:** reproduced ((a), (b), (c) each run at HEAD against scratch files)
- **Evidence:**
  - (a) code order, `engine/utils/db_backup.py::_refuse_if_open`: `probe = sqlite3.connect(str(target), timeout=0.2)` / `probe.execute("PRAGMA locking_mode=EXCLUSIVE")` / `probe.execute("BEGIN IMMEDIATE")` / `probe.execute("COMMIT")` … `finally: probe.close()`. The connection — and with it the exclusive lock — is closed before the function returns. In `restore`, after `_refuse_if_open(target_path)` come: tmp-name check, `src.backup(dst)` into the staging file (the whole copy), `staged_fp = fingerprint(tmp_path)` + `compare`, and only then `os.replace(tmp_path, target_path)`. No lock, flock or open handle on the target spans that interval; its length grows with database size (staging copy + a full fingerprint).
  - (c) code order, `restore`, after `os.replace`: `for sidecar in ("-wal", "-shm"):` / `stale = target_path.parent / (target_path.name + sidecar)` / `if stale.exists(): stale.unlink()`. Unconditional — no re-probe, no check that the sidecars are not a live connection's.
  - (b) Reproducer `~/scratch/12f/repro/F03_probe_holders.py` — holder is a SEPARATE PROCESS; the real `_refuse_if_open` is called unmodified; run `cd ~/scratch/12f/g1 && PYTHONPATH=/home/ankitsarin/projects/evidence-engine .venv/bin/python ~/scratch/12f/repro/F03_probe_holders.py`; output (`.out` beside it):
    ```
    DELETE holder=idle       -> _refuse_if_open: ALLOWED (no refusal)
    DELETE holder=never_read -> _refuse_if_open: ALLOWED (no refusal)
    DELETE holder=read_txn   -> _refuse_if_open: REFUSED
    WAL    holder=idle       -> _refuse_if_open: REFUSED
    WAL    holder=never_read -> _refuse_if_open: ALLOWED (no refusal)
    WAL    holder=read_txn   -> _refuse_if_open: REFUSED
    restore() over idle-held DELETE target: COMPLETED, no refusal
      target inode changed: True
      holder (still open on the OLD inode) reads: [('target-original',)]
      holder writes: ERR OperationalError attempt to write a readonly database
      file now at target path holds: [('from-backup',)]
    ```
    B's probe result confirmed: an idle connection to a default DELETE-journal database (one that has completed a read and holds no transaction) is not refused, and the real `restore` completes over it. The holder then keeps READING the replaced database's old content with no error; its writes fail loudly. WAL contrast confirmed: an idle WAL connection that has run a statement IS refused (it keeps a `-shm` read mark, which is what the existing test relies on); a connection merely opened and never used is invisible in both modes (Python/SQLite open lazily).
  - (a)+(c) Reproducer `~/scratch/12f/repro/F03_open_after_probe.py` — the real probe runs, then a wrapper opens a WAL connection and commits one row before `restore` continues; output:
    ```
    late opener committed a row; sidecars: ['target.db-shm', 'target.db-wal']
    restore(): COMPLETED, no refusal
    sidecars after restore: []
    late opener: SELECT v FROM t -> [('target-original',), ('committed-by-late-opener',)]
    late opener: INSERT INTO t VALUES ('second write') -> []        (and its commit raised nothing)
    file at target path holds: [('from-backup',)] | integrity: ok
    ```
    The live connection's `-wal`/`-shm` were unlinked under it; its committed row and a further "successful" write went to unlinked files and are gone, with no error on either side. The restored target itself verified intact (final fingerprint passed, `integrity_check` ok) — no corruption of the restored file was observed.
  - (d) Stated guarantees: `_refuse_if_open` docstring — "Refuse when any connection — this process's or another's — holds `target`. … Measured to refuse against an idle open connection, an open read transaction and an open write transaction alike, and to succeed only once every connection has closed." (false for a DELETE-journal target and for any unused connection); CLAUDE.md "Ops Invariants — the database" — "`restore()` refuses an open target by exclusive lock"; `docs/architecture/modules.md` — "`restore(...)` — Deliberately has no CLI; refuses an open target."; `RestoreRefused` docstring "A restore was refused before anything was written"; `tests/test_db_backup.py::test_restore_refuses_open_target`, docstring "A target any connection still holds open is refused", whose only holder is `holder.execute("PRAGMA journal_mode=WAL")` + a completed SELECT — the WAL-idle row of the table above and no other.
  - Callers: none in production code. `grep` for `restore(` / `_refuse_if_open` / `db_backup.restore` over `engine/`, `scripts/`, `analysis/` finds only `engine/utils/db_backup.py` itself; the callers are `tests/test_db_backup.py` (three calls) and the operator, by hand — `engine/migrations/README.md` names it as the recovery step ("is `engine.utils.db_backup.restore` from the backup taken before the run") and `docs/session-reports/SAFE-GROUND-01_report.md` gives the two-line `python` invocation. There is deliberately no CLI (module docstring: "`restore` is a function and deliberately has no CLI").
- **Overlap:** none given. Found: I1 (`auto_backup` was `shutil.copy2`; "No restore procedure exists in any form") is the row SAFE-GROUND-01 closed by writing this function — parent, not the same defect. No existing row covers the probe's coverage or the unheld interval.
- **Proposed class:** 1 — the one operation that overwrites a review database can complete while another connection holds it: that connection's committed writes vanish silently (WAL late opener) or it goes on serving the pre-restore content as current (DELETE idle holder), and the "refuses an open target" guarantee an operator relies on is not what the code provides. Exposure is narrow (operator-run, never yet run on live per the phase-3ii reports' "No `restore()`"), which bears on priority, not on class.
- **Owning state:** pre-tag (R512)
- **Minimum loud-failure fix:** hold the exclusion instead of probing for it — keep the `locking_mode=EXCLUSIVE` connection open with its `BEGIN IMMEDIATE`/`BEGIN EXCLUSIVE` transaction un-committed from before staging until after `os.replace` and sidecar removal (an EXCLUSIVE-mode connection in a WAL database blocks other openers), re-check for `-wal`/`-shm` that appeared after the probe and refuse rather than unlink them; and correct the three guarantee texts to what is measured.
- **Full fix:** an application-level maintenance lock (the `flock(2)` pattern `engine/utils/ollama_lock.py` already uses) that `restore` holds throughout staging, replacement, sidecar cleanup and final verification and that `ReviewDatabase.__init__` takes shared for its lifetime; plus an operator pre-flight (`lsof`/`fuser` empty, as R85 already requires before a live write) for non-participating tools, since no SQLite-level probe can see an idle rollback-journal reader.
- **Acceptance test:** for DELETE and WAL targets × {idle after read, open read txn, open write txn} holders in a second process, `restore` raises `RestoreRefused` and the target's inode and fingerprint are unchanged; a second process attempting to open/write between the exclusion being taken and the final verification is blocked or fails loudly for the whole operation; no `-wal`/`-shm` belonging to a live connection is ever unlinked. The DELETE-idle case must be red against HEAD.
- **Size:** M — one module, but the lock protocol touches `ReviewDatabase.__init__` (every opener must participate) and needs cross-process tests; the docstring/CLAUDE.md correction alone is S.
- **PI-decision flag:** yes — is the remedy an engine-level maintenance lock every opener takes, or does restore stay an operator procedure whose exclusivity is the operator's pre-flight (lsof/fuser), with the code's claim reduced to match?
- **Differences from the original / the brief's note:**
  - B is accurate on all four points. Additions: the miss is not only "idle + DELETE" — a connection that has been opened but has not yet run a statement is invisible in WAL mode too (reproduced); and the holder outcome differs by mode (DELETE idle holder: stale reads, loud write failure; WAL late opener: silent loss of committed and subsequent writes).
  - Reach of the DELETE case in this engine: `ReviewDatabase.__init__` always sets WAL, so an engine-owned idle connection IS refused. DELETE-journal targets arise from the engine's own tools, though: `auto_backup` deliberately switches every backup to a rollback journal (`dst.execute("PRAGMA journal_mode=DELETE")`), and `restore` copies that header, so a just-restored database is DELETE-journal until the next `ReviewDatabase` opens it — the window in which a plain `sqlite3`/`mode=ro` reader (fingerprint check, ad-hoc inspection) is an undetectable holder.
  - "Deleting target WAL/SHM sidecars after replacement compounds that risk": confirmed in effect, but no corruption of the restored file resulted in the run here; the damage observed is to the other connection's data, not the restored database.
  - The docstring's "Measured to refuse against an idle open connection" was presumably measured on a WAL database only — the fixture-more-permissive-than-production pattern the module's own header warns about, inverted.


---

### F04 — Deduplication can collapse genuinely different reports
- **Source:** outside B
- **Present at HEAD:** yes — per sub-claim: (a) title fallback in exact match, no identifier-conflict check: **yes**; (b) fuzzy match returns first title > 0.9, no conflict check: **yes**; (c) PubMed records appended without within-source dedup: **yes**; (d) identifier indexes not refreshed after a merge adds an identifier: **yes**; (e) secondary source's identity / raw record lost from the returned canonical citation: **yes** (and its conflicting DOI/PMID is lost too, since `_merge` only fills `None`).
- **Evidence level:** reproduced
- **Evidence:** Reproducer `~/scratch/12f/repro/F04_dedup_collapse.py` (real `engine.search.dedup.deduplicate`, citations built with `engine.search.models.Citation`; cwd `~/scratch/12f/g2`, `PYTHONPATH=<repo> .venv/bin/python ../repro/F04_dedup_collapse.py`), output `F04_dedup_collapse.out`:
  - B1 same title, DOIs `10.1000/aaa` (pubmed, pmid 111) vs `10.1000/bbb` (openalex, pmid 222) → `unique=1 duplicates=1`; kept `source=pubmed pmid=111 doi=10.1000/aaa`, raw_data = the PubMed record's only. Same with both records from OpenAlex (`unique=1 duplicates=1`).
  - B2 two identical PubMed entries → `unique=2 duplicates=0`.
  - Fuzzy: "… - part 1" (doi p1, pmid 301) vs "… - part 2" (doi p2, pmid 302) → `unique=1 duplicates=1`.
  - Index: PubMed record without DOI gains `10.1000/x` by a title merge; a later OpenAlex record with DOI `10.1000/X` and another title → `unique=2`, both carrying the same DOI.
  Code, `engine/search/dedup.py`:
  - (a) `_exact_match`: after the two `if key in doi_index` / `if key in pmid_index` lookups miss, `norm = normalize_title(cit.title)` / `if norm in title_norm_index: return title_norm_index[norm]` — nothing compares the two records' identifiers. Docstring of `deduplicate` says "Phase 1 — Exact match on DOI, then PMID"; the title branch is in Phase 1 too.
  - (b) `_fuzzy_title_match`: `for existing_title, idx in title_list: if title_similarity(norm_title, existing_title) > 0.9: return idx` — first hit in dict order, not best, no identifier/year/author check.
  - (c) `deduplicate`: `# Seed with all PubMed records` / `for cit in pubmed_citations: idx = len(unique); unique.append(cit)` — unconditional append; later entries overwrite the index slots (`doi_index[...] = idx`).
  - (d) both merge sites: `unique[match_idx] = _merge(unique[match_idx], oa_cit)` followed only by `duplicate_pairs.append(...)`; `doi_index` / `pmid_index` are written only in the seed loop and the final "genuinely new" branch.
  - (e) `_merge`: `data = primary.model_dump()`; `for field in ("doi", "pmid", "abstract", "journal", "year"): if data.get(field) is None and getattr(secondary, field) is not None` — `source` and `raw_data` stay the primary's; `DedupResult.duplicate_pairs` is `(kept title, removed title)` only (two identical strings in B1), and `_stage_search` passes only `unique_citations` on.
- **Overlap:** no pairing was given. Found: **H1 / H7** (dedup count reaches a log line only; PRISMA `duplicates_removed = 0`) — different defect (reporting of the count, not the correctness of the merge), but they compound: a wrong merge is also uncounted. **F7** (supplementary search needs dedup against existing rows) — adjacent, concerns the persisted side (see F05).
- **Proposed class:** 1 — a distinct report is removed before screening with no stored trace (only a log count), so the corpus and PRISMA identification are silently wrong; within-source duplicates survive.
- **Owning state:** pre-tag (R512). Note the live review's corpus is already ingested; this bites the next search on any review.
- **Minimum loud-failure fix:** in `_exact_match` and `_fuzzy_title_match`, refuse a title-based match when both records carry a non-empty DOI (normalised) or PMID that differ — keep both and record the pair as a conflict in `DedupResult` (a list the search stage logs/persists for human review).
- **Full fix:** one identity routine used for both sources (PubMed seeded through the same matcher), normalised DOI (prefix/case), indexes updated after every merge, best-not-first fuzzy candidate with year/author corroboration, and `DedupResult` carrying per-merge membership (both source ids, raw records, merge reason) that is persisted with the search record (ties to H1/H7).
- **Acceptance test:** same title + different DOIs → 2 unique and 1 recorded conflict; two identical PubMed records → 1 unique, 1 duplicate; part-1/part-2 titles with different ids → 2 unique; identifier learned by merge then met again → 1 unique; result independent of input order; the merged record exposes both source identities.
- **Size:** M — matcher rewrite is small, but where merge membership is stored touches H1/H7 and needs a Phase A.
- **PI-decision flag:** yes — when two records share a title but carry different DOIs/PMIDs, keep both automatically, or queue the pair for human review before ingestion?
- **Differences from the original / the brief's note:** B is accurate on all five sub-claims and both reproductions (1 unique / 1 duplicate; 2 unique / 0 duplicates). Additions: the "duplicates survive" half also follows from (d) (two kept records with the same DOI, differing only in case — the index is lower-cased but the stored value is not); the fuzzy matcher is first-hit not best-hit; the function docstring understates Phase 1.


---

### F05 — Rerunning search duplicates records without PMIDs
- **Source:** outside B
- **Present at HEAD:** yes
- **Evidence level:** reproduced
- **Evidence:** Reproducer `~/scratch/12f/repro/F05_add_papers_twice.py` — the real `ReviewDatabase("f05_synth", data_root=~/scratch/12f/g2/f05_data)` (constructor signature `__init__(self, review_name: str, data_root: Path | None = None)`; all 18 numbered migrations ran, receipts through `022_run_kinds_and_audit_tables`, so unlike B's fixture this is the full HEAD schema), real `add_papers`. Output `F05_add_papers_twice.out`: `first add_papers -> 1`, `second add_papers -> 1`, same citation twice in one call `-> 2`, `rows: 4`, all `(None, '10.1000/doi-only', 'A DOI-only record', 'INGESTED')`. Control: a PMID-bearing citation added twice → `1 0`. `indexes on papers: ['idx_papers_doi', 'idx_papers_status', 'sqlite_autoindex_papers_1']` — the DOI index is non-unique.
  Code: `engine/core/database.py` `_SCHEMA`: `pmid TEXT UNIQUE,` / `doi TEXT,` / `CREATE INDEX IF NOT EXISTS idx_papers_doi ON papers(doi);`. `add_papers` (docstring "Bulk insert citations, skip duplicates by pmid"): `if cit.pmid: row = ... "SELECT id FROM papers WHERE pmid = ?" ... if row: continue` then an unconditional `INSERT INTO papers ... 'INGESTED'`; the log line says "Added %d/%d papers (duplicates skipped)". `scripts/run_pipeline.py` `_stage_search`: `dedup_result = deduplicate(pm_cits, oa_cits)` → `added = db.add_papers(unique)` — dedup is within this run's batch only; nothing compares against stored rows, and `_stage_search` has no "already searched" guard, so any run starting at `search` (the default start) on a populated database re-inserts every PMID-less record. Each new row is `INGESTED`, i.e. picked up by `run_screening`.
  Entry importers (read only): `import_screening_entry` and `import_extraction_entry` do not use `add_papers`; each has a within-file identity check (`_check_id_duplicates`: pmid stripped, doi stripped+lower-cased; screening entry also title for id-less entries) and each refuses a non-empty review (`SELECT COUNT(*) FROM papers` → "review: holds N papers row(s)…"). So they cannot duplicate against existing rows, and they have no cross-run identity check because they never append.
- **Overlap:** no pairing was given. Found: **F7** ("the build needs dedup against existing rows (pmid, normalised doi, normalised title …)") — same missing capability seen from the importer side; F7 is recorded as an unsupported feature, F05 shows the search path already appends without it. Related, not identical. **H1** ("`add_papers` dedups a second time uncounted") — narrower/different: H1 is the uncounted PMID skip; F05 is the absent DOI/identity skip.
- **Proposed class:** 1 — a rerun silently multiplies papers rows, so screening/extraction work and every count derived from `papers` are wrong with no error.
- **Owning state:** pre-tag (R512).
- **Minimum loud-failure fix:** `add_papers` refuses (raises, before any INSERT) when a citation without a PMID matches an existing row's normalised DOI, or — smaller still — `_stage_search` refuses to run against a review that already holds papers rows, mirroring the entry importers' R-r rule.
- **Full fix:** ingest through one identity routine (F04's) against the stored corpus — pmid, normalised DOI, normalised title for id-less records — with the match recorded; then a partial unique index on normalised DOI after existing rows are reviewed.
- **Acceptance test:** the same batch added twice: second call adds 0 rows (or refuses) for PMID-less, DOI-only and id-less citations; DOI case/prefix variants treated as one; a duplicate within one batch is not inserted twice.
- **Size:** M — a refusal is S; DOI identity needs a normalisation decision and (for the constraint) a migration.
- **PI-decision flag:** yes — should a search stage run against a non-empty review refuse outright (as the entry importers do), or append with identity matching?
- **Differences from the original / the brief's note:** B's "two rows" confirmed (2 calls → 2 rows), and here on the fully migrated schema rather than B's base-schema fixture. Additional: the same citation twice in ONE call also inserts twice; the `except sqlite3.IntegrityError: continue` branch silently swallows any integrity error, not only the pmid one its comment names.


---

### F06 — PRISMA counts do not represent the complete identification history
- **Source:** outside B
- **Present at HEAD:** yes (bundled; per sub-claim)
  - (a) identification counted from surviving `papers` rows — **yes**
  - (b) `duplicates_removed` a literal 0 — **yes**
  - (c) `_stage_search` raw/duplicate totals persisted nowhere PRISMA reads — **yes** (return dict + log lines only)
  - (d) exclusion reasons counted per screening DECISION ROW, two excluding passes = two counts for one report — **yes**, and it is the *normal* case, not an edge (see Differences)
  - (e) `studies_included` = papers at `audited_ai`; eligible papers whose extraction failed are outside that box — **yes as code**; **partial as consequence**: they do not disappear from the PRISMA CSV (own failure rows + "Eligible for extraction"), they disappear from the *methods narrative* (F07 vi / C36)
- **Evidence level:** reproduced (a–d: `~/scratch/12f/repro/F06_prisma_counts.py`); code order (c persistence, e)
- **Evidence:**
  - (a)(b) `engine/exporters/prisma.py`, `generate_prisma_flow`:
    `"SELECT source, COUNT(*) as cnt FROM papers GROUP BY source"` → `total_identified = sum(source_counts.values())` → `duplicates_removed = 0  # tracked externally by dedup module`. The comment is a stated guarantee the code does not honour: nothing "tracks" it anywhere readable (grep `duplicates_removed|duplicates_found` over engine/ scripts/ analysis/: only `dedup.py` stats+log, `run_pipeline._stage_search` log+return, `methods_section` reading the 0, and `scripts/test_e2e_search_screen.py`).
  - (c) `scripts/run_pipeline.py`, `_stage_search`: `return {"pubmed": len(pm_cits), "openalex": len(oa_cits), "duplicates": dedup_result.stats["duplicates_found"], "unique": ..., "added": added, ...}`; caller `run_pipeline`: `results["search"] = _stage_search(db, spec, limit)` — `results` is a local dict; `_finish_review_run(db, run_id, "completed")` takes no results. Two further uncounted reductions between "retrieved" and `papers`: `if limit: unique = unique[:limit]`, and `ReviewDatabase.add_papers` ("Bulk insert citations, skip duplicates by pmid" — `if row: continue`, and `except sqlite3.IntegrityError: continue`), whose only trace is `logger.info("Added %d/%d papers (duplicates skipped)")`.
  - per-source loss: `engine/search/dedup.py`, `deduplicate`: a matched OpenAlex record is merged into the PubMed entry (`unique[match_idx] = _merge(unique[match_idx], oa_cit)`), so the surviving row carries `source='pubmed'` and the OpenAlex retrieval is absent from "From openalex".
  - (d) `generate_prisma_flow`:
    ```
    SELECT sd.rationale, COUNT(*) as cnt
    FROM abstract_screening_decisions sd JOIN papers p ON p.id = sd.paper_id
    WHERE p.status = 'ABSTRACT_SCREENED_OUT' AND sd.decision = 'exclude'
    GROUP BY sd.rationale
    ```
    `COUNT(*)` over decision rows, no `DISTINCT paper_id`, no pass filter, grouped by the model's FREE-TEXT `rationale` (not a reason code). `engine/agents/screener.py`, `run_screening`: both passes are stored (`db.add_screening_decision(pid, 1, …)`, `(pid, 2, …)`) and the only automatic route to the status is `elif d1.decision == "exclude" and d2.decision == "exclude": db.update_status(pid, "ABSTRACT_SCREENED_OUT")` — so every auto-excluded report contributes exactly TWO reason counts. The other route (`screening_adjudicator`, `new_status = … else "ABSTRACT_SCREENED_OUT"` for a FLAGGED paper) contributes 1 or 0 (include/exclude split → 1, the model's rationale not the PI's; uncertain/include or a parse-error flag → 0). `validate_prisma_counts` has no identity tying `sum(exclusion_reasons)` to `records_excluded`, so it passes.
  - (e) `generate_prisma_flow`: `if state.processing == "audited_ai": studies_included += 1 / elif state.processing in failures: failures[...] += 1 / elif state.processing in _EXTRACTION_IN_PROGRESS: extraction_in_progress += 1`. `export_prisma_csv` writes `("Eligible for extraction", flow["n_eligible"], "")`, one `(_FAILURE_LABELS[token], count, reason)` row per non-zero failure reason, `"Extraction in progress"`, then `("Studies included", flow["studies_included"], "")`. So a failed-extraction eligible paper is visible as "Extraction failed / <reason>" and inside "Eligible for extraction", and is in the evidence table (paper set = `eligible_paper_ids`), but is not a "study included".
  - Reproducer (B's own acceptance fixture: 10 PubMed, 8 OpenAlex, 3 cross-source duplicates; one report excluded by two passes). Command: `cd ~/scratch/12f/g3 && PYTHONPATH=/home/ankitsarin/projects/evidence-engine .venv/bin/python ~/scratch/12f/repro/F06_prisma_counts.py`. Output (`F06_prisma_counts.out`):
    ```
    dedup stats (what _stage_search returns and logs): {'pubmed_total': 10, 'openalex_total': 8, 'duplicates_found': 3, 'unique_total': 15}
    records_identified: 15
    records_by_source: {'openalex': 5, 'pubmed': 10}
    duplicates_removed: 0
    records_excluded: 1
    exclusion_reasons: {'Not surgical': 1, 'Wrong population': 1}
    sum(exclusion_reasons) = 2 vs records_excluded = 1
    validate_prisma_counts valid: True
    ```
    Expected by PRISMA: 18 identified (10 + 8), 3 removed, 1 exclusion reason.
  - Lead's live measurement (carried, source: lead, this session, mode=ro): records_identified 10,039, duplicates_removed 0, n_eligible 190, studies_included 0, extraction_in_progress 190 — consistent with (a)(b)(e).
- **Overlap:**
  - **H1 — CONFIRMED, same defect** for (a)(b)(c) incl. "`add_papers` dedups a second time uncounted". B adds the per-source consequence (OpenAlex total shrinks) and the `limit` truncation; H1's text does not state those but they follow from it.
  - **H7 — CONFIRMED, same defect** as (b)(c) (quotes the same line and the return-dict-only statistics); H7 is broader on one axis (importer's refused within-file duplicates, R321). H1 and H7 are duplicates of each other on the literal 0.
  - **H2 — CONFIRMED as family, row text STALE.** H2 quotes `SELECT rejected_reason … WHERE status = 'REJECTED'`; that query left `prisma.py` at `2153193`. The surviving exclusion-reason query is the decision-row one above (present since `e743d14`). H2 says "section renders empty"; at HEAD it renders non-empty and wrong (double-counted, free-text keyed, PI reasons absent). B's (d) is therefore a defect H2's text does not describe — H2 should be re-worded or (d) given its own row.
  - **C36 — CONFIRMED, narrower.** C36 is the *methods* rendering of `studies_included`; F06(e) is the definition itself in the flow. Same root (inclusion = `audited_ai`), same PI question.
- **Proposed class:** 1 — a published reporting artifact (PRISMA flow, and the methods sentence fed from it) states wrong identification/duplicate/reason counts silently while the reconciliation reports `valid`.
- **Owning state:** pre-tag (R512) for the loud-failure minimum; the search ledger is migration-dependent (H1/H7 are owned S8/senior in the coverage map) — flag: the full fix cannot be pre-tag without a migration.
- **Minimum loud-failure fix:** stop emitting numbers the engine does not have: render "Duplicates removed" as not-recorded (omit the row / `NOT RECORDED`) instead of `0`, label the identification rows "records retained after de-duplication"; count exclusion reasons `COUNT(DISTINCT sd.paper_id)` per report with one resolved reason, and add a `validate_prisma_counts` identity `sum(exclusion_reasons) == records_excluded` so a mismatch raises.
- **Full fix:** append-only search ledger (source, query, retrieved-at, source record id, raw count, kept/duplicate disposition, canonical paper) written by `_stage_search` and the importers; identification and duplicate counts read from it; one resolved exclusion reason per report (PI adjudication reason over model rationale); `studies_included` per the PI ruling below.
- **Acceptance test:** B's fixture (10 PubMed + 8 OpenAlex, 3 cross-source duplicates) exports 18 identified (10/8 by source) and 3 removed; a report excluded by two passes contributes 1 to the reason table and the reason total equals `records_excluded`; an eligible paper at `extraction_failed` is reported as eligible-and-failed and its inclusion status follows the ruled definition.
- **Size:** L — ledger needs a migration and writer changes in search + importers; the minimum (labels, DISTINCT, new identity) is S.
- **PI-decision flag:** yes — Is a study "included" in the review when it is ELIGIBLE (eligibility axis; live: 190) or only when it has reached `audited_ai` (processing axis; live: 0)? (One line: "eligibility" or "audited_ai".)
- **Differences from the original / the brief's note:** (1) B says two excluding passes "can" add two counts; at HEAD it is the rule for every automatically excluded report, and adjudicated exclusions under-count (0 or 1, never the PI's reason) — the table is wrong in both directions and keyed on free text truncated to 80 chars in the CSV. (2) B's "silently disappearing from scientific inclusion" overstates the PRISMA side: failed papers have their own labelled rows; the disappearance is in the methods count. (3) B does not mention the `limit` slice or `add_papers`' PMID skip as further uncounted reductions. (4) The repro's first draft produced 7 "duplicates" from 5 synthetic OpenAlex titles that differed by one digit — fuzzy title merging of distinct reports (F04's subject, not chased; titles changed to distinct ones).


---

### F07 — Generated methods can describe planned rather than performed work
- **Source:** outside B
- **Present at HEAD:** yes — all six triggers (per sub-claim below; (v) with a correction)
  - (i) cloud wording from `spec.cloud.enabled_arms`, "was additionally performed" with no call — **yes**
  - (ii) mixed sources: abstract model = current spec; FT models = all historical decision rows; extraction/audit = the one requested run — **yes**
  - (iii) left-joined stage declarations yield a model entry with zero papers — **yes**
  - (iv) the one-model branch hides the count — **yes** (so a zero-call run names its model as having performed extraction)
  - (v) export-only run has no local extraction history while the evidence table carries earlier runs' results — **yes**; it renders the placeholder `[MODEL NOT SPECIFIED]`, not a wrong model (an omission, not a false attribution)
  - (vi) narrative study count from `studies_included` — **yes**
- **Evidence level:** reproduced (i, ii, iii, iv, v, vi rendering: `~/scratch/12f/repro/F07_methods_planned_vs_performed.py`, the module's own functions on a synthetic in-memory db + stub spec; `generate_prisma_flow` and `load_codebook_beside` stubbed); code order for the caller side of (v)
- **Evidence:** all in `engine/exporters/methods_section.py`
  - (i) `generate_methods_section`: comment "Cloud arms the spec ENABLES (S3g) … an enabled arm is what a run *could* send", then `cloud_parts = [f"{_PROVIDER_LABEL[spec.arm(name).provider]} {spec.arm(name).model}" for name in spec.cloud.enabled_arms]` and `if cloud_parts: methods += f"Concordance extraction was additionally performed by {' and '.join(cloud_parts)}. "`. No read of `cloud_extractions`, `run_calls` or any run. The code's own comment says "could send"; the sentence says "was performed".
  - (ii) `abstract_primary = spec.screening_models.primary or "[MODEL NOT SPECIFIED]"` (comment: "Abstract screening: from spec") · `_query_ft_screening_models`: `"SELECT model, COUNT(DISTINCT paper_id) as cnt FROM ft_screening_decisions GROUP BY model"` (no run, no date, no current-decision filter) · `_run_extraction_models(db._conn, run_id)` / `_run_audit_models(db._conn, run_id)` with `WHERE sc.run_id = ?`. Three provenance rules in one paragraph. Also: when `ft_model_counts` is non-empty the sentence is `"Full-text screening was performed by {ft_desc}. "` — the "with verification by" clause exists only in the spec-fallback branch, so the FT verifier is unnamed whenever history exists.
  - (iii) `_run_extraction_models`: `"SELECT sc.model_name, COUNT(DISTINCT rc.paper_id) FROM run_stage_configs sc LEFT JOIN run_calls rc ON rc.run_id = sc.run_id AND rc.stage = sc.stage WHERE sc.run_id = ? AND sc.stage_kind IN (…) GROUP BY sc.model_name"`; `_run_audit_models`: same shape with `LEFT JOIN paper_events pe ON pe.run_id = sc.run_id AND pe.to_state = 'audited_ai'`. A declared stage with no call returns `{model: 0}`, which is truthy.
  - (iv) `elif len(extraction_model_counts) == 1: extraction_model_str = next(iter(extraction_model_counts))` (same for audit) — the count is dropped; `_format_model_counts` (which prints `n=`) is reached only for ≥2 models.
  - (v) `if run_id is None: return {}` and, for a run with no stage rows, an empty result → `if not extraction_model_counts: extraction_model_str = "[MODEL NOT SPECIFIED]"`. Caller: `scripts/run_pipeline.py`, `_open_run_manifest`: `for name in STAGES[start_idx:]: stages.extend(_PIPELINE_STAGE_CONFIGS.get(name, ()))` — `_PIPELINE_STAGE_CONFIGS` has no `export` key, so `--skip-to export` opens a manifest with zero stage rows; `--skip-to audit` declares `audit` only (extraction unnamed, audit named). Meanwhile `evidence_table._build_evidence_rows(db, spec, …, arm=arm)` takes an arm and no run: it exports whatever earlier runs produced.
  - (vi) `included_for_extraction = flow["studies_included"]` → `"… approach on {included_for_extraction} included studies across {n_fields} predefined fields. "`. The fixed prose "two-pass reasoning-then-structured-output approach" is also unconditional (E-METHODS). `screened_in = flow["studies_included"] + flow["full_text_assessed"]` is computed and never used.
  - Reproducer output (`F07_methods_planned_vs_performed.out`), run 2 = stages declared, zero calls; run 3 = export-only; cloud arm enabled, no cloud call, no cloud table:
    ```
    run 2 extraction models: {'model-B': 0}
    run 2 audit models     : {'auditor-B': 0}
    run 3 extraction models: {}
    run_id=2: … Title-abstract screening … (CURRENT-SPEC-screener, Ollama) … Full-text screening was performed by old-ft-model:1 (n=2) and qwen3:32b (n=1). Data extraction was performed using model-B with a two-pass reasoning-then-structured-output approach on 0 included studies across 20 predefined fields. Concordance extraction was additionally performed by OpenAI o4-mini-2025-04-16. Cross-model verification was performed by auditor-B.
    run_id=3: … Data extraction was performed using [MODEL NOT SPECIFIED] … on 0 included studies … Concordance extraction was additionally performed by OpenAI o4-mini-2025-04-16. Cross-model verification was performed by [MODEL NOT SPECIFIED].
    ```
    (model-A / auditor-A, the run that actually extracted and audited both papers, appear in neither.)
- **Overlap:**
  - **E-METHODS — CONFIRMED as the owning row, but B is BROADER.** E-METHODS covers the fixed two-pass prose for an elicited run, R364's count fold-in (`COUNT(DISTINCT rc.paper_id)` over all outcomes — the prose must say what n counts) and R447's auditor-object sentence (B23 design D). It does not cover (i) cloud "performed" from enablement, (ii) spec-vs-history-vs-run source mixing, (iii)/(iv) the zero-call model named, or (v) the export-only gap. R364 touches the same query as (iii) but is about counting failed calls, not zero calls. Fold (i)–(v) into E-METHODS or open a sibling row.
  - **C36 — CONFIRMED, same defect as (vi)** ("on 0 included studies" on live; `included_for_extraction = flow["studies_included"]`). Status in the plan is LATENT, ruled for session 10 — still unchanged at HEAD.
  - **C30 (closed) — REJECTED as a regression.** C30 was the spec fallback for the extraction/audit model; HEAD has none (`[MODEL NOT SPECIFIED]`, docstring "never from the spec (row C30)"). B's (v) is the *cost* of C30's fix (no history → placeholder, even though earlier runs' results are exported), and B's (ii) concerns the two model lines C30 never touched: abstract screening still reads the spec directly (`spec.screening_models.primary`, not even the resolver — the same bypass C30 closed for the auditor), and FT falls back to `spec.ft_screening_models.*`. A different thing; not a regression.
- **Proposed class:** 1 — a generated provenance statement (methods draft) asserts work that was not performed (cloud arm; zero-call model) and omits the model that produced the exported values, silently.
- **Owning state:** pre-tag (R512). Note R71 keeps `cloud.enabled_arms` empty on live, so (i) is not live-reachable today; (iii)/(iv)/(v) are reachable by any resumed or `--skip-to` run.
- **Minimum loud-failure fix:** (i) name a cloud arm only if the result set holds a claim from it, else say nothing; (iii)/(iv) inner-join / drop zero-count models and always print `n=`; (v) when the exported arm holds claims but the run has no extraction stage, render an explicit `[EXTRACTION RUN NOT IN THIS EXPORT'S RUN — see run_id …]` placeholder or refuse; (vi) per the F06 ruling.
- **Full fix:** derive every methods clause from the provenance of the exported result set — the `run_id`s of the claims the reader resolved for the exported arm, their `run_stage_configs`, successful `run_calls`, and the screening decision sets actually in force — and distinguish configured / attempted / succeeded. Emit a small methods manifest (clause → run ids → counts) beside the file.
- **Acceptance test:** (1) spec enables a cloud arm, no cloud call exists → the paragraph contains no "performed by <cloud model>". (2) run with declared extract stages and zero `run_calls` → that model is not named as having extracted. (3) export-only run over results produced by run 1 → the paragraph names run 1's extraction and audit models with their paper counts. (4) every model mention carries an n, and each n is defined in the prose.
- **Size:** M — one brief with a read-only Phase A (which runs/claims define "the exported result set"; pinned prose in `tests/` to be read first — tests are callers).
- **PI-decision flag:** yes — Should the methods draft describe (a) the run that invoked the export, or (b) every run that produced a value in the exported table? (One line: "(a)" or "(b)"; B and the evidence table's behaviour imply (b).)
- **Differences from the original / the brief's note:** (1) B's (v) "omit the actual historical extractor" is exact; it does not *mis*-attribute — it prints `[MODEL NOT SPECIFIED]`. (2) B's "can claim an unexecuted cloud arm" is conditional on `enabled_arms` being non-empty, which R71 forbids on live for now. (3) Not in B: the FT verifier is silently dropped from the sentence whenever any `ft_screening_decisions` row exists; the abstract line hard-codes "dual-pass … Ollama" and names only the primary, never the abstract verifier; `screened_in` is dead code. (4) Evidence-level caveat: the reproducer uses minimal synthetic tables (only the columns the module's queries read) and stubs the PRISMA flow and codebook — it exercises `methods_section`'s own code unmodified, not `open_run`.


---

### F08 — Retrying an exhausted generator can turn a failed search into apparent success
- **Source:** outside B
- **Present at HEAD:** partial — the wrapper's control flow is as B describes (yes); the silent partial result does NOT occur with the installed pyalex 0.20 paginator (no); the dependency is unpinned, so the exposure is latent (yes).
- **Evidence level:** conditional — condition: the object returned by `Works.paginate()` is a generator (or any iterator that is exhausted after raising). With pyalex 0.20 as installed it is not; `requirements.txt` carries the bare line `pyalex` (no version), so a reinstall may change it. Could not establish (no network) whether any published pyalex version has a generator-based paginator.
- **Evidence:** Reproducer `~/scratch/12f/repro/F08_paginate_retry.py` (real `engine.search.openalex._paginate_with_retry`, `time.sleep` zeroed, HTTP boundary faked), output `F08_paginate_retry.out`:
  - (A) B's case, plain generator yielding page 1 then raising `requests.ConnectionError`: wrapper returns `[['p1-a', 'p1-b']]`, **no exception propagated** — B's reproduction confirmed.
  - (B) the real `Works().search("x").paginate(per_page=200)` (`type … Paginator`) with only `BaseOpenAlex._get_from_url` faked: page 2 fails once → all 3 pages returned, cursors requested `['*', 'c2', 'c2', 'c3']`; fails twice → same, `['*','c2','c2','c2','c3']`; fails 3× (= `_MAX_RETRIES`) → `RAISED ConnectionError`. The retry re-requests the same cursor and an exhausted budget propagates.
  Code: `engine/search/openalex.py` `_paginate_with_retry`: `paginator = works_query.paginate(per_page=_PER_PAGE)` then `try: page = next(paginator) … except StopIteration: return … except Exception as exc: if attempt == _MAX_RETRIES: raise … time.sleep(wait)` — `StopIteration` on a retry attempt is indistinguishable from normal end. Installed `.venv/lib/python3.12/site-packages/pyalex/api.py` (dist-info `Version: 0.20`): `class Paginator:` with `def __iter__(self): return self` and `def __next__(self):` — a class, not a generator. In `__next__`: `self.endpoint_class._add_params("cursor", self._next_value)` → `r = self.endpoint_class._get_from_url(self.endpoint_class.url, self._session)` → only then `self._next_value = r.meta["next_cursor"]`. An exception inside `_get_from_url` (`res.raise_for_status()`, or a transport error) leaves `_next_value` and `n` untouched, so the next `next()` re-sends the same cursor. pyalex's own session retry is off by default (`max_retries=0`), so the wrapper is the only retry.
  Caller: `search_openalex` — `for page in _paginate_with_retry(works_query): for work in page: …` then `logger.info("OpenAlex total: %d citations", len(citations)); return citations`. No comparison with the advertised `meta["count"]` anywhere in `engine/search/openalex.py`; a short result is accepted as complete. (Also `_parse_work` returns `None` for a title-less work, dropped uncounted.)
- **Overlap:** no pairing was given; no existing inventory row found on OpenAlex pagination or the pyalex pin (grep of row lines for `paginat`, `pyalex`, `n_max`: none). See INT-g2-1 for the completeness gap that IS live.
- **Proposed class:** 1 (latent) — under the stated condition a partial retrieval is stored as a complete search with no signal; today, with pyalex 0.20, it does not fire. If classed by present behaviour only: 3 (unpinned dependency + no completeness check).
- **Owning state:** pre-tag (R512) for the pin and the completeness check; nothing to repair in stored data from this defect on the evidence here.
- **Minimum loud-failure fix:** in `search_openalex`, read the first page's `meta["count"]` and raise if the number of works received differs (see INT-g2-1 for the cap); pin `pyalex==0.20` in `requirements.txt` (R75 pattern).
- **Full fix:** own the cursor in the wrapper (request page by explicit cursor, retry that request) so correctness does not depend on the client's iterator semantics; record advertised vs retrieved counts in the search record.
- **Acceptance test:** with a faked transport, page 2 failing once → page 2 re-requested, full result; failing past the budget → the search raises; an iterator that ends early against a larger advertised count → the search raises, never a short list.
- **Size:** S — pin + count check in one function; the explicit-cursor rewrite is also S.
- **PI-decision flag:** no
- **Differences from the original / the brief's note:** B's control-flow claim and generator reproduction are right, and B was right to hold incidence at "medium": against the installed client the failure does not happen — the paginator is a class with `__next__` that keeps its cursor on error. So the verdict is "conditional", not "reproduced". B's "Pin the actual client version" stands: `pyalex` is the unpinned line in `requirements.txt` while ollama/openai/anthropic are pinned.


---

### F09 — Individually atomic exports do not form one coherent evidence package
- **Source:** outside B
- **Present at HEAD:** yes as code order (per sub-claim); the trigger (a concurrent writer or a second concurrent export) is not established and is unlikely under today's use
  - (a) sequential writers, no shared read transaction/snapshot — **yes**
  - (b) evidence values and processing states from separate queries — **yes**
  - (c) workbook recomputes Field States after building Evidence Table — **yes** (and the corpus id list is read a third time, before both)
  - (d) earlier files already replaced if a later exporter fails — **yes**
  - (e) predictable `.tmp` names collide between concurrent exports to one destination — **yes**
- **Evidence level:** code order (concurrency trigger; not reproduced, per the brief)
- **Evidence:**
  - (a) `engine/exporters/__init__.py`, `export_all`: five calls in sequence on the one `db` — `export_prisma_csv(db, prisma_path)` → `export_evidence_csv(db, spec, evidence_csv_path, arm=arm)` → `export_evidence_excel(...)` → `export_evidence_docx(...)` → `export_methods_md(db, spec, methods_path, run_id=run_id)`. No `BEGIN`, no snapshot, no `with conn:` anywhere in `engine/exporters/` (grep `BEGIN|isolation_level`: no hit). `ReviewDatabase.__init__` opens `sqlite3.connect(str(self.db_path))` with the default isolation and `PRAGMA journal_mode=WAL`: the sqlite3 module opens no transaction for a SELECT, so every statement is its own read snapshot, and WAL lets another process commit between any two. Each exporter recomputes its inputs: CSV, xlsx and docx each call the reader afresh; `export_prisma_csv` and `generate_methods_section` each call `generate_prisma_flow(db)`.
  - (b) `engine/exporters/evidence_table.py`, `_build_evidence_rows`: `paper_ids = eligible_paper_ids(conn)`; then every cell `for … in iter_grid(conn, codebook=codebook, papers=paper_ids, arms=(arm,))` (one `effective_value` call, i.e. separate statements, per cell — `engine/core/effective.py`, `iter_grid`); then `conn.execute(f"SELECT * FROM papers WHERE id IN ({marks}) ORDER BY id", paper_ids)`; then, per paper inside the row loop, `state = effective_state(conn, pid)`. A row's `eligibility/processing/processing_reason/analysis_ready` columns are read after all of its field cells.
  - (c) `export_evidence_excel`: `paper_ids = eligible_paper_ids(db._conn)` → `headers, rows = _build_evidence_rows(...)` (which calls `eligible_paper_ids` again and runs the grid) → Screening Log query → `titles = {…"SELECT id, title FROM papers"}` → a SECOND `for paper_id, field_name, arm_name, ev in iter_grid(db._conn, codebook=codebook, papers=paper_ids, arms=(arm,))` for the "Field States" sheet. The function's own comment records that this sheet replaced one where "ONE FILE DISAGREED WITH ITSELF" (A1 inside a single exporter); two grid passes re-open that possibility under a concurrent write, at a much smaller window.
  - (d) every writer ends `os.replace(tmp_path, output_path)` on its own file before the next exporter starts (`export_prisma_csv`, `export_evidence_csv`, `export_evidence_excel`, `export_evidence_docx`, `export_methods_md`). `export_all` has no staging directory and no rollback; `paths` is returned only on full success. A raise in `export_methods_md` leaves four new files beside the previous `methods_section.md`. The failure itself is loud (`run_pipeline`: `except Exception … _finish_review_run(db, run_id, "failed"); raise`) — but nothing on disk marks the directory as mixed.
  - (e) the temp name, identical in all five writers (and in `review_workbook.py`'s builder: `tmp_path = str(output_path) + ".tmp"`): `tmp_path = output_path + ".tmp"`, with `output_path` fixed by `export_all` (`out / "prisma_flow.csv"`, `"evidence_table.csv"`, `"evidence_table.xlsx"`, `"evidence_table.docx"`, `"methods_section.md"`). No pid, no `tempfile`. Two exports to one directory share the path; and each writer's `except BaseException: if os.path.exists(tmp_path): os.unlink(tmp_path)` would delete the OTHER export's in-flight temp file on its own failure. An interleaving where A's `os.replace` publishes a file B has open for writing makes the published file non-atomic; B's later `os.replace` then raises `FileNotFoundError`.
  - What protects it today: (1) `export_all` has exactly one caller — `scripts/run_pipeline.py`, `_stage_export` (grep over engine/ scripts/ analysis/). (2) the audit-review gate directly before it: `if not is_audit_review_complete(db._conn): … _finish_review_run(db, run_id, "interrupted", reason=rm.REASON_BLOCKED_AUDIT_REVIEW); return` — an export happens only after human review is complete, i.e. when no extraction/audit writer is expected. (3) single-operator use; `open_run` refusals and the experiment flock make two simultaneous pipeline runs unusual but none of them is an export lock. (4) CLAUDE.md's write-window/exclusivity rule (R85/R86) is procedure, not code. Nothing in code prevents an adjudication import or a second `--skip-to export` during an export.
- **Overlap:** none given. Found: **H4 (closed)** — "PRISMA flow computed three times per export" — adjacent, narrower (it removed one recomputation inside `export_prisma_csv`; the methods exporter still recomputes the flow, which is sub-claim (a)). **C29/C33 (closed)** concern `export_all`'s arm/min_status arguments — different. No open row covers package coherence.
- **Proposed class:** 1 by definition (a delivered package could mix database states with no marker), but low likelihood: every trigger needs a concurrent writer or exporter that today's gate and single-operator use make rare. If the architect classes by reachability rather than consequence, 2.
- **Owning state:** pre-tag (R512) for the minimum; the package/manifest design can follow the tag — flag: nothing here is reachable on live before Run 7 (live cannot pass the audit-review gate; `studies_included` is 0).
- **Minimum loud-failure fix:** in `export_all`, hold one read transaction for the whole export (`BEGIN` on the connection before the first exporter, `ROLLBACK` after the last — a WAL read snapshot, no writer blocked) and write all five files into a unique staging directory (`tempfile.mkdtemp(dir=out)`), publishing by rename only after the last exporter returns; on any exception remove the staging directory and leave the previous package untouched.
- **Full fix:** materialise the result grid once and pass it to all three evidence writers and the Field States sheet; publish a versioned package with a manifest (run id, arm, codebook hash, db fingerprint or `data_version`, per-file sha256) and a completion marker written last.
- **Acceptance test:** (1) a second connection commits a field event between two exporter calls (hook the second exporter) — CSV, xlsx sheet 1, xlsx Field States, docx and PRISMA all show the pre-commit value. (2) the last exporter raises — every previously published file is byte-identical to before and no `.tmp`/staging entry remains. (3) two `export_all` calls to one directory, interleaved, never open the same temporary path.
- **Size:** M — one brief; Phase A must read how `effective_value`/`effective_state` behave inside an open read transaction and which tests pin `output_path + ".tmp"` (tests are callers).
- **PI-decision flag:** no
- **Differences from the original / the brief's note:** (1) B lists four exporters' worth of sequencing; the stronger intra-file fact is that one workbook runs the grid twice AND reads the corpus id list twice (once in `export_evidence_excel`, once inside `_build_evidence_rows`), so sheet 1's paper set and sheet 3's can differ. (2) B does not note that the per-writer failure handler unlinks the shared temp path — a failed export can delete a concurrent export's in-flight file. (3) B does not state the mitigations; with one caller behind a human-review gate the practical exposure is the failed-late-exporter case (d), which needs no concurrency at all and is the one sub-claim reachable by a single operator. (4) `export_prisma_csv` reconciles (`validate_prisma_counts(db, flow)`) with fresh queries against a flow computed earlier — a concurrent status write there fails loudly rather than silently, which is the right direction.


---

### F10 — Acquisition does not enforce its size limits on actual transfer/decompression
- **Source:** outside B
- **Present at HEAD:** yes, all five sub-claims —
  (a) whole-body read written straight to the final path: **yes**;
  (b) PMC `stream=True` then full `.content`, ceiling on the header only: **yes**;
  (c) archive member read whole, no member/decompressed ceiling: **yes**;
  (d) first PDF member taken without checking it is the article: **yes**;
  (e) acceptance on a `%PDF` prefix alone: **yes** — and it is also the *resume* test, which B did not say.
- **Evidence level:** code order (no acquisition tool run, no request made — I22/R308)
- **Evidence:** all in `engine/acquisition/download.py`.
  (a) `_download_direct`: `dest.write_bytes(resp.content)` then `if not is_valid_pdf(dest):` → rename to `quarantine/`. `_save_if_pdf`: `dest.write_bytes(data)`. `dest` is the final `pdf_dir / f"{pid}.pdf"` (`download_papers`); no temp file, no `os.replace`, no fsync. Every strategy (`_download_via_doi_redirect`, `_download_via_ieee`, `_download_via_mdpi`) reads `resp*.content` with no `stream=` and no byte cap at all; `timeout=REQUEST_TIMEOUT` is requests' per-read timeout, not a transfer budget.
  (b) `_download_via_pmc`: `resp2 = requests.get(pkg_url, timeout=60, …, stream=True)` … `content_length = int(resp2.headers.get("Content-Length", 0))` / `if content_length > 100 * 1024 * 1024:` → `"archive_too_large"` … `archive_bytes = resp2.content`. A missing header reads as 0 and passes; an understated one passes; the comment says "Check Content-Length before loading into memory (100MB ceiling)". `resp2` is closed only on the too-large branch.
  (c)+(d) same function: `tar = tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz")` / `pdf_members = [m for m in tar.getnames() if m.endswith(".pdf")]` / `content = tar.extractfile(pdf_members[0]).read()`. No size check on the member (`TarInfo.size` is never read), archive order decides which PDF, `tar` is never closed.
  (e) `is_valid_pdf`: `return f.read(4) == PDF_MAGIC`; `_is_pdf_bytes`: `len(data) >= 4 and data[:4] == PDF_MAGIC`. The module docstring states it as the guarantee: "Validates %PDF magic bytes … Idempotent: skips papers with existing valid PDFs."
  **Consequence that makes (a)+(e) more than resource hygiene** — `download_papers`: `if dest.exists() and is_valid_pdf(dest):` → `UPDATE papers SET download_status = 'success', pdf_local_path = ? …` ("Already have it — update DB"). A process killed inside `write_bytes` leaves a `%PDF`-prefixed partial file at the final path; the next invocation records it as a successful acquisition without re-downloading. Not run (would need the tool); established from the two quoted statements.
  Same pattern, not engine code: `scripts/pdf_acquisition/step3_download_oa_pdfs.py` (`dest.write_bytes(resp.content)`) and `step3b_retry_failed.py` (`tarfile.open(fileobj=io.BytesIO(resp.content)…)`, `extractfile(pdf_members[0]).read()` — with no ceiling at all).
- **Overlap:** D20 ↔ I22 — **REJECTED as the same defect**: D20 is "no writer of `full_text_not_obtainable` (nor of `PDF_ACQUIRED`)" — a missing state writer; I22 is "`verify_downloads --pdf-dir` pointed at live's `pdfs/` would rename live's files" — a path hazard for copies. Same stage and same recorded owner ("the acquisition cut-over, junior"), different mechanisms, different files (`download.py`/scripts vs `verify_downloads.py`). F10 ↔ D20: REJECTED (F10 is about what is accepted as a PDF, D20 about what is recorded when none is obtained) — but adjacent: the `'failed'` that D20 calls non-terminal is what `_download_one` returns for `archive_too_large`. F10 ↔ I22: REJECTED as a defect; I22's standing rule binds F10's *verification* (stubbed `requests` only). No inventory row found for F10's content (grep of the plan for `Content-Length`, `write_bytes`, `%PDF` under Step 2: none).
- **Proposed class:** 1 — a truncated file, or a supplement, is recorded `download_status='success'` with `pdf_local_path` and flows to parse/FT/extraction as the paper's full text (the PDF quality check is a later human gate that may or may not catch it — not established). The memory/size sub-claims alone would be class 2.
- **Owning state:** pre-tag (R512 names F10 explicitly). Flag: both neighbouring rows (D20, I22) are owned by the junior acquisition cut-over, so F10 pre-tag lands in a module whose cut-over is later — keep the fix inside `download.py`'s write/accept boundary and do not start the cut-over.
- **Minimum loud-failure fix:** one `_publish_pdf(data_or_stream, dest)` used by every strategy: write to a unique temp file in `dest.parent`, enforce an actual-byte cap while streaming (`iter_content`), require a readable PDF (trailer/`%%EOF` or a PyMuPDF open with ≥1 page), then `os.replace`. PMC: cap bytes read, check `TarInfo.size` against a member cap before `extractfile`, and return a distinct status (`pmc_multiple_pdfs`) instead of taking `[0]` when more than one PDF member exists. The resume test (`dest.exists() and is_valid_pdf(dest)`) uses the same readability check.
- **Full fix:** the above plus a total time budget per transfer, context-managed responses/archives, package-metadata selection of the main article (PMC OA `.nxml` names it), `None`-safe `extractfile`, and retirement or alignment of the two `scripts/pdf_acquisition` copies. The terminal-state writer stays D20's.
- **Acceptance test:** with `requests.get` stubbed (no network, no tool run against a review copy): chunked body without `Content-Length` over the cap → refused, nothing at `dest`; understated header → refused; small `.tar.gz` with an over-cap member → refused before the member is read; an exception mid-write → no file at `dest`, and a following `download_papers` does not mark success; a `%PDF`-prefixed truncated file already at `dest` → not accepted; supplement-before-article archive → distinct status, not `success`.
- **Size:** M — mechanism read, but five strategies plus the resume path and two script copies; tests must be built on a fake transport because I22 forbids running the tool.
- **PI-decision flag:** yes — a PMC package with more than one PDF: refuse to the manual-download list (minimum fix) or select by package metadata now?
- **Differences from the original / the brief's note:** B's line hints still land on the right functions. B understates (a)/(e): the partial final-path file is not only "left behind", it is *accepted* on the next run by the idempotence check. B's "close responses and archives" is confirmed (`tar` never closed; `resp2` closed on one branch). Not in B: `int(resp2.headers.get("Content-Length", 0))` raises `ValueError` on a malformed header and `tar.extractfile(...)` returns `None` for a non-regular member — neither exception is in the `except (requests.RequestException, tarfile.TarError)` tuple, so either aborts the whole batch (class 2, folded here, no separate row). The brief's pairing "D20 and I22" is same-module, not same-defect.


---

### F11 — Prefix truncation is not evidence that an eligibility feature is absent
- **Source:** outside B
- **Present at HEAD:** yes —
  (a) truncation is a prefix, optionally cut earlier at a references-like line: **yes**;
  (b) docstring claims section prioritisation the code does not perform: **yes**;
  (c) the model is not told what was omitted: **yes** (only a generic, constant hedge);
  (d) primary and verifier see the same truncated text: **yes**;
  (e) the input-fit guard cannot see text discarded before the request is built: **yes** — the guard *does* run on FT calls at HEAD, but only on the already-cut message.
- **Evidence level:** reproduced (a, b, c on the pure function); code order (d, e)
- **Evidence:** `engine/agents/ft_screener.py`, `truncate_paper_text`.
  Docstring: "Strategy: Always include title + abstract at the top. Then include as much of the body as fits, prioritizing Introduction, Methods, and Results. Truncate from the end if the text exceeds max_chars."
  Body: `ref_match = re.search(r"^(?:#{1,4}\s+)?(?:references|bibliography|acknowledgements?)\b", full_text, re.IGNORECASE | re.MULTILINE)` / `if ref_match and ref_match.start() <= remaining_budget: body = full_text[:ref_match.start()].rstrip()` / `else: body = full_text[:remaining_budget]` / then trim to the last `". "` if it lies in the final 20% / `return header + body`. Nothing is appended; the return type is `str`, so no coverage metadata exists to store. `_SECTION_RE` (the Introduction/Methods/Results pattern) is defined above the function and referenced nowhere (grep: one occurrence repo-wide). `max_chars` defaults to `FT_MAX_TEXT_CHARS = 32_000  # ~8,000 tokens` (`engine/core/constants.py`).
  Reproducer `~/scratch/12f/repro/F11_truncation.py` (cwd `~/scratch/12f/g4`, `PYTHONPATH=<repo> .venv/bin/python ../repro/F11_truncation.py`), output `F11_truncation.out`:
  `max_chars 32000 | input 46627 | output 31998` · `A. Methods kept: False | Results kept: False` · `A. any omission marker in output: False` · `A. output is a pure prefix of header+body: True` · `B. early 'References ...' prose line: output 50 chars of 46704 -> 'Abstract: A\n\n# Introduction\nShort intro.'` · `C. 40k-char abstract: output 32000 | contains any body: False` · `D. _SECTION_RE occurrences in module source: 1`.
  (c) the only wording is constant, in `build_ft_screening_prompt`: "You have access to the paper's full text (or a substantial portion)." — sent identically for a complete and a cut paper; `build_ft_verification_prompt` has no hedge at all ("PAPER FULL TEXT:"). The decision ends "Based on the full text, classify…".
  (d) `run_ft_screening` and `run_ft_verification` each call `truncate_paper_text(parsed_text, title=paper.get("title", ""), abstract=paper.get("abstract", ""))` on the same `load_parsed_text` result — the same deterministic cut. `add_ft_screening_decision` / `add_ft_verification_decision` store no length, no truncated flag.
  (e) `ft_screen_paper` / `ft_verify_paper` call `ollama_chat(messages=build_ft_messages(...), **cfg.kwargs())`; `ollama_chat` runs `_check_input_fits(model, messages, …)` before and `_check_input_was_read` after every call, over `message_chars(messages)` — i.e. the ≤32,000-char body plus prompt (≈6–7k tokens at `RATIO_MIN = 0.19`), which can never approach a ceiling. The guard reads the request, not the paper.
- **Overlap:** D5 — **CONFIRMED, same defect, D5 narrower in wording**: its first half ("Full-text input cut at 32,000 characters") is F11(a) and is still true. Its second half ("no input-fit guard on the FT calls") is **no longer true at HEAD as written** — every FT call passes through `ollama_chat`'s guard — but true in effect, since the cut happens upstream of the guard (F11(e)). D5's status is still `WRONG (366 FT decisions on partial text)`, coverage `D5 D6 | S3f`. **S3f did not land for full-text screening**: S3f reads "`truncate: false` on every call; FT calls go through the input-fit guard; a full text that exceeds the context is not cut — the paper enters "full text exceeds context" and waits." Clause 1 was replaced by the guard (R120, D6 closed); clause 2 is mechanically true; clause 3 is not — the text is still cut and nothing "waits". The state-allocation table still says for freshman "S3f (`truncate: false` only) … FT input-fit guard absent (FT screening is not re-run in freshman)" — stale on the guard, right that the FT fix was deferred. Also: an `InputFitError` on an FT call is not caught by the FT loops (`except (json.JSONDecodeError, ValidationError)`), so it would fail the run rather than park the paper; unreachable while the 32,000 cut stands.
- **Proposed class:** 1 — an eligibility decision (`full_text_out`, or the verifier's `eligible`) is stored as a full-text decision with no record that part of the text was never shown; both models agree on the same incomplete input.
- **Owning state:** pre-tag (R512). Flag: the plan placed the FT input fix outside freshman on the ground that FT screening is not re-run there; R512 overrides that for the engine, but whether existing decisions are redone is the PI question below.
- **Minimum loud-failure fix:** make truncation a fact the pipeline acts on: `truncate_paper_text` returns `(text, complete: bool, original_chars, sent_chars)`; when `complete` is false the primary may not write `FT_SCREENED_OUT`/`full_text_out` — the paper goes to `FT_FLAGGED` for a human (an include may still pass to the verifier). Delete the references-line shortcut or anchor it to a heading (`^#{1,4}\s+references\s*$`) so prose cannot trigger it; correct the docstring; delete `_SECTION_RE`.
- **Full fix:** S3f as written — send the whole parsed text and let the input-fit guard decide (a paper that cannot fit enters `input_exceeds_context`-style waiting, mapped in the FT loops), or section-aware chunking with coverage recorded on the decision row/event and stated in the prompt. Persist coverage (original length, sent length, parsed-text identity) with every FT decision.
- **Acceptance test:** a synthetic paper whose only qualifying evidence lies after character 32,000 → never an unqualified exclusion (flag, or a decision made on full text); a prose line beginning "References to…" early in the body does not shorten the input; a 40k-character abstract does not produce a body-less "full text"; a complete paper is unchanged byte-for-byte in its request (request-capture pin).
- **Size:** M for the minimum (return shape has callers and tests — `tests/test_ft_screening.py` pins `FT_MAX_TEXT_CHARS == 32_000`; request hashes change only for cut papers); L if existing decisions are re-screened (model run on live, post-R237 ordering).
- **PI-decision flag:** yes — the live review's full-text decisions were made on truncated input (D5's count, 366 — carried from the plan, v49 FT-INPUT-01, NOT re-measured here; the live database was not opened): fix the engine only, or also re-screen those papers?
- **Differences from the original / the brief's note:** B is right on every sub-claim. Two things B did not say, both reproduced: (1) the references shortcut matches any *line* beginning `references|bibliography|acknowledgements` (no end-of-line anchor, heading marker optional), so on an over-length paper an early prose line can cut the body to almost nothing — 50 characters in the reproducer; it is only reachable when the text exceeds the budget; (2) an abstract at or above the budget yields `header[:max_chars]` with no body. B's "input-fit checks cannot detect" is accurate; D5's "no input-fit guard on the FT calls" should be reworded rather than left. CLAUDE.md's "Text truncation to 32K chars" / "32K truncation" are accurate descriptions of the behaviour, not broken guarantees; the broken guarantee is the function's own docstring, plus the module docstring naming "Qwen3.5:27b" as primary where CLAUDE.md says qwen3:32b (class 3, spec not read, not chased).


---

### F12 — The screening stage ignores `limit`
- **Source:** outside B
- **Present at HEAD:** yes
- **Evidence level:** code order
- **Evidence:** `scripts/run_pipeline.py` `_stage_screen`:
  `if limit:` / `papers = db.get_papers_by_status("INGESTED")` / `if len(papers) > limit:` / `logger.info("Limiting screening to first %d of %d papers", limit, len(papers))` / `# Screen only the limited set by temporarily updating the rest` / `# Actually, run_screening processes all INGESTED, so we handle this` / `# by running screen on the full set — the limit was applied at search` / `pass` — then, outside the `if`, `stats = run_screening(db, spec)`. `run_screening` takes no bound (`def run_screening(db: ReviewDatabase, spec: ReviewSpec)`; docstring "Run dual-pass primary screening on all INGESTED papers"; `papers = db.get_papers_by_status("INGESTED")`, minus its checkpoint). So the log line states a limit the next statement does not apply.
  Other bounds: the only other use of `limit` is `_stage_search`: `if limit: unique = unique[:limit]` before `db.add_papers(unique)` — it bounds what THIS run adds, not what is INGESTED. With `--skip-to screen` (`if start_idx <= STAGES.index("search")` is false) the search slice never runs; on a pre-existing database, earlier INGESTED rows are all screened. `run_pipeline` passes `limit` to exactly two calls (`_stage_search(db, spec, limit)`, `_stage_screen(db, spec, limit)`); parse/extract/audit/export never see it. After screening, the C57 adjudication gate stops the run — a stop after the whole pending set has been screened, not a bound.
  CLI: `--limit` (`type=int, default=None`, help "Limit number of papers to process (for testing)") → `run_pipeline(..., limit=args.limit, max_papers=args.max_papers)`. `--max-papers` is separate: `_positive_int`, validated by `check_max_papers` before anything opens, applied by `bound_selection` in the extract stage only, refused for a start after extract, and recorded in the run manifest (`bound = {"stage": "extract", "max_papers": max_papers}`). `limit` is none of those: unvalidated (`if limit:` makes 0 mean unbounded; a negative value slices `unique[:-n]`), not in the manifest, and its help text promises a general bound it does not deliver.
- **Overlap:** no pairing was given. Found: **C55 / C57** (both closed fixes on the same search/screen start path) — different defects. R236 ("smokes declare --max-papers") covers extraction only; nothing bounds a screening smoke.
- **Proposed class:** 2 — no stored result is wrong (every decision written is a real screening decision), but a run asked to be small screens the entire pending set: model time, and on a real review irreversible status transitions for papers the operator did not mean to touch. Arguable as 1 if "the operator's declared bound was not honoured and nothing records that" is read as a silent wrong; the log line is actively false.
- **Owning state:** pre-tag (R512).
- **Minimum loud-failure fix:** in `_stage_screen`, when `limit` is set and more than `limit` papers are INGESTED, raise before `run_screening` ("--limit does not bound screening; N INGESTED") — or remove `--limit` and its false log line.
- **Full fix:** give `run_screening` an explicit ordered paper-id selection (ascending id, first N), declare the bound in the manifest as `--max-papers` does for extract, validate with the same positive-int rule, and state the two flags' scopes in the help text.
- **Acceptance test:** 100 INGESTED, `--skip-to screen --limit 2` → exactly 2 papers receive model calls and decisions, 98 stay INGESTED with no decision rows; or the run refuses before the first call.
- **Size:** S — one function plus the screener's signature; the mechanism is read.
- **PI-decision flag:** yes — keep `--limit` as a real, manifest-recorded screening bound, or retire it in favour of per-stage bounds like `--max-papers`?
- **Differences from the original / the brief's note:** B is accurate. Additions: `limit` is not recorded in the run manifest (unlike `max_papers`), is unvalidated (0 / negative), and reaches only search and screen.


---

### F13 — Screening persistence is incomplete across retries and restarts (abstract screener)
- **Source:** outside B
- **Present at HEAD:** yes, all four sub-claims —
  (a) pass 1, pass 2 and the resolved status are three separate commits: **yes**;
  (b) a rerun after a partial attempt adds another pass-1 row, nothing distinguishes or resumes the attempt: **yes**;
  (c) checkpoints are non-atomic JSON holding paper ids only: **yes**;
  (d) `run_verification` deletes its checkpoint on completion and re-selects every `ABSTRACT_SCREENED_IN` paper next time, with no filter on persisted input/configuration identity: **yes**.
- **Evidence level:** reproduced (a, b, d on a synthetic database with the model call stubbed); code order (c)
- **Evidence:** `engine/agents/screener.py`, `run_screening`: `d1 = screen_paper(paper, spec, pass_number=1, model=primary_model)` / `db.add_screening_decision(pid, 1, …)` / `d2 = screen_paper(… pass_number=2 …)` / `db.add_screening_decision(pid, 2, …)` inside `try: … except (json.JSONDecodeError, ValidationError)`, then `db.update_status(pid, …)`. `engine/core/database.py`, `add_screening_decision`: the INSERT is followed by `self._conn.commit()` unconditionally (no `commit=False` parameter, unlike `add_ft_screening_decision`); `update_status` opens its own `BEGIN IMMEDIATE … COMMIT`. Any other exception from pass 2 (`TimeoutError` after the watchdog, an `InputFitError`, a kill) leaves the pass-1 row committed and the paper at `INGESTED`; selection is `db.get_papers_by_status("INGESTED")` minus the checkpoint, so the paper is taken again. The row carries `(id, paper_id, pass_number, decision, rationale, model, decided_at)` — no attempt, run or configuration column (schema `abstract_screening_decisions`, `CHECK (pass_number IN (1, 2))`).
  (c) `_save_checkpoint`: `path.write_text(json.dumps({"screened_ids": sorted(screened_ids)}))`; written only `if i % 10 == 0 or i == len(pending)`; `_load_checkpoint` returns `set()` on `JSONDecodeError` (a torn file silently reads as "nothing done"). No review, spec-hash, model or run identity in the file.
  (d) `run_verification`: `papers = db.get_papers_by_status("ABSTRACT_SCREENED_IN")` / `pending = [p for p in papers if p["id"] not in verified_ids]` … `if ckpt_path.exists(): ckpt_path.unlink()` — no join against `abstract_verification_decisions`. Contrast the FT verifier at HEAD (`run_ft_verification`, R-V1), which filters on "no live eligible event AND no verification decision row" and calls the checkpoint "only a within-run resume aid". A confirmed paper keeps the status `ABSTRACT_SCREENED_IN`, so it is re-selected by construction.
  Reproducer `~/scratch/12f/repro/F13_screening_persistence.py` (cwd `~/scratch/12f/g4`, `ReviewDatabase("synth", data_root=<scratch>)`, `screen_paper` and `require_preflight` patched), output `F13_screening_persistence.out`:
  `run 1 died: simulated watchdog exhaustion` · `after run 1: status {1: 'INGESTED', 2: 'INGESTED'} | checkpoint exists: False` · decisions `(1, 1, 1, 'include')` · after run 2 `(1,1,1) (2,1,1) (3,1,2) (4,2,1) (5,2,2)` · `paper 1 pass-1 rows: 2` · `verification: invocation 1 made 2 calls; identical invocation 2 made 2 calls` · `abstract_verification_decisions rows: 4 for 2 papers` · `paper_events rows: 0`.
  Readers that the extra rows reach (read, not run): `engine/exporters/prisma.py` — `SELECT sd.rationale, COUNT(*) … FROM abstract_screening_decisions sd JOIN papers p … WHERE p.status = 'ABSTRACT_SCREENED_OUT' AND sd.decision = 'exclude' GROUP BY sd.rationale` counts decision rows, not papers; `engine/adjudication/screening_adjudicator.py` and `abstract_adjudication_html.py` — `for r in rows: if r["pass_number"] == 1: primary_decision = r["decision"]` over `ORDER BY pass_number` (tie order between two pass-1 rows unspecified), so the human queue can show the abandoned attempt's pass-1; `evidence_table.py`'s "Screening Log" sheet lists every row.
- **Overlap:** S4 — **CONFIRMED as the planned home, but broader and different in purpose**: S4d ("Screening decisions are events in S2 with criterion, offsets, verifier verdict and run id") would give decisions run identity and so subsume (b)/(d); S4 is an evidence-backed-exclusion *experiment* "adopted only if it meets" a pre-registered bar, assigned to junior (R183: "The cut-over (S4a–S4d) stays in junior"). F13's atomicity/idempotence defect does not need S4's contract change. E9 — **CONFIRMED as related, not the same**: E9 says `run_verification` "is called only by `tests/test_screener.py`"; re-checked by grep at HEAD — no caller under `engine/`, `scripts/` (`run_pipeline` imports and calls `run_screening` only; `rescreen_with_specialty.py` has its own `run_verification_pass`), or `analysis/` (a docstring mention only). So sub-claim (d) is real but **latent in production** — reachable only by a direct call. Sub-claims (a)–(c) are on `run_pipeline`'s live path. Also the 12e-closure note (R491): "Abstract-screening decisions carry no `run_id`, write no paper event, and their `run_calls` rows have `paper_id` NULL; with S4" — confirmed (`screen_paper` passes no `paper_id` to `ollama_chat`; no `write_paper_event` in `screener.py`; reproducer: 0 paper events). That missing identity is *why* (b) cannot be repaired by labelling alone.
- **Proposed class:** 1 — the stored decision history gains rows indistinguishable from the deciding attempt; PRISMA's exclusion-reason counts and the human adjudication queue read them. The resolved `papers.status` itself is computed from the in-memory pair of one attempt and is not made wrong.
- **Owning state:** pre-tag under R512 as a confirmed outside finding — **flagged**: the plan assigns the screeners' cut-over (S4a–S4d, R183) and E9 (R323) to junior. See the PI flag.
- **Minimum loud-failure fix:** one transaction per decided paper, as the FT screener already does (`_paper_transaction`, R260): give `add_screening_decision` / `add_verification_decision` a `commit=False`, write pass 1 + pass 2 + status together, so an interrupted paper leaves nothing. `run_verification` selects only papers with no `abstract_verification_decisions` row (the R-V1 pattern). Checkpoint written via temp file + `os.replace`, or dropped — with atomic per-paper commits the status/decision rows are the resume authority.
- **Full fix:** S4d — screening decisions as run-identified events (run id, stage, request hash, configuration identity), status resolved from one complete identified attempt, re-verification only under a declared changed configuration. Junior-sized.
- **Acceptance test:** stub the model; fail after pass 1, after pass 2, and before the status write, then rerun: exactly one pass-1 and one pass-2 row per paper and the status of that pair. Invoke verification twice unchanged: the second makes zero calls and writes zero rows. Kill during a checkpoint write: the next run neither crashes nor treats screened papers as unscreened.
- **Size:** S for the minimum (mechanism read; the FT screener is the template; `tests/test_screener.py` calls both functions and both writers — read its coupling first); L for S4d.
- **PI-decision flag:** yes — R512 pulls every confirmed outside finding pre-tag, R183/R323 keep the screeners' cut-over (S4, E9) in junior: for F13, is the pre-tag obligation the minimum fix only (per-paper atomicity + verified-paper filter, no run identity), with run/configuration identity staying in S4?
- **Differences from the original / the brief's note:** B is accurate. Additions: (d) has no production caller (E9), so its "model work repeats" is latent; the abstract path has neither `run_id` nor paper events, which B's fix presupposes; the non-`ValidationError` failure path is the realistic trigger (a watchdog `TimeoutError` after pass 1 — see F14), not only "termination". B's "configuration changes are not represented in the checkpoint identity" is confirmed, but the checkpoint is deleted on every clean completion, so a stale checkpoint survives only a crashed run. Not chased: `scripts/screen_expanded.py` is a second abstract-screening path with its own writes.


---

### F14 — Watchdog timeout does not stop the underlying Ollama call
- **Source:** outside B
- **Present at HEAD:** yes —
  (a) on `future.result` timeout the running task is not cancelled, the thread is abandoned alive: **yes**;
  (b) a new executor is created for each retry while the abandoned call may still be running: **yes**;
  (c) repeated timeouts accumulate live calls/threads: **yes** (up to 3, 4 with the post-restart attempt);
  (d) process exit after a plain timeout is delayed until the blocked workers finish: **yes**;
  (e) what the abandoned request does on the *server* (queued, parallel, GPU contention): **not established** (no Ollama call permitted).
- **Evidence level:** reproduced (a–d, against `ollama_chat` itself with a fake client); code order for the restart branch; (e) conditional
- **Evidence:** `engine/utils/ollama_client.py`, `ollama_chat`: `for attempt in range(1 + max_retries):` / `executor = ThreadPoolExecutor(max_workers=1)` / `future = executor.submit(_client.chat, …)` / `response = future.result(timeout=effective_timeout)` / `except FuturesTimeoutError:` / `# Abandon the hung thread — do not wait for it` / `executor.shutdown(wait=False, cancel_futures=True)` / `if attempt < max_retries: time.sleep(retry_delay)` / else `_restart_ollama_and_retry(...)`. `cancel_futures=True` cancels only queued futures; the one task is already running. Nothing closes the HTTP connection: `_client` is one module-level `ollama.Client(timeout=_httpx_timeout)` shared by every attempt, with `_HTTP_READ_TIMEOUT = 900.0`, so an abandoned request stays connected until it completes or goes 900 s without a byte. Defaults: `DEFAULT_MAX_RETRIES = 2` ("default 2 → 3 total"), `DEFAULT_RETRY_DELAY = 30`; watchdog 300/600/900/1200 s by model-name pattern, else 600.
  Last resort, `_restart_ollama_and_retry`: refuses with `RuntimeError` if `restart_disabled()` or `foreign_lock_held()`; otherwise `subprocess.run(["sudo", "systemctl", "restart", "ollama"], timeout=30, check=True, …)`, `time.sleep(10)`, then a **fourth** `ThreadPoolExecutor(max_workers=1)` and one more `future.result(timeout=effective_timeout)`; on failure `executor.shutdown(wait=False, cancel_futures=True)` and `RuntimeError`, which `ollama_chat` turns into `TimeoutError`. A successful restart is the only thing in the module that ends the abandoned requests (the server dies under them); when the restart is refused — the designed behaviour under a foreign experiment lock or the opt-out — up to three stay live.
  Accounting: `_record("error", exc=exc)` runs once in the outer `except Exception`, so one `run_calls` row stands for up to four requests sent; the abandoned attempts appear only as WARNING log lines.
  Reproducer `~/scratch/12f/repro/F14_watchdog.py` (fake `_client` whose `chat` sleeps 4 s; `EVIDENCE_ENGINE_NO_OLLAMA_RESTART=1`; `subprocess.run` replaced by an assertion; `wall_timeout=0.2, retry_delay=0`), output `F14_watchdog.out`:
  `raised TimeoutError after 0.62s` · `requests started: 3 | still running when ollama_chat returned control: 3 | peak simultaneous: 3` · `live worker threads: 3` · `main done at 0.62s` · `exit code 1; whole-process wall time 4.617215227 s`. The 4 s between "main done" and exit is the interpreter joining the workers (`concurrent/futures/thread.py`: `threading._register_atexit(_python_exit)`, which joins every started worker).
  Callers: the extraction path classifies the result and moves on — `engine/core/extraction_events.py`: `if isinstance(exc, (TimeoutError, httpx.HTTPError)) … return fail(PS.REASON_MODEL_CALL_FAILED)` — so the next paper's call is issued while the abandoned ones may still be connected. The screeners catch only `(json.JSONDecodeError, ValidationError)`, so there the `TimeoutError` ends the run (and leaves F13's partial attempt).
- **Overlap:** E-EXEC — **CONFIRMED as PARTIAL, as the brief expected**. E-EXEC (closed `d4d64c2`, R368) is the *exit* half for *interrupts* only: `engine/core/run_manifest.py`, `exit_process`: `if code in INTERRUPT_EXIT_CODES:` flush, `os._exit(code)`; its docstring: "Every other code goes through `sys.exit(code)` and normal shutdown, unchanged." It is called from two `__main__` lines only (`scripts/run_pipeline.py`, `engine/agents/ft_screener.py`). So (i) a process ending on a watchdog `TimeoutError` (exit code 1 / traceback) still waits for the blocked workers — reproduced above, bounded in production by generation completing or the 900 s read timeout per abandoned request; (ii) every other entry point (cloud runner, judge CLIs, eval runners, parser, quality check) has no `exit_process` at all, interrupt or not; (iii) E-EXEC never touched the in-process half — overlapping retries while the process keeps running. E-EXEC's own row describes the mechanism F14 generalises ("the process may wait for the in-flight generation at exit").
- **Proposed class:** 2 — control is not restored and work overlaps, but no stored result is made wrong by it (a late response from an abandoned thread is discarded; nothing reads the dead future). One class-3 edge: `run_calls` under-counts requests sent. If (e) showed that overlapping requests change a *response* (e.g. context eviction altering output), that would need re-classing — not established.
- **Owning state:** pre-tag (R512). Note it touches the transport every model call uses; `tests/test_request_capture.py` and `tests/test_ollama_client.py` are callers of this shape.
- **Minimum loud-failure fix:** do not send another request while one is outstanding: on `FuturesTimeoutError`, close the abandoned request's connection (a per-attempt `ollama.Client`, closed on timeout — E-EXEC-A V4 measured that Ollama cancels on client disconnect) and confirm the worker has returned, bounded; if it has not, raise `TimeoutError` instead of retrying. Record each attempt (`run_calls` or `outcome_detail`) rather than one row per `ollama_chat`.
- **Full fix:** a cancellable transport with one explicit deadline (streaming read with a total budget, or a worker process that can be killed), no thread abandonment, bounded retries keyed on confirmed cancellation; `exit_process`-style exit (or daemon/explicitly joined workers) on every CLI and for non-interrupt failure codes.
- **Acceptance test:** fake client blocking past the deadline (no Ollama): at no time more than one in-flight `chat`; live worker count ≤ 1 when `ollama_chat` raises; the process exits within a small bound of the raise, with restart disabled and with it enabled (subprocess patched); every attempt is visible in telemetry.
- **Size:** M — mechanism read and reproduced, but the fix changes the client-construction seam that the request-capture gate and the QUALGAP harness (`_client` rebinding) depend on, and its key premise (closing the client cancels server-side generation on this Ollama version, for a non-streaming call) needs one measured confirmation on a throwaway model call — Phase A.
- **PI-decision flag:** no
- **Differences from the original / the brief's note:** B is right on all points; its line hints still fall on the loop and the restart branch. Additions: the abandoned thread is bounded by the 900 s HTTP read timeout (for a 32b model that equals the watchdog, so there the read timeout and the watchdog fire together); the count is 3 attempts plus 1 post-restart, each with its own executor; on success no executor is shut down either (the idle worker ends when the executor is collected — harmless, noted only); a refused restart is the common case in which nothing ends the abandoned calls. The brief's expectation about E-EXEC (partial; non-interrupt exits still delayed) is confirmed by the code and the stub run. Not established: server-side behaviour of the overlapped requests (queueing vs parallel, GPU contention), and whether closing the client cancels a non-streaming generation — both need a model call.


---

### F15 — Text exported to spreadsheets can be interpreted as a formula
- **Source:** outside B
- **Present at HEAD:** yes for xlsx (a string starting with `=` and longer than one character is stored as a formula); yes for CSV (no neutralisation anywhere); **narrower than B implies for the other prefixes** — openpyxl stores leading `+`, `-`, `@`, and space/tab/CR-prefixed `=` as plain strings, so those matter only for CSV opened in a spreadsheet application
- **Evidence level:** reproduced (library check + the repo's shared workbook builder); code order for the per-writer trace
- **Evidence:**
  - Library check, installed openpyxl 3.1.5 — `~/scratch/12f/repro/F15_openpyxl_formula.py` (run with `.venv/bin/python -I`), output `F15_openpyxl_formula.out`:
    ```
    append '=1+1'       -> data_type='f'      append '+1+1'  -> 's'     append '-1+1' -> 's'
    append '@SUM(1,1)'  -> 's'                append ' =1+1' -> 's'     append '\t=1+1' -> 's'   append '\r=1+1' -> 's'
    append '='          -> 's'                append '-5'    -> 's'
    append '=HYPERLINK("http://x","click")' -> data_type='f'
    ws.cell(value='=1+1') -> f ;  after c.data_type='s' -> s '=1+1' ;  quotePrefix only -> f
    reloaded: ('f','=1+1') … ('f','=HYPERLINK("http://x","click")')   (the saved file carries <f> cells)
    csv.writer output: '=1+1\r\n+1+1\r\n-1+1\r\n"@SUM(1,1)"\r\n =1+1\r\n\t=1+1\r\n'
    ```
    Both write paths the repo uses (`ws.append([...])` and `ws.cell(..., value=...)`) behave the same. Setting `cell.data_type = "s"` after assignment is a working neutraliser that keeps the raw string.
  - Through repo code — `~/scratch/12f/repro/F15_review_workbook_formula.py` (cwd `~/scratch/12f/g3`, `PYTHONPATH=<repo>`), `engine.exporters.review_workbook.create_review_workbook` with a formula-leading title and rationale, reloaded:
    ```
    [('n', 1), ('f', '=HYPERLINK("http://example.invalid","Robotic suturing")'), ('f', '=1+1')]
    [('n', 2), ('s', '-12% leak rate after robotic anastomosis'), ('s', '@note')]
    ```
  - **Writer trace at HEAD** (grep for `=`-handling — `startswith("=")`, `data_type`, `quotePrefix` — over engine/ analysis/ scripts/: no hit; the only sanitisers are control-character strippers):
    | writer (file · function) | format | untrusted columns | neutralisation |
    |---|---|---|---|
    | `engine/exporters/evidence_table.py` · `export_evidence_csv` (`writer.writerows(rows)`) | CSV | `title`, `authors`, `journal`, `doi`; per field `value` (model-produced or reviewer-entered) and `{field}_snippet` (paper text); `processing_reason` | none |
    | same · `export_evidence_excel` sheet "Evidence Table" (`ws1.append(row)`) | xlsx | same as above | none |
    | same · sheet "Screening Log" (`ws2.append(list(dict(r).values()))`) | xlsx | `title`, `rationale` (model free text), `model` | none |
    | same · sheet "Field States" (`ws3.append([... titles.get(paper_id) …, ev.value, …])`) | xlsx | `title`, `value` | none |
    | `engine/exporters/prisma.py` · `export_prisma_csv` | CSV | Detail column: `reason[:80]` = the screener's free-text `rationale`; source name; failure `processing_reason` | none |
    | `engine/exporters/review_workbook.py` · `_build_review_queue_sheet` (`cell = ws.cell(row=row_idx, column=col_idx, value=value)`), shared builder | xlsx | whatever the caller's rows carry; also `_build_reference_sheet` (`ws.cell(row=i, column=1, value=line)`, spec text — operator-authored) | none (only `truncate`) |
    | `engine/adjudication/screening_adjudicator.py` · `export_adjudication_queue` → builder | xlsx | `title`, `abstract[:2000]`, `journal`, `doi`, `primary_rationale[:1000]`, `verifier_rationale[:1000]` (model) | none |
    | `engine/adjudication/ft_screening_adjudicator.py` · `export_ft_adjudication_queue` → builder | xlsx | `title`, `abstract[:2000]`, `journal`, `doi`, `primary_rationale`, `verifier_rationale`, `text_excerpt[:500]` (paper text) | none |
    | `analysis/paper1/pi_audit_sampler.py` · blinded sheet + key sheet (`ws.cell(row=i, column=col, value=_sanitize_for_xlsx(val))`) | xlsx | `arm_value` (model), `source_text` (paper text), key-sheet reasoning | control chars only (`ILLEGAL_CHARACTERS_RE.sub("", value)`); `=` untouched |
    | `analysis/paper1/pi_audit_sampler_v2.py` · blinded + key (`value=_xml_safe(val)`) | xlsx | `arm_value`, `field_definition`, `source_text` | control/XML-illegal chars only; `=` untouched |
    | `analysis/paper1/pi_audit_unblind.py` · results workbook (`value=_sanitize(val)`) | xlsx | joined rows incl. `pi_notes` (human-typed), verdict text | control chars only |
    | `analysis/paper1/export_disagreement_pairs.py` · CSV (`csv.DictWriter`) and xlsx `_write_sheet` (`ws.cell(…, value=val or "")`) | CSV + xlsx | `paper_title`, `local_value`, `o4mini_value`, `sonnet_value` | none |
    | `engine/analysis/report.py` · `_write_disagreements_csv` | CSV | `value_a`, `value_b`, `detail` | none |
    | `engine/acquisition/manual_list.py` · manual download CSV (`csv.DictWriter`) | CSV | `title`, `first_author`, `publisher`, `doi` | none |
    | scripts (legacy/one-off): `scripts/screen_expanded.py`, `scripts/rescreen_original_251.py`, `scripts/pdf_acquisition/step1_export_citations.py`, `step4_manual_download_list.py`; `analysis/provenance/census.py`, `analysis/eval/score_screen2f.py` | CSV | titles / rationales / snippets (not read line by line — located by grep, listed for completeness) | none seen |
    DOCX (`docx_export.py`) out of scope per the brief.
  - Where it bites a human gate: in the PI-audit blinded workbooks the cell under adjudication IS `arm_value`; a model value beginning `=` is shown by Excel as the formula's result or `#NAME?`, so the PI adjudicates something other than the stored claim. In the adjudication queues the same applies to the title/rationale the PI reads. The xlsx importers (`load_workbook(input_path)` in both adjudicators) read only identifiers and the decision columns, so no formula text is re-imported.
- **Overlap:** none given; no inventory row found on formula handling (grep of the row table for formula/openpyxl/injection: only B1's kappa "formula").
- **Proposed class:** 1 (narrowly) — a human-review workbook or delivered evidence table can display something other than the stored value, with no error; no stored result is altered and no trigger is known in the present corpus. If classed by observed reach rather than consequence, 2.
- **Owning state:** pre-tag (R512) for the two engine paths that feed a human gate or a deliverable (review workbook builder, evidence table); `analysis/paper1` and `scripts/` writers can follow (legacy under R31 — retain-or-retire first).
- **Minimum loud-failure fix:** one helper (e.g. `engine/exporters/cells.py: write_text_cell`) that, for every `str` value, assigns the value and then forces `cell.data_type = "s"`; use it in `review_workbook._build_review_queue_sheet` and replace the three `ws.append(row)` loops in `export_evidence_excel`. Numbers stay numeric because only `str` is forced.
- **Full fix:** route every workbook writer through that helper (and fold the two existing control-character strippers into it — see INT-g3-1); for CSVs intended for spreadsheet use, prefix-neutralise (`'`) cells whose first character is one of `= + - @ \t \r` in a clearly named spreadsheet-safe export while keeping the raw CSV (or the event store) as the machine record; pin with a test.
- **Acceptance test:** export rows whose title / value / rationale are `=1+1`, `=HYPERLINK(...)`, `+1+1`, `-5`, `@x`, `\t=1+1`; reload each workbook: every such cell has `data_type == 's'` and its value equals the stored string byte for byte; an `int`/`float` stays `'n'`; the raw CSV round-trips through `csv.reader` unchanged; the spreadsheet-safe CSV has no cell starting with a formula marker.
- **Size:** S for the two engine writers (mechanism read, one helper, one commit); M if the analysis/paper1 and CSV policy are included (R31 retention decisions first).
- **PI-decision flag:** no (optional one-liner for the architect: keep the machine CSVs raw and add a spreadsheet-safe variant, or neutralise in place?)
- **Differences from the original / the brief's note:** (1) B's "formula markers" generalisation does not hold for openpyxl: only a leading `=` (length > 1) becomes `'f'`; `+`, `-`, `@`, and whitespace/control-prefixed `=` stay `'s'` (measured). They remain relevant to CSV only. (2) B cites only `evidence_table.py`; the human-gate exposure is larger in the shared review-workbook builder and the PI-audit workbooks, where the affected cell is what the reviewer judges. (3) The PI-audit writers DO have a sanitiser, but for control characters only — it should not be read as formula protection. (4) CLAUDE.md says the shared builder is "Used by all 3 adjudication exporters"; at HEAD `create_review_workbook(` has two callers (abstract and FT adjudication) — the audit-adjudication exporter no longer exists. Stale sentence. (5) Adjacent defect found while running the check, written separately: INT-g3-1.


---

### F16 — The integrity fingerprint does not encode text boundaries unambiguously
- **Source:** outside B
- **Present at HEAD:** yes (serialisation collision: yes · live database currently exposed: no — 0 separator-bearing cells)
- **Evidence level:** reproduced
- **Evidence:**
  - `engine/tools/db_fingerprint.py`, `encode_cell`: `return "T:" + str(value)` (last line; no escaping, no length prefix); NULL is the bare `"N"`, INTEGER `"I:" + str(value)`.
  - same file, `fingerprint_within`, the row loop: `if not first: digest.update(_RS.encode())` … `digest.update(_US.join(encode_cell(v) for v in row).encode())`; overall: `_RS.join(f"{t}{_US}{per_table[t]['sha256']}" for t in tables)`; schema hash: `_RS.join(_US.join(map(str, row)) for row in sorted(master, …))` (no type prefix at all there).
  - `CANONICAL_SERIALIZATION` states the same format in prose ("Cells within a row joined by U+001F (US), rows joined by U+001E (RS)") and is embedded in every record; the record carries **no scheme-version field** (grep of the record of reference `docs/session-reports/migration-02/review_db_fingerprint_20260929T172123Z.json` for a version/scheme key: none).
  - Reproducer `~/scratch/12f/repro/F16_fingerprint_collision.py` (cwd `~/scratch/12f/g5`, `PYTHONPATH=<repo>`, `.venv/bin/python`), output `F16_fingerprint_collision.out`:
    - B's exact pair, unchanged — A `('a\x1fT:b','c')`, B `('a','b\x1fT:c')`: both overall `541d981bf27a6b85d3df7515c793fdbfc9966476bca7f234a29fb62b91e3f56a`; `compare()` returns `[]`; the real CLI `python -m engine.tools.db_fingerprint B.db --compare A.fp.json` exits **0** and prints `IDENTICAL — schema, every table, and the overall hash all match.`
    - Type impersonation (not in B's text): A `('a\x1fI:5','z',NULL)` vs B `('a', 5, 'z\x1fN')` in `CREATE TABLE t (x, y, z)` — equal overall hash, `compare()` `[]`. A TEXT cell can stand in for an INTEGER cell and for a NULL.
    - Row boundary: one row `'a\x1eT:b'` vs two rows `'a'`,`'b'` — table sha and overall sha **equal**; only `row_count` (1 vs 2) makes `compare()` report a difference. A reader comparing `overall_sha256` alone (the value CLAUDE.md says a closeout quotes) sees equality.
  - Live read (the one authorised; `sqlite3.connect("file:data/surgical_autonomy/review.db?mode=ro", uri=True)`, cwd repo root, nothing written; `~/scratch/12f/repro/F16_live_separator_census.py` / `.out`): `tables=36 columns=384 text_cells_scanned=590185`; cells with U+001F or U+001E, per table/column: **NONE — all 0**; `sqlite_master` rows with either: `(0, 0)`. Scope: every column of every table, cells with `typeof()='text'` (BLOB cells are hex-encoded by `encode_cell`, so cannot carry a raw separator). The expected 0 is now measured. A count of a day, not an invariant: nothing in the engine refuses a separator in parsed or model text.
  - Where the fingerprint is relied on: (1) session open/close integrity check against the committed record of reference (CLAUDE.md "Ops Invariants — the database"; CLI `--compare`); (2) `engine/utils/db_backup.py` `auto_backup`: `diffs = compare(source_fp, backup_fp, …)` → on a difference deletes the copy and raises `BackupVerificationError`; (3) `db_backup.restore`: three comparisons (`expected_fingerprint` vs backup, backup vs staged temp, backup vs final target), each `RestoreRefused` on a difference; (4) migration rehearsal/apply records under `docs/session-reports/migration-*/`. No test in `tests/` feeds a separator to the fingerprint (grep for `x1f`/`x1e`/`char(31)` in tests: no hit).
- **Overlap:** none given. No existing inventory row found for the fingerprint's serialisation (plan grep for "F16" hits only the unrelated Phase-1a fork F16 "legacy-fixture slicing" — name clash, different subject).
- **Proposed class:** 3 — the stated guarantee ("the fingerprint that proves the copy", "IDENTICAL") is stronger than the encoding supports, but a miss needs a content change that moves a cell/row boundary across an embedded separator followed by a valid type prefix; the live database holds no separator, and neither the backup API nor an ordinary stray write produces that shape. Arguable as Class 1 on the definition's letter ("silent"): the check it weakens is the integrity check itself.
- **Owning state:** pre-tag (R512). Flag: a new scheme changes every `overall_sha256`, so the record of reference must be superseded in the same change (no database write, but a live read under the 07:00–10:35 UTC rule's spirit and a new committed record).
- **Minimum loud-failure fix:** in `fingerprint_within`, count TEXT cells containing U+001F/U+001E per table and either raise or carry `ambiguous_text_cells` in the record with `compare()` reporting any non-zero as a difference-class line — turns "collision possible" into a visible state while leaving every existing hash unchanged.
- **Full fix:** a versioned injective cell encoding (type tag + byte length + bytes; explicit row terminator), `scheme_version` in every record, `compare()` refusing to compare records of different schemes; keep v1 callable for the historical records; write and commit a new record of reference.
- **Acceptance test:** the reproducer's three pairs hash differently (and B's CLI compare exits 1); NULL/int/real/blob/Unicode/empty-string/multi-row cases covered; `auto_backup` of a WAL database still verifies equal; a v1 record compared to a v2 record refuses rather than reporting "differences".
- **Size:** S for the minimum (one function, mechanism read); M for the full fix (scheme version + record-of-reference supersession + historical-record policy).
- **PI-decision flag:** yes — "Version the fingerprint scheme now (supersedes the record of reference and makes all prior committed records non-comparable), or ship only the separator-count guard and keep v1 hashes?"
- **Differences from the original / the brief's note:** B's exact strings collide at HEAD as written — the prefix format has not changed. B does not mention (a) the type-impersonation variant (TEXT as INTEGER/NULL), (b) that the row-boundary variant is still caught by `compare()` via `row_count` but not by `overall_sha256` alone, (c) that the schema hash and the overall hash use the same unescaped joins. B's "backup verification can miss some changes" is true of the comparison but not reachable through `auto_backup`'s own mechanism (the online backup API copies pages; it cannot move text between cells). Observed while reading, not chased: `auto_backup` and `restore` have **no caller** in `engine/`, `scripts/` or `analysis/` (grep: only `tests/test_db_backup.py`) — they are session-invoked tools, so "relied on" means by operators, not by a pipeline path.


---

### F17 — Installation and test schedules need explicit reproducibility boundaries
- **Source:** outside B
- **Present at HEAD:** yes, per sub-claim:
  - (a) most requirements unbounded — **yes**
  - (b) validated Docling/Pydantic pair named in a comment but Pydantic and Docling's transitives unlocked — **yes**
  - (c) undeclared direct imports of requests / httpx / PyMuPDF — **yes** (and two more: `fontTools`, `pandas`)
  - (d) pytest not declared — **yes**
  - (e) pyproject.toml has no package / dependency / Python-version metadata — **yes**
  - (f) nightly script runs every test, no marker selection — **yes**
  - (g) importing the cloud schema eagerly imports both provider SDKs, coupling schema initialisation to them — **yes**, and narrower than stated: only a database that still lacks the 014 receipt reaches it
- **Evidence level:** code order for (a)–(f); reproduced for (g)
- **Evidence:**
  - (a) `requirements.txt`, whole file (15 entries, no other requirements/constraints/lock file in the repo root — `ls`: only `requirements.txt`, `pyproject.toml`). Pinned (5): `ollama==0.6.1`, `docling==2.74.0`, `openai==2.24.0`, `anthropic==0.84.0`, `pysbd==0.3.4`. Bare (10): `pydantic`, `biopython`, `pyalex`, `chromadb`, `sentence-transformers`, `gradio`, `pyyaml`, `python-docx`, `openpyxl`, `tiktoken`.
  - (b) same file, the comment on the docling line: "pinned PARSE-GATE-06a: 2.74.0 + pydantic 2.12.5 is the combination whose PdfHyperlink URL validation the sanitized retry works around; docling-core is transitive (2.65.1 installed) and not listed here." — while the first line of the file is the bare `pydantic`. Installed (importlib.metadata, no network): pydantic 2.12.5, docling-core 2.65.1.
  - (c) AST census of every `import`/`from` in `engine/`, `scripts/`, `analysis/` against the file. Imported, not declared: `requests` (`engine/acquisition/check_oa.py`, `engine/acquisition/download.py`, `scripts/screen_expanded.py`, `scripts/pdf_acquisition/step2_unpaywall_check.py`, `step3_download_oa_pdfs.py`, `step3b_retry_failed.py` — each a module-level `import requests`); `httpx` (`engine/utils/ollama_client.py` and `engine/agents/extractor.py` module-level `import httpx`; `engine/core/extraction_events.py` function-level; `analysis/eval/run_screen2f.py`, `run_qualgap01.py`); `fitz` (`engine/parsers/pdf_parser.py`, `engine/parsers/font_audit.py`, `engine/acquisition/pdf_quality_check.py`: `import fitz  # PyMuPDF`); `fontTools` (`engine/parsers/font_audit.py`: `from fontTools.ttLib import TTFont` and two more); `pandas` (`analysis/paper1/pass1_inspection.py`: `import pandas as pd`). All five are installed in `.venv` (requests 2.32.5, httpx 0.28.1, PyMuPDF 1.27.1, fonttools 4.61.1, pandas 2.3.3) — present only as someone's transitive. The reverse also holds: `chromadb`, `sentence-transformers`, `gradio` are declared and imported nowhere in `engine/`, `scripts/`, `analysis/` (grep: no hit).
  - (d) grep for `pytest` across `requirements*.txt`, `pyproject.toml`, `setup.*`, `*.cfg`: the only hit is the section header `[tool.pytest.ini_options]`. Installed: pytest 9.0.2.
  - (e) `pyproject.toml`, whole file (7 lines): `[tool.pytest.ini_options]` and the four `markers`; no `[project]`, no `[build-system]`, no `requires-python`. No `.python-version`. The interpreter is 3.12.3 by observation only.
  - (f) `scripts/nightly_tests.sh`: `python -m pytest tests/ -v --tb=short` inside `{ … } > "$LOG_FILE" 2>&1`, followed by an unconditional `exit 0`. No `-m`. `crontab -l`: `0 9 * * * /bin/bash /home/ankitsarin/projects/evidence-engine/scripts/nightly_tests.sh` (the repo path directly, not the `~/scripts` symlink). So the 09:00 UTC run includes the marked tiers: 8 `@pytest.mark.network` ids' decorators (`tests/test_pubmed.py`, `tests/test_openalex.py`), 3 `@pytest.mark.ollama` (`tests/test_screener.py::test_screen_relevant_paper` / `test_screen_irrelevant_paper` — `screen_paper(paper, spec, pass_number=1)` against the live server; `tests/analysis/paper1/test_judge_pass2.py::test_paper_366_grammar_prevents_four_element_emission` — "Requires a running Ollama with gemma3:27b"), 7 `@pytest.mark.integration` decorators. (Decorator counts are grep counts of this tree, not collected ids.) The script's exit status is discarded, so cron never sees a red run; the log file is the only signal.
  - (g) chain, quoted: `engine/cloud/__init__.py` — `from engine.cloud.anthropic_extractor import AnthropicExtractor` / `from engine.cloud.openai_extractor import OpenAIExtractor` / `from engine.cloud.schema import init_cloud_tables`; `engine/cloud/anthropic_extractor.py` — module-level `import anthropic`; `engine/cloud/openai_extractor.py` — module-level `import openai`. `engine/cloud/schema.py` itself imports only `sqlite3`, but importing it executes the package `__init__` first. Reached from schema initialisation by: `engine/core/database.py` `ReviewDatabase.__init__` → `self._run_migrations()` → `result = runner.run(self.db_path)` → `engine/migrations/runner.py` `run`: `module = importlib.import_module(f"engine.migrations.{migration_id}")` for each migration without a receipt → `engine/migrations/014_cloud_tables.py` `run_migration`: `from engine.cloud.schema import init_cloud_tables`. Also `engine/core/effective_config.py`, the `stage.startswith(CLOUD_STAGE_PREFIX)` branch: `from engine.cloud.base import outbound_messages` (cloud stages only).
    Reproducer `~/scratch/12f/repro/F17_eager_sdk_import.py` (cwd `~/scratch/12f/g5`, database under `./f17_data`, one interpreter per probe), output `.out`:
    `import engine.core.database` → openai False, anthropic False; `import engine.cloud.schema` → **True, True**; fresh `ReviewDatabase('f17', data_root=…)` → **True, True**; second construction on the same now-receipted database → False, False; fresh construction with both SDKs made unimportable (`sys.modules[...] = None`) → `construction FAILED: MigrationError 014_cloud_tables failed: ModuleNotFoundError: import of anthropic halted; None in sys.modules`.
    So: a NEW review cannot be created on a machine without both SDKs even with `cloud.enabled_arms` empty; an existing receipted database (live) opens without importing either.
- **Overlap:**
  - C18 — REJECTED as the same defect; it is a closed subset. C18 pinned `ollama`/`openai`/`anthropic` (R75) and is `closed`; F17(a) is the ten entries C18 left bare plus the undeclared ones. Nothing in C18 needs reopening.
  - I20 — REJECTED. I20 is the standard gate's wall time and one unpatched `ollama.ps` read inside the *deselected-tier-free* gate; F17(f) is the nightly's lack of tier selection. Same family (a test run that depends on a live server), different mechanism and different run.
  - I3 / NIGHTLY-LOCK-01 — CONFIRMED as the same cron line and script, narrower: I3 states the lock consequence of the ollama tier running at 09:00; F17(f) is the cause (no `-m`) and also covers the network and integration tiers and the swallowed exit status. See row A1-NIGHTLY.
  - No existing row found for (b)–(e) or (g).
- **Proposed class:** 2 for (g) — a fresh review's creation fails loudly without an unused provider SDK; nothing is corrupted. 3 for (a)–(f) — environment declaration and test scheduling hygiene. Row overall: 2.
- **Owning state:** pre-tag (R512) for (g) and for declaring what is imported; the nightly split (f) rides with I3. A lock file is a tag-time artifact (it should describe the tagged state) — flag, not a reason to defer the declarations.
- **Minimum loud-failure fix:** (g) make `engine/cloud/__init__.py` stop importing the two extractor modules (callers already import them by full path — `scripts/run_cloud_extraction.py`: `from engine.cloud.anthropic_extractor import AnthropicExtractor`), so 014 and the resolver reach `schema`/`base` without the SDKs. (f) add the standard gate's `-m "not network and not ollama and not integration"` to the nightly and stop discarding the status.
- **Full fix:** declare every directly imported distribution (requests, httpx, PyMuPDF, fonttools, pandas) and pin pydantic to the validated 2.12.5; add a dev requirements file with pytest; add `[project]` + `requires-python` to pyproject; commit a constraints/lock file generated from the tagged `.venv`; drop or justify chromadb / sentence-transformers / gradio. Schedule the network/ollama/integration tiers separately, under the experiment lock.
- **Acceptance test:** a test that constructs a fresh `ReviewDatabase` in a subprocess with `sys.modules['openai'] = sys.modules['anthropic'] = None` and succeeds (fails today — the reproducer's last probe); an AST-census test asserting every third-party top-level import under `engine/`, `scripts/`, `analysis/` maps to a declared distribution; the nightly log shows the deselection line and the cron wrapper propagates a red run.
- **Size:** S for (g); S for the declarations; M for a lock/constraints file and the nightly tier split (needs a decision on when the heavy tiers run).
- **PI-decision flag:** yes — "Should the nightly run only the standard gate, with the ollama/network/integration tiers moved to a separate scheduled, lock-holding job — or keep one all-tier nightly and make it take the experiment lock?"
- **Differences from the original / the brief's note:** B's list of undeclared imports omits `fontTools` and `pandas`; B does not note the three declared-but-unimported packages, nor that the nightly's `exit 0` hides its result. B's (g) reads as if every schema load imports the SDKs — at HEAD only migration 014's execution does, i.e. first construction of a database without the 014 receipt; `import engine.core.database` alone and re-opening a receipted database import neither. B cites `engine/cloud/__init__.py:1–5` correctly; the import that matters downstream is 014's `from engine.cloud.schema import init_cloud_tables`. Not established: whether a clean `pip install -r requirements.txt` today resolves to a working set (no pip/network allowed).


---

### A1 — An Ollama restart is gated only against a *different lock holder*; every non-holding workload can be restarted beneath
- **Source:** outside A
- **Present at HEAD:** partial, per sub-claim:
  - "a restart can tear down the daemon beneath sibling tasks" — **yes**, for any sibling that does not itself hold the experiment lock
  - "child workers bypass the lock check" — **no** for the one harness that spawns workers (exec'd children of a holder are refused); not reachable by fork (no engine/scripts path forks a lock holder)
  - "children rely on `EVIDENCE_ENGINE_NO_OLLAMA_RESTART`" — **partial**: the variable disarms only the *recovery* path of the process that sets it; it protects nobody from being restarted, and the proactive path does not read it
- **Evidence level:** code order (nothing run, per brief)
- **Evidence:**
  - `engine/utils/ollama_lock.py`, `foreign_lock_held`: `return check_experiment_lock() and not self_holds_lock()`. `check_experiment_lock` returns `False` when the non-blocking `fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)` succeeds, i.e. when **nobody** holds the lock. So the predicate is False — restart permitted — in two cases: (i) no process holds the lock at all; (ii) the caller is the holder (`_SELF_DEPTH > 0`). It is True only when a *different* process holds it. The lock is exclusive, so at most one workload on the box is ever protected, and only from other processes' restarts.
  - Path 1, `engine/agents/extractor.py` `restart_ollama`: `if foreign_lock_held(): logger.warning("… RESTART SKIPPED …"); return` then `subprocess.run(["sudo", "systemctl", "restart", "ollama"], timeout=30, check=True, capture_output=True)`. **No read of the opt-out variable** anywhere in this function. Callers: `_extract_selected` (`if restart_every > 0 and papers_since_restart >= restart_every: … restart_ollama(` — `RESTART_EVERY_N = 25`, under the run's own lock), `analysis/paper1/pass2_full.py` (`restart_ollama(reason="proactive", papers_done=processed)` — **the module takes no lock**: grep for `hold_experiment`/`experiment_lock` under `analysis/paper1/` returns nothing), `analysis/eval/elicit01/runner.py` and `analysis/eval/run_capture01.py` (both inside `with hold_experiment_lock():`).
  - Path 2, `engine/utils/ollama_client.py` `_restart_ollama_and_retry`: `if restart_disabled(): raise RuntimeError(…)` / `if foreign_lock_held(): raise RuntimeError(…)` / `subprocess.run(["sudo", "systemctl", "restart", "ollama"], …)`. Reached from `ollama_chat`'s `except FuturesTimeoutError:` … `else: # All retries exhausted — attempt Ollama restart as last resort` (`DEFAULT_MAX_RETRIES = 2`). `ollama_chat` is the one call path of **every** local stage, so every caller carries this branch.
  - Lock-holder census (grep for `hold_experiment_lock(` + read of each site). Production: **only** `engine/agents/extractor.py` `run_extraction` — `with hold_experiment_lock(): return _run_extraction_unlocked(…)` (blocking; `experiment_lock=False` opts out, no production caller passes it) — reached from `scripts/run_pipeline.py` (`stats = run_extraction(db, spec, review_name, selection=selection, run_id=run_id)`). Eval harnesses: `analysis/eval/run_local_ab.py`, `run_local_abc.py`, `run_qualgap01.py`, `run_capture01.py`, `smoke_regression01.py`, `elicit01/runner.py` (blocking), `run_screen2f.py` (`hold_experiment_lock(blocking=False)`).
  - `ollama_chat` callers that take **no** lock (grep for `ollama_chat(` minus the list above, each file read for a lock import): `engine/agents/screener.py`, `engine/agents/ft_screener.py`, `engine/agents/auditor.py`, `engine/parsers/pdf_parser.py` (vision tier), `engine/acquisition/pdf_quality_check.py`, `engine/utils/ollama_preflight.py`, `analysis/paper1/judge.py`, `analysis/paper1/pass2_retry_single.py` — i.e. `run_pipeline`'s screen / parse / FT-screen / audit stages (the lock is held for the extract stage only, inside `run_extraction`), `scripts/screen_expanded.py`, the `ft_screener` and `pdf_quality_check` CLIs, the judge passes, and the nightly's ollama tier (row A1-NIGHTLY).
  - Consequences, from the above: (1) a lock-holding extraction restarts the service every 25 papers and on watchdog exhaustion, beneath every concurrently running non-holder (a screening run, a judge pass, the 09:00 suite's model tests, any other project using the same Ollama); (2) when nobody holds the lock, any process whose `ollama_chat` exhausts its watchdog retries restarts the service beneath every other user, and `pass2_full` does so on a fixed cadence; (3) two non-holders can restart each other. The victim's in-flight call lands in `ollama_chat`'s `except (httpx.TimeoutException, httpx.ConnectError)` or generic `except Exception` branch: retried after `DEFAULT_RETRY_DELAY = 30` s up to `max_retries`, then raised — loud, not silent.
  - Opt-out: `ollama_client.restart_disabled()` — `os.environ.get(RESTART_OPT_OUT_ENV, "").strip().lower() not in ("", "0", "false", "no")` — read only in `_restart_ollama_and_retry`. It is a restarter-side disarm for the recovery path. It is not "the only protection for a non-holder": a non-holder has **no** protection as a victim; holding the lock is the only one, and it is single-occupancy.
  - A's worker scenario: `analysis/eval/run_screen2f.py` holds the lock in the parent and launches `[sys.executable, "-P", str(WORKER), …]` with `env = {…, "EVIDENCE_ENGINE_NO_OLLAMA_RESTART": "1"}`. An exec'd worker has `_SELF_DEPTH == 0` and the parent's flock is foreign to it, so `foreign_lock_held()` is True there — refused by the lock even without the variable. The only `multiprocessing` use in the three trees is `analysis/provenance/census.py` (no Ollama call, no lock).
  - Stated guarantee vs behaviour — CLAUDE.md "Ops Invariants — Ollama service safety": "Both are now gated" and "Both restart paths gate on `foreign_lock_held()` … restarting under *someone else's* experiment destroys it" — accurate as mechanism. "**Long runs wrap themselves in `hold_experiment_lock()`**" — true of extraction and the eval harnesses only; multi-hour screening (`screen_expanded.py`), FT screening, audit and the judge passes do not. "`EVIDENCE_ENGINE_NO_OLLAMA_RESTART` disarms the recovery branch" — accurate, and by its own words leaves the proactive branch armed. `ollama_lock.py`'s docstring ("the bug this module prevents") and `extractor.restart_ollama`'s comment ("never restart Ollama out from under someone else's experiment") hold only where "experiment" means "the lock holder".
- **Overlap:** I3 / NIGHTLY-LOCK-01 — same family, narrower (one non-holder: the nightly). No other inventory row found for the non-holder exposure (grep of `| I` rows for restart/lock: I3 and I20 only).
- **Proposed class:** 2 — a restart beneath a non-holder surfaces as connection errors, retries and at worst loud call/paper failures; no stored result is made wrong.
- **Owning state:** pre-tag (R512). Flag: Run 7 (R237) is a 20–35 h lock holder that will restart every 25 papers; any concurrent non-holder on the box (including the 09:00 suite) is restarted beneath ~hourly for its duration — an operating constraint to state even if the code is left as is.
- **Minimum loud-failure fix:** make `pass2_full` take the lock (the one production-adjacent path that restarts on a cadence without holding it), and have `restart_ollama` honour `restart_disabled()` so the variable means one thing on both paths.
- **Full fix:** decide the policy the lock expresses — either every model-calling entry point holds it (serialising all Ollama work on the box), or restarts additionally check live load (`/api/ps`, another client's in-flight request) and refuse/defer when a non-holder is active — and correct CLAUDE.md's "Long runs wrap themselves" to the list that is true.
- **Acceptance test:** with no lock held and a second process mid-call, neither restart path issues `systemctl` (per whichever policy is chosen); `restart_ollama` with the variable set does not reach `subprocess.run`; `pass2_full`'s loop runs inside `hold_experiment_lock()`.
- **Size:** S for the minimum (two small edits, mechanism read); M for the policy change (every entry point, a Phase A census of who may run concurrently).
- **PI-decision flag:** yes — "Should the experiment lock be mandatory for every model-calling entry point (one Ollama workload at a time), or stay extraction/eval-only with non-holders accepted as restartable?"
- **Differences from the original / the brief's note:** A locates the hazard in child workers bypassing the lock or lacking the env var; at HEAD exec'd children of a holder are refused by the lock itself, and the real opening is the opposite one — the gate passes whenever *nobody* holds the lock, and the holder's own restarts are unconditional. A says the lock "blocks foreign crons": the 07:00 health script does check it (`~/scripts/ollama_health_check.sh`: `LOCKFILE=…`, `flock -n 9`), the 09:00 nightly does not (A1-NIGHTLY). A does not notice that the proactive path ignores the opt-out variable, or that `pass2_full` restarts without holding the lock. Not established (nothing run): how often a real non-holder actually loses a call to a restart; that depends on retry timing against Ollama's restart time.


---

### A1-NIGHTLY — The 09:00 UTC nightly suite loads models without taking or checking the experiment lock
- **Source:** architect-derived (existing row I3 / NIGHTLY-LOCK-01)
- **Present at HEAD:** yes
- **Evidence level:** code order (cron line and script read; today's nightly log read as corroboration — not a run of mine)
- **Evidence:**
  - `crontab -l`: `0 9 * * * /bin/bash /home/ankitsarin/projects/evidence-engine/scripts/nightly_tests.sh` — the repo path directly; cron does **not** go through the `~/scripts` symlink for this job, and the line carries no `flock` wrapper and no output redirect.
  - `scripts/nightly_tests.sh`, the whole body of the group: `cd "$PROJECT_DIR"` / `source .venv/bin/activate` / `python -m pytest tests/ -v --tb=short`, then `exit 0`. No `flock`, no reference to `~/.ollama_experiment.lock` or `OLLAMA_EXPERIMENT_LOCK`, no `-m` expression. `grep -n "ollama_lock|experiment_lock|hold_experiment|foreign_lock" scripts/nightly_tests.sh tests/conftest.py` → one hit, a docstring mention of the flock *tests*. Contrast `~/scripts/ollama_health_check.sh` (07:00): `LOCKFILE="${OLLAMA_EXPERIMENT_LOCK:-$HOME/.ollama_experiment.lock}"` … `elif flock -n 9; then` — the health script checks, the nightly does not.
  - The unfiltered run includes the `ollama` tier, which calls the live server: `tests/test_screener.py::test_screen_relevant_paper` / `test_screen_irrelevant_paper` — `result = screen_paper(paper, spec, pass_number=1)` → `engine/agents/screener.py`: `response = ollama_chat(messages=build_messages(paper, spec, role=role), **cfg.kwargs())` (the spec's primary screener, sent with `keep_alive` default `-1` — `engine/core/review_spec.py` `OllamaRuntime.keep_alive`, "default=-1", so the model stays resident after the test); `tests/analysis/paper1/test_judge_pass2.py::test_paper_366_grammar_prevents_four_element_emission` — "Requires a running Ollama with gemma3:27b". `logs/nightly_test_20261008.log` shows all three `PASSED` and the run ending `3210 passed, 1 xfailed … in 1023.55s` — they execute, they are not skipped.
  - `tests/conftest.py` states it knowingly, module docstring: "**No tier is exempt, including the nightly full-suite run.** … the `ollama`-marked tier loads models over HTTP … `scripts/nightly_tests.sh` (which runs `pytest tests/` with no marker filter) is covered by it too."
  - Does the fence prevent a restart **from** the nightly? Yes, by two independent gates. (1) `tests/conftest.py` `block_service_calls` — `@pytest.fixture(autouse=True, scope="function")`, wraps `subprocess.run/Popen/call/check_call/check_output` and `os.system`; `BLOCKED_COMMANDS = frozenset({"systemctl", "service", "sudo", "doas", "pkexec", …})` matched on `os.path.basename(word)`; `class ServiceCallBlocked(BaseException)`. Both restart paths issue `["sudo", "systemctl", "restart", "ollama"]` through `subprocess.run`, so both are refused in every tier. (2) Under a running experiment, `_restart_ollama_and_retry`'s `if foreign_lock_held(): raise RuntimeError(…)` refuses first. The fence is in-process only — it covers `subprocess` in the pytest process, not a child interpreter a test spawns — which is not a path either restart function uses.
  - What the fence does **not** prevent, and what I3 is about: model loads. A lock-holding run (Run 7: 20–35 h, so it necessarily spans a 09:00) shares VRAM with a nightly that loads the screener model and `gemma3:27b` and leaves them resident; nothing makes the nightly wait or skip. The reverse also holds (row A1): the holder's restart every 25 papers lands beneath the nightly's model tests, and `exit 0` hides the resulting red run from cron.
- **Overlap:** I3 / NIGHTLY-LOCK-01 — **CONFIRMED, same defect.** I3's text is "The nightly cron loads models without the experiment lock … ARMED"; that is exactly this, unchanged at HEAD. F17(f) (no marker selection) is its cause; A1 is the general non-holder case. A's own text does not contain this item — A asserts the lock "blocks foreign crons", which is true of the 07:00 job only.
- **Proposed class:** 2 — contention and possible loud failures of a long run's calls (VRAM pressure, eviction/reload time, watchdog timeouts); no path here writes a wrong result. Not Class 1: the nightly opens no live database (the `conftest.py` live-data fence, JUDGE-DBGUARD-01) and cannot restart the service.
- **Owning state:** pre-tag (R512) — Run 7 is the first run long enough to be certain to overlap 09:00 UTC, and R237 makes it the first claim-bearing run on live.
- **Minimum loud-failure fix:** in `scripts/nightly_tests.sh`, probe the lock with `flock -n` on `${OLLAMA_EXPERIMENT_LOCK:-$HOME/.ollama_experiment.lock}` (the health script's idiom) and, when held, run the standard gate's `-m "not network and not ollama and not integration"` and log that the model tier was skipped and why.
- **Full fix:** split the schedule — the offline gate nightly, unconditional; the ollama/integration tier as a separate job that takes the lock non-blocking and records "skipped: lock held" as a distinct outcome — and propagate pytest's status instead of `exit 0`.
- **Acceptance test:** with a process holding the lock, the nightly log shows the model tier deselected and names the lock; `journalctl -u ollama` over the window shows no model load attributable to the suite; without a holder the model tier runs.
- **Size:** S — one shell script, idiom already on the box.
- **PI-decision flag:** no (the scheduling question is posed once, in F17).
- **Differences from the original / the brief's note:** the brief suggested cron might point through the `~/scripts` symlink — it does not; the line names the repo's script. The script's own header comment gives the path as `~/projects/evidence-engine/scripts/nightly_tests.sh`, consistent with the crontab. The nightly's wall time today was 1023 s (09:00–09:17 UTC by log mtime), which sits inside the 07:00–10:35 no-live-write window CLAUDE.md already declares (R86) but is not otherwise coordinated with a lock holder.


---

### A2 — Foreign-key enforcement is per-connection; census of writer connections

- **Source:** outside A (generalised by the brief into a census)
- **Present at HEAD:** partial
  - (a) "SQLite enforces FKs only where the pragma is set on that connection" — yes, reproduced (`PRAGMA foreign_keys` reads 0 on a plain `sqlite3.connect`, sqlite 3.45.1).
  - (b) "orphaned foreign keys … can persist silently" on a production run path — no. Every connection the event store / manifest / audit writers are handed at HEAD has the pragma ON (table below).
  - (c) Writer connections without the pragma exist — yes, nine non-migration sites plus every migration and the runner. None of them writes an FK-bearing table today, except `init_cloud_tables` (DDL/rebuild of `cloud_evidence_spans`, whose FK parent it copies alongside).
  - (d) A's cited file (`015_drop_prerename_adjudication_indices.py`) is irrelevant to FKs: it drops indices only.
- **Evidence level:** reproduced (inertness without the pragma; triggers independent of it) + code order (the census)
- **Evidence:**
  Reproducer `~/scratch/12f/repro/A2_fk_inert_without_pragma.py` (synthetic DB via `ReviewDatabase("synthetic_a2", data_root=<scratch tmp>)`, all migrations), run as `cd ~/scratch/12f/g6/a2 && PYTHONPATH=<repo> .venv/bin/python …`; output `A2_fk_inert_without_pragma.out`:
  ```
  ReviewDatabase._conn  PRAGMA foreign_keys = 1
  plain sqlite3.connect PRAGMA foreign_keys = 0
  FK-on  (ReviewDatabase): INSERT -> REFUSED: IntegrityError: FOREIGN KEY constraint failed
  FK-off (plain connect): INSERT paper_events(paper_id=999999, no such paper) -> ACCEPTED
  orphans now visible to foreign_key_check(paper_events): [('paper_events', 1, 'papers', 0)]
  FK-off (plain connect): UPDATE -> REFUSED by trigger: paper_events is append-only: correct by appending an event
  FK-off (plain connect): DELETE -> REFUSED by trigger: …
  ```

  **Census — every `sqlite3.connect` that is written through** (complete over `engine/`, `scripts/`, `analysis/`; there is no other opener: no `from sqlite3 import`, no `Connection(`, no ORM. Read-only `mode=ro` / `immutable=1` opens are in A3 and omitted here).

  | Site (file :: function) | Writes | `foreign_keys` |
  |---|---|---|
  | `engine/core/database.py :: ReviewDatabase.__init__` — `self._conn.execute("PRAGMA foreign_keys=ON")` | everything the engine writes through `db._conn` (see below) | **ON** |
  | `engine/core/database.py :: ReviewDatabase._run_migrations` (reconnect in `finally:` after `runner.run`) — same three pragmas re-issued | same | **ON** |
  | `engine/cloud/base.py :: CloudExtractorBase.__init__` — `self._conn.execute("PRAGMA foreign_keys=ON")` | `cloud_extractions`, `cloud_evidence_spans`, events via the shared writers | **ON** |
  | `scripts/advance_to_pdf_acquired.py :: main` | `UPDATE papers SET status='PDF_ACQUIRED'` + INSERT | **ON** |
  | `scripts/backfill_cloud_spans.py :: main`, `scripts/reparse_cloud_spans.py :: main` | `INSERT INTO cloud_evidence_spans` | **ON** |
  | `analysis/provenance/census.py :: persist` | provenance classification rows | **ON** |
  | `engine/acquisition/pdf_quality_check.py :: run_quality_check` | `UPDATE papers SET pdf_ai_* , pdf_quality_check_status='AI_CHECKED'` | **not set (OFF)** |
  | `engine/acquisition/pdf_quality_import.py :: import_dispositions` | `UPDATE papers SET pdf_quality_check_status…`; `UPDATE papers SET status='PDF_EXCLUDED', pdf_exclusion_*` | **not set (OFF)** |
  | `engine/cloud/schema.py :: init_cloud_tables` (called from `CloudExtractorBase.__init__` before its own FK-ON connection) | `executescript(_CLOUD_SCHEMA)`, `ALTER TABLE`, a `cloud_evidence_spans` rebuild/copy | **not set (OFF)** |
  | `engine/migrations/runner.py :: run` (both connects) — `conn.execute("PRAGMA foreign_keys = OFF")  # table rebuilds re-point FKs` | `schema_migrations` receipts | **explicitly OFF** |
  | `engine/migrations/runner.py :: register_preapplied` | `schema_migrations` receipts | not set (OFF) |
  | migrations `002`, `006`, `011`, `013` | rebuilds | OFF, then ON at the end |
  | migrations `018`, `019`, `020`, `021`, `022` | rebuilds / event-store tables | explicitly OFF for the whole connection (020/022/018/011 run `PRAGMA foreign_key_check` afterwards and refuse on a violation) |
  | migrations `003`, `004`, `005`, `007`–`010`, `012`, `014`, `015`, `016`, `017` | DDL; 003 and 017 write data (017 seeds `field_events`/`paper_events` through `engine.core.events`) | not set (OFF) |
  | `engine/utils/db_backup.py :: auto_backup` (`dst`), `:: restore` (`dst`, a temp file) | the backup API copy, a journal-mode switch | not set (OFF) — copies, not row writers |
  | `engine/utils/db_backup.py :: _refuse_if_open` (`probe`) | lock probe only | not set |
  | `scripts/backfill_authors.py :: main` | `UPDATE papers SET authors` | not set (OFF) |
  | `analysis/paper1/adjudication.py :: import_adjudication_decisions` | `CREATE TABLE`/`INSERT INTO concordance_adjudications` (no `REFERENCES`) | not set (OFF) |
  | `analysis/paper1/consensus.py :: store_consensus` | `consensus_values` (no `REFERENCES`) | not set (OFF) |
  | `analysis/paper1/human_import.py :: store_human_extractions` | `human_extractions` (no `REFERENCES`) | not set (OFF) |

  Writers that take a caller's connection and open none: `engine/core/events.py` (`field_events`, `paper_events`, `claim_inputs`, `arms`), `engine/core/run_manifest.py` (`run_manifests`, `run_stage_configs`, `arms`, `run_calls`), `engine/core/audit_telemetry.py` (`audit_verdicts`), `engine/core/parsed_text.py` (`parsed_text_refs`), `analysis/paper1/judge_storage.py` (`db._conn`). A connection can only originate at one of the sites tabled above, and the only read-write ones that reach these writers are `ReviewDatabase._conn` and `CloudExtractorBase._conn` (the `._conn` users — 41 files by grep, not each read line by line), so the two tables 023 touches — `audit_verdicts` and `claim_inputs` — are written only on FK-ON connections at HEAD. None of the pragma-less sites above calls those writers. **Telemetry writers** (`engine/core/run_telemetry.py`, extraction/audit JSONL) write files, not SQLite.

  **What the event store relies on.** Append-only is **triggers** (`016_event_store.py`: `CREATE TRIGGER IF NOT EXISTS {table}_no_update BEFORE UPDATE … RAISE(ABORT, '{table} is append-only…')`, re-declared in 019/020/021/022), vocabulary and run-link are **CHECKs**, arm freeze is a trigger — all independent of the pragma (reproduced above). **Referential** guarantees are FKs only and are inert without it: `paper_id REFERENCES papers(id)`, `arm REFERENCES arms(arm_name)`, `run_id REFERENCES run_manifests(run_id)`, `FOREIGN KEY (run_id, stage) REFERENCES run_stage_configs(run_id, stage)` on `run_calls`. Two places lean on the FK *in code*: `engine/core/events.py :: _refuse_claim_on_arm`
  ```python
  if row is None:
      return  # the FK refuses an unknown arm, naming it
  ```
  and CLAUDE.md's "`run_calls` … its `(run_id, stage)` must name a declared stage" (the composite FK; C55's refusal "today by the `run_calls` foreign key"). `_run_link` does check `run_manifests` in code (`SELECT 1 FROM run_manifests WHERE run_id = ?`).
- **Overlap:** C39 (`audit_verdicts.arm` no FK) and D25 (`claim_inputs.parsed_text_uid` no FK) — RELATED, not the same: they add the FKs; this row is the precondition that makes them effective. A11 (phantom-table FK under `foreign_keys=ON`) — different defect, same mechanism. C55 — its refusal is delivered by an FK, so it depends on this row holding.
- **Proposed class:** 1 (latent) — an FK added by 023 is silently inert on any writer connection lacking the pragma, and one engine code path (`_refuse_claim_on_arm`) delegates a refusal to the FK. No production writer of an FK-bearing event-store table lacks it today, so nothing is wrong at HEAD.
- **Owning state:** pre-tag (R512); naturally lands with migration 023, which is what makes it matter.
- **Minimum loud-failure fix:** in the shared writers (`events.write_field_event` / `write_paper_event`, `run_manifest.open_run`, `audit_telemetry` insert) refuse when `conn.execute("PRAGMA foreign_keys").fetchone()[0] != 1` (migration callers exempt by the existing `_called_from_migration` test).
- **Full fix:** one connection factory (`engine/core/…connect_rw(path)`) that sets WAL, busy_timeout and `foreign_keys=ON`, used by `ReviewDatabase`, `CloudExtractorBase`, `pdf_quality_check`, `pdf_quality_import`, `init_cloud_tables` and the paper1 importers; a static test that no file outside `engine/migrations/` and `db_backup.py` calls `sqlite3.connect` on a non-`mode=ro` path directly. Replace the `return  # the FK refuses` branch with an explicit `UnknownArm` refusal.
- **Acceptance test:** on a synthetic DB, a `write_paper_event` / `write_field_event` on a pragma-less connection raises before any INSERT; the static census test fails when a new bare `sqlite3.connect` writer is added; after 023, an `audit_verdicts` insert naming an unregistered arm is refused on every writer connection.
- **Size:** S for the guard in the shared writers; M for the factory + census test (touches nine sites).
- **PI-decision flag:** no
- **Differences from the original / the brief's note:** A cites migration 015 (indices only) and speaks of a "connection pool" — there is none; each site opens its own connection. A's risk scenario ("paper removals or re-extractions" leaving orphans) has no code path: nothing deletes `papers` rows and the event tables refuse DELETE by trigger. The real exposure is narrower than A states (no production FK-table writer lacks the pragma) and more specific (023's new FKs, and the code branch that defers to the FK). Not chased: `pdf_quality_import` and `pdf_parser.parse_all` set `papers.status='PDF_EXCLUDED'` by direct UPDATE, bypassing `update_status` — recorded under A6.


---

### A3 — `immutable=1` opens of the live review database (six, all in `analysis/eval/`)

- **Source:** outside A
- **Present at HEAD:** yes — six `immutable=1` opens, every one aimed at `data/<review>/review.db` (the live database), none at a frozen snapshot or eval store. None in `engine/`, none in `scripts/`. (A's three named files: `elicit01/manifest.py` yes, `parse01/sweep.py` yes, `analysis/paper1/adjudication.py` **no** — it opens plain read-write, no URI.)
- **Evidence level:** reproduced (the mechanism: `immutable=1` does not see a committed, un-checkpointed WAL write) + code order (the six sites and their targets)
- **Evidence:**
  Reproducer `~/scratch/12f/repro/A3_immutable_misses_wal.py` (synthetic WAL db in `~/scratch/12f/g6/a3/`), output `.out`:
  ```
  mode=ro      -> [(1, 'FT_ELIGIBLE'), (2, 'PARSED')]
  immutable=1  -> [(1, 'INGESTED')]
  ```

  **Census** (`grep immutable` over `engine/`, `scripts/`, `analysis/`; every other hit is prose saying "never `immutable=1`" or the word "immutable" about manifests/arms):

  | # | Site (file :: function) | Quoted open | Database it can be pointed at | Tables read | Mark |
  |---|---|---|---|---|---|
  | 1 | `analysis/eval/analyze_qualgap01.py :: run6_spans` | `sqlite3.connect(f"file:{db_path}?immutable=1", uri=True)` | `main`: `review_dir = Path(args.data_root) / args.review` → `db = review_dir / "review.db"` (CLAUDE.md "Running": `--review surgical_autonomy`) | `evidence_spans` ⋈ `extractions` | **at risk (live-writable target)** |
  | 2 | `analysis/eval/analyze_qualgap01.py :: run6_traces` | same | same | `extractions.reasoning_trace` | **at risk** |
  | 3 | `analysis/eval/prime01.py :: load_run6_traces` | same | called by `analyze_prime01.py` with `review_dir / "review.db"` (CLAUDE.md "Running": `analyze_prime01 --review surgical_autonomy`) | `extractions.reasoning_trace` | **at risk** |
  | 4 | `analysis/eval/prime01.py :: load_run6_spans` | same | same | `evidence_spans` ⋈ `extractions` | **at risk** |
  | 5 | `analysis/eval/parse01/sweep.py :: main` | `sqlite3.connect(f"file:{db}?immutable=1", uri=True)` | `db = review_dir / "review.db"`, `--review` required | `extractions`, `cloud_extractions` (`input_tokens` per arm) | **at risk** |
  | 6 | `analysis/eval/elicit01/manifest.py :: build` | `sqlite3.connect(f"file:{review_dir/'review.db'}?immutable=1", uri=True)` | caller-supplied `review_dir`; the module's own entry hard-codes `review_dir = Path("data/surgical_autonomy")` | `papers.status`, `extractions` | **at risk** |
  | — | `analysis/eval/run_capture01.py` | docstring only: "The DB is opened read-only (`immutable=1`) only if a caller asks for study types" | no `sqlite3.connect` in the file | — | no open exists; the sentence describes nothing |

  **Safe (frozen target): none.** No site takes a snapshot path; each derives `<data-root>/<review>/review.db`.

  How exposed each read is at HEAD: sites 1–4 read `extractions` / `evidence_spans`, which have **no writer at HEAD** (see A6) — their rows are static, so staleness needs an un-checkpointed WAL frame on one of their pages (unlikely) and the residual is a page read during a concurrent checkpoint. Site 5 also reads `cloud_extractions`, which `CloudExtractorBase.store_result` still INSERTs. Site 6 reads `papers.status`, which screening, parsing and adjudication write on every run — the stale-read case reproduced above applies directly.

  **Against the stated rule.** CLAUDE.md, "Ops Invariants — the database": "**Read-only means `mode=ro`, never `immutable=1`.** `immutable` promises SQLite the file cannot change while open; `review.db` is live, so the promise would be a lie and the reader could see a torn page." The six sites break it. The rule is enforced by test for two files only — `tests/test_readers_on_the_reader.py` (I5: "`mode=ro`, never `immutable=1` — `review.db` is live"), scoped to `engine/analysis/concordance.py` and `analysis/paper1/export_disagreement_pairs.py` — and by `tests/test_db_fingerprint.py::test_fingerprint_is_read_only_and_never_immutable` for the fingerprint tool. Nothing guards `analysis/eval/`.
- **Overlap:** I5 (analysis readers open the live database read-write; closed session 6) — RELATED, narrower: I5 covered three plain-`connect` opens in two files and its guard test is scoped to them. I13 — different route (private `_conn`). C46 (a `mode=ro` read moves `-shm` mtime) — the cost of the correct mode, not this defect. No existing row names these six sites (grep of the plan's inventory for `immutable=1`: no row).
- **Proposed class:** 3 for sites 1–5 (analysis-only readers of static legacy tables; no engine result, provenance or gate is derived from them) — but site 6 is closest to Class 1 in kind (it reads `papers.status` to build an eval manifest) and stays Class 3 only because `elicit01` is a closed study writing to an eval store, not to `review.db`. One-line reason: a stated repo invariant is violated in six places and no production path reads through them.
- **Owning state:** pre-tag (R512) if the rule is to be true of the tree at the tag; otherwise these are R31 candidates (legacy eval scripts serving a closed study) and could retire instead of being fixed.
- **Minimum loud-failure fix:** replace `immutable=1` with `mode=ro` at the six sites (one-token change each); delete the `run_capture01.py` docstring sentence.
- **Full fix:** widen the I5 guard test from two named files to a tree walk — no line under `engine/`, `scripts/`, `analysis/` may contain both `immutable=1` and `connect`. Decide under R31 whether `analysis/eval/{prime01,analyze_qualgap01,parse01,elicit01}` survive at all.
- **Acceptance test:** the widened guard fails on the unmodified tree (six hits) and passes after; reproducer semantics unchanged.
- **Size:** S — six one-token edits and one test widening, mechanism read.
- **PI-decision flag:** yes — fix the six eval readers to `mode=ro`, or retire those eval scripts under R31?
- **Differences from the original / the brief's note:** A says the pattern "protects against accidental lock escalation" and lists `analysis/paper1/adjudication.py` among URI readers — that file uses neither `mode=ro` nor `immutable=1` (`conn = sqlite3.connect(db_str)` / `sqlite3.connect(str(db_path))`, and `import_adjudication_decisions` writes). A's "uncommitted pages" is imprecise: the reproduced failure is *missing committed* writes; a torn read needs a concurrent checkpoint and was not reproduced. A's remediation ("restrict `immutable=1` to frozen snapshots") matches the repo's own rule, which is already stricter (never).


---

### A4 — The shared Ollama client is rebindable; the manifest digest is read from a different host object than the calls

Two rows in one file.

---

#### A4-a — `run_qualgap01` rebinds `engine.utils.ollama_client._client` and never restores it

- **Source:** outside A
- **Present at HEAD:** yes (the rebind; nothing restores it) / no (A's stated consequence — "requests intended for the production server … route to ephemeral test instances" in a production path)
- **Evidence level:** code order
- **Evidence:** `analysis/eval/run_qualgap01.py :: bind_runtime`
  ```python
  oc._client = ollama.Client(host=host, timeout=oc._httpx_timeout)
  os.environ[oc.RESTART_OPT_OUT_ENV] = "1"
  ```
  called once from `main` (`version = bind_runtime(args.host)`, `--host` default `qualgap01.DEFAULT_HOST = "http://127.0.0.1:11435"`). There is no `finally`, no context manager and no saved original in the module — the rebind and the env var both last for the life of the process. The only restore in the repository is a test fixture (`tests/test_qualgap01.py :: _restore_client_module`: "`bind_runtime` rebinds module globals; never let that leak into the suite").
  It is deliberate and documented in the module docstring ("**The client is rebound to the 0.17.7 port explicitly**, not left to `OLLAMA_HOST`"), and `ollama_client.n_ctx_train` is written to tolerate it ("a caller that rebinds the client to another server (run_qualgap01) reads that server's value"; its cache key is `(_client_host(), model)`).
  Scope: `run_qualgap01` is a one-shot CLI. Nothing in `engine/`, `scripts/` or elsewhere in `analysis/` imports it or calls `bind_runtime` (grep: only `tests/test_qualgap01.py` and a path list in `tests/test_review_paths.py`). It opens no run manifest and writes no event (it writes a JSONL eval store). So within its own process the rebind is the intended behaviour and leaks to nothing.
- **Overlap:** none found in the inventory (grep of rows for `_client`, `OLLAMA_HOST`, `11435`: only I20, unrelated).
- **Proposed class:** 3 — an eval-only script mutating a module global by design; no production process shares its interpreter.
- **Owning state:** pre-tag (R512) only as part of A4-b's fix; alone it needs nothing before the tag.
- **Minimum loud-failure fix:** none needed for integrity. If wanted: `bind_runtime` as a context manager that restores `_client` and the env var.
- **Full fix:** a supported `ollama_client.use_host(host)` (or a client passed explicitly) instead of assigning a private global from outside the module; see A4-b.
- **Acceptance test:** after the eval's `main` returns in-process, `oc._client_host()` equals its value before.
- **Size:** S
- **PI-decision flag:** no
- **Differences from the original:** A's "any concurrent imports, background tasks, or scripts executed in the same Python process will silently inherit this rebound socket" is true of the mechanism but there is no such process: no caller imports the module, and it is run as `python -m`. A's remediation (per-worker client instances) is not required to remove any present defect.

---

#### A4-b — Digest host vs chat host: two independent sources, nothing compares them

- **Source:** architect-derived
- **Present at HEAD:** yes (the structural split) — conditional as a wrong manifest (needs `OLLAMA_HOST` set in a run's environment, or an in-process rebind before `open_run`; neither occurs on any engine or `scripts/` path today)
- **Evidence level:** reproduced (the two hosts diverge; no network call made) for the mechanism; conditional for a wrong manifest on a real run
- **Evidence:**
  *Chat path.* `engine/utils/ollama_client.py`, module level: `_client = ollama.Client(timeout=_httpx_timeout)` — no `host`, so the installed library (`ollama/_client.py`, `BaseClient.__init__`) resolves `base_url=_parse_host(host or os.getenv('OLLAMA_HOST'))`, default `http://127.0.0.1:11434`. `ollama_chat` sends through `_client.chat` (both call sites), and `n_ctx_train` through `_client.show`.
  *Digest path.* `engine/core/effective_config.py :: resolve_run`
  ```python
  if digest_fn is None:
      from engine.utils.ollama_client import fetch_model_digest as digest_fn
  ...
  digests[model] = digest_fn(model)
  ```
  and `engine/utils/ollama_client.py`
  ```python
  OLLAMA_BASE_URL = "http://localhost:11434"
  def fetch_model_digest(model_name, *, base_url: str = OLLAMA_BASE_URL, timeout=5.0) -> str:
      url = f"{base_url.rstrip('/')}/api/tags"
      resp = httpx.get(url, timeout=timeout)
  ```
  A bare `httpx.get` to a hard-coded literal: it does not go through `_client`, does not read `OLLAMA_HOST`, and `resolve_run` calls it with the model name only, so `base_url` is always the default. No engine or `scripts/` caller passes `base_url` (grep) and every production `open_run` caller leaves `digest_fn=None` (the parameter is documented as "a test seam; production defaults").
  *Nothing reconciles them.* `_client_host()` is used only as the ceiling-cache key and in `server_context_length` (`if _client_host() not in _LOCAL_SERVICE_HOSTS: return None`). `run_manifest.open_run` records `"host": host or socket.gethostname()` — the machine name, not the Ollama endpoint — so a manifest cannot say which server answered.
  Reproducer `~/scratch/12f/repro/A4_digest_host_vs_chat_host.py` (constructs clients, requests nothing), output `.out`:
  ```
  --- env OLLAMA_HOST=<unset>
  chat  (_client.chat) host        : http://127.0.0.1:11434
  digest (fetch_model_digest) host : http://localhost:11434
  after an in-process rebind       : chat http://127.0.0.1:11435 | digest http://localhost:11434
  --- env OLLAMA_HOST=http://127.0.0.1:11435
  chat  (_client.chat) host        : http://127.0.0.1:11435
  digest (fetch_model_digest) host : http://localhost:11434
  ```
  **Answer to the key question: no, not the same host object; yes, a rebound `_client` (or `OLLAMA_HOST` in the run's environment) makes `run_stage_configs.model_digest` and the arm pin record the :11434 server's digests while every call goes to the other server**, with no error and no field in the manifest that shows it. The pin would then be compared on later runs against the wrong server's digest too.

  **Census — engine/scripts paths that rebind `_client` or build their own Ollama client/host:**

  | Site | What | Host source |
  |---|---|---|
  | `engine/utils/ollama_client.py` module level `_client` | the one chat/show client | `OLLAMA_HOST` env, else `127.0.0.1:11434` |
  | `engine/utils/ollama_client.py :: fetch_model_digest` | `httpx.get` `/api/tags` | literal `OLLAMA_BASE_URL = "http://localhost:11434"` |
  | `engine/utils/ollama_preflight.py :: _get_model_vram_gb` and the second `ollama.ps()` site | the `ollama` package's module-level default client | `OLLAMA_HOST` env, else `127.0.0.1:11434` — a third client object |
  | `engine/agents/extractor.py :: restart_ollama` | readiness poll `httpx.get("http://127.0.0.1:11434/api/tags", timeout=5)` | literal |
  | `engine/utils/ollama_client.py :: server_context_length` | reads the local unit/drop-in files | gated on `_LOCAL_SERVICE_HOSTS` |
  | rebinding `_client` | **none in `engine/` or `scripts/`**; only `analysis/eval/run_qualgap01.py` (A4-a). `analysis/eval/run_capture01.py` and `analysis/eval/elicit01/runner.py` read through `oc._client._client.get("/api/version")` / `oc._client.list()` without rebinding | — |

  Condition not met today: `OLLAMA_HOST` is unset in this shell, in `~/.bashrc`/`~/.profile`/`/etc/environment`, and in the crontab (checked); the systemd unit environment of an engine service was not checked (none is deployed for the pipeline).
- **Overlap:** C15 (digest never filled; closed by adopting `fetch_model_digest`) — RELATED, the same function, different defect. C21 (judge digest identity kinds) — different. No row covers the host split.
- **Proposed class:** 1 (latent) — provenance: a manifest and an arm pin can name a model digest that is not the model that produced the claims, silently. Conditional on an environment that no current path sets.
- **Owning state:** pre-tag (R512) — the first claim-bearing run on live pins the arm (R237), and a pin cannot be re-made (R21).
- **Minimum loud-failure fix:** in `open_run` (or `resolve_run`), refuse when the chat client's host and the digest host differ after normalising `localhost`/`127.0.0.1` — i.e. derive `fetch_model_digest`'s `base_url` from `_client_host()` and raise if a caller overrides it to something else.
- **Full fix:** one host: `fetch_model_digest` reads `/api/tags` through `_client` (or from `_client_host()`); preflight's `ollama.ps()` uses `_client.ps()`; the restart poll uses the same host and is refused for a non-local host; the manifest records the Ollama endpoint and `/api/version`.
- **Acceptance test:** with `_client` bound to a fake at another host, `open_run` either reads the digest from that same fake or raises — never returns a manifest whose digest came from a third object (extend `tests/test_request_capture.py`'s fake to answer `/api/tags`).
- **Size:** S for the refusal; M for the single-host change (three client objects, the capture instrument and the manifest column).
- **PI-decision flag:** no
- **Differences from the brief's note:** the brief frames the hazard as a rebound `_client`; the wider and more likely trigger is plain `OLLAMA_HOST` in the environment, which moves the chat client and preflight's `ollama.ps()` but not the digest read or the restart poll. Could not establish: whether `localhost` and `127.0.0.1` can resolve to different listeners on this box (IPv6 `::1`) — not tested, no network call permitted.


---

### A5 — Imports of `analysis.*` from inside `engine/` (and `scripts/`)

- **Source:** architect-derived
- **Present at HEAD:** yes, three engine imports, all of one function — `analysis.provenance.segment.sentences`. **No** engine import of `analysis.eval.*` or `analysis.paper1.*`. The elicited path does **not** import or call `analysis/eval/elicit01/units.py`; it has its own port, `engine/elicitation/units.py`.
- **Evidence level:** code order
- **Evidence:** census by `grep -E "^\s*(from|import) analysis\b"` over `engine/` (module-level and indented/lazy imports both match), plus a search for `import_module`/`__import__` naming `analysis` (none) and for the strings `analysis.(paper1|eval|provenance)` (prose only).

  **Engine**

  | Importing module | Quoted import | Used for | Production run path? |
  |---|---|---|---|
  | `engine/elicitation/units.py` | `from analysis.provenance.segment import sentences` | `build_unit_map`: `units = merge_short([s for s in sentences(stripped) if s.strip()], min_tokens)` — the index space Pass 1 cites | yes when `extraction_models.elicitation` is true (`extractor.extract_paper`: `if getattr(getattr(spec, "extraction_models", None), "elicitation", False): from engine.elicitation.pipeline import extract_paper_elicited`). The live spec has `elicitation: false` (`review_specs/surgical_autonomy.yaml`); CLAUDE.md names this path "the Run 7 extraction design" |
  | `engine/elicitation/contracts.py` | `from analysis.provenance.segment import sentences` | INFERABLE contract: `<= len(sentences(inference)) <= INFERENCE_MAX_SENTENCES` (the 1–3 sentence check) | same condition |
  | `engine/parsers/parse_quality.py` | `from analysis.provenance.segment import sentences` | `units = sentences(raw)` in the parse-quality gate | **yes, unconditionally** — `engine/parsers/pdf_parser.py` imports `engine.parsers.parse_quality`, and the gate's verdict sets `papers.status = 'PDF_EXCLUDED'` |

  The dependency is declared, not accidental: `engine/elicitation/units.py` docstring — "**Layering note.** `analysis.provenance.segment` is imported rather than forked. It is the segmenter whose output the frozen v1.1 taxonomy scores, and a production copy of it would be free to drift from the thing that grades it. The dependency is read-only and one-way."; `engine/parsers/markers.py` — "`parse_quality` imports the segmenter from `analysis.provenance.segment`, taking an explicit exception to the rule that the study lane is not an engine dependency, because there must be exactly one segmenter." `pysbd==0.3.4` is pinned in `requirements.txt`. `segment.py` itself imports only `re`, `functools`, `pysbd`.

  **The elicited path and `analysis/eval/elicit01/units.py`.** `engine/elicitation/pipeline.py`: `from engine.elicitation.units import UnitMap, build_unit_map`; `materialize.py` and `contracts.py`: `from engine.elicitation.units import UnitMap`. `engine/elicitation/units.py` opens "Ported unchanged from `analysis/eval/elicit01/units.py`"; a `diff` of the two bodies shows exactly two differences: the `MIN_UNIT_TOKENS` comment, and `UnitMap.resolve` rejecting `bool` (the port documents it as its "one deliberate deviation"). `COMMENT_RE`, `strip_comments`, `merge_short` and `build_unit_map` are byte-identical. So A's remarks about the eval module's behaviour apply to the engine port (see A10).

  **`scripts/`** (listed separately)

  | Script | Quoted import | Production entry point? |
  |---|---|---|
  | `scripts/_pass2_stability.py` | `from analysis.paper1.judge import run_pass2` / `from analysis.paper1.judge_loader import (` | no — an underscore-prefixed Paper 1 judge diagnostic |

  No other `scripts/*.py` imports `analysis.*` directly; `scripts/run_pipeline.py` reaches `analysis.provenance.segment` only transitively through `pdf_parser → parse_quality`.
- **Overlap:** none found as a row (grep of the inventory for `pysbd` / `segment.py` / `analysis.provenance.segment`: no row). The exception is documented in module docstrings only.
- **Proposed class:** 3 — a documented layering exception with no behavioural defect of its own; the risk is that a change made in the study lane (`analysis/provenance/segment.py`, e.g. `_ELLIPSIS_SPLIT_RE` or `MIN_SENTENCE_TOKENS`) changes the production index space and the parse gate without touching `engine/`.
- **Owning state:** pre-tag (R512) only if the tag is meant to freeze the segmenter's identity; otherwise post-tag.
- **Minimum loud-failure fix:** a pin test on the segmenter's observable identity as the engine uses it (`TOKENIZER_VERSION == "0.3.4"`, a small golden input → units), living in the engine's tests, so a study-lane edit turns the engine suite red. (Not established whether such a pin already exists — tests were not run and only grepped for other purposes.)
- **Full fix:** move `segment.py` to `engine/` (e.g. `engine/core/segment.py`) and have `analysis/provenance` import it from there — same single segmenter, dependency pointing the conventional way; add a static test that `engine/` imports nothing from `analysis`.
- **Acceptance test:** `grep -rE "^\s*(from|import) analysis\b" engine/` is empty; the provenance taxonomy's pinned figures are unchanged.
- **Size:** S — one module move and three import lines (plus the analysis-side imports).
- **PI-decision flag:** yes — keep the documented exception (engine imports the study lane's segmenter), or invert it (move the segmenter into `engine/`)?
- **Differences from the original / the brief's note:** A never states this dependency; it describes `analysis/eval/elicit01/units.py` as if it were the pipeline's unit builder. It is not on any engine path — the engine has a byte-equivalent port — but the port's segmenter *is* an `analysis.*` import.


---

### A6 — "State checks still perform direct updates on relational state tables outside the event transaction"

- **Source:** outside A
- **Present at HEAD:** no for the three files A names and for `extractions` / `evidence_spans`; partial for the wider question (some `papers.status` writes carry no event at all, and two bypass `update_status`).
  - (a) `engine/core/paper_state.py` writes relational tables — **no**. It executes no SQL.
  - (b) `engine/core/completeness.py` writes relational tables — **no**. It executes no SQL.
  - (c) `engine/adjudication/advance_stage.py` updates paper state + audit event + placeholder records — **no**. It is a CLI over `workflow_state` only.
  - (d) "the event store record may not commit in the same transaction as the `evidence_spans` or `extractions` tables" — **not present**: nothing writes those two tables at HEAD, so there is no second store to drift from.
  - (e) Engine code still writing `papers.status`: yes, seven sites; where an event accompanies the write it is in the same transaction; several writes have no event (the screening/acquisition side is not cut over).
- **Evidence level:** code order (not present at HEAD for a–d)
- **Evidence:**
  **(a)(b)** `grep -nE "conn|sqlite|INSERT|UPDATE|DELETE"` over `engine/core/paper_state.py` and `engine/core/completeness.py` returns one hit, a docstring in `completeness.py` ("…before any INSERT. There is no "store what we got" path"). `paper_state.py` is vocabulary and pure functions (`axis_of`, `is_failure`, reason-code maps); `completeness.py` is `check_completeness` / `enforce_completeness` / `enforce_terminal_states` over in-memory spans, raising `IncompleteExtractionError`, `DuplicateFieldError`, `TerminalStateError`. Neither takes a connection.
  **(c)** `engine/adjudication/advance_stage.py :: main` — `db = ReviewDatabase(args.review)` then `advance_stage(db._conn, args.stage, args.note, force=args.force)` or `format_workflow_status(db._conn, …)`. `engine/adjudication/workflow.py :: advance_stage` ends in `complete_stage(conn, stage_name, metadata=note)` or `bypass_stage(conn, stage_name, metadata=note)` — each one `UPDATE workflow_state …` followed by its own `conn.commit()` (`complete_stage`: `if commit: ensure_workflow_table(conn)` … `conn.commit()`). No `papers` write, no event, no placeholder rows. A single-statement transaction on `ReviewDatabase`'s connection.
  **(d)** Writers of `extractions` / `evidence_spans` (grep `INSERT (OR \w+ )?INTO|UPDATE|DELETE FROM` for both names over `engine/`, `scripts/`, `analysis/`, migrations excluded): **one hit**, `engine/core/database.py`'s `_EVIDENCE_SPANS_REBUILD` DDL constant (`INSERT INTO evidence_spans … FROM _evidence_spans_old`), run by `ReviewDatabase._run_migrations` only `if row and "contested" not in row[0]` — a schema rebuild, not a result write. `ReviewDatabase` has no `add_extraction` method (its writers at HEAD: `add_papers`, `update_status`, four `add_*_decision`). Extraction results are events: `engine/core/extraction_events.py :: write_extraction_events`
  ```python
  conn.execute("SAVEPOINT extraction_events")
  try:
      for fe in plan.field_events:
          write_field_event(conn, **fe, commit=False)
      write_paper_event(conn, **plan.paper_event, commit=False)
      conn.execute("RELEASE extraction_events")
      if conn.in_transaction:
          conn.commit()
  except BaseException:
      if conn.in_transaction:
          conn.execute("ROLLBACK TO extraction_events")
          conn.execute("RELEASE extraction_events")
      raise
  ```
  — claims, `claim_inputs` and the paper event in one unit, and no relational result table beside them. (The cloud path is the exception in kind: `engine/cloud/base.py :: store_result` still INSERTs `cloud_extractions` / `cloud_evidence_spans` in its own commit and `engine/cloud/*.py` contains no call to any event writer — existing row C28, cloud half F15, barred by R71.)

  **(e) `papers.status` writers at HEAD**

  | Site | Write | Event with it? | Same transaction? |
  |---|---|---|---|
  | `engine/core/database.py :: update_status` | `UPDATE papers SET status = ?, updated_at = ?` after the `ALLOWED_TRANSITIONS` / `RetiredTransition` checks; `own_txn = not self._conn.in_transaction` → `BEGIN IMMEDIATE` … `COMMIT` only when it opened the transaction | per caller, below | joins the caller's when one is open |
  | `engine/agents/ft_screener.py` primary, decided paper | `add_ft_screening_decision(commit=False)` → `update_status` → (exclude only) `_write_eligibility_event(… commit=False)` | exclude: `screened`→`full_text_out`; include: none by design ("'eligible' is the verifier's to write") | **yes** — `with _paper_transaction(db):` (`db._conn.commit()` / `except BaseException: db._conn.rollback()`) |
  | `engine/agents/ft_screener.py` verifier | confirm: decision row + `verified`→`eligible` event, **no status write**; flag: decision row + `update_status(pid, "FT_FLAGGED")`, no event | as stated | yes, same context manager |
  | `engine/agents/ft_screener.py` no-text / malformed-output branches | `db.update_status(pid, "FT_FLAGGED")` alone | none | own transaction |
  | `engine/adjudication/ft_screening_adjudicator.py` (import) | `INSERT INTO ft_screening_adjudication` → `review_db.update_status(paper_id, decision)` → `write_paper_event(… "adjudicated" …, commit=False)` → `complete_stage(…, commit=False)` → one `conn.commit()` | yes | **yes** (`ensure_adjudication_table` is called before the loop because "Its executescript commits") |
  | `engine/agents/screener.py`, `engine/adjudication/screening_adjudicator.py` (abstract stage) | `db.update_status(...)` | **none** — no abstract-stage eligibility event has a writer (`abstract_out` appears only in `paper_state.py`); ruled junior (R183, R207) | status alone |
  | `engine/parsers/pdf_parser.py :: parse_all`, success | `db.update_status(pid, "PARSED")` | none on success; `write_paper_event(… to_state="parse_failed" …)` on failure only (default `commit=True`, no status write beside it) | — |
  | `engine/parsers/pdf_parser.py :: parse_all`, quality gate | **direct** `UPDATE papers SET status = 'PDF_EXCLUDED', pdf_exclusion_reason = ?, …` + `db._conn.commit()` — not through `update_status` | none | — |
  | `engine/acquisition/pdf_quality_import.py :: import_dispositions` | **direct** `UPDATE papers SET status = 'PDF_EXCLUDED', …` inside its own `BEGIN`/`COMMIT`, on its own pragma-less connection | none, and no run manifest | — |
  | `engine/adjudication/import_extraction_entry.py` | `insert_paper_at_status(… status=ENTRY_STATUS …)` + `record_parsed_text` + `write_paper_event("adjudicated" → "eligible", commit=False)` | yes | yes (one import transaction) |
  | scripts (not engine): `scripts/rescreen_with_specialty.py :: _force_status` ("Direct SQL status update — bypasses state machine"), `scripts/advance_to_pdf_acquired.py` ("mirrors ReviewDatabase.update_status") | direct UPDATE | none | — |

  So wherever HEAD writes a status **and** an event for one decision, both are inside one transaction on one connection. What remains is not a two-store race but stores with a single writer each: the screening/acquisition tokens live only in `papers.status` (no event), and extraction results live only in events (no relational row). `effective_state` "never reads `papers.status`" (CLAUDE.md), so the two cannot disagree about a state that both hold except on the FT-exclude/adjudication paths, which are atomic.
- **Overlap:**
  - **C50** (`commit=False` event helper ends the caller's whole transaction on error) — REJECTED as the same defect; CONFIRMED present at HEAD and adjacent. `engine/core/events.py :: write_field_event` still ends `if commit: conn.commit()` / `except Exception: conn.rollback(); raise`, and `write_paper_event` has no handler. That is about *how much* an erroring helper rolls back, not about a relational write escaping the event's transaction. `write_extraction_events`' `ROLLBACK TO extraction_events` after such a `conn.rollback()` is guarded by `if conn.in_transaction`, so it does not raise.
  - **C56** (broad handlers converting run faults into paper outcomes) — REJECTED: a different defect (exception classification). The only contact point is `parse_all`'s `except Exception:` writing `parse_failed`, which C56 already names.
  - **C51** (`papers.status` has no CHECK and no trigger; "`insert_paper_at_status` … is the one sanctioned bypass of `update_status`") — RELATED and broader in fact than its text: two engine sites bypass `update_status` by direct UPDATE (see INT-g6-2).
  - **C28** — the cloud tables are the one place A's "relational result table beside the events" still describes, and there the events half is absent, not unsynchronised.
- **Proposed class:** 3 — A's claim is not true of HEAD; what survives is a wording/accuracy correction. (The two direct `PDF_EXCLUDED` writes are classed separately in INT-g6-2.)
- **Owning state:** none needed for A6 itself. pre-tag (R512) applies to INT-g6-2 if ruled a defect.
- **Minimum loud-failure fix:** none for A6.
- **Full fix:** none for A6; the screening/acquisition cut-over (junior, R183) is what would put those status writes behind events.
- **Acceptance test:** a static test that no file under `engine/` outside `engine/migrations/` and `database.py`'s rebuild constant contains `INTO extractions` / `INTO evidence_spans` — would have failed before the 9b cut-over and passes at HEAD (not checked whether one already exists).
- **Size:** S (the static test only)
- **PI-decision flag:** no
- **Differences from the original / the brief's note:** the lead's expectation is confirmed and stronger than stated: the files are present but contain no SQL at all (`paper_state.py`, `completeness.py`), and `advance_stage.py` touches `workflow_state` only. A's remedy ("explicit `BEGIN IMMEDIATE`") already describes `update_status`. A's second-pass claim that advancing "requires updating the paper state, recording an audit event, and creating placeholder records" describes no code at HEAD; `scripts/advance_to_pdf_acquired.py` (which A also names) is the script that inserts a `full_text_assets` row and updates status in one `BEGIN`/`COMMIT`, with `foreign_keys=ON` and no `busy_timeout`. `ReviewDatabase` already sets `journal_mode=WAL` and `busy_timeout=5000` (A recommends 30000; not evaluated).


---

### A7 — SQLite lock errors during a long extract run: where they would land (assessment A "Transaction Safety", retained narrowly)
- **Source:** outside A  (NB: this ID is the assessment's; the plan's inventory already has an unrelated row `A7` — `concordance.load_arm` run selection. Rename before filing.)
- **Present at HEAD:** partial, per sub-claim:
  - "background workers write telemetry to SQLite and contend with the run" — **no**: run telemetry is a JSONL file, and every database writer inside a `run_pipeline` process uses one connection
  - "gate-check writes contend with the run's own writes" — **no** inside the run (same connection, sequential); **yes** from a *second process* (C37: any status/gate check on a writable connection writes)
  - "the 5 s busy timeout may be exceeded" — **conditional**: only if a second process holds the write lock for > 5 s, or commits inside a read-then-write window (below); the run itself never holds a write transaction across a model call
  - where a lock error lands — established by code order: mostly as a **paper failure with reason `unclassified_error`**, in one place **swallowed**, at the gates as a **run fault**
- **Evidence level:** conditional (landing sites are code order; the triggering second writer was not established and nothing was run)
- **Evidence:**
  - Timeouts. `engine/core/database.py` `ReviewDatabase.__init__` and again after `runner.run` in `_run_migrations`: `sqlite3.connect(str(self.db_path))` + `PRAGMA journal_mode=WAL` + `PRAGMA busy_timeout=5000` + `PRAGMA foreign_keys=ON`. `engine/cloud/base.py` `CloudExtractorBase.__init__`: `sqlite3.connect(db_path)` + `PRAGMA busy_timeout=5000`. Every other engine opener passes no `timeout=` and sets no pragma — `engine/migrations/runner.py` (three `sqlite3.connect(str(db_path))`), `engine/cloud/schema.py` `init_cloud_tables`, `engine/acquisition/pdf_quality_check.py` / `pdf_quality_import.py` / `pdf_quality_html.py`, `engine/adjudication/abstract_adjudication_html.py` / `ft_adjudication_html.py` — so they run on Python's `sqlite3.connect` default (`timeout=5.0`). The one explicit other value is `engine/utils/db_backup.py` `_refuse_if_open`: `sqlite3.connect(str(target), timeout=0.2)`, deliberate. Read-only openers (`mode=ro`): `db_fingerprint`, `concordance`, `distribution_monitor`, `extraction_validator`. Net: **5 s everywhere that writes**; no opener retries on `OperationalError`; grep for `OperationalError|locked|busy` in `engine/core/events.py`, `extraction_events.py`, `run_manifest.py`, `engine/adjudication/workflow.py` returns nothing.
  - Writers in a `run_pipeline --skip-to extract` process, all on `db._conn` (`scripts/run_pipeline.py` `run_pipeline`: `db = ReviewDatabase(review_name)`): (1) workflow gate checks — `format_workflow_status(db._conn, …)`, `is_adjudication_complete(db._conn)`, `_advance_extraction_workflow(db._conn)`, `is_audit_review_complete(db._conn)`, `get_current_blocker(db._conn)`; each reaches `engine/adjudication/workflow.py` `ensure_workflow_table`: `conn.executescript(_WORKFLOW_STATE_TABLE)` / `conn.commit()` / `INSERT OR IGNORE INTO workflow_state …` per stage / `conn.commit()` (row C37, unchanged). They run **before** the extract loop and **after** the audit stage, never during the loop. (2) the manifest — `rm.open_run(db._conn, …)`, `rm.close_run(db._conn, …)`. (3) the `run_calls` recorder — `run_token = rm.activate(db._conn, run_id)`; `engine/core/run_manifest.py` `record_active_ollama_call`: `conn, run_id = active` → `record_call`: `INSERT INTO run_calls …` / `conn.commit()`, once per model call, on the calling thread (only `_client.chat` runs in `ollama_chat`'s `ThreadPoolExecutor`). (4) the event writer — `engine/agents/extractor.py` `record_failure`: `write_extraction_events(db._conn, outcome, review_dir=review_dir)`; `engine/core/extraction_events.py` `write_extraction_events`: `conn.execute("SAVEPOINT extraction_events")` … `conn.execute("RELEASE extraction_events")` / `conn.commit()`, once per paper. (5) telemetry — **not a database writer**: `engine/core/run_telemetry.py` `record_run_event`: `with path.open("a") as fh: fh.write(line + "\n")` on `run_events.jsonl`, under `except Exception … logger.warning("Run telemetry write failed (continuing)")`.
    No `INSERT`/`UPDATE`/`commit` appears in `engine/agents/extractor.py`, `engine/elicitation/pipeline.py`, `engine/core/completeness.py` or `citation_guard.py` (grep), and every `ollama_chat` ends in `record_call`'s `conn.commit()` — so the run holds the write lock for single statements/one savepoint at a time, never across a model call.
  - So a `database is locked` needs a **second process**: one holding the write lock for more than 5 s, or — independent of the timeout — one that commits between a read and the write that follows it inside the run's savepoint. `engine/core/events.py` `write_field_event` reads before it writes within that savepoint (`claimed = conn.execute("SELECT ci.parsed_text_sha256 FROM field_events fe …` and `existing = conn.execute("SELECT arm, paper_id, reuse_key, …` precede `cur = conn.execute(` INSERT); under WAL a deferred transaction that has read and then finds a newer commit fails its write upgrade at once (SQLITE_BUSY_SNAPSHOT), which the busy timeout does not retry. Candidate second writers that exist as documented commands: `python -m engine.adjudication.advance_stage --review <id> --status` (`db = ReviewDatabase(args.review)` then the C37 write — the command `run_pipeline`'s own BLOCKED message tells the operator to run), `python -m engine.validators.distribution_monitor` (`db = ReviewDatabase(args.review)`), a second `run_pipeline`, an adjudication import. Each of these writes for milliseconds; none was shown to hold the lock for 5 s. Not writers: the nightly suite (live-data fence), `db_fingerprint` and the snapshot timer (readers; under WAL they do not block the run's writes).
  - Landing sites, in the order the run meets them:
    1. `format_workflow_status(db._conn, …)` — before `_open_run_manifest` and outside the `try`: the `OperationalError` propagates out of `run_pipeline`; no manifest exists. Loud, clean.
    2. `is_adjudication_complete(db._conn)` — inside the `try`: `except Exception as exc: logger.error("Pipeline failed: %s", exc, exc_info=True)` / `_finish_review_run(db, run_id, "failed")` / `raise`. Run fault; the close is itself a write (`close_run`: `UPDATE run_manifests SET ended_at = …` / `conn.commit()`), so if the lock persists the close raises inside the handler and the manifest is left with no end.
    3. `record_call` inside `ollama_chat` — `_finish`: `_record("completed", response=checked)` raises → `ollama_chat`'s outer `except Exception as exc: _record("error", exc=exc)` / `raise`. If that second insert succeeds, `run_calls` holds an **`error` row whose detail is the lock error for a call that completed**; if it also fails, a sent call has **no** `run_calls` row. Either way the model's response is discarded and the `OperationalError` goes to the caller (4).
    4. Any `OperationalError` from inside `extract_paper_with_completeness` (3, or the claim write in `write_extraction_events`, which does `ROLLBACK TO extraction_events` / `RELEASE` / `raise`) — `_extract_selected`: `except Exception as exc:` → `logger.exception("Paper %d extraction failed: …")` → `stats["failed"] += 1` → `record_failure(pid, exc, "extract_pass2")` → `outcome_for_exception`: none of the named classes match, so its last line `return fail(PS.REASON_UNCLASSIFIED_ERROR)` → a `PaperFailure` to `extraction_failed`. **The lock error becomes a paper failure**: the paper's recorded outcome is `extraction_failed` / `unclassified_error` (exception type and message kept in `detail`), and `counts_toward_abort` is True for it, so three in a row (`CONSECUTIVE_FAILURE_ABORT = 3`) raise `RunAborted`.
    5. If the failure event's own write is also locked — `record_failure` is called from inside the `except` block, not under a `try`, so the `OperationalError` leaves the loop and `run_extraction`, reaching (2)'s handler: run fault `failed`, with that paper holding neither claim nor event.
    6. `_advance_extraction_workflow(db._conn)` after the audit stage — `try: … except Exception: pass  # workflow table may not exist`. **Swallowed, unlogged**: `EXTRACTION_COMPLETE` / `AI_AUDIT_COMPLETE_STAGE` stay pending and nothing says why.
    7. `is_audit_review_complete(db._conn)` — as (2): a run whose extraction and audit are fully committed closes `failed`.
- **Overlap:** C37 — CONFIRMED as the mechanism that makes a *status read* a writer (the only reason an operator's `--status` during Run 7 contends at all); A7 adds nothing to C37's own defect and C37's owner (S3c, schema into migrations) removes the second-process writer. No other row found for lock-error classification.
- **Proposed class:** 2 — refusals and loud failures; the wrongness is attribution (an infrastructure fault recorded as the paper's `unclassified_error` and counted toward abort) plus one silent `except: pass`, not a wrong stored value.
- **Owning state:** pre-tag (R512) for the narrow items below, because Run 7 is the 20–35 h run in question; C37 keeps its own owner.
- **Minimum loud-failure fix:** in `outcome_for_exception`, add `sqlite3.OperationalError` to the propagate-as-run-fault tuple (a database fault is not the paper's outcome — the function's own R5 rule), and replace `_advance_extraction_workflow`'s `except Exception: pass` with a logged, named exception.
- **Full fix:** remove the second-process writer at its source (C37: gate/status checks stop writing; `advance_stage --status` opens `mode=ro`), and make `ollama_chat`'s recorder distinguish "call completed, record failed" from a call error so R216's one-row-per-call holds.
- **Acceptance test:** a stub whose `record_call` / event write raises `sqlite3.OperationalError("database is locked")` once: the run closes `failed` naming the lock, no `extraction_failed`/`unclassified_error` event is written for that paper, and the consecutive-failure counter is untouched; `_advance_extraction_workflow` under the same stub logs the failure; `advance_stage --status` against a database with an open write transaction returns without waiting.
- **Size:** S for the minimum (two sites, mechanism read); the full fix is C37's size (M/L, migration-dependent).
- **PI-decision flag:** yes — "Is a `database is locked` during extraction a run fault (stop the run, paper untouched) or a retryable paper failure as now?"
- **Differences from the original / the brief's note:** A names `engine/adjudication/advance_stage.py`, `engine/core/paper_state.py` and a script `scripts/advance_to_pdf_acquired.py` "while background workers write telemetry" — at HEAD there are no background workers and telemetry is not in SQLite; A's "connection pooling" does not exist (one connection per process). A's 5 s figure is right (Python's default and the explicit pragma agree). A's recommended pragma change is not carried here, per the brief. The brief frames the gate-check writes as happening *during* the long run: inside the run they bracket the extract loop (before it and after audit) on the run's own connection, so they cannot contend with it; the contention path is an operator's or another job's process. Not established: whether `ReviewDatabase` construction by a second process takes the write lock when the schema is already current (it issues `executescript(_SCHEMA)`, `_SIMPLE_MIGRATIONS` and `runner.run`, which read as no-ops but were not run here); how the audit stage's writes are transacted (not read — outside the brief's extract scope); whether `scripts/advance_to_pdf_acquired.py` exists (not looked for).


---

### A10 — Comment stripping in the unit map: no offsets exist, but stripped-text quotes are located against raw text

- **Source:** outside A (conditional on A5)
- **Present at HEAD:** partial
  - (a) "the engine's elicited path uses that code" — yes, as a byte-equivalent port (`engine/elicitation/units.py`; see A5).
  - (b) "character offsets drift / character-level span locators" — **no**. There is no offset-based locator on any engine path: `UnitMap` stores unit **text** (`units: tuple[str, ...]`), `to_json()` persists text, and `UnitMap.source_stripped` is assigned and never read anywhere in `engine/` or `analysis/`. `PaperIndex` does not exist in `engine/`.
  - (c) The underlying mismatch A points at — quotes built from the comment-**stripped** text, checked against the **raw** parsed text — **yes**, in text-matching form: a materialised citation that touches a Docling comment is not an exact substring of the raw text, and for short passages is not located at all.
- **Evidence level:** reproduced (pure functions on synthetic strings; plus a file-only measurement over 60 parsed-text files — no database, no model)
- **Evidence:**
  Build side — `engine/elicitation/units.py`:
  ```python
  COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
  def strip_comments(text: str) -> str:
      return COMMENT_RE.sub(" ", text)
  def build_unit_map(paper_id, raw_text, min_tokens=MIN_UNIT_TOKENS) -> UnitMap:
      stripped = strip_comments(raw_text)
      units = merge_short([s for s in sentences(stripped) if s.strip()], min_tokens)
  ```
  Materialise side — `engine/elicitation/materialize.py :: source_snippet`: `return " ".join(unit_map.resolve(i) or "" for i in runs[0]).strip()` (the first contiguous run of cited units, joined by one space).
  Locate side — `engine/agents/audit_events.py` (the audit's citation pass): `text = read_parsed_text(ref)` … `res = locate(text, c.source_snippet)` — the raw parsed file; `engine/core/locator.py :: locate`: `norm_text = normalize(parsed_text); if norm_snippet in norm_text: return LocateResult(True, EXACT, …)`, else a word-window `SequenceMatcher` scan, `located = best > threshold` (0.85). `normalize` collapses whitespace and case; it does not remove `<!-- … -->`.

  Reproducer `~/scratch/12f/repro/A10_stripped_vs_raw_locate.py`, output `.out` (abridged):
  ```
  [control: no comment] … locate vs RAW parsed text  : located=True kind=exact
  [comment INSIDE one short unit]            stored snippet 'The trial enrolled   40 patients in total.'
     locate vs RAW parsed text  : located=False kind=none score=0.642   | vs stripped: located=True exact
  [comment BETWEEN two cited adjacent units] stored snippet 'The trial enrolled 40 patients. Suturing was fully autonomous.'
     locate vs RAW parsed text  : located=False kind=none score=0.642   | vs stripped: located=True exact
  [comment inside one LONG unit]             locate vs RAW parsed text  : located=True kind=fuzzy score=0.918
  ```
  Size on real parsed text — `~/scratch/12f/repro/A10_corpus_measure.py` and `A10_corpus_attribution.py` (first 60 of 446 files in `data/surgical_autonomy/parsed_text/`, exact test only): 442 of 446 files contain a comment (11,934 comments; 6,968 `<!-- image -->`, 4,893 `<!-- formula-not-decoded -->`, always alone on their line). Of 36,817 single units, 110 are not exact-locatable in the raw text **because of comment stripping**; of 36,757 adjacent-unit pairs, 1,665 (4.5%) are not, for the same reason. (A further 143 units / 1,911 pairs miss for a different cause — see INT-g6-1.) Of the first 40 single-unit exact misses of ≤60 words (both causes pooled), 17 were not located by the fuzzy pass either.

  What a miss does downstream (`audit_events`, quoted above): `if res.located or not is_populated(…): continue` — an unlocated populated claim goes to `semantic_verify` (a model call judging the value against the snippet, the paper text not shown) and the `citation_located` event records `located = false`. The value is not altered and nothing is dropped.

  Stated guarantees this contradicts: `materialize.py` docstring — "Every materialized quote is therefore verbatim by construction … the span carries the FIRST CONTIGUOUS RUN of cited units, which is verbatim-contiguous and ANCHORED by construction"; CLAUDE.md — "A materialized quote is ANCHORED by construction". Both hold against the stripped text only; against the raw parsed text (which is what the locator, the claim's `parsed_text_sha256` and any human reader use) a run that spans a comment is neither contiguous nor verbatim.
- **Overlap:** none found (inventory grep for `strip_comments` / `source_stripped` / comment: no row). Related by mechanism to A5 (same port) and INT-g6-1.
- **Proposed class:** 1 — provenance: a citation the engine itself built from the paper is recorded as `located = false`, and the "anchored by construction" premise that the elicited path's measures rest on is false for comment-adjacent citations. It fails toward review (an extra semantic verification), not toward a silent wrong value. Applies only when `extraction_models.elicitation` is on (off on the live spec today; it is the declared Run 7 design).
- **Owning state:** pre-tag (R512) if Run 7 uses the elicited path — the first claim-bearing run pins the arm and its `citation_located` events are append-only.
- **Minimum loud-failure fix:** at materialisation, test the stored snippet with `locate(raw_text, snippet)` and refuse (or record a distinct code) when it is not EXACT — the engine then never writes a "verbatim" quote that its own locator cannot find.
- **Full fix:** make the two sides use one text. Either locate elicited claims against the same comment-stripped text (the locator applying `strip_comments` before `normalize`, as a new `LOCATOR_VERSION`), or keep comments out of the run definition — treat a stripped comment between two units as a run break so a stored snippet never spans one — and stop merging a short unit across a comment. Delete or use `source_stripped`.
- **Acceptance test:** for every parsed-text file, every single unit and every adjacent pair that `contiguous_runs` would call one run is EXACT-located against the text the audit reads (the corpus measurement above returning 0 comment-caused misses); the synthetic cases above all report `located=True kind=exact`.
- **Size:** M — a read-only Phase A to choose between the two fixes (one changes `LOCATOR_VERSION`, the other the unit/run semantics that ELICIT-01's 505/505 measurement was taken on), then one commit.
- **PI-decision flag:** yes — should an elicited citation be located against the comment-stripped text (new locator version) or should a comment break a citation run (unit map semantics change)?
- **Differences from the original / the brief's note:** A describes offset drift in a `PaperIndex` locator "if the locator references the original raw markdown rather than `source_stripped`". No offsets and no `PaperIndex` exist on the engine path, so the stated failure cannot occur. The condition A names is nevertheless real — the locator does read the raw markdown — and it surfaces as failed text location rather than shifted offsets. A's "bijection guarantees no content is dropped" is also only true of the stripped text.


---

### A11 — Pass 2's `format` is still the array-wrapped schema without `minItems`; a collapse is refused loudly, then stored as `extracted` after the retry budget

- **Source:** outside A (conditional)
- **Present at HEAD:** partial
  - (a) "`extract_pass2` sends the array form `{"fields": [...]}` with no `minItems`" — **yes**, on both paths (non-elicited and elicited), identical schema.
  - (b) "single-span collapse passes" silently into storage, as in SPANLOSS-01 — **no**: the write boundary refuses it (`IncompleteExtractionError`) and the driver re-issues the request, up to 3 attempts, with telemetry and a WARNING per attempt.
  - (c) What happens if all 3 attempts collapse — the paper **is stored**: one `asserted` claim, paper event `extracted` (not `extraction_failed`), the missing fields named only in `payload.incomplete_fields`. That is ruling R140, so it is by design, but two in-tree statements say the opposite (below).
- **Evidence level:** reproduced (resolver output; pure-function planning of the exhausted record) + code order
- **Evidence:**
  **The schema sent.** `engine/core/effective_config.py :: _format`
  ```python
  if stage == "extract_pass2":
      from engine.agents.models import ExtractionOutput
      return ExtractionOutput.model_json_schema()
  ```
  `engine/agents/models.py`: `class ExtractionOutput(BaseModel): fields: list[EvidenceSpan]`. One resolver branch serves both paths: `engine/agents/extractor.py :: extract_pass2_structured` sends `**cfg.kwargs()`, and `engine/elicitation/pipeline.py :: extract_paper_elicited` takes `cfg_p2 = stage_config("extract_pass2", spec)` and passes `cfg=cfg_p2` to the same function. No required-slot schema exists under `engine/` (grep `required_slot` / `additionalProperties` / `minItems`: none).
  `~/scratch/12f/repro/A11_resolved_pass2_format.py` (imports the resolver, loads `review_specs/surgical_autonomy.yaml`; no DB, no model), `.out`:
  ```
  --- live spec file: elicitation=False model=deepseek-r1:32b sent_keys=['format', 'keep_alive', 'messages', 'model', 'options', 'think']
  top-level type: object | properties: ['fields'] | required: ['fields'] | additionalProperties: <absent>
  fields: {'items': {'$ref': '#/$defs/EvidenceSpan'}, 'title': 'Fields', 'type': 'array'}
  minItems anywhere in schema: False
  --- same spec, elicitation=True: … (same)
  identical format on both paths: True
  ```
  So the grammar constrains each element's shape (`field_name`, `value`, `source_snippet`, `confidence`, `tier` all required) and nothing about how many elements there are or which `field_name`s appear; `{"fields": []}` is valid.

  **What catches a short response.** Non-elicited — `engine/agents/extractor.py :: extract_paper`: `enforce_completeness(span_dicts, expected, paper_id=paper_id, arm=cfg1.model)` before any write; elicited — `pipeline.py`: `enforce_terminal_states(…)` then `enforce_completeness(span_dicts, field_names, …)`. `engine/core/completeness.py :: enforce_completeness`: `if not result.complete: raise IncompleteExtractionError(… missing=result.missing, n_stored=result.n_produced, n_expected=result.n_expected …)`. The driver, `extract_paper_with_completeness`: `RETRYABLE = (IncompleteExtractionError, UncitedValueError, ValidationError)`, `for attempt in range(1, max_attempts + 1)` (`MAX_COMPLETENESS_ATTEMPTS = 3`), each refusal → `record_call(… outcome=f"{kind}_retry" if attempt < max_attempts else f"{kind}_exhausted", spans_parsed=…, missing_fields=…)` and `logger.warning("… Re-issuing identical request.")`.

  **After exhaustion.** The driver logs and re-raises:
  ```python
  logger.error("Paper %d (%s): UNSTORABLE after %d attempts — failing the paper, "
               "NOT storing a partial extraction. %s", …)
  raise last_error
  ```
  but the caller's `record_failure` → `outcome_for_exception`: `if isinstance(exc, (IncompleteExtractionError, UncitedValueError)): … return exc.record` → `write_extraction_events(db._conn, outcome, …)`, and `plan_extraction_events` for a record with ≥1 field ends `_paper_event(rec, event_type="extracted", to_state="extracted", … payload={"asserted": …, "incomplete_fields": list(rec.incomplete_fields), …})`. `~/scratch/12f/repro/A11_collapse_after_exhaustion.py`, `.out`:
  ```
  expected fields: 20; a one-span response validates against the Pass-2 schema model: 1 span
  an EMPTY array validates too: 0 spans
  write boundary: REFUSED IncompleteExtractionError n_stored=1 n_expected=20 missing=19  (budget 3 attempts)
  after exhaustion: paper event type=extracted to_state=extracted reason_code=None
    field events written: [('asserted', 'study_type')]
    payload asserted=1 incomplete_fields=19
    to_state in COMPLETED_PROCESSING_STATES (=> analysis_ready for an eligible paper): True
    counts toward the consecutive-failure abort: False
  ```
  (An exhausted record with **zero** fields is different: `if not rec.fields:` → `PaperFailure(… REASON_NO_FIELDS_RETURNED …)` → `extraction_failed`, counted toward the abort.) Ruling: R140 — "A paper stores what the arm produced … Fields missing after the completeness budget get no field event and are listed in the extracted paper event's payload.incomplete_fields (reader row 1); the paper is not failed."

  **So: loud at each attempt, recorded-but-quiet at the end.** The 19 cells resolve as MISSING (rule row 1) — no wrong value is stored — but the paper is `extracted`, `analysis_ready` is true for it, the run's abort counter is reset by it (`elif not hasattr(outcome, "reason_code"): consecutive_failures = 0`), and `incomplete_fields` has no reader in `engine/exporters`, `engine/analysis`, `engine/validators`, `engine/core/effective.py` or `scripts/` (grep). The retry re-issues the identical request at temperature 0 — the policy CLAUDE.md itself calls wrong for content failures ("F7 is why the identical-retry policy was wrong here") — so a deterministic collapse is likely to repeat three times; whether Ollama's reply is in fact identical across attempts was not tested (no model call permitted).

  **Stated guarantees the code does not honour:** (1) the log line above — "failing the paper, NOT storing a partial extraction" — is false at HEAD for any record with ≥1 field. (2) `engine/core/completeness.py` module docstring: "**Fail the call, never write partially.** An incomplete result raises before any INSERT. There is no "store what we got" path, because that is exactly what produced the 21 collapsed extractions." — true of `enforce_completeness`, false of the path as a whole since R140. (3) `extract_paper_with_completeness`' in-code comment "exhausted, it is `response_unparseable` (F9), not a stored paper" is correct for `ValidationError` only.
- **Overlap:** no inventory row found for the schema form or for `incomplete_fields` (grep of rows for `SPANLOSS`, `minItems`, `required-slot`, `incomplete_fields`, `R140`: none). SCHEMA-EVAL-02 (CLAUDE.md: "RETAIN_B by the pinned rule") is the standing decision that keeps the array contract; R139/R140 are the standing rulings for exhaustion.
- **Proposed class:** 1 — a paper with one of twenty fields can end `extracted` / analysis-ready with nothing a reader consumes distinguishing it from a complete extraction except 19 MISSING cells; no stored value is wrong, so it is the weak form of Class 1 (silent incompleteness rather than a wrong result). The two false statements alone are Class 3.
- **Owning state:** pre-tag (R512) — Run 7 is the first claim-bearing run and its paper events are append-only.
- **Minimum loud-failure fix:** a floor on R140: when the exhausted record carries fewer than a declared share of the expected fields (or `incomplete_fields` is non-empty at all — PI's choice), plan it as `extraction_failed` with a new F9 reason (e.g. `fields_incomplete_after_budget`) so the paper is reselected next run and counted toward the abort; correct the log line and the `completeness.py` docstring either way.
- **Full fix:** send the required-slot object as `extract_pass2`'s `format` (named properties from the codebook, `required`, `additionalProperties: false` — SCHEMA-EVAL-02's condition C), which makes a short response ungrammatical instead of retried; that changes the wire request, so it is a new prompt hash and must precede the arm's pin. Plus a reader for `incomplete_fields` (PRISMA / run summary).
- **Acceptance test:** the reproducer's last block prints `extraction_failed` for a 1-of-20 record (or the PI-ruled threshold), `counts toward the abort: True`; `tests/test_request_capture.py`'s pass-2 case asserts the new format's key set; the log text and docstring match the behaviour.
- **Size:** S for the floor and the two text corrections; M for the format change (a Phase A against SCHEMA-EVAL-02's "RETAIN_B" record first, since that study measured C and kept B).
- **PI-decision flag:** yes — under R140, should a paper whose exhausted record is missing fields (e.g. 19 of 20) still be stored as `extracted`, or fail as `extraction_failed` below some completeness floor?
- **Differences from the original / the brief's note:** A describes the eval harness's Condition B/C and says C "fixes this structurally"; at HEAD the engine still sends B on both paths, by SCHEMA-EVAL-02's decision. A implies the collapse reaches storage unnoticed; it does not on the first two attempts, and on the third it is stored deliberately. A's "empty … arrays pass validation" is true of the schema; an empty result ends `extraction_failed` (`no_fields_returned`), not stored.


---

### INT-g2-1 — OpenAlex retrieval is silently capped at 10,000 works (pyalex `n_max` default), with no check against the advertised count
- **Source:** internal-12f (noticed while reading the paginator for F08)
- **Present at HEAD:** yes
- **Evidence level:** reproduced
- **Evidence:** `~/scratch/12f/repro/F08_paginate_retry.py` part (C), output `F08_paginate_retry.out`: `Works.paginate signature: (self, method='cursor', page=1, per_page=None, cursor='*', n_max=10000)`; with a faked transport advertising `meta.count=25000`, the real `_paginate_with_retry` "yielded 10000 works in 50 requests, no error". Code: `engine/search/openalex.py` `_paginate_with_retry`: `paginator = works_query.paginate(per_page=_PER_PAGE)` — no `n_max`; pyalex 0.20 `Paginator.__next__`: `if self._next_value is None or self._is_max(): raise StopIteration`, `_is_max`: `if self.n_max and self.n >= self.n_max: return True`. `search_openalex` returns whatever arrived; nothing reads `meta["count"]`.
- **Overlap:** none found among inventory rows. Possibly related to H1's "run today it would read 10,039" (a number of that size is what PubMed-only additions on top of a 10,000 cap would look like) — NOT established; I did not open the live database or any search log.
- **Proposed class:** 1 — a search whose OpenAlex result set exceeds 10,000 is truncated and stored as complete; PRISMA identification is wrong with no signal.
- **Owning state:** pre-tag (R512); whether the live review's stored search was affected is a separate, unanswered question (needs the original search logs / counts).
- **Minimum loud-failure fix:** pass `n_max=None` and compare retrieved works with the first page's `meta["count"]`; raise on a shortfall.
- **Full fix:** as F08's full fix — wrapper owns the cursor, advertised and retrieved counts stored with the search record.
- **Acceptance test:** faked transport advertising 25,000 → 25,000 retrieved, or an explicit refusal; never 10,000 returned as success.
- **Size:** S — same function and commit as F08's fix.
- **PI-decision flag:** yes — was the live review's OpenAlex query above 10,000 hits when it was run (i.e. does the stored corpus need a check against the cap)?
- **Differences from the original / the brief's note:** not in Assessment B; B's F08 names "advertised total counts" in its fix but not the cap.


---

### INT-g3-1 — Engine workbook writers raise on control characters that paper text is known to contain
- **Source:** internal-12f (noticed while verifying F15; not chased further)
- **Present at HEAD:** yes
- **Evidence level:** reproduced for the shared builder; code order for the evidence table
- **Evidence:**
  - `~/scratch/12f/repro/F15_review_workbook_formula.py` (last block): `create_review_workbook` with a title containing `\x0c` → `control char -> IllegalCharacterError`. Library level (`F15_openpyxl_formula.py`): `ws.append(["… \x0b …"])` → `IllegalCharacterError … cannot be used in worksheets.`
  - `engine/exporters/review_workbook.py`, `_build_review_queue_sheet`: `cell = ws.cell(row=row_idx, column=col_idx, value=value)` — no stripping; rows carry `title`, `abstract`, and (FT queue) `text_excerpt` from parsed text.
  - `engine/exporters/evidence_table.py`, `export_evidence_excel`: `for row in rows: ws1.append(row)` where a row carries `{field}_snippet` (`(ev.provenance.get("located") or {}).get("snippet", "")` — paper text) and the model's `value`; no stripping.
  - The repo already knows the input occurs: `analysis/paper1/pi_audit_sampler.py`, `_sanitize_for_xlsx` docstring — "Control chars show up in parsed_text / PDF OCR output (e.g., FORM FEED, VERTICAL TAB) and openpyxl rejects them"; `pi_audit_sampler_v2.py` comment — "parsed PDF/markdown also carries DEL + C1 controls (0x7f–0x9f), surrogates, and noncharacters that lxml rejects on save". Only the three `analysis/paper1` writers strip; the engine's two do not.
  - Consequence in `export_all`: the xlsx is the third writer, so the raise comes after `prisma_flow.csv` and `evidence_table.csv` have been replaced (F09 sub-claim d) — a mixed package, loudly.
- **Overlap:** F09 (d) is how it leaves a mixed directory; F15's full fix (one text-cell helper) is the natural home. Not established: whether any snippet or title on live carries such a character (live not opened).
- **Proposed class:** 2 — the export or queue build fails loudly; nothing stored is corrupted.
- **Owning state:** pre-tag (R512), with F15's helper.
- **Minimum loud-failure fix:** already loud. Make it survivable: strip XML-illegal characters in the F15 text-cell helper (v2's `_XML_ILLEGAL_RE` set), and record that a cell was altered.
- **Full fix:** one shared sanitiser owned by the exporters and imported by `analysis/paper1` (one predicate, two programs — the two existing copies already differ in the set they strip).
- **Acceptance test:** an evidence-table export and an FT adjudication queue whose snippet / excerpt contains `\x0b`, `\x0c`, `\x7f`, `\x85` and a lone surrogate both save; the workbook reloads; the count of altered cells is reported.
- **Size:** S — rides on F15's commit.
- **PI-decision flag:** no
- **Differences from the original / the brief's note:** not in either assessment.


---

### INT-g6-1 — Unit-map units are not always verbatim text of the paper (segmenter rewrites; ellipsis drop), independent of comments

- **Source:** internal-12f (noticed while measuring A10; not chased further)
- **Present at HEAD:** yes
- **Evidence level:** reproduced (synthetic strings) + a file-only measurement on parsed text (exact test only)
- **Evidence:** `engine/elicitation/units.py :: build_unit_map` takes units from `analysis.provenance.segment.sentences` (`pysbd.Segmenter(language="en", clean=False)` after `_ELLIPSIS_SPLIT_RE.split(text)`, whose docstring says of the ellipsis "The marker itself is discarded"). Two effects, `~/scratch/12f/repro/INT-g6-1_segmenter_not_verbatim.py` / `.out`:
  ```
  'Springer Nature 2021. Vol.:(0123456789) is the footer line here. …'
     S2: 'Vol. :(0123456789) is the footer line here.'   verbatim in input: False | locate: fuzzy 0.944
  'The first half of the claim... and the second half of the claim. …'
     S1: 'The first half of the claim'  S2: 'and the second half of the claim.'
     run S1+S2 joined verbatim in input: False | locate: fuzzy
  ```
  (1) pysbd alters characters inside a unit despite `clean=False` (a space inserted after an abbreviation period; seen in real text as `i.e. ,` for `i.e.,` in `100_v1.md` and `Vol. :(0123456789)` in `105_v1.md`). (2) Two units either side of an ellipsis join into a "contiguous run" that omits the ellipsis. Measured over the first 60 parsed-text files (`A10_corpus_attribution.out`): **143** single units and **1,911** adjacent-unit pairs are not exact substrings of even the comment-stripped text (normalised as the locator normalises).
  Stated guarantees contradicted: `units.py` docstring property 2 — "**Bijection over the remaining text.** Every character of the paper that survives step 1 belongs to exactly one numbered unit. Nothing is dropped."; `materialize.py` — "Every materialized quote is therefore verbatim by construction"; CLAUDE.md — "A materialized quote is ANCHORED by construction". ELICIT-01's "505/505 round-trips" (cited in the same docstring) is a statement about the cited sample, not about every unit.
- **Overlap:** A10 (same symptom, different cause); A5 (the segmenter is the study lane's). No inventory row found.
- **Proposed class:** 1 — provenance: the engine stores as `source_snippet` text that is not character-for-character in the paper while describing it as verbatim; most cases still locate FUZZY, short ones may not. Elicited path only (off on the live spec).
- **Owning state:** pre-tag (R512) if Run 7 is elicited — same reason as A10.
- **Minimum loud-failure fix:** the same guard as A10 — at materialisation, require the stored snippet to be EXACT-located in the text the audit will read, else a distinct refusal code.
- **Full fix:** build units as **slices** of the stripped text (recover each pysbd sentence's span in the source and store the source slice, so a unit is verbatim by construction and offsets exist); treat an ellipsis as a run break. Re-take the round-trip measurement on all units, not a cited sample.
- **Acceptance test:** for every parsed-text file, every unit is a substring of the comment-stripped source and `"".join` of the slices reproduces it; 0 exact misses in the corpus measurement.
- **Size:** M — a Phase A to confirm the pysbd rewrite inventory and that slicing leaves the index space (unit count and boundaries) unchanged, then one commit.
- **PI-decision flag:** no
- **Differences from the original / the brief's note:** not in either assessment. Not established: how many of these units a model would actually cite, or the fuzzy-located share over the whole corpus (fuzzy re-test was run on 40 units only).


---

### INT-g6-2 — Two engine paths set `papers.status = 'PDF_EXCLUDED'` by direct UPDATE, bypassing `update_status`

- **Source:** internal-12f (found under A6's census; not chased further)
- **Present at HEAD:** yes
- **Evidence level:** code order
- **Evidence:**
  `engine/acquisition/pdf_quality_import.py :: import_dispositions` — on its own `sqlite3.connect(str(db_path))` (no `foreign_keys`, no `busy_timeout`, no run manifest):
  ```python
  conn.execute(
      """UPDATE papers
         SET status = 'PDF_EXCLUDED',
             pdf_exclusion_reason = ?,
             pdf_exclusion_detail = ?,
             updated_at = ?
         WHERE id = ?""",
      (reason, db_detail, now, pid),
  )
  ```
  The only pre-checks are `validate_disposition_json` (per paper: `SELECT id FROM papers WHERE id = ?` — existence, not current status) and the skip sets `already_excluded` / `already_confirmed`. So any existing paper, at any status — `FT_ELIGIBLE`, `ABSTRACT_SCREENED_OUT`, a retired token — can be moved to `PDF_EXCLUDED`; `ALLOWED_TRANSITIONS` (only `PDF_ACQUIRED → PDF_EXCLUDED`) and `RetiredTransition` are never consulted; no event and no manifest record the human decision.
  `engine/parsers/pdf_parser.py :: parse_all` — the same statement with `("PARSE_QUALITY", detail, …)` followed by `db._conn.commit()`. Here the paper was selected at `PDF_ACQUIRED`, so the transition is one `update_status` would allow; the bypass is of the check, not of the rule.
  Stated guarantees: CLAUDE.md "Paper Lifecycle" — "(`papers.status` as `update_status` permits it — `ALLOWED_TRANSITIONS` …)"; inventory row C51 — "`insert_paper_at_status` / `IMPORT_ENTRY_STATUSES` … is the one sanctioned bypass of `update_status`".
  Why it can matter: corpus membership is the eligibility axis (`eligible_paper_ids`), which `papers.status` does not feed, while `engine/exporters/prisma.py` counts `pdf_excluded = n("PDF_EXCLUDED")` from `papers.status`. A disposition file naming a paper that already holds an `eligible` event would leave it in the corpus and in the PDF-excluded count. Not established: whether the PRISMA reconciliation refuses that state, or whether the HTML generator can emit such a paper (`pdf_quality_html` filters `status != 'PDF_EXCLUDED'` but the JSON is hand-editable and the importer validates existence only).
- **Overlap:** C51 — same family, and its "one sanctioned bypass" sentence is inaccurate at HEAD. D20 / R183 (acquisition cut-over is junior) — the eventless write is a known deferral; the unchecked transition is not stated there.
- **Proposed class:** 1 — a human gate's import can write a lifecycle state the state machine forbids, with no record of who or under which run; conditional on a disposition file naming a paper not at `PDF_ACQUIRED`.
- **Owning state:** pre-tag (R512) for the transition check; the event/manifest half belongs to the junior acquisition cut-over.
- **Minimum loud-failure fix:** in `import_dispositions`, refuse the file (it already validates atomically) when an EXCLUDE row names a paper whose status is not `PDF_ACQUIRED` — or route the write through `ReviewDatabase.update_status` and set the reason/detail columns beside it in the same transaction. Same routing in `parse_all`.
- **Full fix:** an `import` run manifest for the disposition file and a paper event per decision, at the acquisition cut-over; then C51's CHECK/trigger.
- **Acceptance test:** a disposition JSON excluding a paper at `FT_ELIGIBLE` is refused whole, database unchanged; one excluding a `PDF_ACQUIRED` paper still succeeds.
- **Size:** S
- **PI-decision flag:** no
- **Differences from the original / the brief's note:** not in either assessment.


---

