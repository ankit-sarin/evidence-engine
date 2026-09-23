"""Migration 021: parsed_text_refs gains its content hash (S3e, R93, R100, R101).

INPUT-IDENTITY-01 Phase 2a. Phase 1 measured that `parsed_text_refs` — the
reference store the S3e resolver reads — carries **no hash**: 017 seeded paths
and versions only, so "which text did this claim see" could be answered by name
and never by content. S3d's reuse key (R91) is `(arm, paper_id,
parsed_text_hash)`, and R95 makes the resolver verify the recorded hash on every
read. Both need the hash stored, once, beside the reference.

This migration rebuilds `parsed_text_refs` with:

* **`parsed_text_sha256 TEXT NOT NULL`**, CHECKed as the states it permits
  (rule 11): exactly 64 characters, every one of them a lowercase hex digit. The
  brief's `GLOB '[0-9a-f]*'` tests only the FIRST character — `'a' || 63 × 'z'`
  would pass it — so the CHECK is written as "no character outside [0-9a-f]",
  which is what the brief's text meant. NOT NULL makes a NULL unrepresentable;
  the CHECK repeats `IS NOT NULL` so it is NULL-safe on its own (R78).
* **`UNIQUE (paper_id, parsed_text_version)`**, replacing 016's
  `UNIQUE (paper_id, parsed_text_version, parsed_text_path)`. The resolver picks
  the greatest version; the old constraint allowed two rows at one version with
  different paths, so "the greatest version" could be two rows. The new one is
  what makes a tie impossible, and it implies the old one.
* **Canonical paths (R100):** relative to the repository root when the file is
  under it, absolute otherwise. On the rebuild every stored path is normalised;
  the number of rows whose stored string changed is returned (on live it must
  be 0 — every seeded path is already repo-relative).

**Backfill, by recomputation, checked against a committed baseline (R93, R101).**
For every existing row the file at its (resolved) path is read and hashed. The
whole migration refuses — rollback, nothing written, the row named — on a
missing file, an unreadable file, a row absent from the baseline, a row whose
paper or version disagrees with its baseline entry, or a hash that disagrees
with it. The baseline is the JSON committed in INPUT-IDENTITY-01 Phase 1, and
its own SHA-256 is checked first: a baseline that is not the committed one is
refused before any row is read. **The baseline applies only when the table has
rows.** On a fresh database there is nothing to hash, nothing is read, and the
receipt is `executed`. A database whose rows match no baseline is refused, by
design (R101): the only database with pre-021 rows is the live one.

**Why schema kind, and one migration (L3).** `KINDS` decides whether a migration
runs on a fresh database; it says nothing about what the module may read. A
fresh database needs the column and the constraint as much as the live one does,
and on it the table is empty, so the backfill reads no file. Splitting into a
nullable-column schema step and a data backfill would leave a window in which a
reference with no hash is representable, which is the state this exists to end.

**Self-contained (R35).** Every DDL string, the trigger text and the baseline's
location and digest are declared here and imported from nowhere. The trigger
text is 016's, character for character, so the append-only guard is restored
verbatim (R87 verification: `tests/test_migration_021_parsed_text_sha256.py`).

**Transaction discipline (the 019 template).** One `BEGIN`; every statement
through `Connection.execute`, never `executescript`; the replacement built under
a temporary name; the guard dropped and restored inside the transaction;
`ROLLBACK` on any exception. The files are read and every refusal is decided
BEFORE the transaction opens, so a refusal writes nothing at all.

**Idempotent by postcondition (R44):** the table's own SQL naming
`parsed_text_sha256`. A second run reports `already_applied`.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from pathlib import Path

# ── R35: re-declared here, never imported ────────────────────────────
#: The repository root, derived from this file's own location
#: (engine/migrations/021_….py → three parents up).
REPO_ROOT = Path(__file__).resolve().parent.parent.parent

#: R101: the committed baseline and its digest. Tests pass `baseline_path=`,
#: `baseline_sha256=` and `repo_root=` to `run_migration`; the runner never does.
BASELINE_PATH = (REPO_ROOT / "docs" / "session-reports" / "input-identity-01"
                 / "parsed_text_hashes_20260923T214241Z.json")
BASELINE_SHA256 = "67754a477be575d285654b44cbf025d3ae17db1567b04970c0b6409bab6cb2e7"

TABLE = "parsed_text_refs"
TEMP_TABLE = "parsed_text_refs_new_021"
HASH_COLUMN = "parsed_text_sha256"

#: Column order: 016's six, then the hash. Appending keeps the six in the order
#: `db_fingerprint` has always hashed them in.
COLUMNS = (
    "parsed_text_uid", "paper_id", "parsed_text_path", "parsed_text_version",
    "source_full_text_assets_id", "recorded_at", HASH_COLUMN,
)

#: Rule 11: the states permitted — present, 64 characters, and no character
#: outside the lowercase hex digits.
HASH_CHECK = (
    f"{HASH_COLUMN} IS NOT NULL AND length({HASH_COLUMN}) = 64 "
    f"AND {HASH_COLUMN} NOT GLOB '*[^0-9a-f]*'"
)


def table_sql(name: str = TABLE) -> str:
    return f"""
    CREATE TABLE {name} (
        parsed_text_uid   TEXT PRIMARY KEY,
        paper_id          INTEGER NOT NULL REFERENCES papers(id),
        parsed_text_path  TEXT    NOT NULL,
        parsed_text_version INTEGER NOT NULL,
        source_full_text_assets_id INTEGER,
        recorded_at       TEXT    NOT NULL,
        {HASH_COLUMN} TEXT NOT NULL CHECK ({HASH_CHECK}),
        UNIQUE (paper_id, parsed_text_version)
    )
    """


def trigger_statements() -> tuple[str, str]:
    """016's two append-only triggers, verbatim (SQLite stores them without
    `IF NOT EXISTS`, and this text is what 016 left in `sqlite_master`)."""
    msg = f"{TABLE} is append-only: correct by appending an event"
    return (
        f"CREATE TRIGGER {TABLE}_no_update BEFORE UPDATE ON {TABLE}\n"
        f"      BEGIN SELECT RAISE(ABORT, '{msg}'); END",
        f"CREATE TRIGGER {TABLE}_no_delete BEFORE DELETE ON {TABLE}\n"
        f"      BEGIN SELECT RAISE(ABORT, '{msg}'); END",
    )


class Migration021Refused(RuntimeError):
    """021 found something it will not paper over; nothing was written."""


def canonical_path(stored: str, repo_root: Path) -> str:
    """R100: relative to the repo root when under it, absolute otherwise.

    `os.path.normpath`, not `Path.resolve`: resolving would follow a symlink out
    of the tree and turn a path that IS under the root into an absolute one.
    """
    root = os.path.normpath(str(repo_root))
    p = stored if os.path.isabs(stored) else os.path.join(root, stored)
    p = os.path.normpath(p)
    if p == root or p.startswith(root + os.sep):
        return Path(os.path.relpath(p, root)).as_posix()
    return p


def resolve_path(stored: str, repo_root: Path) -> Path:
    return Path(stored) if os.path.isabs(stored) else Path(repo_root) / stored


def _already_applied(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)
    ).fetchone()
    return bool(row) and HASH_COLUMN in (row[0] or "")


def _load_baseline(path: Path, expected_sha256: str) -> dict[str, dict]:
    try:
        raw = Path(path).read_bytes()
    except OSError as exc:
        raise Migration021Refused(
            f"021 refuses: the baseline {path} cannot be read ({exc!r})") from exc
    got = hashlib.sha256(raw).hexdigest()
    if got != expected_sha256:
        raise Migration021Refused(
            f"021 refuses: the baseline {path} has sha256 {got}, not the committed "
            f"{expected_sha256}. A baseline that is not the committed one is not a "
            "baseline.")
    doc = json.loads(raw)
    return {e["parsed_text_uid"]: e for e in doc["entries"]}


def _compute_rows(conn, repo_root: Path, baseline: dict[str, dict]) -> tuple[list, int]:
    """Read and check every row BEFORE any write. Returns (rows, paths_changed)."""
    rows = conn.execute(
        f"SELECT parsed_text_uid, paper_id, parsed_text_path, parsed_text_version, "
        f"source_full_text_assets_id, recorded_at FROM {TABLE} ORDER BY rowid"
    ).fetchall()

    dup = conn.execute(
        f"SELECT paper_id, parsed_text_version, COUNT(*) FROM {TABLE} "
        "GROUP BY paper_id, parsed_text_version HAVING COUNT(*) > 1").fetchall()
    if dup:
        raise Migration021Refused(
            "021 refuses: two references share a (paper_id, version), which the "
            "new UNIQUE forbids — " + "; ".join(
                f"paper_id={p} version={v} ({n} rows)" for p, v, n in dup))

    out, changed = [], 0
    for uid, pid, stored, version, fta_id, recorded_at in rows:
        where = f"parsed_text_uid={uid} paper_id={pid} version={version} path={stored}"
        entry = baseline.get(uid)
        if entry is None:
            raise Migration021Refused(f"021 refuses: {where} is not in the baseline")
        if entry["paper_id"] != pid or entry["parsed_text_version"] != version:
            raise Migration021Refused(
                f"021 refuses: {where} disagrees with its baseline entry "
                f"(paper_id={entry['paper_id']} version={entry['parsed_text_version']})")
        path = resolve_path(stored, repo_root)
        try:
            data = path.read_bytes()
        except FileNotFoundError as exc:
            raise Migration021Refused(f"021 refuses: {where} — file missing at {path}") from exc
        except OSError as exc:
            raise Migration021Refused(
                f"021 refuses: {where} — file unreadable at {path} ({exc!r})") from exc
        digest = hashlib.sha256(data).hexdigest()
        if digest != entry["sha256"]:
            raise Migration021Refused(
                f"021 refuses: {where} — file hashes to {digest}, the baseline "
                f"records {entry['sha256']}. The text changed after the baseline; "
                "record it as a new version rather than hashing it in place.")
        canon = canonical_path(stored, repo_root)
        changed += canon != stored
        out.append((uid, pid, canon, version, fta_id, recorded_at, digest))
    return out, changed


def run_migration(db_path: str | None = None, *,
                  baseline_path: str | Path | None = None,
                  baseline_sha256: str | None = None,
                  repo_root: str | Path | None = None,
                  _fail_after_copy: bool = False) -> dict:
    """Rebuild `parsed_text_refs` with its content hash. Idempotent by postcondition.

    The three keyword overrides exist for tests (R101); the runner passes only
    `db_path`. `_fail_after_copy` is the forced mid-rebuild failure of the 019
    template and is not reachable from the runner.
    """
    if db_path is None:
        raise ValueError("021 requires an explicit db_path")
    root = Path(repo_root) if repo_root is not None else REPO_ROOT

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        if _already_applied(conn):
            return {"status": "already_applied", "rows": 0, "paths_normalized": 0}

        n = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
        if n:
            baseline = _load_baseline(
                Path(baseline_path) if baseline_path is not None else BASELINE_PATH,
                baseline_sha256 if baseline_sha256 is not None else BASELINE_SHA256)
            rows, changed = _compute_rows(conn, root, baseline)
        else:
            rows, changed = [], 0

        conn.execute("BEGIN")
        conn.execute(f"DROP TRIGGER IF EXISTS {TABLE}_no_update")
        conn.execute(f"DROP TRIGGER IF EXISTS {TABLE}_no_delete")
        conn.execute(table_sql(TEMP_TABLE))
        conn.executemany(
            f"INSERT INTO {TEMP_TABLE} ({', '.join(COLUMNS)}) "
            f"VALUES ({', '.join('?' * len(COLUMNS))})", rows)

        if _fail_after_copy:
            raise RuntimeError("forced mid-rebuild failure (rehearsal)")

        conn.execute(f"DROP TABLE {TABLE}")
        conn.execute(f"ALTER TABLE {TEMP_TABLE} RENAME TO {TABLE}")
        for stmt in trigger_statements():
            conn.execute(stmt)

        # Postcondition, read back inside the transaction.
        after = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
        if after != n:
            raise RuntimeError(f"021 copied {after} rows but found {n} — refusing to commit")
        bad = [p for (p,) in conn.execute(f"SELECT parsed_text_path FROM {TABLE}")
               if canonical_path(p, root) != p]
        if bad:
            raise RuntimeError(f"021 left non-canonical paths: {bad[:5]}")

        conn.commit()
        return {"status": "executed", "rows": after, "paths_normalized": changed}
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
