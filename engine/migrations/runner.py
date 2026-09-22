"""The migration runner: numbered files, in order, with a receipt each.

MIGRATIONS-01. Before this, the live schema was the product of hand-applied
changes with no record. `ReviewDatabase._run_migrations` executed eighteen
inline `ALTER TABLE` statements that duplicated migrations 004 and 005, imported
006/007/008/009/012/013 by `importlib` on **every** construction, and never ran
002, 003, 010 or 011 at all — those two were applied by hand and left no trace,
so a second review could not get the same schema and nothing could say which
migrations a database had.

**A receipt, not a version number.** `PRAGMA user_version` is one integer: it
cannot say which of twelve migrations ran, cannot carry a checksum, and would be
a second source of truth that drifts the first time a migration is registered
out of order. It is deliberately left at 0. The `schema_migrations` table is the
record.

**A migration whose text changed after it ran is a different migration**, and the
database cannot know which one it got. The runner refuses to start when any
receipt's `file_sha256` differs from the file on disk, and names every drifted
id rather than the first.

**Two kinds.** A `schema` migration builds structure and is executed on a fresh
database. A `data` migration moves rows and is **never** executed on a fresh
database: 003 would import one review's nine thousand papers into another
review's database, and 002 renames labels a fresh database does not have. The
kind is declared in `KINDS` below with its reason, because deriving it from the
filename would be guessing.

Adding a migration: see `engine/migrations/README.md`.
"""

from __future__ import annotations

import hashlib
import importlib
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

RUNNER_VERSION = 1

MIGRATIONS_DIR = Path(__file__).resolve().parent

#: Filename prefix -> kind. Every numbered file must appear here; the runner
#: refuses to start if one does not, so a new migration cannot be added without
#: a ruling on what it is.
KINDS: dict[str, str] = {
    # 001 does not exist and never did — the series begins at 002.
    "002": "data",    # renames screening labels and statuses in existing rows;
                      # a fresh database has no rows to rename. Its INDEX half
                      # is now what engine/adjudication/schema.py creates, so a
                      # fresh database reaches the post-002 index set without it.
    "003": "data",    # imports ~9,234 papers from data/surgical_autonomy/
                      # expanded_search/. Its source directory and its default
                      # target are both literals naming one review, so running
                      # it for another review would import the autonomy corpus
                      # into that review's database. See S9.
    "004": "schema",
    "005": "schema",
    "006": "schema",
    "007": "schema",
    "008": "schema",
    "009": "schema",
    "010": "schema",
    "011": "schema",
    "012": "schema",
    "013": "schema",
    "014": "schema",
    "015": "schema",
    "016": "schema",   # the S2 event store: seven new tables, no existing one
                       # touched. A fresh database needs the structure.
    "017": "data",     # seeds the event store from THIS database's corpus,
                       # parsed texts, spec and codebook (R25). A fresh database
                       # has no corpus to seed from and no review directory
                       # beside it, so it must never run there.
    "018": "schema",   # C10 + R16 + R32: cloud_evidence_spans to the fresh
                       # NOT NULL shape, UNIQUE(paper_id, arm) dropped, and
                       # audit_adjudication dropped. Structure only.
    "019": "schema",   # R29/R39: paper_events rebuilt with the two-axis state
                       # vocabulary. Rebuilds an existing table; a fresh database
                       # needs the shape as much as the live one does.
}

_RECEIPTS_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    migration_id    TEXT    PRIMARY KEY,
    file_sha256     TEXT    NOT NULL,
    applied_at      TEXT    NOT NULL,
    mode            TEXT    NOT NULL
                    CHECK (mode IN ('executed', 'registered_preapplied')),
    runner_version  INTEGER NOT NULL,
    note            TEXT
);
"""

_NUMBERED = re.compile(r"^(\d{3})_[a-z0-9_]+\.py$")


class MigrationError(RuntimeError):
    """A migration could not be applied, or the set is not trustworthy."""


class MigrationDrift(MigrationError):
    """A migration file changed after its receipt was written."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def discover() -> list[tuple[str, Path]]:
    """`(migration_id, path)` for every numbered file, in numeric order.

    The id is the filename stem, so it carries the number and the name: a
    receipt reading `010_add_provenance_classifications` says what ran without
    anyone having to look the number up.
    """
    found = []
    for path in sorted(MIGRATIONS_DIR.glob("*.py")):
        m = _NUMBERED.match(path.name)
        if m:
            found.append((m.group(1), path))
    unknown = [n for n, _ in found if n not in KINDS]
    if unknown:
        raise MigrationError(
            "migration(s) with no declared kind: "
            + ", ".join(sorted(unknown))
            + " — add them to engine/migrations/runner.py::KINDS with the reason"
        )
    return [(p.stem, p) for _, p in sorted(found, key=lambda t: t[0])]


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def kind_of(migration_id: str) -> str:
    return KINDS[migration_id.split("_", 1)[0]]


def ensure_receipts(conn: sqlite3.Connection) -> None:
    conn.executescript(_RECEIPTS_DDL)
    conn.commit()


def receipts(conn: sqlite3.Connection) -> dict[str, dict]:
    """Every receipt, by migration id. `{}` when the store does not exist yet."""
    try:
        rows = conn.execute(
            "SELECT migration_id, file_sha256, applied_at, mode, runner_version, note "
            "FROM schema_migrations"
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    keys = ("migration_id", "file_sha256", "applied_at", "mode", "runner_version", "note")
    return {r[0]: dict(zip(keys, r)) for r in rows}


def check_drift(conn: sqlite3.Connection) -> list[str]:
    """Migration ids whose file no longer matches the receipt that recorded it."""
    drifted = []
    have = receipts(conn)
    for migration_id, path in discover():
        rec = have.get(migration_id)
        if rec and rec["file_sha256"] != file_sha256(path):
            drifted.append(migration_id)
    return drifted


def _write_receipt(conn, migration_id, path, mode, note=None) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO schema_migrations "
        "(migration_id, file_sha256, applied_at, mode, runner_version, note) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (migration_id, file_sha256(path), _now(), mode, RUNNER_VERSION, note),
    )


def run(db_path: str | Path, *, include_data: bool = False) -> dict:
    """Apply every pending migration to `db_path`, newest last.

    One transaction per RECEIPT, committed before the next migration begins.
    The migration itself runs in its own connection and owns its own
    transaction: this function closes its handle (`conn.close()  # migrations
    open their own connection`) before calling `run_migration(db_path)`, so a
    module that needs atomicity writes its own `BEGIN`/`ROLLBACK`, as 017, 018
    and 019 do. A failure leaves the earlier receipts intact and the failing one
    absent — the database then says exactly how far it got. (Class C row C11:
    this docstring previously said "one transaction per migration", which was
    true of the receipt and not of the migration.)

    Two things a module-owned transaction must get right, both found by
    rehearsal rather than by reading: `executescript` issues an implicit COMMIT
    before it runs, so every statement goes through `execute`; and a table
    rebuild creates the replacement under a temporary name rather than renaming
    the original away, because renaming a REFERENCED table rewrites its
    referrers' `REFERENCES` clauses — which is how A11's phantom
    `_evidence_spans_old` came to exist.

    `include_data` is False by default: data migrations are never executed on a
    fresh database. It exists so an operator can run one deliberately, naming it.

    Returns `{"executed": [...], "skipped": [...], "already": [...]}`.

    Raises:
        MigrationDrift: a migration file changed after its receipt.
        MigrationError: a migration failed; earlier ones stay applied.
    """
    db_path = Path(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = OFF")  # table rebuilds re-point FKs
        ensure_receipts(conn)

        drifted = check_drift(conn)
        if drifted:
            raise MigrationDrift(
                "refusing to run: these migration files changed after they were "
                "applied — " + ", ".join(drifted) + ". A migration whose text "
                "changed is a different migration, and this database cannot know "
                "which one it got."
            )

        have = receipts(conn)
        result = {"executed": [], "skipped": [], "already": []}

        for migration_id, path in discover():
            if migration_id in have:
                result["already"].append(migration_id)
                continue
            if kind_of(migration_id) == "data" and not include_data:
                result["skipped"].append(migration_id)
                continue

            module = importlib.import_module(f"engine.migrations.{migration_id}")
            conn.close()  # migrations open their own connection
            try:
                module.run_migration(str(db_path))
            except Exception as exc:  # noqa: BLE001 - re-raised with the id
                raise MigrationError(
                    f"{migration_id} failed: {type(exc).__name__}: {exc}"
                ) from exc
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA foreign_keys = OFF")
            _write_receipt(conn, migration_id, path, "executed")
            conn.commit()
            have[migration_id] = True
            result["executed"].append(migration_id)
            logger.info("migration %s executed", migration_id)

        return result
    finally:
        conn.close()


def register_preapplied(
    db_path: str | Path, migration_ids: list[str] | None = None, *, note: str = "",
) -> list[str]:
    """Write `registered_preapplied` receipts without executing anything.

    For a database whose schema already carries a migration's target state
    because it was applied by hand. The caller is asserting that, on evidence;
    this function verifies nothing about the schema and says so.

    Returns the ids registered (those that had no receipt already).
    """
    db_path = Path(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_receipts(conn)
        have = receipts(conn)
        wanted = {m: p for m, p in discover()}
        targets = migration_ids if migration_ids is not None else list(wanted)
        registered = []
        for migration_id in targets:
            if migration_id not in wanted:
                raise MigrationError(f"no such migration: {migration_id}")
            if migration_id in have:
                continue
            _write_receipt(conn, migration_id, wanted[migration_id],
                           "registered_preapplied", note or None)
            registered.append(migration_id)
        conn.commit()
        return registered
    finally:
        conn.close()
