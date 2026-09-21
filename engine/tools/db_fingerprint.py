"""Content fingerprint of a SQLite database — the standing integrity check.

SAFE-GROUND-01. Size and mtime cannot see a committed-but-uncheckpointed write:
under WAL the main file is untouched until a checkpoint runs, so a database can
gain rows without either changing. This module reads the rows.

**Read-only, always.** `mode=ro`, never `immutable=1` — `immutable` is a promise
to SQLite that the file cannot change while it is open, and `review.db` is a
live database, so the promise would be a lie and the reader could silently see a
torn page. One read transaction is held for the whole pass, so every table is
hashed against the same snapshot.

The hashing here is the ONE implementation. `engine.utils.db_backup` imports it
to self-verify a backup rather than carrying its own copy: two hash functions
that are supposed to agree recreate, one file over, exactly the divergence the
fingerprint exists to detect.

Usage:
    python -m engine.tools.db_fingerprint <db>                  # print a summary
    python -m engine.tools.db_fingerprint <db> --out fp.json    # write the record
    python -m engine.tools.db_fingerprint <db> --compare fp.json  # exit 1 on any difference
"""

from __future__ import annotations

import argparse
import binascii
import hashlib
import json
import sqlite3
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

#: Unit Separator and Record Separator. Named by codepoint rather than embedded
#: so the statement below survives being printed, pasted and diffed; the bytes
#: fed to SHA-256 are the characters themselves, unchanged.
_US = "\x1f"
_RS = "\x1e"

CANONICAL_SERIALIZATION = """\
Canonical serialization (the definition the hashes below are taken under):
  - Row ordering: by rowid ASC where the table has a rowid; else by the declared
    PRIMARY KEY columns ASC in declaration order (WITHOUT ROWID tables).
  - Column order: the table's declared column order from PRAGMA table_info.
  - Each cell is encoded as one of:
        "N"                      for SQL NULL
        "I:" + str(int)          for INTEGER
        "F:" + repr(float)       for REAL
        "T:" + text              for TEXT
        "B:" + hexlify(bytes)    for BLOB
  - Cells within a row joined by U+001F (US), rows joined by U+001E (RS),
    all encoded UTF-8, fed incrementally to SHA-256.
  - Schema hash: SHA-256 over
        RS.join(US.join(type, name, tbl_name, sql-or-empty))
    for every sqlite_master row, ordered by (type, name).
  - Overall hash: SHA-256 over RS.join(name + US + per_table_hash)
    for user tables in name order."""

STRUCTURE_DEFINITION = """\
Structural facts (the definition `schema_structure_hash` is taken over):
  - Tables: every name in sqlite_master, sorted, excluding sqlite_ internals.
  - Columns: PRAGMA table_info, as (name, type-uppercased, notnull, default,
    pk-ordinal), sorted BY COLUMN NAME — so a column appended by ALTER TABLE and
    the same column declared in a fresh CREATE compare equal.
  - Indices: PRAGMA index_list and index_info, as (name, unique, partial,
    columns-in-index-order), sorted by name.
  - Foreign keys: PRAGMA foreign_key_list, as (from, to-table, to-column,
    on_update, on_delete), sorted.
Column ORDER, quoting of the table name, comments and whitespace are excluded:
they are what ALTER TABLE and a rename leave behind, and two schemas that differ
only in those are the same schema. The textual `schema_hash_sha256` keeps them,
and it stays the live integrity check."""

#: Fields a comparison ignores: they describe the reading, not the content, and
#: a backup legitimately differs from its source in every one of them.
_VOLATILE_KEYS = (
    "tool", "task", "generated_utc", "connection_uri", "read_transaction",
    "db_file", "wal_file", "shm_file", "wall_time_seconds", "immutable_flag_used",
    "canonical_serialization", "structure_definition", "E5_comparison",
)


# ── Cell encoding ────────────────────────────────────────────────────


def encode_cell(value) -> str:
    """One cell, canonically encoded. See CANONICAL_SERIALIZATION."""
    if value is None:
        return "N"
    if isinstance(value, bool):
        # Before the int branch on purpose: bool is a subclass of int, and
        # SQLite has no boolean type, so a driver that hands one back must
        # still encode as the integer it is stored as.
        return "I:" + str(int(value))
    if isinstance(value, int):
        return "I:" + str(value)
    if isinstance(value, float):
        return "F:" + repr(value)
    if isinstance(value, bytes):
        return "B:" + binascii.hexlify(value).decode()
    return "T:" + str(value)


# ── Reading ──────────────────────────────────────────────────────────


@contextmanager
def read_snapshot(target):
    """A consistent read snapshot of `target`, yielding (conn, uri, db_path).

    `target` is a path, or an open `sqlite3.Connection` whose view is to be
    fingerprinted. A path gets its own `mode=ro` connection inside a read
    transaction. A supplied connection is wrapped in a read transaction only if
    it is not already in one — a caller mid-transaction already has a fixed
    snapshot, and issuing BEGIN there would raise.
    """
    if isinstance(target, sqlite3.Connection):
        conn = target
        db_path = _main_db_path(conn)
        uri = f"<supplied connection: {db_path}>"
        owned = False
        began = not conn.in_transaction
    else:
        db_path = Path(target).resolve()
        uri = f"file:{db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        owned = True
        began = True

    try:
        if began:
            conn.execute("BEGIN")
        # A deferred BEGIN takes no snapshot until the first read, so read now:
        # everything after this point sees one state of the database.
        conn.execute("SELECT count(*) FROM sqlite_master").fetchone()
        yield conn, uri, db_path
    finally:
        if began:
            try:
                conn.execute("COMMIT")
            except sqlite3.Error:  # pragma: no cover - a rolled-back snapshot
                pass
        if owned:
            conn.close()


def _main_db_path(conn: sqlite3.Connection) -> Path:
    """The file behind a connection's `main` schema."""
    for _seq, name, file in conn.execute("PRAGMA database_list"):
        if name == "main":
            if not file:
                raise ValueError(
                    "connection has no file behind its main schema "
                    "(an in-memory database cannot be fingerprinted by path)"
                )
            return Path(file).resolve()
    raise ValueError("connection reports no main schema")


def structure(conn: sqlite3.Connection) -> dict:
    """PRAGMA-derived structural facts. See STRUCTURE_DEFINITION."""
    tables = sorted(
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'"
        )
    )
    out: dict[str, dict] = {}
    for t in tables:
        cols = sorted(
            (c[1], (c[2] or "").upper(), int(c[3]), c[4], int(c[5]))
            for c in conn.execute(f'PRAGMA table_info("{t}")')
        )
        idx = []
        for row in conn.execute(f'PRAGMA index_list("{t}")'):
            name, unique, origin, partial = row[1], int(row[2]), row[3], int(row[4])
            cols_in = [c[2] for c in conn.execute(f'PRAGMA index_info("{name}")')]
            idx.append((name, unique, origin, partial, cols_in))
        fks = sorted(
            (f[3], f[2], f[4], f[5], f[6])  # from, to-table, to-column, on_update, on_delete
            for f in conn.execute(f'PRAGMA foreign_key_list("{t}")')
        )
        out[t] = {
            "columns": [list(c) for c in cols],
            "indices": [list(i) for i in sorted(idx)],
            "foreign_keys": [list(f) for f in fks],
        }
    return out


def structure_hash(struct: dict) -> str:
    """SHA-256 over the structural facts, serialized canonically."""
    payload = json.dumps(struct, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def structure_differences(left: dict, right: dict, *,
                          left_label: str = "left",
                          right_label: str = "right") -> list[str]:
    """Every structural fact that differs, named."""
    diffs: list[str] = []
    for name in sorted(set(left) | set(right)):
        l, r = left.get(name), right.get(name)
        if l is None:
            diffs.append(f"table {name!r}: absent in {left_label}")
            continue
        if r is None:
            diffs.append(f"table {name!r}: absent in {right_label}")
            continue
        for facet in ("columns", "indices", "foreign_keys"):
            lv = {tuple(map(_freeze, x)) for x in l[facet]}
            rv = {tuple(map(_freeze, x)) for x in r[facet]}
            for missing in sorted(rv - lv, key=str):
                diffs.append(f"{name}.{facet}: only in {right_label}: {list(missing)}")
            for extra in sorted(lv - rv, key=str):
                diffs.append(f"{name}.{facet}: only in {left_label}: {list(extra)}")
    return diffs


def _freeze(v):
    return tuple(v) if isinstance(v, list) else v


def fingerprint_within(conn: sqlite3.Connection, *, uri: str, db_path: Path) -> dict:
    """Fingerprint through an already-open snapshot. See `fingerprint`."""
    t0 = time.time()

    master = conn.execute(
        "SELECT type, name, tbl_name, COALESCE(sql,'') FROM sqlite_master"
    ).fetchall()
    schema_hash = hashlib.sha256(
        _RS.join(
            _US.join(map(str, row))
            for row in sorted(master, key=lambda r: (r[0], r[1]))
        ).encode()
    ).hexdigest()

    tables = sorted(row[1] for row in master if row[0] == "table")

    per_table: dict[str, dict] = {}
    for table in tables:
        cols = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
        colnames = [c[1] for c in cols]
        pk_cols = [c[1] for c in sorted((c for c in cols if c[5]), key=lambda c: c[5])]

        has_rowid = True
        try:
            conn.execute(f'SELECT rowid FROM "{table}" LIMIT 1').fetchone()
        except sqlite3.OperationalError:
            has_rowid = False

        if has_rowid:
            order, basis = "rowid ASC", "rowid"
        else:
            order = ", ".join(f'"{c}" ASC' for c in pk_cols) or "1"
            basis = "primary key: " + ", ".join(pk_cols)

        select = ", ".join(f'"{c}"' for c in colnames)
        digest = hashlib.sha256()
        count = 0
        first = True
        for row in conn.execute(f'SELECT {select} FROM "{table}" ORDER BY {order}'):
            if not first:
                digest.update(_RS.encode())
            first = False
            digest.update(_US.join(encode_cell(v) for v in row).encode())
            count += 1

        per_table[table] = {
            "row_count": count,
            "columns": colnames,
            "order_basis": basis,
            "sha256": digest.hexdigest(),
        }

    overall = hashlib.sha256(
        _RS.join(f"{t}{_US}{per_table[t]['sha256']}" for t in tables).encode()
    ).hexdigest()

    struct = structure(conn)

    wal = db_path.parent / (db_path.name + "-wal")
    shm = db_path.parent / (db_path.name + "-shm")
    stat = db_path.stat()

    return {
        "tool": "engine.tools.db_fingerprint",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "connection_uri": uri,
        "immutable_flag_used": False,
        "read_transaction": "single BEGIN ... COMMIT held for the whole pass",
        "canonical_serialization": CANONICAL_SERIALIZATION,
        "db_file": {
            "path": str(db_path),
            "size_bytes": stat.st_size,
            "mtime_utc_ns": stat.st_mtime_ns,
            "mtime_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
        },
        "wal_file": {
            "path": str(wal),
            "exists": wal.exists(),
            "size_bytes": wal.stat().st_size if wal.exists() else None,
            "non_empty_at_read": bool(wal.exists() and wal.stat().st_size > 0),
        },
        "shm_file": {
            "exists": shm.exists(),
            "size_bytes": shm.stat().st_size if shm.exists() else None,
        },
        "schema_hash_sha256": schema_hash,
        "structure_definition": STRUCTURE_DEFINITION,
        "schema_structure_hash": structure_hash(struct),
        "structure": struct,
        "table_count": len(tables),
        "tables": per_table,
        "overall_sha256": overall,
        "wall_time_seconds": round(time.time() - t0, 3),
    }


def fingerprint(target) -> dict:
    """The fingerprint record for a database file or an open connection.

    `target` is a path (a fresh `mode=ro` connection is opened and closed) or an
    open `sqlite3.Connection` (its own view is fingerprinted, which is what a
    caller holding uncommitted work would want to see).
    """
    with read_snapshot(target) as (conn, uri, db_path):
        return fingerprint_within(conn, uri=uri, db_path=db_path)


# ── Comparison ───────────────────────────────────────────────────────


def compare(left: dict, right: dict, *,
            left_label: str = "left", right_label: str = "right") -> list[str]:
    """Differences in CONTENT between two fingerprint records.

    Everything in `_VOLATILE_KEYS` is ignored: a backup differs from its source
    in its path, its mtime and how long it took to read, and none of that is a
    difference in what the database holds.
    """
    diffs: list[str] = []

    for key in ("schema_hash_sha256", "schema_structure_hash",
                "table_count", "overall_sha256"):
        # Only when BOTH carry the key. A record written before a field existed
        # is not evidence that the field changed, and treating its absence as a
        # difference would make every older record compare dirty forever.
        if key not in left or key not in right:
            continue
        lv, rv = left[key], right[key]
        if lv != rv:
            diffs.append(f"{key}: {left_label}={lv!r} {right_label}={rv!r}")

    lt = left.get("tables") or {}
    rt = right.get("tables") or {}
    for name in sorted(set(lt) | set(rt)):
        lrow, rrow = lt.get(name), rt.get(name)
        if lrow is None:
            diffs.append(f"table {name!r}: absent in {left_label}, "
                         f"{rrow['row_count']} rows in {right_label}")
            continue
        if rrow is None:
            diffs.append(f"table {name!r}: {lrow['row_count']} rows in {left_label}, "
                         f"absent in {right_label}")
            continue
        if lrow["row_count"] != rrow["row_count"]:
            diffs.append(f"table {name!r}: row_count {left_label}={lrow['row_count']} "
                         f"{right_label}={rrow['row_count']}")
        if lrow["sha256"] != rrow["sha256"]:
            diffs.append(f"table {name!r}: sha256 {left_label}={lrow['sha256'][:16]}… "
                         f"{right_label}={rrow['sha256'][:16]}…")
        if lrow.get("columns") != rrow.get("columns"):
            diffs.append(f"table {name!r}: columns {left_label}={lrow.get('columns')} "
                         f"{right_label}={rrow.get('columns')}")

    ls, rs = left.get("structure"), right.get("structure")
    if ls is not None and rs is not None:
        diffs.extend(structure_differences(ls, rs, left_label=left_label,
                                           right_label=right_label))
    return diffs


# ── CLI ──────────────────────────────────────────────────────────────


def _summary(fp: dict) -> str:
    lines = [
        f"database : {fp['db_file']['path']}",
        f"           {fp['db_file']['size_bytes']} B @ {fp['db_file']['mtime_utc']}",
        f"uri      : {fp['connection_uri']}  (immutable={fp['immutable_flag_used']})",
        f"-wal     : {fp['wal_file']['size_bytes']} B at read "
        f"(non-empty={fp['wal_file']['non_empty_at_read']})",
        f"tables   : {fp['table_count']}",
        f"schema   : {fp['schema_hash_sha256']}  (textual)",
        f"structure: {fp['schema_structure_hash']}  (PRAGMA-derived)",
        f"overall  : {fp['overall_sha256']}",
        f"wall     : {fp['wall_time_seconds']} s",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only content fingerprint of a SQLite database.",
    )
    parser.add_argument("database", help="path to the database file")
    parser.add_argument("--out", metavar="JSON",
                        help="write the fingerprint record to this path")
    parser.add_argument("--compare", metavar="JSON",
                        help="compare against a previously written record; "
                             "exit 1 on any difference in content")
    args = parser.parse_args(argv)

    db = Path(args.database)
    if not db.exists():
        print(f"no such database: {db}", file=sys.stderr)
        return 2

    fp = fingerprint(db)

    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(fp, indent=2) + "\n")
        print(f"wrote {args.out}")

    print(_summary(fp))

    if args.compare:
        ref_path = Path(args.compare)
        if not ref_path.exists():
            print(f"no such reference: {ref_path}", file=sys.stderr)
            return 2
        ref = json.loads(ref_path.read_text())
        diffs = compare(ref, fp, left_label="reference", right_label="measured")
        print()
        print(f"compared against {ref_path}")
        if diffs:
            print(f"DIFFERENCES ({len(diffs)}):")
            for d in diffs:
                print(f"  {d}")
            return 1
        print("IDENTICAL — schema, every table, and the overall hash all match.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
