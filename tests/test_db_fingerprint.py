"""Tests for the database content fingerprint (SAFE-GROUND-01).

Temp databases only. There is deliberately no test pinning the live review
database's hash: that hash is *meant* to change at the first migration, so a
test asserting it would have to be edited every time the database changed by
design — the "a number written down as a rule decays from the moment it is
taken" failure. The standing check on the live database is the CLI's
`--compare` against the committed fingerprint record at session open and close,
which is a procedure, not a test.
"""

import json
import sqlite3
from pathlib import Path

import pytest

from engine.tools.db_fingerprint import (
    CANONICAL_SERIALIZATION,
    compare,
    encode_cell,
    fingerprint,
    main,
)


@pytest.fixture
def wal_db(tmp_path):
    """A WAL database with an open connection — production's shape."""
    db_path = tmp_path / "review.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE alpha (id INTEGER PRIMARY KEY, v TEXT)")
    conn.execute("CREATE TABLE beta (id INTEGER PRIMARY KEY, n INTEGER)")
    conn.execute("INSERT INTO alpha (v) VALUES ('one')")
    conn.execute("INSERT INTO beta (n) VALUES (1)")
    conn.commit()
    yield db_path, conn
    conn.close()


def test_fingerprint_reproducible(wal_db):
    """Twice over an unchanged database: identical. After one INSERT: exactly one table differs."""
    db_path, conn = wal_db

    first = fingerprint(db_path)
    second = fingerprint(db_path)

    assert compare(first, second) == []
    assert first["overall_sha256"] == second["overall_sha256"]
    assert first["schema_hash_sha256"] == second["schema_hash_sha256"]
    assert first["tables"]["alpha"]["sha256"] == second["tables"]["alpha"]["sha256"]

    conn.execute("INSERT INTO alpha (v) VALUES ('two')")
    conn.commit()
    third = fingerprint(db_path)

    diffs = compare(second, third)
    assert diffs, "an added row must show up"
    assert third["overall_sha256"] != second["overall_sha256"]
    assert third["schema_hash_sha256"] == second["schema_hash_sha256"], (
        "adding a row is not a schema change"
    )
    # Exactly that table, and no other.
    assert third["tables"]["alpha"]["sha256"] != second["tables"]["alpha"]["sha256"]
    assert third["tables"]["alpha"]["row_count"] == 2
    assert third["tables"]["beta"]["sha256"] == second["tables"]["beta"]["sha256"]
    assert all("beta" not in d for d in diffs), diffs


def test_fingerprint_sees_uncheckpointed_wal_rows(wal_db):
    """The point of hashing rows: size and mtime cannot see this, the fingerprint can."""
    db_path, conn = wal_db
    before = fingerprint(db_path)
    stat_before = db_path.stat()

    conn.execute("INSERT INTO beta (n) VALUES (99)")
    conn.commit()  # committed; no checkpoint; connection still open

    after = fingerprint(db_path)
    stat_after = db_path.stat()

    assert (stat_before.st_size, stat_before.st_mtime_ns) == (
        stat_after.st_size, stat_after.st_mtime_ns
    ), "the main file must be unchanged, or this test proves nothing"
    assert after["overall_sha256"] != before["overall_sha256"]
    assert after["wal_file"]["non_empty_at_read"] is True


def test_fingerprint_is_read_only_and_never_immutable(wal_db):
    db_path, _conn = wal_db
    fp = fingerprint(db_path)
    assert fp["connection_uri"] == f"file:{db_path.resolve()}?mode=ro"
    assert "immutable" not in fp["connection_uri"]
    assert fp["immutable_flag_used"] is False
    assert fp["read_transaction"].startswith("single BEGIN")
    assert "U+001F" in fp["canonical_serialization"]
    assert fp["canonical_serialization"] == CANONICAL_SERIALIZATION


def test_fingerprint_from_a_supplied_connection_sees_that_connection(wal_db):
    """A connection's own view is what gets fingerprinted, uncommitted work included."""
    db_path, conn = wal_db
    committed = fingerprint(db_path)

    conn.execute("BEGIN")
    conn.execute("INSERT INTO alpha (v) VALUES ('uncommitted')")
    inside = fingerprint(conn)
    assert inside["tables"]["alpha"]["row_count"] == 2

    outside = fingerprint(db_path)
    assert outside["tables"]["alpha"]["row_count"] == 1
    assert compare(committed, outside) == []
    conn.execute("ROLLBACK")


def test_encode_cell_covers_every_storage_class():
    assert encode_cell(None) == "N"
    assert encode_cell(7) == "I:7"
    assert encode_cell(True) == "I:1"
    assert encode_cell(1.5) == "F:1.5"
    assert encode_cell(b"\x00\xff") == "B:00ff"
    assert encode_cell("x") == "T:x"


def test_compare_ignores_volatile_fields_and_names_real_ones(wal_db):
    db_path, conn = wal_db
    a = fingerprint(db_path)
    b = json.loads(json.dumps(a))
    b["db_file"]["path"] = "/somewhere/else.db"
    b["wall_time_seconds"] = 99.0
    b["generated_utc"] = "1999-01-01T00:00:00+00:00"
    assert compare(a, b) == []

    b["tables"]["alpha"]["row_count"] = 5
    diffs = compare(a, b, left_label="source", right_label="backup")
    assert any("alpha" in d and "row_count" in d for d in diffs), diffs


def test_cli_compare_exits_nonzero_on_a_difference(wal_db, tmp_path, capsys):
    db_path, conn = wal_db
    ref = tmp_path / "fp.json"

    assert main([str(db_path), "--out", str(ref)]) == 0
    assert ref.exists()
    assert main([str(db_path), "--compare", str(ref)]) == 0
    assert "IDENTICAL" in capsys.readouterr().out

    conn.execute("INSERT INTO alpha (v) VALUES ('drift')")
    conn.commit()
    assert main([str(db_path), "--compare", str(ref)]) == 1
    out = capsys.readouterr().out
    assert "DIFFERENCES" in out and "alpha" in out


def test_cli_reports_a_missing_database(tmp_path, capsys):
    assert main([str(tmp_path / "nope.db")]) == 2


def test_without_rowid_tables_order_by_primary_key(tmp_path):
    db_path = tmp_path / "wr.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE wr (a TEXT, b TEXT, PRIMARY KEY (a, b)) WITHOUT ROWID")
    conn.executemany("INSERT INTO wr VALUES (?, ?)", [("z", "1"), ("a", "2")])
    conn.commit()
    conn.close()

    fp = fingerprint(db_path)
    assert fp["tables"]["wr"]["order_basis"] == "primary key: a, b"
    assert fp["tables"]["wr"]["row_count"] == 2
    assert compare(fp, fingerprint(db_path)) == []
