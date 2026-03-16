"""Tests for DB auto-backup before destructive operations."""

import sqlite3
from pathlib import Path

import pytest

from engine.utils.db_backup import auto_backup


@pytest.fixture
def temp_db(tmp_path):
    """Create a small SQLite database for testing."""
    db_path = tmp_path / "review.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
    conn.execute("INSERT INTO t VALUES (1, 'hello')")
    conn.commit()
    conn.close()
    return db_path


def test_backup_creates_valid_copy(temp_db):
    """Backup file is a readable SQLite DB with identical content."""
    backup_path = auto_backup(temp_db, "pre-test")

    assert backup_path.exists()
    conn = sqlite3.connect(str(backup_path))
    row = conn.execute("SELECT val FROM t WHERE id = 1").fetchone()
    conn.close()
    assert row[0] == "hello"


def test_backup_filename_format(temp_db):
    """Backup filename includes the reason and a timestamp."""
    backup_path = auto_backup(temp_db, "pre-cleanup")

    name = backup_path.name
    assert name.startswith("review.db.bak-pre-cleanup-")
    # Timestamp portion: YYYYMMDD-HHMMSS (15 chars)
    timestamp_part = name.split("pre-cleanup-")[1]
    assert len(timestamp_part) == 15  # e.g. 20260316-041500


def test_backup_file_size_matches(temp_db):
    """Backup file size matches the original database."""
    original_size = temp_db.stat().st_size
    backup_path = auto_backup(temp_db, "pre-reset")
    assert backup_path.stat().st_size == original_size
