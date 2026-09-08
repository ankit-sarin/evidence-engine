"""The live-data fence: no test may open a real review database.

Companion to `tests/conftest.py`'s `block_live_database`. The fence exists
because `test_paper_366_grammar_prevents_four_element_emission` constructed
`ReviewDatabase("surgical_autonomy")` — the production corpus database — from a
test the unmarked nightly run executes every night (JUDGE-DBGUARD-01).

Both tests here assert against the **path**, never the file: the refusal happens
before `ReviewDatabase.__init__` is entered, so nothing opens, and the first
test additionally pins the live file's size and mtime across the attempt so a
future guard that checked by opening would fail loudly here.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from engine.core.database import ReviewDatabase
from _live_db_guard import LiveDatabaseBlocked

LIVE_DB = Path(__file__).resolve().parent.parent / "data" / "surgical_autonomy" / "review.db"


@pytest.mark.dbguard_selftest
def test_guard_refuses_a_construction_under_the_live_data_root():
    """The exact construction that caused the PARSE-GATE-06c deviation."""
    before = LIVE_DB.stat() if LIVE_DB.exists() else None

    with pytest.raises(LiveDatabaseBlocked) as excinfo:
        ReviewDatabase("surgical_autonomy")

    message = str(excinfo.value)
    assert "surgical_autonomy/review.db" in message, message
    assert "data_root=tmp_path" in message, message
    assert "test_guard_refuses_a_construction_under_the_live_data_root" in message, message

    if before is not None:
        after = LIVE_DB.stat()
        assert (after.st_size, after.st_mtime_ns) == (before.st_size, before.st_mtime_ns), (
            "the live database moved during a refused construction — the guard "
            "must resolve paths, never open the file"
        )


def test_guard_allows_a_construction_under_tmp_path(tmp_path):
    """The form every other test in the suite already uses must stay silent."""
    db = ReviewDatabase("some_review", data_root=tmp_path)
    try:
        assert db.db_path == tmp_path / "some_review" / "review.db"
        assert db.db_path.exists()
    finally:
        db.close()
