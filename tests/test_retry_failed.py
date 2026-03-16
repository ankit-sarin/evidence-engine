"""Tests for --retry-failed extraction flag."""

import pytest

from engine.core.database import ReviewDatabase
from engine.search.models import Citation

# Import the functions under test from the script
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from run5_extract_and_audit import parse_args, reset_failed_papers


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("test_retry", data_root=tmp_path)
    yield rdb
    rdb.close()


def _add_paper(db, pmid="1"):
    db.add_papers([Citation(title=f"Paper {pmid}", source="pubmed", pmid=pmid)])
    return db._conn.execute(
        "SELECT id FROM papers WHERE pmid = ?", (pmid,)
    ).fetchone()["id"]


def _advance_to_extract_failed(db, pid):
    """Walk paper through lifecycle to EXTRACT_FAILED."""
    for status in [
        "ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED",
        "FT_ELIGIBLE", "EXTRACT_FAILED",
    ]:
        db.update_status(pid, status)


def test_retry_failed_finds_and_resets(db):
    """--retry-failed resets EXTRACT_FAILED papers to FT_ELIGIBLE."""
    pid1 = _add_paper(db, "1")
    pid2 = _add_paper(db, "2")
    _advance_to_extract_failed(db, pid1)
    _advance_to_extract_failed(db, pid2)

    reset_ids = reset_failed_papers(db)

    assert sorted(reset_ids) == sorted([pid1, pid2])
    for pid in reset_ids:
        row = db._conn.execute(
            "SELECT status FROM papers WHERE id = ?", (pid,)
        ).fetchone()
        assert row["status"] == "FT_ELIGIBLE"


def test_retry_failed_zero_papers(db):
    """--retry-failed with no EXTRACT_FAILED papers returns empty list."""
    # Add a paper at FT_ELIGIBLE — should NOT be picked up
    pid = _add_paper(db, "1")
    for status in ["ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "FT_ELIGIBLE"]:
        db.update_status(pid, status)

    reset_ids = reset_failed_papers(db)
    assert reset_ids == []


def test_mutually_exclusive_flags():
    """--retry-failed and --paper-ids together cause exit."""
    with pytest.raises(SystemExit):
        # parse_args succeeds, but main() would call sys.exit(1)
        # We test the conflict in main() logic directly
        from run5_extract_and_audit import main
        main(["--retry-failed", "--paper-ids", "1", "2"])
