"""One predicate, two programs: `engine/core/paper_state.py` and migration 019.

R35 forbids a migration from 018 onward importing anything from `engine/` that
can change — editing a constant must never change what an already-applied
migration meant. So 019 re-declares every token list. A re-declaration is a copy,
and a copy diverges; this file is what stops it doing so silently.

The demonstration is by CONSTRUCTION, not by assertion of equivalent behaviour:
the tokens the reader believes in are read back out of the CHECK constraint the
migration actually wrote into a database, so a list that agreed in the module and
disagreed in the DDL would still fail here.
"""

from __future__ import annotations

import importlib
import sqlite3

import pytest

m019 = importlib.import_module("engine.migrations.019_paper_state_axes")

from engine.core import paper_state
from tests.test_migration_019_paper_axes import _pre_019_db


@pytest.mark.parametrize("name", [
    "ELIGIBILITY_STATES", "PROCESSING_STATES", "FAILURE_STATES",
    "COMPLETED_PROCESSING_STATES", "EVENT_TYPE_AXIS",
])
def test_the_reader_and_the_migration_declare_the_same_vocabulary(name):
    assert getattr(paper_state, name) == getattr(m019, name), (
        f"{name} has diverged between engine/core/paper_state.py and "
        f"engine/migrations/019_paper_state_axes.py. The migration is frozen "
        f"(R35) — if the vocabulary must change, it changes in a NEW migration."
    )


def test_the_two_axes_are_disjoint():
    """The axis is a function of the token only because these do not overlap."""
    assert not set(paper_state.ELIGIBILITY_STATES) & set(paper_state.PROCESSING_STATES)


def test_every_failure_token_is_a_processing_token():
    assert set(paper_state.FAILURE_STATES) <= set(paper_state.PROCESSING_STATES)


def test_no_completed_token_is_a_failure_token():
    assert not (set(paper_state.COMPLETED_PROCESSING_STATES)
                & set(paper_state.FAILURE_STATES))


def test_every_paper_event_type_is_mapped_to_an_axis():
    """Step 2's contract: PAPER_EVENT_TYPES mapped to an axis, all of them."""
    assert set(paper_state.EVENT_TYPE_AXIS) == set(m019.PAPER_EVENT_TYPES)
    assert set(paper_state.EVENT_TYPE_AXIS.values()) <= {
        "eligibility", "processing", "both"}


def test_the_tokens_in_the_written_ddl_are_exactly_the_readers(tmp_path):
    """Read the vocabulary back out of a real CHECK, not out of the source."""
    path = _pre_019_db(tmp_path)
    m019.run_migration(str(path))
    conn = sqlite3.connect(path)
    try:
        ddl = conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='paper_events'").fetchone()[0]
    finally:
        conn.close()

    every = set(paper_state.ELIGIBILITY_STATES) | set(paper_state.PROCESSING_STATES)
    for token in every:
        assert f"'{token}'" in ddl, f"{token} is in the reader but not in the DDL"
    # and nothing the reader does not know about is in the to_state CHECK
    to_state_check = ddl.split("to_state    TEXT    NOT NULL CHECK (to_state IN (")[1]
    to_state_check = to_state_check.split("))")[0]
    in_ddl = {p.strip().strip("'") for p in to_state_check.split(",")}
    assert in_ddl == every


def test_axis_of_and_is_failure_agree_with_the_sets():
    for t in paper_state.ELIGIBILITY_STATES:
        assert paper_state.axis_of(t) == "eligibility"
    for t in paper_state.PROCESSING_STATES:
        assert paper_state.axis_of(t) == "processing"
    assert paper_state.axis_of("not_a_token") is None
    for t in paper_state.FAILURE_STATES:
        assert paper_state.is_failure(t)
    assert not paper_state.is_failure("extracted")
