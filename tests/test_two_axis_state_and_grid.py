"""The two-axis reader, the arm registry lister, and the grid enumerator.

R29 as corrected by R39, plus R28's "the verdict is a feature, not a filter".

Every history here is CONSTRUCTED. On live, 11,400 cells return `missing` at rule
row 1 and all 190 papers read `eligible` / `no_recorded_state`, so a gate
evaluated there could not fail for any reason these tests are about (READERS-01
contradiction 7, standing constraint).
"""

from __future__ import annotations

import importlib
import sqlite3

import pytest

from engine.core import events, paper_state
from engine.core.effective import (
    NO_RECORDED_STATE, EffectiveState, effective_state, eligible_paper_ids,
    grid_cells, iter_grid, registered_arms,
)

m016 = importlib.import_module("engine.migrations.016_event_store")
m019 = importlib.import_module("engine.migrations.019_paper_state_axes")


# ── fixture: an event store at the post-019 shape ────────────────────

@pytest.fixture
def db(tmp_path):
    conn = sqlite3.connect(tmp_path / "t.db")
    conn.executescript(
        """
        CREATE TABLE papers (
            id INTEGER PRIMARY KEY, title TEXT NOT NULL, source TEXT NOT NULL,
            status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE evidence_spans (id INTEGER PRIMARY KEY);
        """
    )
    m016.create_schema(conn)
    for pid in range(1, 8):
        conn.execute(
            "INSERT INTO papers (id,title,source,status,created_at,updated_at) "
            "VALUES (?,'t','manual','AI_AUDIT_COMPLETE','x','x')", (pid,))
    conn.commit()
    conn.close()
    m019.run_migration(str(tmp_path / "t.db"))
    conn = sqlite3.connect(tmp_path / "t.db")
    conn.execute("PRAGMA foreign_keys = ON")
    yield conn
    conn.close()


def _paper_event(conn, paper_id, event_type, to_state, reason=None):
    events.write_paper_event(
        conn, event_type=event_type, paper_id=paper_id, to_state=to_state,
        actor_kind="engine", actor_role="system", actor_name="test",
        reason_code=reason)


# ── T3: every eligibility x processing combination in R39 ────────────

def test_no_events_reads_no_recorded_state_on_both_axes(db):
    s = effective_state(db, 1)
    assert (s.eligibility, s.processing) == (NO_RECORDED_STATE, NO_RECORDED_STATE)
    assert s.processing_reason is None
    assert not s.analysis_ready and not s.in_corpus


@pytest.mark.parametrize("token", paper_state.ELIGIBILITY_STATES)
def test_each_eligibility_token_is_read_back_on_its_own_axis(db, token):
    etype = {"eligible": "adjudicated", "abstract_out": "screened",
             "full_text_out": "screened"}[token]
    _paper_event(db, 1, etype, token)
    s = effective_state(db, 1)
    assert s.eligibility == token
    assert s.processing == NO_RECORDED_STATE
    assert s.in_corpus == (token == "eligible")


@pytest.mark.parametrize("token,etype", [
    ("parsed", "parsed"), ("extracted", "extracted"), ("audited_ai", "audited"),
])
def test_each_non_failure_processing_token_reads_back_with_no_reason(db, token, etype):
    _paper_event(db, 1, "adjudicated", "eligible")
    _paper_event(db, 1, etype, token)
    s = effective_state(db, 1)
    assert s.eligibility == "eligible"
    assert s.processing == token
    assert s.processing_reason is None
    assert s.analysis_ready == (token in paper_state.COMPLETED_PROCESSING_STATES)


@pytest.mark.parametrize("token", paper_state.FAILURE_STATES)
def test_each_failure_token_reads_back_with_its_reason_and_stays_eligible(db, token):
    """G3's shape, for every failure token R39 declares."""
    _paper_event(db, 1, "adjudicated", "eligible")
    etype = {"full_text_not_obtainable": "not_obtainable"}.get(token, "extraction_failed")
    _paper_event(db, 1, etype, token, reason=f"{token} detail")
    s = effective_state(db, 1)
    assert s.eligibility == "eligible"
    assert s.in_corpus is True, "a processing failure must not remove a paper (A9)"
    assert s.processing == token
    assert s.processing_reason == f"{token} detail"
    assert s.analysis_ready is False


def test_a_processing_event_does_not_erase_the_eligibility_fact(db):
    """The defect the single `state` field had: last row wins, one axis only."""
    _paper_event(db, 1, "adjudicated", "eligible")
    _paper_event(db, 1, "parsed", "parsed")
    _paper_event(db, 1, "extraction_failed", "extraction_failed",
                 reason="extraction failed after retries")
    s = effective_state(db, 1)
    assert s.eligibility == "eligible"
    assert s.processing == "extraction_failed"


def test_an_eligibility_event_does_not_erase_the_processing_fact(db):
    _paper_event(db, 1, "parsed", "parsed")
    _paper_event(db, 1, "adjudicated", "eligible")
    s = effective_state(db, 1)
    assert s.processing == "parsed"
    assert s.eligibility == "eligible"


def test_the_latest_event_on_each_axis_wins_independently(db):
    _paper_event(db, 1, "screened", "abstract_out")
    _paper_event(db, 1, "parsed", "parsed")
    _paper_event(db, 1, "adjudicated", "eligible")     # overturns the exclusion
    _paper_event(db, 1, "extracted", "extracted")
    s = effective_state(db, 1)
    assert (s.eligibility, s.processing) == ("eligible", "extracted")
    assert s.analysis_ready is True


def test_the_reader_never_reads_papers_status(db):
    """Every paper here is AI_AUDIT_COMPLETE and none of them is in the corpus."""
    assert db.execute("SELECT status FROM papers WHERE id=1").fetchone()[0] \
        == "AI_AUDIT_COMPLETE"
    assert effective_state(db, 1).eligibility == NO_RECORDED_STATE
    assert eligible_paper_ids(db) == ()


def test_analysis_ready_is_derived_and_stored_nowhere(db):
    """R39. It is a property of the returned object, not a column."""
    assert "analysis_ready" not in {
        r[1] for r in db.execute("PRAGMA table_info(paper_events)")}
    assert isinstance(EffectiveState("eligible", "extracted").analysis_ready, bool)


# ── the corpus predicate ─────────────────────────────────────────────

def test_eligible_paper_ids_is_the_eligibility_axis_only(db):
    _paper_event(db, 1, "adjudicated", "eligible")
    _paper_event(db, 2, "adjudicated", "eligible")
    _paper_event(db, 2, "extraction_failed", "extraction_failed", reason="r")
    _paper_event(db, 3, "screened", "abstract_out")
    assert eligible_paper_ids(db) == (1, 2), (
        "paper 2's extraction failed and it is still in the corpus (A9)")


# ── G6 / A12: the registry is the one routing predicate ──────────────

def test_registered_arms_is_ordered_and_excludes_retired_by_default(db):
    events.register_arm(db, "local", "model")
    events.register_arm(db, "human_A", "human_extractor")
    events.register_arm(db, "openai_o4_mini_high", "model")
    db.commit()
    assert registered_arms(db) == ("human_A", "local", "openai_o4_mini_high")
    assert registered_arms(db, kind="model") == ("local", "openai_o4_mini_high")
    assert registered_arms(db, kind="human_extractor") == ("human_A",)

    events.retire_arm(db, "human_A")
    db.commit()
    assert registered_arms(db) == ("local", "openai_o4_mini_high")
    assert registered_arms(db, include_retired=True) == (
        "human_A", "local", "openai_o4_mini_high")


# ── T5 / F5: the grid, and the verdict as a feature not a filter ─────

class _CB:
    """A codebook stand-in: field names and sentinels are all the grid needs."""
    field_names = ("study_type", "sample_size", "autonomy_level", "country")
    absence_sentinels = ("NR", "NOT_FOUND")


def test_the_grid_is_papers_times_fields_times_registered_arms(db):
    for pid in (1, 2, 3):
        _paper_event(db, pid, "adjudicated", "eligible")
    events.register_arm(db, "local", "model")
    events.register_arm(db, "openai_o4_mini_high", "model")
    db.commit()

    cells = grid_cells(db, codebook=_CB)
    papers = eligible_paper_ids(db)
    arms = registered_arms(db)
    # a DERIVATION, never a literal (a count is a measurement of a day)
    assert len(cells) == len(papers) * len(_CB.field_names) * len(arms)
    assert len(set(cells)) == len(cells)
    assert {c[0] for c in cells} == set(papers)
    assert {c[2] for c in cells} == set(arms)


def test_the_grid_uses_the_live_codebooks_twenty_fields(db):
    """T5: read through `engine/core/codebook.py`, the one loader (R26)."""
    from engine.core.codebook import load_codebook
    cb = load_codebook("data/surgical_autonomy/extraction_codebook.yaml")
    assert len(cb.field_names) == 20
    _paper_event(db, 1, "adjudicated", "eligible")
    events.register_arm(db, "local", "model")
    db.commit()
    assert len(grid_cells(db, codebook=cb)) == 1 * 20 * 1


def test_iter_grid_yields_every_cell_including_the_empty_ones(db):
    """F5 / R28: a cell with no claim is still a cell. B3 is the defect where it
    was not — the judge's universe was the scorer's disagreement set, so 1,535 of
    3,802 cells were never judged, one-directionally."""
    _paper_event(db, 1, "adjudicated", "eligible")
    _paper_event(db, 2, "adjudicated", "eligible")
    events.register_arm(db, "local", "model")
    events.register_arm(db, "openai_o4_mini_high", "model")
    db.commit()

    out = list(iter_grid(db, codebook=_CB))
    assert len(out) == len(grid_cells(db, codebook=_CB))
    assert {ev.state for _, _, _, ev in out} == {"missing"}
    assert {ev.rule_row for _, _, _, ev in out} == {1}


def test_a_populated_cell_and_an_empty_one_are_both_yielded(db):
    _paper_event(db, 1, "adjudicated", "eligible")
    events.register_arm(db, "local", "model")
    db.commit()
    uid = events.mint_extraction_uid()
    events.write_field_event(
        db, event_type="asserted", paper_id=1, field_name="study_type",
        arm="local", value="RCT", extraction_uid=uid,
        actor_kind="model", actor_role="extractor", actor_name="deepseek-r1:32b")
    db.commit()

    by_field = {f: ev for _, f, _, ev in iter_grid(db, codebook=_CB)}
    assert by_field["study_type"].value == "RCT"
    assert by_field["sample_size"].state == "missing"
    assert len(by_field) == len(_CB.field_names)
