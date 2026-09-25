"""B9 (R167, 9c-C7): the distribution-collapse check on the local extract stage.

`run_pipeline._stage_extract` calls `run_post_extraction_check` at its end — in
both branches — non-strict and non-raising, with the arm's population rule, and
records the result as a run event (`engine/core/run_telemetry.py`). A COLLAPSED
result is logged and recorded; it never aborts the run, never becomes a paper
outcome and does not close the manifest.

The run's manifest check, its selection and `run_extraction` are stubbed: the
subject is the wiring, not extraction. No model is called; the monitor reads the
scratch database through the reader, on a mode=ro connection.
"""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.core.run_telemetry import read_run_events
from _event_store_fixture import add_values, fixture_run

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")


def _db(tmp_path, spec, values):
    """A scratch review whose arm holds one study_type claim per value."""
    db = ReviewDatabase("wire", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    for pid in range(1, len(values) + 1):
        db._conn.execute("INSERT INTO papers (id, title, source, created_at, updated_at) "
                         "VALUES (?, 't', 's', 'n', 'n')", (pid,))
    db._conn.commit()
    add_values(db.db_path, spec.extraction_models.arm, "study_type", values)
    run_id = fixture_run(db._conn)
    return db, run_id


def _stub_stage(monkeypatch, rp, stats, *, selected=True):
    monkeypatch.setattr(rp, "verify_extraction_run", lambda *a, **k: "a" * 64)
    monkeypatch.setattr(rp, "select_for_extraction", lambda conn, *, arm: SimpleNamespace(
        arm=arm, to_extract=((1, None),) if selected else (),
        skipped_asserted=(), skipped_refused=()))
    monkeypatch.setattr(rp, "run_extraction", lambda *a, **k: dict(stats))


def _spy(monkeypatch, rp):
    calls = []
    real = rp.run_post_extraction_check

    def spy(*args, **kwargs):
        calls.append((args, kwargs))
        return real(*args, **kwargs)
    monkeypatch.setattr(rp, "run_post_extraction_check", spy)
    return calls


def _paper_event_count(db):
    return db._conn.execute("SELECT COUNT(*) FROM paper_events").fetchone()[0]


def _manifest_end(db, run_id):
    return tuple(db._conn.execute(
        "SELECT ended_at, end_status FROM run_manifests WHERE run_id = ?", (run_id,)).fetchone())


# ── T1 ────────────────────────────────────────────────────────────────
def test_t1_the_stage_calls_the_check_with_the_runs_arm_and_counts_and_skips_under_ten(
        tmp_path, spec, monkeypatch):
    import scripts.run_pipeline as rp
    db, run_id = _db(tmp_path, spec, ["Original Research"] * 3)
    try:
        _stub_stage(monkeypatch, rp, {"extracted": 3, "failed": 2, "total_spans": 60})
        calls = _spy(monkeypatch, rp)

        out = rp._stage_extract(db, spec, "wire", run_id=run_id)

        (args, kwargs), = calls
        review_dir = Path(db.db_path).parent
        assert args == (Path(db.db_path), "wire", spec.extraction_models.arm,
                        review_dir / "extraction_codebook.yaml")
        assert kwargs == {"extracted_count": 3, "failed_count": 2, "strict": False,
                          "raise_on_collapse": False, "skip_on_failures": False,
                          "min_population": "arm"}
        assert out["distribution_check"]["skipped"] is True
        assert out["distribution_check"]["arm_population"] == 3

        (row,), = [[r for r in read_run_events(review_dir)
                    if r["kind"] == "distribution_check"]]
        p = row["payload"]
        assert row["run_id"] == run_id
        assert (p["arm"], p["extracted_count"], p["failed_count"], p["arm_population"],
                p["strict"], p["skipped"]) == (spec.extraction_models.arm, 3, 2, 3, False, True)
        assert "only 3 eligible papers" in p["skip_reason"]
    finally:
        db.close()


def test_t1_the_nothing_selected_branch_writes_a_skip_row_too(tmp_path, spec, monkeypatch):
    import scripts.run_pipeline as rp
    db, run_id = _db(tmp_path, spec, ["Original Research"] * 2)
    try:
        _stub_stage(monkeypatch, rp, {}, selected=False)
        calls = _spy(monkeypatch, rp)

        out = rp._stage_extract(db, spec, "wire", run_id=run_id)

        (_, kwargs), = calls
        assert (kwargs["extracted_count"], kwargs["failed_count"]) == (0, 0)
        assert out["extracted"] == 0 and out["distribution_check"]["skipped"] is True
        rows = [r for r in read_run_events(Path(db.db_path).parent)
                if r["kind"] == "distribution_check"]
        assert len(rows) == 1 and rows[0]["payload"]["skipped"] is True
    finally:
        db.close()


# ── T2 ────────────────────────────────────────────────────────────────
def test_t2_a_collapse_is_recorded_and_logged_and_the_stage_returns(
        tmp_path, spec, monkeypatch, caplog):
    import scripts.run_pipeline as rp
    db, run_id = _db(tmp_path, spec, ["Original Research"] * 12)   # one value: COLLAPSED
    try:
        _stub_stage(monkeypatch, rp, {"extracted": 1, "failed": 1, "total_spans": 20})
        events_before = _paper_event_count(db)
        end_before = _manifest_end(db, run_id)

        with caplog.at_level("ERROR"):
            out = rp._stage_extract(db, spec, "wire", run_id=run_id)   # no exception

        check = out["distribution_check"]
        assert check["skipped"] is False and check["arm_population"] == 12
        assert "study_type" in check["collapsed_fields"]
        (row,), = [[r for r in read_run_events(Path(db.db_path).parent)
                    if r["kind"] == "distribution_check"]]
        p = row["payload"]
        assert p["collapsed"] >= 1 and "study_type" in p["collapsed_fields"]
        assert any(r["field_name"] == "study_type" and r["status"] == "COLLAPSED"
                   for r in p["results"])
        msgs = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
        assert any("DISTRIBUTION CHECK" in m and f"run {run_id}" in m
                   and spec.extraction_models.arm in m and "the run continues (R167)" in m
                   for m in msgs)
        assert _paper_event_count(db) == events_before                  # no paper outcome
        assert _manifest_end(db, run_id) == end_before == (None, None)  # manifest open
    finally:
        db.close()
