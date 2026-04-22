"""Tests for pass2_full: triple-universe query + abort guard logic.

Skips the run_pass2 Ollama-dependent path (covered by test_judge_pass2).
"""

from __future__ import annotations

import time

import pytest

from analysis.paper1 import pass2_full


# ── fetch_triples ─────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows: list[dict]):
        self._rows = rows
        self.calls: list[tuple[str, tuple]] = []

    def execute(self, sql: str, params: tuple):
        self.calls.append((sql, params))
        return _FakeCursor(self._rows)


class _FakeDb:
    def __init__(self, rows: list[dict]):
        self._conn = _FakeConn(rows)


class TestFetchTriples:
    def test_returns_candidates_sorted_by_int_paper_id(self):
        db = _FakeDb([
            {"paper_id": "719", "field_name": "autonomy_level",
             "field_type": "categorical", "pass1_fabrication_risk": "medium"},
            {"paper_id": "9", "field_name": "sample_size",
             "field_type": "numeric", "pass1_fabrication_risk": "medium"},
            {"paper_id": "39", "field_name": "autonomy_level",
             "field_type": "categorical", "pass1_fabrication_risk": "high"},
        ])
        out = pass2_full.fetch_triples(db, "pass1_xyz")
        pids = [c.paper_id for c in out]
        assert pids == ["9", "39", "719"]

    def test_secondary_sort_by_field_name(self):
        db = _FakeDb([
            {"paper_id": "9", "field_name": "sample_size",
             "field_type": "numeric", "pass1_fabrication_risk": "medium"},
            {"paper_id": "9", "field_name": "autonomy_level",
             "field_type": "categorical", "pass1_fabrication_risk": "medium"},
        ])
        out = pass2_full.fetch_triples(db, "r")
        assert [c.field_name for c in out] == ["autonomy_level", "sample_size"]

    def test_query_filters_to_medium_and_high(self):
        db = _FakeDb([])
        pass2_full.fetch_triples(db, "r")
        sql, params = db._conn.calls[0]
        assert "pass1_fabrication_risk IN ('medium', 'high')" in sql
        assert params == ("r",)

    def test_preserves_risk_and_field_type(self):
        db = _FakeDb([
            {"paper_id": "1", "field_name": "f",
             "field_type": "free_text", "pass1_fabrication_risk": "high"},
        ])
        c = pass2_full.fetch_triples(db, "r")[0]
        assert c.risk == "high"
        assert c.field_type == "free_text"


# ── abort guards ──────────────────────────────────────────────────


class TestAbortConditions:
    def test_passes_when_fail_rate_below_threshold(self):
        t_start = time.time() - 60.0
        # 1% failure rate
        pass2_full._check_abort_conditions(
            i=100, total=1200, failures=1, t_start=t_start
        )

    def test_aborts_when_fail_rate_above_five_percent(self):
        t_start = time.time() - 60.0
        with pytest.raises(pass2_full.AbortError) as exc:
            pass2_full._check_abort_conditions(
                i=100, total=1200, failures=6, t_start=t_start
            )
        assert "failure rate" in str(exc.value)

    def test_fail_rate_exactly_five_percent_passes(self):
        # Threshold is strict > 5%; 5.0% itself should pass.
        t_start = time.time() - 60.0
        pass2_full._check_abort_conditions(
            i=100, total=1200, failures=5, t_start=t_start
        )

    def test_aborts_when_projected_runtime_exceeds_thirty_hours(self):
        # 10 triples done in 10 minutes → 1 triple/min; 1,202 left = 20h
        # Scale up: 10 triples in 20 minutes (30s per triple) →
        # with 10,000 left at 30s/triple = 500,000s ≈ 139h
        t_start = time.time() - 20 * 60.0
        with pytest.raises(pass2_full.AbortError) as exc:
            pass2_full._check_abort_conditions(
                i=10, total=10_000, failures=0, t_start=t_start
            )
        assert "projected wall-clock" in str(exc.value)

    def test_normal_projection_passes(self):
        # 100 triples done in 100 minutes → 1/min; 1,112 remaining = ~18h
        t_start = time.time() - 100 * 60.0
        pass2_full._check_abort_conditions(
            i=100, total=1212, failures=0, t_start=t_start
        )


# ── _log_checkpoint ───────────────────────────────────────────────


class TestLogCheckpoint:
    def test_emits_info_log_with_running_counts(self, caplog):
        caplog.set_level("INFO", logger=pass2_full.logger.name)
        t_start = time.time() - 60.0
        pass2_full._log_checkpoint(
            i=100, total=1212, failures=2,
            running={"SUPPORTED": 200, "PARTIALLY_SUPPORTED": 50, "UNSUPPORTED": 50},
            t_start=t_start,
        )
        msg = caplog.records[-1].getMessage()
        assert "CHECKPOINT 100/1212" in msg
        assert "S=200" in msg
        assert "PS=50" in msg
        assert "U=50" in msg
        assert "failures=2" in msg


# ── _new_run_id ───────────────────────────────────────────────────


class TestNewRunId:
    def test_default_tag_is_full(self):
        rid = pass2_full._new_run_id("myreview")
        assert "myreview_pass2_full_" in rid

    def test_custom_tag(self):
        rid = pass2_full._new_run_id("myreview", tag="smoke10")
        assert "myreview_pass2_smoke10_" in rid

    def test_timestamp_is_iso_basic_z(self):
        rid = pass2_full._new_run_id("r")
        ts_part = rid.split("_")[-1]
        assert ts_part.endswith("Z")
        assert "T" in ts_part
