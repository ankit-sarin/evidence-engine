"""Tests for ProgressReporter."""

import io

from engine.utils.progress import ProgressReporter


def test_tracks_counts():
    """Completed, failed, and elapsed list are tracked correctly."""
    buf = io.StringIO()
    pr = ProgressReporter(5, "test-run", file=buf)

    pr.report(1, "EXTRACTED", 10.0)
    pr.report(2, "EXTRACTED", 12.0)
    pr.report(3, "FAILED", 5.0)
    pr.report(4, "EXTRACTED", 11.0)

    assert pr.completed == 4
    assert pr.failed == 1
    assert len(pr.elapsed_list) == 4
    assert pr.elapsed_list == [10.0, 12.0, 5.0, 11.0]

    # Verify progress lines were printed
    output = buf.getvalue()
    assert "[test-run]" in output
    assert "1/5" in output
    assert "4/5" in output
    assert "1 failed" in output


def test_eta_calculation():
    """ETA should be reasonable after several papers."""
    buf = io.StringIO()
    pr = ProgressReporter(100, "eta-test", file=buf)

    # Simulate 10 papers at 60s each
    for i in range(1, 11):
        pr.report(i, "EXTRACTED", 60.0)

    # After 10/100 papers at 60s avg, ETA should be ~90 remaining * 60s = 5400s = 1h30m
    lines = buf.getvalue().strip().split("\n")
    last_line = lines[-1]

    # The last report is 10/100 → 90 remaining × 60s avg = 5400s = 1h30m
    assert "ETA ~1h30m" in last_line
    assert "10/100" in last_line


def test_summary_includes_all_fields():
    """Summary output has total, succeeded, failed, wall time, mean/median/max."""
    buf = io.StringIO()
    pr = ProgressReporter(3, "summary-test", file=buf)

    pr.report(1, "EXTRACTED", 10.0)
    pr.report(2, "FAILED", 5.0)
    pr.report(3, "EXTRACTED", 20.0)

    result = pr.summary()

    assert "Total papers : 3" in result
    assert "Succeeded    : 2" in result
    assert "Failed       : 1" in result
    assert "Wall time" in result
    assert "Mean/paper" in result
    assert "Median/paper" in result
    assert "Max/paper" in result
    assert "COMPLETE" in result
