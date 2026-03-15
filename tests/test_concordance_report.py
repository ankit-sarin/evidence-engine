"""Tests for concordance report generation."""

import csv
from pathlib import Path

import pytest

from engine.analysis.concordance import ConcordanceReport, Disagreement
from engine.analysis.metrics import FieldSummary
from engine.analysis.report import print_summary, write_report


def _make_report(
    arm_a="local",
    arm_b="openai_o4_mini_high",
    n_papers=5,
    fields=None,
    disagreements=None,
) -> ConcordanceReport:
    """Build a mock ConcordanceReport."""
    if fields is None:
        fields = {
            "autonomy_level": FieldSummary(
                field_name="autonomy_level", kappa=0.85,
                percent_agreement=0.90, n=5, n_match=4,
                n_mismatch=1, n_ambiguous=0, ci_lower=0.60, ci_upper=1.00,
            ),
            "task_monitor": FieldSummary(
                field_name="task_monitor", kappa=1.0,
                percent_agreement=1.0, n=5, n_match=5,
                n_mismatch=0, n_ambiguous=0, ci_lower=1.0, ci_upper=1.0,
            ),
            "robot_platform": FieldSummary(
                field_name="robot_platform", kappa=0.50,
                percent_agreement=0.60, n=5, n_match=3,
                n_mismatch=2, n_ambiguous=1, ci_lower=0.10, ci_upper=0.90,
            ),
        }
    if disagreements is None:
        disagreements = [
            Disagreement(
                paper_id=42, field_name="autonomy_level",
                value_a="2", value_b="3 (Conditional autonomy)",
                result="MISMATCH", detail="'2 (Task autonomy)' vs '3 (Conditional autonomy)'",
            ),
            Disagreement(
                paper_id=99, field_name="robot_platform",
                value_a="da Vinci Xi", value_b="da Vinci Si",
                result="AMBIGUOUS", detail="token Jaccard=0.67",
            ),
        ]
    return ConcordanceReport(
        arm_a=arm_a, arm_b=arm_b, n_papers=n_papers,
        n_papers_a_only=2, n_papers_b_only=1,
        field_summaries=fields, disagreements=disagreements,
    )


class TestPrintSummary:
    def test_empty_reports(self, capsys):
        print_summary([])
        captured = capsys.readouterr()
        assert "No concordance reports" in captured.out

    def test_single_report(self, capsys):
        report = _make_report()
        print_summary([report])
        captured = capsys.readouterr()
        assert "autonomy_level" in captured.out
        assert "task_monitor" in captured.out
        assert "robot_platform" in captured.out

    def test_multiple_reports(self, capsys):
        r1 = _make_report(arm_a="local", arm_b="openai_o4_mini_high")
        r2 = _make_report(arm_a="local", arm_b="anthropic_sonnet_4_6")
        print_summary([r1, r2])
        captured = capsys.readouterr()
        assert "Local v o4-mini" in captured.out
        assert "Local v Sonnet" in captured.out

    def test_truncation_message(self, capsys):
        reports = [_make_report(arm_b=f"arm_{i}") for i in range(5)]
        print_summary(reports)
        captured = capsys.readouterr()
        assert "omitted" in captured.out


class TestWriteReport:
    def test_creates_all_files(self, tmp_path):
        report = _make_report()
        write_report([report], tmp_path)
        assert (tmp_path / "concordance_summary.csv").exists()
        assert (tmp_path / "disagreements.csv").exists()
        assert (tmp_path / "concordance_report.html").exists()

    def test_summary_csv_parseable(self, tmp_path):
        report = _make_report()
        write_report([report], tmp_path)
        with open(tmp_path / "concordance_summary.csv") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 3  # 3 fields
        field_names = {r["field_name"] for r in rows}
        assert "autonomy_level" in field_names
        assert all(r["arm_a"] == "local" for r in rows)

    def test_disagreements_csv_parseable(self, tmp_path):
        report = _make_report()
        write_report([report], tmp_path)
        with open(tmp_path / "disagreements.csv") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 2
        scores = {r["score"] for r in rows}
        assert "MISMATCH" in scores
        assert "AMBIGUOUS" in scores

    def test_html_contains_kappa_colors(self, tmp_path):
        report = _make_report()
        write_report([report], tmp_path)
        html = (tmp_path / "concordance_report.html").read_text()
        assert "k-good" in html  # kappa 0.85 and 1.0
        assert "k-poor" in html  # kappa 0.50
        assert "Concordance" in html
