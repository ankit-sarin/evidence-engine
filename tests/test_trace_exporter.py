"""Tests for trace export module using the backup database with real data."""

import csv
import json
from pathlib import Path

import pytest

from engine.exporters.trace_exporter import (
    export_disagreement_pairs,
    export_trace_quality_report,
    export_traces_markdown,
)

BACKUP_DB = Path(__file__).resolve().parent.parent / "data" / "surgical_autonomy" / "review_backup_v1_schema.db"

# Skip all tests if backup DB doesn't exist
pytestmark = pytest.mark.skipif(
    not BACKUP_DB.exists(),
    reason="Backup database not available",
)


# ── Trace Quality Report ────────────────────────────────────────────


class TestTraceQualityReport:
    def test_generates_json_with_all_keys(self, tmp_path):
        out = str(tmp_path / "report.json")
        result = export_trace_quality_report(str(BACKUP_DB), out)

        assert Path(out).exists()

        expected_keys = {
            "generated_at", "corpus_size", "trace_stats",
            "length_distribution", "per_field_stats",
            "per_tier_stats", "flagged_papers",
        }
        assert set(result.keys()) == expected_keys

        # Verify JSON matches returned dict
        with open(out) as f:
            from_file = json.load(f)
        assert from_file["corpus_size"] == result["corpus_size"]

    def test_generates_companion_markdown(self, tmp_path):
        out = str(tmp_path / "report.json")
        export_trace_quality_report(str(BACKUP_DB), out)

        md_path = tmp_path / "report.md"
        assert md_path.exists()

        content = md_path.read_text()
        assert "# Reasoning Trace Quality Report" in content
        assert "## Summary" in content
        assert "## Trace Length Distribution" in content
        assert "## Per-Tier Verification Rates" in content
        assert "## Per-Field Verification Rates" in content

    def test_trace_stats_are_positive(self, tmp_path):
        out = str(tmp_path / "report.json")
        result = export_trace_quality_report(str(BACKUP_DB), out)

        ts = result["trace_stats"]
        assert ts["total_traces"] > 0
        assert ts["min_chars"] > 0
        assert ts["max_chars"] > 0
        assert ts["mean_chars"] > 0
        assert ts["median_chars"] > 0
        assert ts["std_chars"] >= 0

    def test_length_distribution_sums_to_corpus(self, tmp_path):
        out = str(tmp_path / "report.json")
        result = export_trace_quality_report(str(BACKUP_DB), out)

        dist_total = sum(result["length_distribution"].values())
        assert dist_total == result["corpus_size"]

    def test_per_field_stats_has_entries(self, tmp_path):
        out = str(tmp_path / "report.json")
        result = export_trace_quality_report(str(BACKUP_DB), out)

        # Backup DB has 17 fields from old schema
        assert len(result["per_field_stats"]) > 0

        # Each field entry has required keys
        for fname, fs in result["per_field_stats"].items():
            assert "total_spans" in fs
            assert "verified" in fs
            assert "flagged" in fs
            assert "verification_rate" in fs
            assert "mean_confidence" in fs
            assert "tier" in fs
            assert 0 <= fs["verification_rate"] <= 1

    def test_flagged_papers_is_list(self, tmp_path):
        out = str(tmp_path / "report.json")
        result = export_trace_quality_report(str(BACKUP_DB), out)

        assert isinstance(result["flagged_papers"], list)
        for fp in result["flagged_papers"]:
            assert "paper_id" in fp
            assert "trace_length" in fp
            assert fp["trace_length"] < 500


# ── Per-Paper Markdown Traces ────────────────────────────────────────


class TestTracesMarkdown:
    def test_creates_output_directory(self, tmp_path):
        out_dir = str(tmp_path / "traces")
        export_traces_markdown(str(BACKUP_DB), out_dir)
        assert Path(out_dir).is_dir()

    def test_creates_one_file_per_extraction(self, tmp_path):
        out_dir = str(tmp_path / "traces")
        paths = export_traces_markdown(str(BACKUP_DB), out_dir)

        # Should have files
        assert len(paths) > 0

        # Count extractions in DB for comparison
        import sqlite3
        conn = sqlite3.connect(str(BACKUP_DB))
        count = conn.execute("SELECT COUNT(DISTINCT paper_id) FROM extractions").fetchone()[0]
        conn.close()

        assert len(paths) == count

    def test_markdown_contains_required_sections(self, tmp_path):
        out_dir = str(tmp_path / "traces")
        paths = export_traces_markdown(str(BACKUP_DB), out_dir)

        # Check first file
        content = Path(paths[0]).read_text()
        assert "# Reasoning Trace" in content
        assert "# Structured Extraction" in content
        assert "# Evidence Spans (Detail)" in content
        assert "---" in content  # frontmatter

    def test_files_are_markdown(self, tmp_path):
        out_dir = str(tmp_path / "traces")
        paths = export_traces_markdown(str(BACKUP_DB), out_dir)

        for p in paths:
            assert p.endswith(".md")


# ── Disagreement Pairs ──────────────────────────────────────────────


class TestDisagreementPairs:
    def test_template_csv_when_no_human_data(self, tmp_path):
        out = str(tmp_path / "template.csv")
        result = export_disagreement_pairs(str(BACKUP_DB), None, out)

        assert result == out
        assert Path(out).exists()

        with open(out, newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)

        # Header + comment row
        assert len(rows) == 2
        assert rows[0] == ["paper_id", "field_name", "human_value"]

    def test_template_csv_when_path_missing(self, tmp_path):
        out = str(tmp_path / "template.csv")
        result = export_disagreement_pairs(
            str(BACKUP_DB), str(tmp_path / "nonexistent.csv"), out
        )

        assert result == out
        assert Path(out).exists()

    def test_template_has_correct_columns(self, tmp_path):
        out = str(tmp_path / "template.csv")
        export_disagreement_pairs(str(BACKUP_DB), None, out)

        with open(out, newline="") as f:
            reader = csv.reader(f)
            header = next(reader)

        assert header == ["paper_id", "field_name", "human_value"]
