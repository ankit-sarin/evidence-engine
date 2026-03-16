"""Tests for metrics and concordance pipeline.

Uses mock data (in-memory dicts) — no real DB required.
"""

import logging
import math
import sqlite3

import pytest

from engine.analysis.concordance import Disagreement, align_arms, check_schema_parity, ConcordanceReport
from engine.analysis.metrics import (
    KappaResult,
    FieldSummary,
    cohens_kappa,
    field_summary,
    percent_agreement,
)
from engine.analysis.scoring import FieldScore, score_pair


# ── Metrics tests ────────────────────────────────────────────────────


class TestCohensKappa:
    def test_perfect_agreement(self):
        scores = [FieldScore("MATCH", "")] * 10
        kr = cohens_kappa(scores)
        assert kr.kappa == 1.0
        assert kr.n == 10
        assert kr.n_agree == 10
        assert kr.n_disagree == 0

    def test_no_agreement(self):
        scores = [FieldScore("MISMATCH", "")] * 10
        kr = cohens_kappa(scores)
        # With binary agree/disagree kappa and 100% disagree,
        # p_e = 0^2 + 1^2 = 1.0, so kappa is degenerate (0/0 → 0.0)
        assert kr.kappa == 0.0
        assert kr.n_disagree == 10

    def test_mixed_agreement(self):
        scores = [FieldScore("MATCH", "")] * 7 + [FieldScore("MISMATCH", "")] * 3
        kr = cohens_kappa(scores)
        assert 0 < kr.kappa < 1.0
        assert kr.n == 10
        assert kr.n_agree == 7
        assert kr.n_disagree == 3

    def test_ambiguous_excluded_from_n(self):
        scores = [
            FieldScore("MATCH", ""),
            FieldScore("MATCH", ""),
            FieldScore("AMBIGUOUS", ""),
            FieldScore("MISMATCH", ""),
        ]
        kr = cohens_kappa(scores)
        assert kr.n == 3  # AMBIGUOUS excluded
        assert kr.n_ambiguous == 1
        assert kr.n_agree == 2
        assert kr.n_disagree == 1

    def test_empty_scores(self):
        kr = cohens_kappa([])
        assert math.isnan(kr.kappa)
        assert kr.n == 0

    def test_all_ambiguous(self):
        scores = [FieldScore("AMBIGUOUS", "")] * 5
        kr = cohens_kappa(scores)
        assert math.isnan(kr.kappa)
        assert kr.n == 0
        assert kr.n_ambiguous == 5

    def test_ci_bounds(self):
        scores = [FieldScore("MATCH", "")] * 8 + [FieldScore("MISMATCH", "")] * 2
        kr = cohens_kappa(scores)
        assert kr.ci_lower <= kr.kappa <= kr.ci_upper


class TestPercentAgreement:
    def test_perfect(self):
        scores = [FieldScore("MATCH", "")] * 5
        assert percent_agreement(scores) == 1.0

    def test_half(self):
        scores = [FieldScore("MATCH", "")] * 5 + [FieldScore("MISMATCH", "")] * 5
        assert percent_agreement(scores) == 0.5

    def test_ambiguous_excluded(self):
        scores = [
            FieldScore("MATCH", ""),
            FieldScore("MISMATCH", ""),
            FieldScore("AMBIGUOUS", ""),
        ]
        assert percent_agreement(scores) == 0.5  # 1/2, AMBIGUOUS excluded

    def test_empty(self):
        assert math.isnan(percent_agreement([]))


class TestFieldSummary:
    def test_combines_metrics(self):
        scores = [FieldScore("MATCH", "")] * 3 + [FieldScore("MISMATCH", "")] * 1
        fs = field_summary("autonomy_level", scores)
        assert fs.field_name == "autonomy_level"
        assert fs.n == 4
        assert fs.n_match == 3
        assert fs.n_mismatch == 1
        assert fs.n_ambiguous == 0
        assert fs.percent_agreement == 0.75


# ── Alignment tests ──────────────────────────────────────────────────


class TestAlignArms:
    def test_shared_papers(self):
        arm_a = {1: {"f1": "a", "f2": "b"}, 2: {"f1": "c"}}
        arm_b = {1: {"f1": "x", "f2": "y"}, 2: {"f1": "z"}}
        aligned, a_only, b_only = align_arms(arm_a, arm_b)
        assert len(aligned) == 3  # paper 1: f1,f2 + paper 2: f1
        assert a_only == set()
        assert b_only == set()

    def test_papers_only_in_a(self):
        arm_a = {1: {"f1": "a"}, 2: {"f1": "b"}, 3: {"f1": "c"}}
        arm_b = {1: {"f1": "x"}}
        aligned, a_only, b_only = align_arms(arm_a, arm_b)
        assert a_only == {2, 3}
        assert b_only == set()
        # Only paper 1 aligned
        assert all(pid == 1 for pid, _, _, _ in aligned)

    def test_papers_only_in_b(self):
        arm_a = {1: {"f1": "a"}}
        arm_b = {1: {"f1": "x"}, 5: {"f1": "y"}}
        aligned, a_only, b_only = align_arms(arm_a, arm_b)
        assert a_only == set()
        assert b_only == {5}

    def test_no_overlap(self):
        arm_a = {1: {"f1": "a"}}
        arm_b = {2: {"f1": "x"}}
        aligned, a_only, b_only = align_arms(arm_a, arm_b)
        assert len(aligned) == 0
        assert a_only == {1}
        assert b_only == {2}

    def test_field_missing_in_one_arm(self):
        """Paper exists in both arms but field only in one."""
        arm_a = {1: {"f1": "a", "f2": "b"}}
        arm_b = {1: {"f1": "x"}}
        aligned, _, _ = align_arms(arm_a, arm_b)
        assert len(aligned) == 2  # f1 + f2
        # f2 should have None for arm_b
        f2_pair = [t for t in aligned if t[1] == "f2"][0]
        assert f2_pair == (1, "f2", "b", None)

    def test_empty_arms(self):
        aligned, a_only, b_only = align_arms({}, {})
        assert len(aligned) == 0


# ── Full pipeline tests (mock data, no DB) ───────────────────────────


class TestPipelineWithMockData:
    """Test the full score → metrics pipeline with known data."""

    def _build_scores(self, arm_a_data, arm_b_data, field_name):
        """Helper: align two single-field arms and score."""
        # Wrap in arm format: {paper_id: {field_name: value}}
        a = {pid: {field_name: val} for pid, val in arm_a_data.items()}
        b = {pid: {field_name: val} for pid, val in arm_b_data.items()}
        aligned, _, _ = align_arms(a, b)
        return [score_pair(fn, va, vb) for _, fn, va, vb in aligned]

    def test_all_match_categorical(self):
        a = {1: "H", 2: "R", 3: "Shared"}
        b = {1: "H", 2: "R", 3: "Shared"}
        scores = self._build_scores(a, b, "task_monitor")
        kr = cohens_kappa(scores)
        assert kr.kappa == 1.0
        assert kr.n == 3

    def test_all_mismatch_categorical(self):
        a = {1: "H", 2: "R", 3: "Shared"}
        b = {1: "R", 2: "Shared", 3: "H"}
        scores = self._build_scores(a, b, "task_monitor")
        kr = cohens_kappa(scores)
        assert kr.kappa == 0.0  # degenerate: p_e = 1.0

    def test_nr_handling_both_absent(self):
        """NR vs NOT_FOUND → MATCH across pipeline."""
        a = {1: "NR", 2: "NOT_FOUND"}
        b = {1: None, 2: "NR"}
        scores = self._build_scores(a, b, "country")
        assert all(s.result == "MATCH" for s in scores)

    def test_mixed_results_with_normalization(self):
        """autonomy_level: bare integers normalize to match full values."""
        a = {1: "2", 2: "3 (Conditional autonomy)", 3: "4"}
        b = {1: "2 (Task autonomy)", 2: "3 (Conditional autonomy)", 3: "2 (Task autonomy)"}
        scores = self._build_scores(a, b, "autonomy_level")
        # Paper 1: "2" → "2 (Task autonomy)" = MATCH
        # Paper 2: exact match = MATCH
        # Paper 3: "4 (High autonomy)" vs "2 (Task autonomy)" = MISMATCH
        assert scores[0].result == "MATCH"
        assert scores[1].result == "MATCH"
        assert scores[2].result == "MISMATCH"

    def test_multi_value_jaccard_through_pipeline(self):
        """Multi-value sets flow through alignment → scoring → metrics."""
        a = {
            1: "In vivo (animal); Phantom/Simulation",
            2: "Ex vivo",
            3: "Phantom/Simulation",
        }
        b = {
            1: "Phantom/Simulation; In vivo (animal)",  # same set, different order
            2: "Ex vivo",
            3: "In vivo (animal); Phantom/Simulation",  # superset
        }
        scores = self._build_scores(a, b, "validation_setting")
        assert scores[0].result == "MATCH"  # identical sets
        assert scores[1].result == "MATCH"  # exact single value
        assert scores[2].result == "AMBIGUOUS"  # subset

        kr = cohens_kappa(scores)
        assert kr.n == 2  # 2 decisive (AMBIGUOUS excluded)
        assert kr.n_ambiguous == 1

    def test_free_text_substring_through_pipeline(self):
        a = {1: "da Vinci Xi", 2: "KUKA iiwa"}
        b = {1: "da Vinci Xi (Intuitive Surgical)", 2: "Raven II"}
        scores = self._build_scores(a, b, "robot_platform")
        assert scores[0].result == "MATCH"
        assert scores[1].result == "MISMATCH"

    def test_sample_size_numeric_normalization(self):
        a = {1: "10", 2: "n=42", 3: "100 patients"}
        b = {1: "10", 2: "42", 3: "100"}
        scores = self._build_scores(a, b, "sample_size")
        assert all(s.result == "MATCH" for s in scores)

    def test_field_summary_with_pipeline(self):
        a = {1: "2", 2: "3", 3: "Mixed/Multiple", 4: "NR"}
        b = {1: "2 (Task autonomy)", 2: "4 (High autonomy)", 3: "Mixed/Multiple", 4: None}
        scores = self._build_scores(a, b, "autonomy_level")
        fs = field_summary("autonomy_level", scores)
        assert fs.n_match == 3  # papers 1, 3, 4
        assert fs.n_mismatch == 1  # paper 2


# ── Disagreement tracking ───────────────────────────────────────────


class TestDisagreementTracking:
    def test_disagreements_collected(self):
        """Verify disagreements are properly identified in the pipeline."""
        arm_a = {
            1: {"autonomy_level": "2", "task_monitor": "H"},
            2: {"autonomy_level": "3", "task_monitor": "R"},
        }
        arm_b = {
            1: {"autonomy_level": "2 (Task autonomy)", "task_monitor": "R"},  # monitor mismatch
            2: {"autonomy_level": "4 (High autonomy)", "task_monitor": "R"},  # level mismatch
        }
        aligned, _, _ = align_arms(arm_a, arm_b)
        disagreements = []
        for pid, fname, va, vb in aligned:
            fs = score_pair(fname, va, vb)
            if fs.result != "MATCH":
                disagreements.append(Disagreement(
                    paper_id=pid, field_name=fname,
                    value_a=va, value_b=vb,
                    result=fs.result, detail=fs.detail,
                ))
        # autonomy_level paper 1: MATCH (normalization)
        # task_monitor paper 1: H vs R → MISMATCH
        # autonomy_level paper 2: 3→"3 (Conditional)" vs "4 (High)" → MISMATCH
        # task_monitor paper 2: R vs R → MATCH
        assert len(disagreements) == 2
        field_names = {d.field_name for d in disagreements}
        assert "task_monitor" in field_names
        assert "autonomy_level" in field_names


# ── Schema parity check ────────────────────────────────────────────


class TestCheckSchemaParity:

    def _make_db(self, tmp_path):
        """Create a minimal DB with extractions + cloud_extractions tables."""
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        conn.executescript("""
            CREATE TABLE papers (
                id INTEGER PRIMARY KEY,
                title TEXT, source TEXT, status TEXT,
                created_at TEXT, updated_at TEXT
            );
            CREATE TABLE extractions (
                id INTEGER PRIMARY KEY,
                paper_id INTEGER,
                extraction_schema_hash TEXT
            );
            CREATE TABLE cloud_extractions (
                id INTEGER PRIMARY KEY,
                paper_id INTEGER,
                arm TEXT,
                extraction_schema_hash TEXT
            );
        """)
        return db_path, conn

    def test_matching_hashes_no_warning(self, tmp_path, caplog):
        db_path, conn = self._make_db(tmp_path)
        conn.execute(
            "INSERT INTO extractions (paper_id, extraction_schema_hash) VALUES (1, 'abc123')"
        )
        conn.execute(
            "INSERT INTO cloud_extractions (paper_id, arm, extraction_schema_hash) VALUES (1, 'openai', 'abc123')"
        )
        conn.commit()
        conn.close()

        with caplog.at_level(logging.WARNING, logger="engine.analysis.concordance"):
            result = check_schema_parity(db_path, ["local", "openai"])

        assert result["local"] == {"abc123"}
        assert result["openai"] == {"abc123"}
        assert "Schema hash mismatch" not in caplog.text

    def test_mismatched_hashes_warns(self, tmp_path, caplog):
        db_path, conn = self._make_db(tmp_path)
        conn.execute(
            "INSERT INTO extractions (paper_id, extraction_schema_hash) VALUES (1, 'hash_local')"
        )
        conn.execute(
            "INSERT INTO cloud_extractions (paper_id, arm, extraction_schema_hash) VALUES (1, 'anthropic', 'hash_cloud')"
        )
        conn.commit()
        conn.close()

        with caplog.at_level(logging.WARNING, logger="engine.analysis.concordance"):
            result = check_schema_parity(db_path, ["local", "anthropic"])

        assert result["local"] == {"hash_local"}
        assert result["anthropic"] == {"hash_cloud"}
        assert "Schema hash mismatch" in caplog.text
        assert "hash_local" in caplog.text
        assert "hash_cloud" in caplog.text
