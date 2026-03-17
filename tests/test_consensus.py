"""Tests for analysis.paper1.consensus — majority-vote consensus derivation."""

import json
import sqlite3
from pathlib import Path

import pytest

from analysis.paper1.consensus import (
    identify_shared_papers,
    derive_consensus,
    store_consensus,
    export_no_consensus_for_adjudication,
    _majority_vote,
    _count_votes,
)


# ── Helpers ──────────────────────────────────────────────────────────


CODEBOOK_PATH = Path("data/surgical_autonomy/extraction_codebook.yaml")

# 20 extraction fields (must match human_import._EXTRACTION_FIELDS)
_EXTRACTION_FIELDS = [
    "study_type", "robot_platform", "task_performed", "sample_size",
    "surgical_domain", "autonomy_level", "validation_setting",
    "task_monitor", "task_generate", "task_select", "task_execute",
    "system_maturity", "study_design", "country",
    "primary_outcome_metric", "primary_outcome_value",
    "comparison_to_human", "secondary_outcomes",
    "key_limitation", "clinical_readiness_assessment",
]


def _make_db(tmp_path: Path) -> Path:
    """Create DB with human_extractions table."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""\
        CREATE TABLE IF NOT EXISTS human_extractions (
            id INTEGER PRIMARY KEY,
            paper_id TEXT NOT NULL,
            extractor_id TEXT NOT NULL,
            field_name TEXT NOT NULL,
            value TEXT,
            source_quote TEXT,
            notes TEXT,
            imported_at TEXT NOT NULL,
            UNIQUE(paper_id, extractor_id, field_name)
        )""")
    conn.commit()
    conn.close()
    return db_path


def _insert_extraction(db_path: Path, paper_id: str, extractor_id: str,
                        field_name: str, value: str | None) -> None:
    """Insert a single human extraction row."""
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO human_extractions "
        "(paper_id, extractor_id, field_name, value, source_quote, notes, imported_at) "
        "VALUES (?, ?, ?, ?, NULL, NULL, '2026-03-17T00:00:00+00:00')",
        (paper_id, extractor_id, field_name, value),
    )
    conn.commit()
    conn.close()


def _populate_paper(db_path: Path, paper_id: str, extractors: dict[str, dict]) -> None:
    """Populate extractions for a paper from multiple extractors.

    extractors: {extractor_id: {field_name: value, ...}}
    Missing fields get None.
    """
    conn = sqlite3.connect(str(db_path))
    for eid, fields in extractors.items():
        for field_name in _EXTRACTION_FIELDS:
            val = fields.get(field_name)
            conn.execute(
                "INSERT INTO human_extractions "
                "(paper_id, extractor_id, field_name, value, source_quote, notes, imported_at) "
                "VALUES (?, ?, ?, ?, NULL, NULL, '2026-03-17T00:00:00+00:00')",
                (paper_id, eid, field_name, val),
            )
    conn.commit()
    conn.close()


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def db_with_shared_papers(tmp_path):
    """DB with 5 papers × 4 extractors, controlled agreement patterns."""
    db_path = _make_db(tmp_path)

    # Paper 1: UNANIMOUS on study_type (all 4 agree)
    _populate_paper(db_path, "EE-001", {
        "A": {"study_type": "Original Research", "sample_size": "50",
               "robot_platform": "da Vinci Xi", "autonomy_level": "2 (Task autonomy)"},
        "B": {"study_type": "Original Research", "sample_size": "50",
               "robot_platform": "da Vinci Xi", "autonomy_level": "2 (Task autonomy)"},
        "C": {"study_type": "Original Research", "sample_size": "50",
               "robot_platform": "da Vinci Xi", "autonomy_level": "2 (Task autonomy)"},
        "D": {"study_type": "Original Research", "sample_size": "50",
               "robot_platform": "da Vinci Xi", "autonomy_level": "2 (Task autonomy)"},
    })

    # Paper 2: 3-1 split on study_type (MAJORITY), unanimous sample_size
    _populate_paper(db_path, "EE-002", {
        "A": {"study_type": "Original Research", "sample_size": "25",
               "robot_platform": "STAR robot", "autonomy_level": "3 (Conditional autonomy)"},
        "B": {"study_type": "Original Research", "sample_size": "25",
               "robot_platform": "STAR robot", "autonomy_level": "3 (Conditional autonomy)"},
        "C": {"study_type": "Original Research", "sample_size": "25",
               "robot_platform": "STAR robot", "autonomy_level": "2 (Task autonomy)"},
        "D": {"study_type": "Case Report/Series", "sample_size": "25",
               "robot_platform": "STAR robot", "autonomy_level": "3 (Conditional autonomy)"},
    })

    # Paper 3: 2-2 split on study_type (NO_CONSENSUS), free-text disagreement
    _populate_paper(db_path, "EE-003", {
        "A": {"study_type": "Original Research", "sample_size": "100",
               "robot_platform": "KUKA LBR iiwa", "autonomy_level": "2 (Task autonomy)"},
        "B": {"study_type": "Original Research", "sample_size": "100",
               "robot_platform": "KUKA iiwa", "autonomy_level": "2 (Task autonomy)"},
        "C": {"study_type": "Case Report/Series", "sample_size": "100",
               "robot_platform": "kuka lbr iiwa", "autonomy_level": "4 (High autonomy)"},
        "D": {"study_type": "Case Report/Series", "sample_size": "100",
               "robot_platform": "KUKA LBR iiwa", "autonomy_level": "4 (High autonomy)"},
    })

    # Paper 4: all different study_type (NO_CONSENSUS), mixed nulls on sample_size
    _populate_paper(db_path, "EE-004", {
        "A": {"study_type": "Original Research", "sample_size": "30",
               "robot_platform": "Custom", "autonomy_level": "1 (Robot assistance)"},
        "B": {"study_type": "Case Report/Series", "sample_size": None,
               "robot_platform": "Custom robot", "autonomy_level": "2 (Task autonomy)"},
        "C": {"study_type": "Review", "sample_size": "30",
               "robot_platform": "Custom", "autonomy_level": "3 (Conditional autonomy)"},
        "D": {"study_type": "Systematic Review", "sample_size": None,
               "robot_platform": "Custom", "autonomy_level": "4 (High autonomy)"},
    })

    # Paper 5: mixed nulls — only 1 non-null (INSUFFICIENT)
    _populate_paper(db_path, "EE-005", {
        "A": {"study_type": "Original Research", "sample_size": None},
        "B": {"study_type": None, "sample_size": None},
        "C": {"study_type": None, "sample_size": None},
        "D": {"study_type": None, "sample_size": "10"},
    })

    return db_path


# ── Tests: identify_shared_papers ────────────────────────────────────


class TestIdentifySharedPapers:

    def test_finds_papers_with_enough_extractors(self, db_with_shared_papers):
        shared = identify_shared_papers(db_with_shared_papers)
        assert len(shared) == 5
        assert "EE-001" in shared
        assert "EE-005" in shared

    def test_min_extractors_threshold(self, db_with_shared_papers):
        # All 5 papers have 4 extractors, so min=4 should return all
        shared = identify_shared_papers(db_with_shared_papers, min_extractors=4)
        assert len(shared) == 5

        # min=5 should return none
        shared = identify_shared_papers(db_with_shared_papers, min_extractors=5)
        assert len(shared) == 0

    def test_empty_db(self, tmp_path):
        db_path = _make_db(tmp_path)
        shared = identify_shared_papers(db_path)
        assert shared == []

    def test_paper_below_threshold_excluded(self, tmp_path):
        """Paper with only 2 extractors excluded at min_extractors=3."""
        db_path = _make_db(tmp_path)
        _insert_extraction(db_path, "EE-001", "A", "study_type", "Original Research")
        _insert_extraction(db_path, "EE-001", "B", "study_type", "Original Research")
        shared = identify_shared_papers(db_path, min_extractors=3)
        assert shared == []


# ── Tests: _majority_vote (unit) ─────────────────────────────────────


class TestMajorityVote:

    def test_unanimous(self):
        result = _majority_vote(["Original Research"] * 4, "categorical")
        assert result["consensus_status"] == "UNANIMOUS"
        assert result["consensus_value"] == "Original Research"

    def test_majority_3_1(self):
        result = _majority_vote(
            ["Original Research", "Original Research", "Original Research", "Case Report/Series"],
            "categorical",
        )
        assert result["consensus_status"] == "MAJORITY"
        assert result["consensus_value"] == "Original Research"

    def test_no_consensus_2_2(self):
        result = _majority_vote(
            ["Original Research", "Original Research", "Case Report/Series", "Case Report/Series"],
            "categorical",
        )
        assert result["consensus_status"] == "NO_CONSENSUS"
        assert result["consensus_value"] is None

    def test_no_consensus_all_different(self):
        result = _majority_vote(
            ["Original Research", "Case Report/Series", "Review", "Systematic Review"],
            "categorical",
        )
        assert result["consensus_status"] == "NO_CONSENSUS"

    def test_insufficient_single_value(self):
        result = _majority_vote(["Original Research", None, None, None], "categorical")
        assert result["consensus_status"] == "INSUFFICIENT"
        assert result["consensus_value"] == "Original Research"

    def test_insufficient_all_null(self):
        result = _majority_vote([None, None, None, None], "categorical")
        assert result["consensus_status"] == "INSUFFICIENT"
        assert result["consensus_value"] is None

    def test_nulls_excluded_from_vote(self):
        """3 non-null with 2 agreeing + 1 different = MAJORITY (2/3 > 50%)."""
        result = _majority_vote(
            ["Original Research", "Original Research", "Case Report/Series", None],
            "categorical",
        )
        assert result["consensus_status"] == "MAJORITY"
        assert result["consensus_value"] == "Original Research"

    def test_free_text_exact_cluster(self):
        result = _majority_vote(
            ["da Vinci Xi", "da Vinci Xi", "Da Vinci XI", "STAR robot"],
            "free_text",
        )
        # "da Vinci Xi" and "Da Vinci XI" normalize to same → cluster of 3
        assert result["consensus_status"] == "MAJORITY"

    def test_free_text_no_cluster(self):
        result = _majority_vote(
            ["da Vinci Xi", "STAR robot", "KUKA iiwa", "Custom"],
            "free_text",
        )
        assert result["consensus_status"] == "NO_CONSENSUS"

    def test_numeric_exact_match(self):
        result = _majority_vote(["50", "50", "50", "75"], "numeric")
        assert result["consensus_status"] == "MAJORITY"
        assert result["consensus_value"] == "50"

    def test_numeric_disagreement(self):
        result = _majority_vote(["50", "75", "50", "75"], "numeric")
        assert result["consensus_status"] == "NO_CONSENSUS"

    def test_set_values_majority(self):
        """Multi-value categorical fields stored as sets."""
        result = _majority_vote(
            [{"Ex vivo", "In vivo (animal)"}, {"Ex vivo", "In vivo (animal)"}, {"Ex vivo"}],
            "categorical",
        )
        assert result["consensus_status"] == "MAJORITY"

    def test_three_extractors_2_1_is_majority(self):
        """With 3 extractors, 2 agreeing is >50%."""
        result = _majority_vote(
            ["Original Research", "Original Research", "Case Report/Series"],
            "categorical",
        )
        assert result["consensus_status"] == "MAJORITY"


# ── Tests: derive_consensus (integration) ────────────────────────────


class TestDeriveConsensus:

    def test_derives_all_fields(self, db_with_shared_papers):
        results = derive_consensus(db_with_shared_papers, CODEBOOK_PATH)
        assert len(results) > 0

        # Should have results for all 5 papers
        paper_ids = {r["paper_id"] for r in results}
        assert "EE-001" in paper_ids
        assert "EE-005" in paper_ids

    def test_unanimous_paper(self, db_with_shared_papers):
        results = derive_consensus(db_with_shared_papers, CODEBOOK_PATH)
        # Paper 1, study_type should be UNANIMOUS
        st = [r for r in results if r["paper_id"] == "EE-001" and r["field_name"] == "study_type"]
        assert len(st) == 1
        assert st[0]["consensus_status"] == "UNANIMOUS"
        assert st[0]["consensus_value"] == "Original Research"

    def test_majority_paper(self, db_with_shared_papers):
        results = derive_consensus(db_with_shared_papers, CODEBOOK_PATH)
        # Paper 2, study_type: 3 "Original Research" vs 1 "Case Report/Series"
        st = [r for r in results if r["paper_id"] == "EE-002" and r["field_name"] == "study_type"]
        assert len(st) == 1
        assert st[0]["consensus_status"] == "MAJORITY"

    def test_no_consensus_paper(self, db_with_shared_papers):
        results = derive_consensus(db_with_shared_papers, CODEBOOK_PATH)
        # Paper 3, study_type: 2-2 split
        st = [r for r in results if r["paper_id"] == "EE-003" and r["field_name"] == "study_type"]
        assert len(st) == 1
        assert st[0]["consensus_status"] == "NO_CONSENSUS"

    def test_insufficient_paper(self, db_with_shared_papers):
        results = derive_consensus(db_with_shared_papers, CODEBOOK_PATH)
        # Paper 5, study_type: only 1 non-null
        st = [r for r in results if r["paper_id"] == "EE-005" and r["field_name"] == "study_type"]
        assert len(st) == 1
        assert st[0]["consensus_status"] == "INSUFFICIENT"

    def test_extractor_values_included(self, db_with_shared_papers):
        results = derive_consensus(db_with_shared_papers, CODEBOOK_PATH)
        r = results[0]
        assert "extractor_values" in r
        assert isinstance(r["extractor_values"], dict)

    def test_empty_db_returns_empty(self, tmp_path):
        db_path = _make_db(tmp_path)
        results = derive_consensus(db_path, CODEBOOK_PATH)
        assert results == []

    def test_free_text_clustering(self, db_with_shared_papers):
        """Paper 3 robot_platform: 'KUKA LBR iiwa' x2, 'KUKA iiwa' x1, 'kuka lbr iiwa' x1.
        After whitespace normalization, 'kuka lbr iiwa' clusters with 'KUKA LBR iiwa' → 3 votes.
        """
        results = derive_consensus(db_with_shared_papers, CODEBOOK_PATH)
        rp = [r for r in results if r["paper_id"] == "EE-003" and r["field_name"] == "robot_platform"]
        assert len(rp) == 1
        # 3 normalize to "kuka lbr iiwa", 1 is "kuka iiwa" → MAJORITY
        assert rp[0]["consensus_status"] == "MAJORITY"

    def test_numeric_with_nulls(self, db_with_shared_papers):
        """Paper 4 sample_size: '30', None, '30', None → 2 non-null, both agree → UNANIMOUS."""
        results = derive_consensus(db_with_shared_papers, CODEBOOK_PATH)
        ss = [r for r in results if r["paper_id"] == "EE-004" and r["field_name"] == "sample_size"]
        assert len(ss) == 1
        assert ss[0]["consensus_status"] == "UNANIMOUS"


# ── Tests: store_consensus ───────────────────────────────────────────


class TestStoreConsensus:

    def test_stores_and_retrieves(self, tmp_path):
        db_path = tmp_path / "consensus.db"
        results = [
            {
                "paper_id": "EE-001",
                "field_name": "study_type",
                "consensus_value": "Original Research",
                "consensus_status": "UNANIMOUS",
                "vote_counts": {"Original Research": 4},
                "extractor_values": {"A": "Original Research", "B": "Original Research"},
            },
        ]
        inserted = store_consensus(results, db_path)
        assert inserted == 1

        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT consensus_value, consensus_status, vote_distribution "
            "FROM consensus_values WHERE paper_id = 'EE-001' AND field_name = 'study_type'"
        ).fetchone()
        assert row[0] == "Original Research"
        assert row[1] == "UNANIMOUS"
        assert json.loads(row[2]) == {"Original Research": 4}
        conn.close()

    def test_null_consensus_value_stored(self, tmp_path):
        db_path = tmp_path / "consensus.db"
        results = [
            {
                "paper_id": "EE-003",
                "field_name": "study_type",
                "consensus_value": None,
                "consensus_status": "NO_CONSENSUS",
                "vote_counts": {"Original Research": 2, "Case Report/Series": 2},
                "extractor_values": {},
            },
        ]
        store_consensus(results, db_path)

        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT consensus_value FROM consensus_values "
            "WHERE paper_id = 'EE-003'"
        ).fetchone()
        assert row[0] is None
        conn.close()

    def test_duplicate_raises(self, tmp_path):
        db_path = tmp_path / "consensus.db"
        results = [
            {
                "paper_id": "EE-001",
                "field_name": "study_type",
                "consensus_value": "Original Research",
                "consensus_status": "UNANIMOUS",
                "vote_counts": {"Original Research": 4},
                "extractor_values": {},
            },
        ]
        store_consensus(results, db_path)
        with pytest.raises(RuntimeError, match="Duplicate consensus"):
            store_consensus(results, db_path)

    def test_derived_at_set(self, tmp_path):
        db_path = tmp_path / "consensus.db"
        results = [
            {
                "paper_id": "EE-001",
                "field_name": "study_type",
                "consensus_value": "X",
                "consensus_status": "UNANIMOUS",
                "vote_counts": {},
                "extractor_values": {},
            },
        ]
        store_consensus(results, db_path)

        conn = sqlite3.connect(str(db_path))
        ts = conn.execute("SELECT derived_at FROM consensus_values LIMIT 1").fetchone()[0]
        assert ts is not None
        assert "T" in ts
        conn.close()


# ── Tests: export_no_consensus_for_adjudication ──────────────────────


class TestExportNoConsensus:

    def test_exports_no_consensus_fields(self, db_with_shared_papers):
        results = derive_consensus(db_with_shared_papers, CODEBOOK_PATH)
        store_consensus(results, db_with_shared_papers)

        nc = export_no_consensus_for_adjudication(db_with_shared_papers)
        assert len(nc) > 0

        # All exported items should have extractor_values
        for item in nc:
            assert "paper_id" in item
            assert "field_name" in item
            assert "extractor_values" in item
            assert isinstance(item["extractor_values"], dict)

    def test_no_consensus_only(self, db_with_shared_papers):
        results = derive_consensus(db_with_shared_papers, CODEBOOK_PATH)
        store_consensus(results, db_with_shared_papers)

        nc = export_no_consensus_for_adjudication(db_with_shared_papers)
        # Paper 3 study_type (2-2 split) should be in results
        nc_fields = [(item["paper_id"], item["field_name"]) for item in nc]
        assert ("EE-003", "study_type") in nc_fields

    def test_empty_when_all_agreed(self, tmp_path):
        """If everything is UNANIMOUS, nothing to adjudicate."""
        db_path = _make_db(tmp_path)
        _populate_paper(db_path, "EE-001", {
            "A": {"study_type": "Original Research"},
            "B": {"study_type": "Original Research"},
            "C": {"study_type": "Original Research"},
        })
        results = derive_consensus(db_path, CODEBOOK_PATH)
        store_consensus(results, db_path)

        nc = export_no_consensus_for_adjudication(db_path)
        # study_type should be unanimous, all other fields are null → INSUFFICIENT
        # Neither UNANIMOUS nor INSUFFICIENT produce NO_CONSENSUS
        nc_study = [item for item in nc if item["field_name"] == "study_type"]
        assert nc_study == []


# ── Tests: _count_votes ─────────────────────────────────────────────


class TestCountVotes:

    def test_counts_strings(self):
        result = _count_votes(["A", "A", "B"])
        assert result == {"A": 2, "B": 1}

    def test_counts_nulls(self):
        result = _count_votes(["A", None, None])
        assert result == {"A": 1, "<null>": 2}

    def test_counts_sets(self):
        result = _count_votes([{"X", "Y"}, {"X", "Y"}])
        assert result == {"X; Y": 2}
