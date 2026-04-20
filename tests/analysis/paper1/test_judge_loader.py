"""Tests for analysis/paper1/judge_loader.py."""

from __future__ import annotations

import csv
import logging

import pytest

from analysis.paper1.judge_loader import (
    LoaderError,
    compute_codebook_sha256,
    load_ai_triples_csv,
    load_codebook,
)
from engine.cloud.schema import init_cloud_tables
from engine.core.database import ReviewDatabase

CODEBOOK_YAML = """
fields:
  - name: study_type
    tier: 1
    type: categorical
    definition: |-
      Type of study as described in the methods.
    valid_values:
      - value: "Original Research"
        definition: primary data
      - value: "Case Report/Series"
        definition: small case report
      - value: "Review"
        definition: narrative review
  - name: sample_size
    tier: 1
    type: numeric
    definition: Number of cases or subjects.
    tolerance: 2
  - name: robot_platform
    tier: 1
    type: free_text
    definition: Name of the robot.
"""

INVALID_YAML_MISSING_TYPE = """
fields:
  - name: study_type
    definition: T
"""

INVALID_YAML_MISSING_DEF = """
fields:
  - name: study_type
    type: categorical
"""

INVALID_YAML_UNKNOWN_TYPE = """
fields:
  - name: study_type
    type: blob
    definition: T
"""


# ---------------------------------------------------------------------------
# Codebook parsing
# ---------------------------------------------------------------------------


class TestLoadCodebook:
    def _write(self, tmp_path, body, name="codebook.yaml"):
        p = tmp_path / name
        p.write_text(body)
        return p

    def test_happy_path(self, tmp_path):
        p = self._write(tmp_path, CODEBOOK_YAML)
        cb = load_codebook(p)
        assert set(cb.keys()) == {"study_type", "sample_size", "robot_platform"}

        cat = cb["study_type"]
        assert cat.field_type == "categorical"
        assert cat.valid_values == [
            "Original Research", "Case Report/Series", "Review"
        ]
        assert cat.numeric_tolerance == 0.0

        num = cb["sample_size"]
        assert num.field_type == "numeric"
        assert num.numeric_tolerance == 2.0
        assert num.valid_values is None

        ft = cb["robot_platform"]
        assert ft.field_type == "free_text"
        assert ft.valid_values is None

    def test_missing_type(self, tmp_path):
        p = self._write(tmp_path, INVALID_YAML_MISSING_TYPE)
        with pytest.raises(LoaderError) as exc_info:
            load_codebook(p)
        assert "study_type" in str(exc_info.value)

    def test_missing_definition(self, tmp_path):
        p = self._write(tmp_path, INVALID_YAML_MISSING_DEF)
        with pytest.raises(LoaderError):
            load_codebook(p)

    def test_unknown_type(self, tmp_path):
        p = self._write(tmp_path, INVALID_YAML_UNKNOWN_TYPE)
        with pytest.raises(LoaderError):
            load_codebook(p)

    def test_missing_fields_top_level(self, tmp_path):
        p = self._write(tmp_path, "version: '1'\n")
        with pytest.raises(LoaderError):
            load_codebook(p)

    def test_valid_values_plain_strings_supported(self, tmp_path):
        yml = (
            "fields:\n"
            "  - name: study_type\n"
            "    type: categorical\n"
            "    definition: T\n"
            "    valid_values: [RCT, Cohort]\n"
        )
        p = self._write(tmp_path, yml)
        cb = load_codebook(p)
        assert cb["study_type"].valid_values == ["RCT", "Cohort"]


class TestCodebookSha:
    def test_deterministic(self, tmp_path):
        p = tmp_path / "c.yaml"
        p.write_text(CODEBOOK_YAML)
        a = compute_codebook_sha256(p)
        b = compute_codebook_sha256(p)
        assert a == b
        assert len(a) == 64

    def test_changes_with_content(self, tmp_path):
        p = tmp_path / "c.yaml"
        p.write_text(CODEBOOK_YAML)
        a = compute_codebook_sha256(p)
        p.write_text(CODEBOOK_YAML + "\n# tweak\n")
        b = compute_codebook_sha256(p)
        assert a != b


# ---------------------------------------------------------------------------
# CSV loader
# ---------------------------------------------------------------------------


@pytest.fixture()
def seeded_db(tmp_path):
    """ReviewDatabase pre-populated with 2 papers + parsed text + spans."""
    rdb = ReviewDatabase("loader_test", data_root=tmp_path)
    init_cloud_tables(str(rdb.db_path))
    c = rdb._conn

    parsed_dir = rdb.db_path.parent / "parsed_text"
    parsed_dir.mkdir(exist_ok=True)
    (parsed_dir / "1_v1.md").write_text(
        "This was a Case Report/Series with 45 patients. "
        "The robot platform was the da Vinci Xi system."
    )
    (parsed_dir / "2_v1.md").write_text(
        "We conducted an Original Research study of 30 subjects "
        "using the STAR surgical robot."
    )

    now = "2026-04-20T00:00:00+00:00"
    c.execute(
        "INSERT INTO papers (id, pmid, doi, title, abstract, authors, journal, "
        "year, source, status, created_at, updated_at) VALUES "
        "(1, 'p1', '', 'Paper 1', '', '[]', 'J', 2024, 'pubmed', 'EXTRACTED', ?, ?)",
        (now, now),
    )
    c.execute(
        "INSERT INTO papers (id, pmid, doi, title, abstract, authors, journal, "
        "year, source, status, created_at, updated_at) VALUES "
        "(2, 'p2', '', 'Paper 2', '', '[]', 'J', 2024, 'pubmed', 'EXTRACTED', ?, ?)",
        (now, now),
    )

    def _add_local_extraction(paper_id, spans):
        cur = c.execute(
            "INSERT INTO extractions (paper_id, extraction_schema_hash, "
            "extracted_data, reasoning_trace, model, model_digest, "
            "auditor_model_digest, extracted_at) VALUES (?, '', '{}', '', "
            "'deepseek-r1:32b', '', '', ?)",
            (paper_id, now),
        )
        ext_id = cur.lastrowid
        for fn, val, src in spans:
            c.execute(
                "INSERT INTO evidence_spans (extraction_id, field_name, value, "
                "source_snippet, confidence, tier, audit_status) VALUES "
                "(?, ?, ?, ?, 0.9, 1, 'pending')",
                (ext_id, fn, val, src),
            )

    def _add_cloud_extraction(paper_id, arm, spans):
        cur = c.execute(
            "INSERT INTO cloud_extractions (paper_id, arm, model_string, "
            "extracted_data, extraction_schema_hash, extracted_at) "
            "VALUES (?, ?, 'm', '{}', '', ?)",
            (paper_id, arm, now),
        )
        ext_id = cur.lastrowid
        for fn, val, src in spans:
            c.execute(
                "INSERT INTO cloud_evidence_spans (cloud_extraction_id, "
                "field_name, value, source_snippet, confidence, tier) "
                "VALUES (?, ?, ?, ?, 0.9, 1)",
                (ext_id, fn, val, src),
            )

    _add_local_extraction(1, [
        ("study_type", "Case Report/Series",
         "This was a Case Report/Series with 45 patients."),
        ("robot_platform", "da Vinci Xi",
         "The robot platform was the da Vinci Xi system."),
    ])
    _add_cloud_extraction(1, "openai_o4_mini_high", [
        ("study_type", "Case Report/Series",
         "a Case Report/Series with 45 patients"),
        ("robot_platform", "da Vinci Xi system",
         "robot platform was the da Vinci Xi system"),
    ])
    _add_cloud_extraction(1, "anthropic_sonnet_4_6", [
        ("study_type", "Review",
         "Review of prior cases"),
        ("robot_platform", "da Vinci",
         "the da Vinci"),
    ])
    _add_local_extraction(2, [
        ("study_type", "Original Research",
         "We conducted an Original Research study"),
    ])
    _add_cloud_extraction(2, "openai_o4_mini_high", [
        ("study_type", "Original Research",
         "Original Research study of 30 subjects"),
    ])
    _add_cloud_extraction(2, "anthropic_sonnet_4_6", [
        ("study_type", "Original Research",
         "an Original Research study"),
    ])

    c.commit()
    yield rdb
    rdb.close()


def _write_csv(path, rows):
    headers = [
        "paper_id", "paper_label", "paper_title", "field_name", "field_tier",
        "field_type", "local_value", "o4mini_value", "sonnet_value",
        "local_vs_o4mini_score", "local_vs_sonnet_score",
        "o4mini_vs_sonnet_score",
    ]
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in headers})


def _row(paper_id, field_name, values, field_type="categorical"):
    local_v, o4_v, s_v = values
    return {
        "paper_id": paper_id, "paper_label": f"P{paper_id}",
        "paper_title": "", "field_name": field_name, "field_tier": "1",
        "field_type": field_type,
        "local_value": local_v,
        "o4mini_value": o4_v,
        "sonnet_value": s_v,
    }


def _codebook_for_tests(tmp_path):
    cb_path = tmp_path / "cb.yaml"
    cb_path.write_text(CODEBOOK_YAML)
    return load_codebook(cb_path)


class TestLoadAITriplesCSV:
    def test_happy_path(self, tmp_path, seeded_db):
        cb = _codebook_for_tests(tmp_path)
        csv_path = tmp_path / "pairs.csv"
        _write_csv(csv_path, [
            _row(1, "study_type",
                 ("Case Report/Series", "Case Report/Series", "Review")),
            _row(1, "robot_platform",
                 ("da Vinci Xi", "da Vinci Xi system", "da Vinci"),
                 field_type="free_text"),
            _row(2, "study_type",
                 ("Original Research", "Original Research", "Original Research")),
        ])
        inputs = load_ai_triples_csv(csv_path, seeded_db, cb)
        assert len(inputs) == 3
        first = inputs[0]
        assert first.paper_id == "1"
        assert first.field_name == "robot_platform"  # sorted by (pid, fname)
        assert len(first.arms) == 3
        assert [a.arm_name for a in first.arms] == [
            "local", "openai_o4_mini_high", "anthropic_sonnet_4_6",
        ]

    def test_limit(self, tmp_path, seeded_db):
        cb = _codebook_for_tests(tmp_path)
        csv_path = tmp_path / "pairs.csv"
        _write_csv(csv_path, [
            _row(1, "study_type", ("X", "Y", "Z")),
            _row(1, "robot_platform", ("A", "B", "C"), field_type="free_text"),
            _row(2, "study_type", ("P", "Q", "R")),
        ])
        inputs = load_ai_triples_csv(csv_path, seeded_db, cb, limit=1)
        assert len(inputs) == 1

    def test_unknown_field_skipped_with_warning(self, tmp_path, seeded_db, caplog):
        cb = _codebook_for_tests(tmp_path)
        csv_path = tmp_path / "pairs.csv"
        _write_csv(csv_path, [
            _row(1, "mystery_field", ("a", "b", "c"), field_type="categorical"),
            _row(1, "study_type", ("Review", "Review", "Review")),
        ])
        with caplog.at_level(logging.WARNING):
            inputs = load_ai_triples_csv(csv_path, seeded_db, cb)
        assert len(inputs) == 1
        assert inputs[0].field_name == "study_type"
        assert any("mystery_field" in r.message for r in caplog.records)

    def test_missing_parsed_text_skipped(self, tmp_path, seeded_db, caplog):
        cb = _codebook_for_tests(tmp_path)
        csv_path = tmp_path / "pairs.csv"
        _write_csv(csv_path, [
            _row(99, "study_type", ("a", "b", "c")),
            _row(1, "study_type", ("Review", "Review", "Review")),
        ])
        with caplog.at_level(logging.WARNING):
            inputs = load_ai_triples_csv(csv_path, seeded_db, cb)
        paper_ids = [i.paper_id for i in inputs]
        assert "99" not in paper_ids
        assert "1" in paper_ids

    def test_null_value_yields_none_and_all_false_flags(self, tmp_path, seeded_db):
        cb = _codebook_for_tests(tmp_path)
        csv_path = tmp_path / "pairs.csv"
        _write_csv(csv_path, [
            _row(1, "study_type", ("", "None", "null")),
        ])
        inputs = load_ai_triples_csv(csv_path, seeded_db, cb)
        assert len(inputs) == 1
        arms = inputs[0].arms
        for a in arms:
            assert a.value is None
            assert a.precheck_flags.span_present in (True, False)
            assert a.precheck_flags.value_in_span is False

    def test_output_ordering_deterministic(self, tmp_path, seeded_db):
        cb = _codebook_for_tests(tmp_path)
        csv_path = tmp_path / "pairs.csv"
        _write_csv(csv_path, [
            _row(2, "study_type", ("x", "y", "z")),
            _row(1, "study_type", ("a", "b", "c")),
            _row(1, "robot_platform", ("p", "q", "r"), field_type="free_text"),
        ])
        inputs = load_ai_triples_csv(csv_path, seeded_db, cb)
        keys = [(i.paper_id, i.field_name) for i in inputs]
        assert keys == sorted(keys)

    def test_arm_name_canonicalization(self, tmp_path, seeded_db):
        cb = _codebook_for_tests(tmp_path)
        csv_path = tmp_path / "pairs.csv"
        _write_csv(csv_path, [
            _row(1, "study_type",
                 ("Case Report/Series", "Case Report/Series", "Review")),
        ])
        inputs = load_ai_triples_csv(csv_path, seeded_db, cb)
        arm_names = {a.arm_name for a in inputs[0].arms}
        assert arm_names == {
            "local", "openai_o4_mini_high", "anthropic_sonnet_4_6",
        }

    def test_precheck_flags_are_computed(self, tmp_path, seeded_db):
        cb = _codebook_for_tests(tmp_path)
        csv_path = tmp_path / "pairs.csv"
        _write_csv(csv_path, [
            _row(1, "study_type",
                 ("Case Report/Series", "Case Report/Series", "Review")),
        ])
        inputs = load_ai_triples_csv(csv_path, seeded_db, cb)
        by_arm = {a.arm_name: a for a in inputs[0].arms}
        local = by_arm["local"]
        # Local span is verbatim in paper text; value matches.
        assert local.precheck_flags.span_in_source is True
        assert local.precheck_flags.value_in_span is True
        # Sonnet value "Review" is NOT in the paper; its span is fabricated
        # (not in paper text either).
        sonnet = by_arm["anthropic_sonnet_4_6"]
        assert sonnet.precheck_flags.span_in_source is False
