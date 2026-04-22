"""Tests for analysis/paper1/judge_cli.py."""

from __future__ import annotations

import csv
import re
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from analysis.paper1 import judge as judge_module
from analysis.paper1 import judge_cli
from analysis.paper1.judge import JudgeCallError
from analysis.paper1.judge_schema import (
    DisagreementPair,
    EquivalentPair,
    JudgeResult,
    Pass1Output,
)
from engine.cloud.schema import init_cloud_tables
from engine.core.database import ReviewDatabase


CODEBOOK_YAML = """
fields:
  - name: study_type
    type: categorical
    definition: Type of study.
    valid_values: [Review, Original Research]
  - name: robot_platform
    type: free_text
    definition: Name of robot.
"""


# ---------------------------------------------------------------------------
# Shared fixture (copies the loader seed)
# ---------------------------------------------------------------------------


@pytest.fixture()
def setup_review(tmp_path):
    rdb = ReviewDatabase("cli_test", data_root=tmp_path)
    init_cloud_tables(str(rdb.db_path))
    c = rdb._conn

    parsed_dir = rdb.db_path.parent / "parsed_text"
    (parsed_dir / "1_v1.md").write_text(
        "Case Report/Series of 45 patients using the da Vinci Xi."
    )
    (parsed_dir / "2_v1.md").write_text(
        "Original Research with 30 subjects using the STAR robot."
    )
    (parsed_dir / "3_v1.md").write_text(
        "Review of prior work with the da Vinci robot."
    )

    now = "2026-04-20T00:00:00+00:00"
    for pid, title in ((1, "P1"), (2, "P2"), (3, "P3")):
        c.execute(
            "INSERT INTO papers (id, pmid, doi, title, abstract, authors, "
            "journal, year, source, status, created_at, updated_at) "
            "VALUES (?, ?, '', ?, '', '[]', 'J', 2024, 'pubmed', "
            "'EXTRACTED', ?, ?)",
            (pid, f"p{pid}", title, now, now),
        )

    def _add_local(paper_id, spans):
        cur = c.execute(
            "INSERT INTO extractions (paper_id, extraction_schema_hash, "
            "extracted_data, reasoning_trace, model, model_digest, "
            "auditor_model_digest, extracted_at) VALUES (?, '', '{}', '', "
            "'m', '', '', ?)",
            (paper_id, now),
        )
        ext = cur.lastrowid
        for fn, val, src in spans:
            c.execute(
                "INSERT INTO evidence_spans (extraction_id, field_name, value, "
                "source_snippet, confidence, tier, audit_status) VALUES "
                "(?, ?, ?, ?, 0.9, 1, 'pending')",
                (ext, fn, val, src),
            )

    def _add_cloud(paper_id, arm, spans):
        cur = c.execute(
            "INSERT INTO cloud_extractions (paper_id, arm, model_string, "
            "extracted_data, extraction_schema_hash, extracted_at) "
            "VALUES (?, ?, 'm', '{}', '', ?)",
            (paper_id, arm, now),
        )
        ext = cur.lastrowid
        for fn, val, src in spans:
            c.execute(
                "INSERT INTO cloud_evidence_spans (cloud_extraction_id, "
                "field_name, value, source_snippet, confidence, tier) "
                "VALUES (?, ?, ?, ?, 0.9, 1)",
                (ext, fn, val, src),
            )

    for pid in (1, 2, 3):
        _add_local(pid, [("study_type", "X", "span")])
        _add_cloud(pid, "openai_o4_mini_high",
                   [("study_type", "Y", "span")])
        _add_cloud(pid, "anthropic_sonnet_4_6",
                   [("study_type", "Z", "span")])
    c.commit()
    rdb.close()

    codebook_path = tmp_path / "codebook.yaml"
    codebook_path.write_text(CODEBOOK_YAML)

    csv_path = tmp_path / "pairs.csv"
    headers = [
        "paper_id", "paper_label", "paper_title", "field_name", "field_tier",
        "field_type", "local_value", "o4mini_value", "sonnet_value",
        "local_vs_o4mini_score", "local_vs_sonnet_score",
        "o4mini_vs_sonnet_score",
    ]
    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        for pid in (1, 2, 3):
            w.writerow({
                "paper_id": pid, "paper_label": f"P{pid}", "paper_title": "",
                "field_name": "study_type", "field_tier": 1,
                "field_type": "categorical",
                "local_value": "Review",
                "o4mini_value": "Review",
                "sonnet_value": "Review",
            })

    return SimpleNamespace(
        data_root=tmp_path,
        db_path=rdb.db_path,
        csv=csv_path,
        codebook=codebook_path,
    )


def _fake_judge_result(paper_id, field_name, rating="EQUIVALENT"):
    def _pair(a, b):
        if rating == "EQUIVALENT":
            return EquivalentPair(slot_a=a, slot_b=b, rating="EQUIVALENT",
                                  rationale="x")
        return DisagreementPair(slot_a=a, slot_b=b, rating=rating,
                                disagreement_type="GRANULARITY", rationale="x")

    pairs = [_pair(1, 2), _pair(1, 3), _pair(2, 3)]
    pass1 = Pass1Output(
        pairwise_ratings=pairs,
        fabrication_risk="low",
        proposed_consensus="Review",
        overall_rationale="x",
    )
    return JudgeResult(
        paper_id=paper_id, field_name=field_name,
        arm_permutation=["local", "openai_o4_mini_high",
                         "anthropic_sonnet_4_6"],
        pass1=pass1,
        prompt_hash="p" * 64,
        judge_model_digest="sha256:abc",
        judge_model_name="gemma3:27b",
        raw_response="{}",
        seed=1,
        timestamp_iso="2026-04-20T00:00:00+00:00",
    )


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


class TestArgParsing:
    def test_help_exits_zero(self, capsys):
        with pytest.raises(SystemExit) as exc:
            judge_cli.run(["--help"])
        assert exc.value.code == 0
        out = capsys.readouterr().out
        assert "--review" in out

    def test_missing_review(self):
        with pytest.raises(SystemExit) as exc:
            judge_cli.run([
                "--input", "AI_TRIPLES",
                "--codebook", "/tmp/nonexistent.yaml",
            ])
        assert exc.value.code == 2

    def test_unknown_input_rejected(self):
        with pytest.raises(SystemExit) as exc:
            judge_cli.run([
                "--review", "r", "--input", "BOGUS",
                "--codebook", "/tmp/nonexistent.yaml",
            ])
        assert exc.value.code == 2

    def test_pairs_csv_missing_for_ai_triples_returns_2(self, setup_review):
        code = judge_cli.run([
            "--review", "cli_test",
            "--input", "AI_TRIPLES",
            "--codebook", str(setup_review.codebook),
            "--data-root", str(setup_review.data_root),
        ])
        assert code == 2

    def test_pass_2_raises_not_implemented(self, setup_review):
        with pytest.raises(NotImplementedError):
            judge_cli.run([
                "--review", "cli_test",
                "--input", "AI_TRIPLES",
                "--pairs-csv", str(setup_review.csv),
                "--codebook", str(setup_review.codebook),
                "--pass", "2",
                "--data-root", str(setup_review.data_root),
            ])

    def test_human_pairs_raises_not_implemented(self, setup_review):
        with pytest.raises(NotImplementedError):
            judge_cli.run([
                "--review", "cli_test",
                "--input", "HUMAN_PAIRS",
                "--codebook", str(setup_review.codebook),
                "--data-root", str(setup_review.data_root),
            ])


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------


class TestDryRun:
    def test_dry_run_skips_db_writes(self, setup_review, capsys):
        called = {"n": 0}

        def fake(inp, run_id, model=judge_cli.DEFAULT_MODEL):
            called["n"] += 1
            return _fake_judge_result(inp.paper_id, inp.field_name)

        with patch.object(judge_module, "fetch_model_digest", lambda m: "d"), \
             patch.object(judge_cli, "run_pass1", fake), \
             patch.object(judge_cli, "fetch_model_digest", lambda m: "d"):
            code = judge_cli.run([
                "--review", "cli_test",
                "--input", "AI_TRIPLES",
                "--pairs-csv", str(setup_review.csv),
                "--codebook", str(setup_review.codebook),
                "--data-root", str(setup_review.data_root),
                "--dry-run",
            ])
        assert code == 0
        assert called["n"] == 3

        conn = sqlite3.connect(str(setup_review.db_path))
        assert conn.execute(
            "SELECT COUNT(*) FROM judge_runs"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM judge_ratings"
        ).fetchone()[0] == 0
        conn.close()

        out = capsys.readouterr().out
        assert "JUDGE RUN SUMMARY" in out
        assert "dry_run:               True" in out


# ---------------------------------------------------------------------------
# Full flow
# ---------------------------------------------------------------------------


class TestFullFlow:
    def test_all_succeed_writes_expected_rows(self, setup_review, capsys):
        def fake(inp, run_id, model=judge_cli.DEFAULT_MODEL):
            return _fake_judge_result(inp.paper_id, inp.field_name)

        with patch.object(judge_cli, "run_pass1", fake), \
             patch.object(judge_cli, "fetch_model_digest", lambda m: "sha256:z"):
            code = judge_cli.run([
                "--review", "cli_test",
                "--input", "AI_TRIPLES",
                "--pairs-csv", str(setup_review.csv),
                "--codebook", str(setup_review.codebook),
                "--data-root", str(setup_review.data_root),
            ])
        assert code == 0

        conn = sqlite3.connect(str(setup_review.db_path))
        n_runs = conn.execute("SELECT COUNT(*) FROM judge_runs").fetchone()[0]
        n_ratings = conn.execute(
            "SELECT COUNT(*) FROM judge_ratings"
        ).fetchone()[0]
        n_pairs = conn.execute(
            "SELECT COUNT(*) FROM judge_pair_ratings"
        ).fetchone()[0]
        row = conn.execute(
            "SELECT completed_at, n_triples_attempted, n_triples_succeeded, "
            "n_triples_failed FROM judge_runs"
        ).fetchone()
        conn.close()
        assert (n_runs, n_ratings, n_pairs) == (1, 3, 9)
        assert row[0] is not None   # completed_at
        assert row[1] == 3
        assert row[2] == 3
        assert row[3] == 0

    def test_partial_failure(self, setup_review, capsys):
        call = {"n": 0}

        def sometimes_fail(inp, run_id, model=judge_cli.DEFAULT_MODEL):
            call["n"] += 1
            if call["n"] == 2:
                raise JudgeCallError("mocked")
            return _fake_judge_result(inp.paper_id, inp.field_name)

        with patch.object(judge_cli, "run_pass1", sometimes_fail), \
             patch.object(judge_cli, "fetch_model_digest", lambda m: "d"):
            code = judge_cli.run([
                "--review", "cli_test",
                "--input", "AI_TRIPLES",
                "--pairs-csv", str(setup_review.csv),
                "--codebook", str(setup_review.codebook),
                "--data-root", str(setup_review.data_root),
            ])
        # 2/3 succeeded = 66.7% < 90% threshold → exit 1
        assert code == 1

        conn = sqlite3.connect(str(setup_review.db_path))
        row = conn.execute(
            "SELECT n_triples_attempted, n_triples_succeeded, "
            "n_triples_failed, completed_at FROM judge_runs"
        ).fetchone()
        conn.close()
        assert row[0] == 3
        assert row[1] == 2
        assert row[2] == 1
        assert row[3] is not None

    def test_all_failed_exit_1(self, setup_review):
        def always_fail(inp, run_id, model=judge_cli.DEFAULT_MODEL):
            raise JudgeCallError("boom")

        with patch.object(judge_cli, "run_pass1", always_fail), \
             patch.object(judge_cli, "fetch_model_digest", lambda m: "d"):
            code = judge_cli.run([
                "--review", "cli_test",
                "--input", "AI_TRIPLES",
                "--pairs-csv", str(setup_review.csv),
                "--codebook", str(setup_review.codebook),
                "--data-root", str(setup_review.data_root),
            ])
        assert code == 1

    def test_exit_zero_at_exactly_90pct(self, setup_review):
        # 10 triples, 1 failure → 90% succeed → exit 0
        call = {"n": 0}

        def one_fail(inp, run_id, model=judge_cli.DEFAULT_MODEL):
            call["n"] += 1
            if call["n"] == 1:
                raise JudgeCallError("first fails")
            return _fake_judge_result(inp.paper_id, inp.field_name)

        # Expand CSV to 10 rows.
        csv_path = setup_review.csv
        headers = [
            "paper_id", "paper_label", "paper_title", "field_name",
            "field_tier", "field_type", "local_value", "o4mini_value",
            "sonnet_value", "local_vs_o4mini_score", "local_vs_sonnet_score",
            "o4mini_vs_sonnet_score",
        ]
        with csv_path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
            w.writeheader()
            for i in range(10):
                pid = (i % 3) + 1
                w.writerow({
                    "paper_id": pid, "paper_label": f"P{pid}", "paper_title": "",
                    "field_name": "study_type", "field_tier": 1,
                    "field_type": "categorical",
                    "local_value": f"L{i}", "o4mini_value": f"O{i}",
                    "sonnet_value": f"S{i}",
                })

        with patch.object(judge_cli, "run_pass1", one_fail), \
             patch.object(judge_cli, "fetch_model_digest", lambda m: "d"):
            code = judge_cli.run([
                "--review", "cli_test",
                "--input", "AI_TRIPLES",
                "--pairs-csv", str(csv_path),
                "--codebook", str(setup_review.codebook),
                "--data-root", str(setup_review.data_root),
                "--dry-run",  # no UNIQUE collisions from repeating (pid,field)
            ])
        # Note: in dry-run the CLI still counts + returns the threshold result.
        assert code == 0


# ---------------------------------------------------------------------------
# Run ID format
# ---------------------------------------------------------------------------


class TestRunId:
    def test_pattern(self):
        rid = judge_cli._new_run_id("surgical_autonomy", 1)
        assert re.match(
            r"^surgical_autonomy_pass1_\d{8}T\d{6}Z_[0-9a-f]{8}$", rid
        ), rid

    def test_pass2_in_id(self):
        rid = judge_cli._new_run_id("r", 2)
        assert "_pass2_" in rid
