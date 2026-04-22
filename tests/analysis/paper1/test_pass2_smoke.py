"""Smoke tests for analysis/paper1/pass2_smoke.py — selection + gates."""

from __future__ import annotations

import pytest

from analysis.paper1 import pass2_smoke as mod
from analysis.paper1.pass2_smoke import (
    Candidate,
    GateResult,
    SelectionResult,
    TripleExec,
    evaluate_gates,
    select_triples,
)


# ── deterministic selection ────────────────────────────────────────


class _FakeDB:
    """Minimal ReviewDatabase substitute for selection tests."""

    def __init__(self, rows):
        self._rows = rows

        class _Conn:
            def execute(_, sql, params):
                pass1_run_id, risk = params
                filtered = [
                    r for r in rows
                    if r["run_id"] == pass1_run_id
                    and r["pass1_fabrication_risk"] == risk
                ]

                class _Cur:
                    def fetchall(__): return filtered
                return _Cur()
        self._conn = _Conn()


def _row(paper_id, field_name, field_type, risk):
    return {
        "run_id": "r",
        "paper_id": paper_id,
        "field_name": field_name,
        "field_type": field_type,
        "pass1_fabrication_risk": risk,
    }


def test_select_triples_all_strata_populate():
    rows = []
    # 14 high triples.
    for i in range(14):
        rows.append(_row(str(i), f"f{i}", "categorical", "high"))
    # Enough medium to fill every stratum.
    for i in range(10):
        rows.append(_row(f"100{i}", "task_performed", "free_text", "medium"))
    for i in range(10):
        rows.append(_row(f"200{i}", "robot_platform", "free_text", "medium"))
    for i in range(10):
        rows.append(_row(f"300{i}", "study_design", "categorical", "medium"))
    for i in range(5):
        rows.append(_row(f"400{i}", "sample_size", "numeric", "medium"))
    # Five 719 medium triples so paper719 stratum still has >=2 after the
    # earlier strata claim any that overlap.
    for fn in ("key_limitation", "secondary_outcomes", "surgical_domain",
               "system_maturity", "task_generate"):
        rows.append(_row("719", fn, "free_text", "medium"))

    db = _FakeDB(rows)
    # Without judge_input_lookup, short_circuit stratum cannot populate;
    # expect 23 chosen and that one documented gap.
    sel = select_triples(db, "r", judge_input_lookup={})
    assert len(sel.chosen) == 23
    strata = [s for _, s in sel.chosen]
    # spec counts.
    assert strata.count("high") == 14
    assert strata.count("task_performed_saturated") == 2
    assert strata.count("robot_platform_saturated") == 2
    assert strata.count("categorical_any") == 2
    assert strata.count("numeric") == 1
    assert strata.count("paper719_windowing") == 2
    # short_circuit stratum won't populate — no judge_input_lookup.
    assert sel.gaps == ["short_circuit_eligible: got 0/1"]


def test_select_triples_deterministic_same_pool_same_output():
    rows = []
    for i in range(14):
        rows.append(_row(str(i), f"f{i}", "categorical", "high"))
    for i in range(5):
        rows.append(_row(f"100{i}", "task_performed", "free_text", "medium"))
    for i in range(5):
        rows.append(_row(f"200{i}", "robot_platform", "free_text", "medium"))
    for i in range(5):
        rows.append(_row(f"300{i}", "study_design", "categorical", "medium"))
    rows.append(_row("400", "sample_size", "numeric", "medium"))

    db = _FakeDB(rows)
    s1 = select_triples(db, "r", judge_input_lookup={})
    s2 = select_triples(db, "r", judge_input_lookup={})
    assert [c.paper_id for c, _ in s1.chosen] == [c.paper_id for c, _ in s2.chosen]


def test_select_triples_dedup_across_strata():
    """A triple eligible for multiple strata should only appear once."""
    rows = [
        # paper 719 also has a task_performed medium that could match both.
        _row("719", "task_performed", "free_text", "medium"),
        _row("719", "robot_platform", "free_text", "medium"),
        _row("719", "other_field", "categorical", "medium"),
    ]
    # No high triples, so high stratum is empty.
    db = _FakeDB(rows)
    sel = select_triples(db, "r", judge_input_lookup={})
    seen: set[tuple[str, str]] = set()
    for c, _ in sel.chosen:
        key = (c.paper_id, c.field_name)
        assert key not in seen, f"duplicate pick: {key}"
        seen.add(key)


# ── gate logic ─────────────────────────────────────────────────────


def _exec(
    verdicts_by_arm, short_circuit_by_arm=None,
    windowed=False, wall=30.0,
    overall=False, error=None,
    paper_id="p", field_name="f",
):
    return TripleExec(
        candidate=Candidate(paper_id, field_name, "categorical", "high"),
        stratum="high",
        wall_sec=wall,
        windowed=windowed,
        source_tokens=5000,
        verdicts_by_arm=verdicts_by_arm,
        short_circuit_by_arm=short_circuit_by_arm or {},
        verification_span_by_arm={a: None for a in verdicts_by_arm},
        reasoning_by_arm={a: None for a in verdicts_by_arm},
        fabrication_hypothesis_by_arm={a: None for a in verdicts_by_arm},
        overall_fabrication_detected=overall,
        raw_response="{}",
        error=error,
    )


def _by_name(gates: list[GateResult]) -> dict[str, GateResult]:
    return {g.name: g for g in gates}


def test_gates_happy_path_all_pass():
    arms = ("local", "openai_o4_mini_high", "anthropic_sonnet_4_6")
    execs = []
    # 24 triples: mix of verdicts, one windowed, some short-circuit.
    for i in range(24):
        # No arm is 100% SUPPORTED.
        v = {
            "local": "SUPPORTED" if i % 3 else "UNSUPPORTED",
            "openai_o4_mini_high": "UNSUPPORTED" if i == 0 else "SUPPORTED",
            "anthropic_sonnet_4_6": "PARTIALLY_SUPPORTED" if i % 4 == 0 else "SUPPORTED",
        }
        sc = {a: (a == "local" and i == 0) for a in arms}
        execs.append(_exec(v, short_circuit_by_arm=sc, windowed=(i == 5)))
    gates = _by_name(evaluate_gates(db=None, run_id="rx", execs=execs, dry_run=True))
    for name, g in gates.items():
        assert g.passed, f"{name} unexpectedly failed: {g.detail}"


def test_gate4_fails_without_unsupported():
    execs = [_exec({"local": "SUPPORTED", "openai_o4_mini_high": "SUPPORTED",
                    "anthropic_sonnet_4_6": "SUPPORTED"},
                   short_circuit_by_arm={"local": True}) for _ in range(24)]
    gates = _by_name(evaluate_gates(db=None, run_id="rx", execs=execs, dry_run=True))
    assert not gates["4. unsupported_present"].passed


def test_gate5_fails_without_short_circuit():
    execs = [_exec({"local": "SUPPORTED", "openai_o4_mini_high": "UNSUPPORTED",
                    "anthropic_sonnet_4_6": "SUPPORTED"}) for _ in range(24)]
    gates = _by_name(evaluate_gates(db=None, run_id="rx", execs=execs, dry_run=True))
    assert not gates["5. short_circuit_firing"].passed


def test_gate6_fails_without_windowing():
    execs = [_exec({"local": "SUPPORTED", "openai_o4_mini_high": "UNSUPPORTED",
                    "anthropic_sonnet_4_6": "SUPPORTED"},
                   short_circuit_by_arm={"local": True}) for _ in range(24)]
    gates = _by_name(evaluate_gates(db=None, run_id="rx", execs=execs, dry_run=True))
    assert not gates["6. windowing_exercised"].passed


def test_gate7_fails_on_high_latency():
    execs = [_exec({"local": "SUPPORTED", "openai_o4_mini_high": "UNSUPPORTED",
                    "anthropic_sonnet_4_6": "SUPPORTED"},
                   short_circuit_by_arm={"local": True},
                   windowed=(i == 0), wall=200.0)
             for i in range(24)]
    gates = _by_name(evaluate_gates(db=None, run_id="rx", execs=execs, dry_run=True))
    assert not gates["7. latency"].passed


def test_gate8_fails_on_100pct_supported_arm():
    execs = []
    for i in range(24):
        v = {
            "local": "SUPPORTED",  # always SUPPORTED — gate 8 violator
            "openai_o4_mini_high": "SUPPORTED" if i % 2 else "UNSUPPORTED",
            "anthropic_sonnet_4_6": "SUPPORTED" if i % 3 else "PARTIALLY_SUPPORTED",
        }
        execs.append(_exec(v, short_circuit_by_arm={"local": True},
                           windowed=(i == 0)))
    gates = _by_name(evaluate_gates(db=None, run_id="rx", execs=execs, dry_run=True))
    assert not gates["8. arm_verdict_balance"].passed
    assert "local" in gates["8. arm_verdict_balance"].detail


def test_gate1_fails_on_errors():
    bad = [_exec({}, error="boom") for _ in range(2)]
    good = [_exec({"local": "SUPPORTED", "openai_o4_mini_high": "SUPPORTED",
                   "anthropic_sonnet_4_6": "UNSUPPORTED"},
                  short_circuit_by_arm={"local": True},
                  windowed=(i == 0))
            for i in range(22)]
    gates = _by_name(evaluate_gates(db=None, run_id="rx", execs=bad + good,
                                    dry_run=True))
    assert not gates["1. parse_cleanliness"].passed
