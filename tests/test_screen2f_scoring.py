"""SCREEN-AUTH-01 Phase 2f scorer — metrics, call-pattern gate, disagreement lists.

Checkpoints are written with the harness's own Checkpoint, so the scorer is
tested against the format the run produces, not a hand-typed imitation of it.
"""

import ast
import json
from pathlib import Path

import pytest

from analysis.eval import score_screen2f as sc
from analysis.eval import screen2f as lib

REPO = Path(__file__).resolve().parents[1]


def _call(role, pass_number, decision=None, parse_error=None, error=None, rationale="r"):
    return {"decision": decision, "rationale": rationale if decision else None, "confidence": 0.9 if decision else None,
            "parse_error": parse_error, "error": error,
            "call": {"role": role, "pass_number": pass_number, "model": "m-" + role, "wall_s": 1.0,
                     "rationale": None, "metrics": {"load_duration": 0}}}


def _write_arm(run_dir: Path, arm: str, spec: dict):
    """spec: {pid: (d1, d2, verifier_decision_or_None)}; d values may be 'PARSE'."""
    pck = lib.Checkpoint(run_dir / f"arm_{arm}" / "primary.jsonl")
    vck = lib.Checkpoint(run_dir / f"arm_{arm}" / "verifier.jsonl")
    for pid, (d1, d2, v) in spec.items():
        c1 = _call(lib.PRIMARY, 1, parse_error="bad" if d1 == "PARSE" else None,
                   decision=None if d1 == "PARSE" else d1)
        calls = [c1]
        if d1 == "PARSE":
            outcome = lib.FLAGGED
        else:
            c2 = _call(lib.PRIMARY, 2, parse_error="bad" if d2 == "PARSE" else None,
                       decision=None if d2 == "PARSE" else d2)
            calls.append(c2)
            outcome = lib.FLAGGED if d2 == "PARSE" else lib.combine_primary(d1, d2)
        pck.result({"paper_id": pid, "outcome": outcome, "d1": None if d1 == "PARSE" else d1,
                    "d2": None if d1 == "PARSE" or d2 == "PARSE" else d2,
                    "d1_ne_d2": d1 not in ("PARSE",) and d2 not in ("PARSE", None) and d1 != d2,
                    "parse_error": "PARSE" in (d1, d2), "error": False, "calls": calls,
                    "reprocessed_after_interrupt": False})
        if v is not None:
            vck.result({"paper_id": pid, "outcome": lib.verifier_outcome(v), "decision": v, "forced": False,
                        "parse_error": False, "error": False, "calls": [_call(lib.VERIFIER, 1, decision=v)],
                        "reprocessed_after_interrupt": False})


LABELS = {1: "include", 2: "include", 3: "exclude", 4: "exclude", 5: "exclude"}


def _labels():
    return {p: {"ee": f"EE-{p:03d}", "title": f"t{p}", "pi": pi, "pi_notes": "", "category": "",
                "wb_primary": "include", "wb_primary_reasoning": "r", "wb_verifier": "exclude",
                "wb_verifier_reasoning": "r"} for p, pi in LABELS.items()}


def _recs(tmp_path, arm, spec):
    _write_arm(tmp_path, arm, spec)
    p, v = sc.load_arm(tmp_path, arm)
    return sc.arm_records(_labels(), p, v), p, v


def test_final_outcomes_and_the_three_flagged_treatments(tmp_path):
    recs, p, v = _recs(tmp_path, "A", {
        1: ("include", "include", "include"),   # IN   | PI include  -> TP
        2: ("include", "include", "exclude"),   # FLAG | PI include
        3: ("exclude", "exclude", None),        # OUT  | PI exclude  -> TN
        4: ("include", "exclude", None),        # FLAG | PI exclude
        5: ("include", "include", "include"),   # IN   | PI exclude  -> FP
    })
    assert {k: r["final"] for k, r in recs.items()} == {1: lib.IN, 2: lib.FLAGGED, 3: lib.OUT, 4: lib.FLAGGED, 5: lib.IN}
    labels = _labels()
    as_inc = sc.binary_metrics(recs, labels, "include")
    assert (as_inc["tp"], as_inc["fn"], as_inc["tn"], as_inc["fp"]) == (2, 0, 1, 2)
    as_exc = sc.binary_metrics(recs, labels, "exclude")
    assert (as_exc["tp"], as_exc["fn"], as_exc["tn"], as_exc["fp"]) == (1, 1, 2, 1)
    dec = sc.binary_metrics(recs, labels, None)
    assert (dec["tp"], dec["fn"], dec["tn"], dec["fp"], dec["dropped_flagged"]) == (1, 0, 1, 1, 2)
    assert dec["sensitivity"] == 1.0 and dec["specificity"] == 0.5 and dec["balanced_accuracy"] == 0.75

    t = sc.confusion(recs, labels)
    assert t[lib.IN] == {"include": 1, "exclude": 1} and t[lib.FLAGGED] == {"include": 1, "exclude": 1}

    r = sc.rates(recs)
    assert r["flag_rate"] == 0.4
    assert (r["d1_ne_d2"], r["d1_ne_d2_of"]) == (1, 5)
    assert (r["verifier_overturned"], r["verifier_ran"]) == (1, 3)
    assert sc.check_call_pattern(labels, p, v) == []


def test_parse_errors_count_per_call_and_pass_two_is_not_expected_after_a_bad_pass_one(tmp_path):
    recs, p, v = _recs(tmp_path, "A", {
        1: ("PARSE", None, None), 2: ("include", "PARSE", None),
        3: ("exclude", "exclude", None), 4: ("exclude", "exclude", None), 5: ("exclude", "exclude", None),
    })
    r = sc.rates(recs)
    assert r["parse_error_calls"] == 2 and r["calls"] == 9 and r["papers_with_parse_error"] == 2
    assert sc.check_call_pattern(_labels(), p, v) == []


def test_call_pattern_gate_catches_each_violation(tmp_path):
    _write_arm(tmp_path, "A", {1: ("include", "include", "include"), 2: ("exclude", "exclude", "include"),
                               3: ("include", "include", None), 4: ("exclude", "exclude", None)})
    p, v = sc.load_arm(tmp_path, "A")
    problems = sc.check_call_pattern(_labels(), p, v)
    assert "paper 2: verifier called on primary OUT" in problems
    assert "paper 3: primary IN with no verifier result" in problems
    assert "paper 5: no primary result" in problems


def test_agreement_and_kappa():
    x = {1: {"final": "IN"}, 2: {"final": "OUT"}, 3: {"final": "OUT"}, 4: {"final": "FLAGGED"}}
    y = {1: {"final": "IN"}, 2: {"final": "OUT"}, 3: {"final": "FLAGGED"}, 4: {"final": "FLAGGED"}}
    a = sc.agreement(x, y, "final")
    assert (a["n"], a["agree"], a["pct"]) == (4, 3, 0.75)
    assert a["kappa"] == pytest.approx(0.6364, abs=1e-4)
    assert sc.cohen_kappa(["IN", "IN"], ["IN", "IN"]) == 1.0


def test_flagged_is_never_pi_consistent_and_lists_use_final_outcome(tmp_path):
    labels = _labels()
    base = {1: ("include", "include", "include"), 2: ("include", "include", "include"),
            3: ("exclude", "exclude", None), 4: ("exclude", "exclude", None), 5: ("exclude", "exclude", None)}
    recs = {}
    for arm, over in (("A", {}), ("B", {2: ("include", "include", "exclude")}), ("C", {4: ("include", "exclude", None)})):
        recs[arm], _, _ = _recs(tmp_path, arm, {**base, **over})
    lists = sc.disagreement_lists(labels, recs)
    assert lists == {"any_arm_vs_PI": [2, 4], "A_vs_B": [2], "B_vs_C": [2, 4]}
    rows = sc.disagreement_rows(lists["any_arm_vs_PI"], labels, recs)
    assert rows[0]["B_final"] == lib.FLAGGED and rows[0]["B_rationale"].startswith("verifier:")


def test_wilson_interval_bounds():
    lo, hi = sc.wilson(10, 10)
    assert hi == 1.0 and 0.69 < lo < 0.73
    assert sc.wilson(0, 0) == (None, None)


def test_scorer_source_has_no_database_reference():
    tree = ast.parse((REPO / "analysis" / "eval" / "score_screen2f.py").read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            assert "review.db" not in node.value
        if isinstance(node, ast.Name):
            assert node.id != "ReviewDatabase"
        if isinstance(node, ast.ImportFrom):
            assert node.module != "engine.core.database"
