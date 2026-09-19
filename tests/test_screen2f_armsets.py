"""SCREEN-AUTH-01 2g P3 — arm sets, tree rules, corrections, stability, the rule section.

The 2f arm set is frozen: `test_the_scorer_reproduces_the_committed_2f_outputs`
is the gate, and it runs the scorer over the committed 2f run with no flags.
Everything the 2g set adds is exercised here against fabricated records, except
arm D's placeholder pins, which are checked against what HEAD's screener really
renders — the D analogue of the arm-B constants test.
"""

import csv
import json
from pathlib import Path
from unittest import mock

import pytest

from analysis.eval import score_screen2f as sc
from analysis.eval import screen2f as lib

REPO = Path(__file__).resolve().parents[1]
TWOF = REPO / "docs" / "session-reports" / "screen-auth-2f-smoke"
VERDICTS = REPO / "docs" / "session-reports" / "screen-auth-2g" / "pi_verdicts_2f_part1.csv"


# ── the sets ──────────────────────────────────────────────────────────


def test_the_2f_set_is_unchanged():
    assert lib.ARMS == ("A", "B", "C")
    assert lib.arm_names("2f") == ("A", "B", "C")
    assert lib.pairs_for("2f") == (("A", "B"), ("B", "C"), ("A", "C"))
    assert lib.list_pairs_for("2f") == (("A", "B"), ("B", "C"))
    a, b, c = lib.arm_specs("2f")
    assert (a.commit, a.source, a.rule) == (lib.ARM_A_COMMIT, "worktree", "worktree_at_commit")
    assert (b.commit, b.source, b.rule) == (lib.ARM_B_COMMIT, "repo", "repo_inputs_equal_commit")
    assert (c.spec_kind, c.identity, c.rule) == ("arm_c_variant", "arm_c_derived", "shares_previous_tree")


def test_the_2g_set_is_a_b_d_with_two_worktrees_and_the_fold_in_the_repo():
    assert lib.arm_names("2g") == ("A", "B", "D")
    assert lib.pairs_for("2g") == (("A", "B"), ("B", "D"), ("A", "D"))
    assert lib.list_pairs_for("2g") == (("A", "B"), ("B", "D"), ("A", "D"))
    a, b, d = lib.arm_specs("2g")
    assert (a.commit, a.rule) == ("83defc5", "worktree_at_commit")
    assert (b.commit, b.rule) == ("61326fa", "worktree_at_commit")   # the exact 2f arm-B tree
    # D is its inputs, not a commit id: the pre-flight commit moves HEAD (H2 corrected).
    assert (d.commit, d.source, d.rule) == ("56c5c57", "repo", "repo_inputs_equal_commit")
    assert all(s.spec_kind == "live" and s.identity == "pins" for s in (a, b, d))


def test_an_unknown_arm_set_is_refused():
    with pytest.raises(ValueError, match="unknown arm set"):
        lib.arm_specs("nope")


def test_arms_root_each_arm_in_its_own_tree(tmp_path):
    spec_rel = Path("review_specs/surgical_autonomy.yaml")
    two_f = lib.arms(REPO, spec_rel, {"A": tmp_path / "wtA"}, tmp_path / "c.yaml", "2f")
    assert two_f["A"].root == (tmp_path / "wtA").resolve()
    assert two_f["A"].spec == (tmp_path / "wtA").resolve() / spec_rel
    assert two_f["B"].root == two_f["C"].root == REPO
    assert two_f["C"].spec == (tmp_path / "c.yaml").resolve()

    two_g = lib.arms(REPO, spec_rel, {"A": tmp_path / "wtA", "B": tmp_path / "wtB"}, None, "2g")
    assert two_g["A"].root == (tmp_path / "wtA").resolve()
    assert two_g["B"].root == (tmp_path / "wtB").resolve()
    assert two_g["B"].spec == (tmp_path / "wtB").resolve() / spec_rel
    assert two_g["D"].root == REPO and two_g["D"].spec == REPO / spec_rel


# ── tree rules ────────────────────────────────────────────────────────


def _facts(**kw):
    base = {"head": "sha", "commit_sha": "sha", "dirty": False,
            "inputs_differ_from_commit": False, "inputs_uncommitted": False}
    base.update(kw)
    return base


def test_a_worktree_arm_must_be_at_its_commit_and_clean():
    spec = lib.arm_specs("2g")[0]
    assert lib.tree_problem(spec, _facts()) is None
    assert "expected" in lib.tree_problem(spec, _facts(head="other"))
    assert "not clean" in lib.tree_problem(spec, _facts(dirty=True))


def test_arm_d_is_judged_on_its_inputs_and_survives_a_later_commit():
    spec = lib.arm_specs("2g")[2]
    # A commit that touches neither engine/ nor review_specs/ leaves D intact.
    assert lib.tree_problem(spec, _facts(head="a-later-commit")) is None
    assert "differ from 56c5c57" in lib.tree_problem(spec, _facts(inputs_differ_from_commit=True))
    assert "uncommitted" in lib.tree_problem(spec, _facts(inputs_uncommitted=True))


def test_the_head_equals_commit_rule_still_holds_where_a_set_declares_it():
    spec = lib.ArmSpec("X", "deadbee", "repo", "repo_head_equals_commit")
    assert lib.tree_problem(spec, _facts()) is None
    assert "repo tree is at" in lib.tree_problem(spec, _facts(head="other"))
    assert "uncommitted" in lib.tree_problem(spec, _facts(inputs_uncommitted=True))


def test_the_2f_repo_arm_is_judged_on_its_inputs_and_arm_c_rides_that_tree():
    b, c = lib.arm_specs("2f")[1], lib.arm_specs("2f")[2]
    assert lib.tree_problem(b, _facts()) is None
    assert "differ from 61326fa" in lib.tree_problem(b, _facts(inputs_differ_from_commit=True))
    assert "uncommitted" in lib.tree_problem(b, _facts(inputs_uncommitted=True))
    assert lib.tree_problem(c, _facts(head="anything", dirty=True)) is None


def test_an_unknown_tree_rule_is_refused():
    with pytest.raises(ValueError, match="unknown tree rule"):
        lib.tree_problem(lib.ArmSpec("X", "abc", "repo", "made_up"), _facts())


# ── identity ──────────────────────────────────────────────────────────


def _ident(h, kwargs=None):
    return {"hash": h, "format": {"f": 1}, "messages": [{"role": "system", "content": "s"},
                                                        {"role": "user", "content": "u"}],
            "call_kwargs": kwargs or {"options": {"temperature": 0}, "think": False}}


def _identities_2g(**over):
    E = lib.EXPECTED_PLACEHOLDER_HASHES
    out = {a: {lib.PRIMARY: _ident(E[a][lib.PRIMARY]), lib.VERIFIER: _ident(E[a][lib.VERIFIER])}
           for a in ("A", "B", "D")}
    out.update(over)
    return out


def test_the_2g_identity_gate_checks_three_arms_against_their_pins():
    gate = lib.check_identity(_identities_2g(), "2g")
    assert gate["ok"]
    names = [c["check"] for c in gate["checks"]]
    assert names == ["A.primary placeholder hash", "A.verifier placeholder hash",
                     "B.primary placeholder hash", "B.verifier placeholder hash",
                     "D.primary placeholder hash", "D.verifier placeholder hash",
                     "primary call options identical across arms",
                     "verifier call options identical across arms"]
    assert not any("C." in n for n in names)


def test_the_2g_identity_gate_fails_on_a_drifted_d_hash_or_option():
    bad = _identities_2g(D={lib.PRIMARY: _ident("0" * 64), lib.VERIFIER: _ident("1" * 64)})
    failed = {c["check"] for c in lib.check_identity(bad, "2g")["checks"] if not c["ok"]}
    assert failed == {"D.primary placeholder hash", "D.verifier placeholder hash"}

    drift = _identities_2g()
    drift["D"][lib.PRIMARY] = _ident(lib.EXPECTED_PLACEHOLDER_HASHES["D"][lib.PRIMARY],
                                     {"options": {"temperature": 0.1}, "think": False})
    failed = {c["check"] for c in lib.check_identity(drift, "2g")["checks"] if not c["ok"]}
    assert failed == {"primary call options identical across arms"}


def test_arm_d_pins_are_what_head_actually_renders():
    """Arm D's constants are HEAD's own placeholder requests, captured, not copied."""
    from engine.agents import screener
    from engine.core.review_spec import load_review_spec

    spec = load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")
    got = {}
    for role, pn in ((lib.PRIMARY, 1), (lib.VERIFIER, 2)):
        box = {}

        class Cap(Exception):
            pass

        def rec(**kw):
            box.update(kw)
            raise Cap()

        with mock.patch.object(screener, "ollama_chat", rec), pytest.raises(Cap):
            screener.screen_paper(dict(lib.PLACEHOLDER_PAPER), spec, pn, role=role)
        got[role] = lib.request_hash(box["format"], box["messages"])
    assert got == lib.EXPECTED_PLACEHOLDER_HASHES["D"]


# ── scorer: gate 1, corrections, stability, the rule ──────────────────


def test_the_scorer_reproduces_the_committed_2f_outputs(tmp_path):
    """Gate 1: no flags, the 2f set, byte for byte."""
    sc.score(TWOF / "full", sc.DEFAULT_WORKBOOK, tmp_path)
    committed = [p for p in TWOF.iterdir() if p.is_file() and p.suffix in (".csv", ".json", ".md")
                 and not p.name.startswith("papers_86")]
    assert len(committed) == 11
    for path in committed:
        assert (tmp_path / path.name).read_bytes() == path.read_bytes(), path.name


def _labels(pi):
    return {p: {"ee": f"EE-{p:03d}", "title": "t", "pi": v, "pi_notes": "", "category": "",
                "wb_primary": "include", "wb_primary_reasoning": "", "wb_verifier": "exclude",
                "wb_verifier_reasoning": ""} for p, v in pi.items()}


def test_corrections_replace_only_a_differing_label_and_record_the_change(tmp_path):
    path = tmp_path / "c.csv"
    path.write_text("paper_id,label_after_full,source,date\n569,exclude,sheet,2026-09-19\n"
                    "222,include,sheet,2026-09-19\n999,exclude,sheet,2026-09-19\n")
    labels = _labels({569: "include", 222: "include"})
    out, applied = sc.apply_corrections(labels, sc.load_corrections(path))
    assert out[569]["pi"] == "exclude" and out[222]["pi"] == "include"
    assert [(a["paper_id"], a["was"], a["now"]) for a in applied] == [(569, "include", "exclude")]
    assert applied[0]["source"] == "sheet" and applied[0]["date"] == "2026-09-19"


def test_the_committed_verdicts_file_carries_the_three_flips():
    rows = list(csv.DictReader(open(VERDICTS)))
    flips = [int(r["paper_id"]) for r in rows if r["april_label"] != r["label_after_full"]]
    assert flips == [569, 170, 702]
    assert sum(r["verdict"] == "SILENT" for r in rows) == 12
    assert sum(r["verdict"] == "EVIDENCED" for r in rows) == 5


def _recs(outcomes):
    return {p: {"final": v, "primary_outcome": v} for p, v in outcomes.items()}


def test_the_rule_section_counts_each_clause_and_reports_raw_numbers(tmp_path):
    rows = list(csv.DictReader(open(VERDICTS)))
    silent = [int(r["paper_id"]) for r in rows if r["verdict"] == "SILENT"]
    evidenced = [int(r["paper_id"]) for r in rows if r["verdict"] == "EVIDENCED"]
    # D: ten SILENT back (IN/FLAGGED), no EVIDENCED at IN, flag rate 10/86 vs A's 60/86.
    d = {p: (lib.IN if i < 10 else lib.OUT) for i, p in enumerate(silent)}
    d.update({p: lib.OUT for p in evidenced})
    a = dict(d)
    fill = [p for p in range(1, 200) if p not in d][:86 - len(d)]
    d.update({p: lib.OUT for p in fill})
    a.update({p: lib.FLAGGED for p in fill[:60]})
    a.update({p: lib.OUT for p in fill[60:]})
    recs = {"A": _recs(a), "D": _recs(d)}
    out = sc.rule_section(VERDICTS, recs, "D", "A")
    assert out["O2"]["count"] == 10 and out["O2"]["of"] == 12 and out["O2"]["pass"]
    assert out["O3"]["count_IN"] == 0 and out["O3"]["pass"] and out["O3"]["papers_FLAGGED_reported_only"] == []
    assert out["O4"]["pass"] and out["O4"]["D_flag_rate"] == 0.0
    assert out["verdict"] == "GO"

    recs["D"][evidenced[0]]["final"] = lib.IN          # one EVIDENCED reaches IN
    recs["D"][silent[0]]["final"] = lib.OUT            # and one SILENT stays out
    out = sc.rule_section(VERDICTS, recs, "D", "A")
    assert out["O2"]["count"] == 9 and not out["O2"]["pass"]
    assert out["O3"]["papers_IN"] == [evidenced[0]] and not out["O3"]["pass"]
    assert out["verdict"] == "NO-GO"


def test_a_flagged_evidenced_paper_is_reported_but_does_not_fail_clause_two():
    rows = list(csv.DictReader(open(VERDICTS)))
    evidenced = [int(r["paper_id"]) for r in rows if r["verdict"] == "EVIDENCED"]
    recs = {"A": _recs({p: lib.OUT for p in evidenced}),
            "D": _recs({**{p: lib.OUT for p in evidenced}, evidenced[0]: lib.FLAGGED})}
    out = sc.rule_section(VERDICTS, recs, "D", "A")
    assert out["O3"]["pass"] and out["O3"]["papers_FLAGGED_reported_only"] == [evidenced[0]]


def test_the_stability_control_compares_this_run_against_a_committed_one(tmp_path):
    (tmp_path / "arm_A_papers.csv").write_text(
        "paper_id,final_outcome\n1,IN\n2,OUT\n3,FLAGGED\n")
    recs = {"A": _recs({1: lib.IN, 2: lib.OUT, 3: lib.OUT})}
    out = sc.stability(tmp_path, recs, ["A"])["A"]
    assert (out["n"], out["agree"]) == (3, 2)
    assert out["disagreements"] == [{"arm": "A", "paper_id": 3, "baseline_final": "FLAGGED",
                                     "rerun_final": "OUT"}]


def test_a_missing_stability_baseline_is_reported_not_raised(tmp_path):
    assert "error" in sc.stability(tmp_path, {"A": _recs({1: lib.IN})}, ["A"])["A"]


def test_disagreement_list_names_follow_the_arm_set():
    labels = _labels({1: "include", 2: "exclude"})
    recs2g = {"A": _recs({1: lib.IN, 2: lib.OUT}), "B": _recs({1: lib.IN, 2: lib.FLAGGED}),
              "D": _recs({1: lib.OUT, 2: lib.OUT})}
    lists = sc.disagreement_lists(labels, recs2g, "2g")
    assert sorted(lists) == ["A_vs_B", "A_vs_D", "B_vs_D", "any_arm_vs_PI"]
    assert lists["B_vs_D"] == [1, 2] and lists["A_vs_D"] == [1]
