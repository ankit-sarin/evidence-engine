"""T1 — SCREEN-AUTH-01 Phase 2f harness, with a stubbed client throughout.

Covers arm construction, the pipeline's combination rules as the harness
reproduces them, resume from checkpoint, the quiet window, the gate-1 identity
check, and the harness-never-names-the-database rule. The worker is exercised
end to end in a subprocess against a fake engine tree, which is also how the
one-tree-per-process mechanism is tested: a worker whose `engine` resolves
outside --root must refuse.
"""

import ast
import json
import subprocess
import sys
import textwrap
from datetime import datetime, timezone
from pathlib import Path

import pytest

from analysis.eval import screen2f as lib
from engine.core.review_spec import load_review_spec

REPO = Path(__file__).resolve().parents[1]
LIVE_SPEC = REPO / "review_specs" / "surgical_autonomy.yaml"
WORKER = REPO / "analysis" / "eval" / "screen2f_worker.py"
HARNESS_SOURCES = [
    REPO / "analysis" / "eval" / "screen2f.py",
    REPO / "analysis" / "eval" / "screen2f_worker.py",
    REPO / "analysis" / "eval" / "run_screen2f.py",
]
ARM_C_SCREENING_HASH = "e23b74de9275df8655d3efeacc2ca769542ac41c325d9691c1b2fac11b0f0120"


# ── arm construction ─────────────────────────────────────────────────


def test_arms_root_each_arm_in_its_own_tree(tmp_path):
    from engine.core.review_paths import spec_path_for

    a = lib.arms(REPO, tmp_path / "wt", spec_path_for("surgical_autonomy"), tmp_path / "c.yaml")
    assert a["A"].root == (tmp_path / "wt").resolve()
    assert a["A"].spec == (tmp_path / "wt").resolve() / "review_specs" / "surgical_autonomy.yaml"
    assert a["B"].root == a["C"].root == REPO
    assert a["B"].spec == LIVE_SPEC
    assert a["C"].spec == (tmp_path / "c.yaml").resolve()


def test_arm_c_spec_adds_exactly_four_stage_lines(tmp_path):
    live = LIVE_SPEC.read_text()
    variant = lib.make_arm_c_spec_text(live)
    live_lines, var_lines = live.splitlines(), variant.splitlines()
    assert len(var_lines) == len(live_lines) + 4
    import difflib

    changes = [ln for ln in difflib.ndiff(live_lines, var_lines) if ln[:2] in ("+ ", "- ")]
    assert changes == ["+     - abstract_primary"] * 4

    path = tmp_path / "armC.yaml"
    path.write_text(variant)
    c, b = load_review_spec(path), load_review_spec(LIVE_SPEC)
    assert c.screening_hash() == ARM_C_SCREENING_HASH
    changed = [x.id for x, y in zip(b.eligibility.criteria, c.eligibility.criteria) if x != y]
    assert changed == list(lib.ARM_C_ADDED_CRITERIA)
    assert {k: v for k, v in b.model_dump().items() if k != "eligibility"} == \
           {k: v for k, v in c.model_dump().items() if k != "eligibility"}


def test_arm_c_variant_refuses_to_be_built_twice():
    with pytest.raises(ValueError, match="already at abstract_primary"):
        lib.make_arm_c_spec_text(lib.make_arm_c_spec_text(LIVE_SPEC.read_text()))


# ── combination rules (A2) ───────────────────────────────────────────


@pytest.mark.parametrize("d1,d2,want", [
    ("include", "include", lib.IN),
    ("exclude", "exclude", lib.OUT),
    ("include", "exclude", lib.FLAGGED),
    ("exclude", "include", lib.FLAGGED),
])
def test_primary_agreement_rule(d1, d2, want):
    assert lib.combine_primary(d1, d2) == want


def test_verifier_and_final_rules():
    assert lib.verifier_outcome("include") == lib.IN
    assert lib.verifier_outcome("exclude") == lib.FLAGGED
    assert lib.final_outcome(lib.OUT, None) == lib.OUT
    assert lib.final_outcome(lib.FLAGGED, None) == lib.FLAGGED
    assert lib.final_outcome(lib.IN, lib.IN) == lib.IN
    assert lib.final_outcome(lib.IN, lib.FLAGGED) == lib.FLAGGED
    assert lib.final_outcome(lib.IN, None) is None


class StubScreen:
    """screen(paper, pass_number, role) from a script; records every call."""

    def __init__(self, script):
        self.script = script  # {(paper_id, pass_number, role): "include"|"exclude"|"PARSE"|"ERROR"}
        self.calls = []

    def __call__(self, paper, pass_number, role):
        self.calls.append((paper["id"], pass_number, role))
        v = self.script[(paper["id"], pass_number, role)]
        o = {"decision": None, "rationale": None, "confidence": None, "parse_error": None, "error": None,
             "call": {"role": role, "pass_number": pass_number}}
        if v == "PARSE":
            o["parse_error"] = "JSONDecodeError"
        elif v == "ERROR":
            o["error"] = "TimeoutError"
        else:
            o.update(decision=v, rationale="r", confidence=0.9)
        return o


def _papers(*ids):
    return [{"id": i, "title": f"t{i}", "abstract": f"a{i}"} for i in ids]


def _run_primary(tmp_path, papers, stub, blocked=lambda: False):
    return lib.run_primary(papers, lib.Checkpoint(tmp_path / "primary.jsonl"), stub,
                           blocked=blocked, log=lambda m: None, meta={"arm": "X"})


def test_primary_phase_follows_the_runner_call_pattern(tmp_path):
    P = lib.PRIMARY
    stub = StubScreen({
        (1, 1, P): "include", (1, 2, P): "include",
        (2, 1, P): "exclude", (2, 2, P): "exclude",
        (3, 1, P): "include", (3, 2, P): "exclude",
        (4, 1, P): "PARSE",
        (5, 1, P): "include", (5, 2, P): "PARSE",
        (6, 1, P): "ERROR",
    })
    assert _run_primary(tmp_path, _papers(1, 2, 3, 4, 5, 6), stub) == "done"
    results, interrupted = lib.Checkpoint(tmp_path / "primary.jsonl").load()
    assert {k: v["outcome"] for k, v in results.items()} == {
        1: lib.IN, 2: lib.OUT, 3: lib.FLAGGED, 4: lib.FLAGGED, 5: lib.FLAGGED, 6: lib.ERROR}
    # A malformed or failed pass 1 means pass 2 is never called; nothing is retried.
    assert stub.calls == [(1, 1, P), (1, 2, P), (2, 1, P), (2, 2, P), (3, 1, P), (3, 2, P),
                          (4, 1, P), (5, 1, P), (5, 2, P), (6, 1, P)]
    assert results[3]["d1_ne_d2"] is True and results[1]["d1_ne_d2"] is False
    assert results[4]["parse_error"] and results[6]["error"] and not interrupted


def test_verifier_runs_on_primary_in_only_and_lists_forced_calls(tmp_path):
    P, V = lib.PRIMARY, lib.VERIFIER
    stub = StubScreen({(1, 1, P): "include", (1, 2, P): "include",
                       (2, 1, P): "exclude", (2, 2, P): "exclude",
                       (3, 1, P): "include", (3, 2, P): "include",
                       (1, 1, V): "include", (3, 1, V): "exclude", (2, 1, V): "include"})
    papers = _papers(1, 2, 3)
    _run_primary(tmp_path, papers, stub)
    primary, _ = lib.Checkpoint(tmp_path / "primary.jsonl").load()

    targets = lib.verifier_targets(papers, primary)
    assert [(p["id"], f) for p, f in targets] == [(1, False), (3, False)]
    forced = lib.verifier_targets(papers, primary, forced_ids=[2])
    assert [(p["id"], f) for p, f in forced] == [(1, False), (2, True), (3, False)]

    lib.run_verifier(forced, lib.Checkpoint(tmp_path / "verifier.jsonl"), stub,
                     blocked=lambda: False, log=lambda m: None, meta={})
    ver, _ = lib.Checkpoint(tmp_path / "verifier.jsonl").load()
    assert {k: (v["outcome"], v["forced"]) for k, v in ver.items()} == {
        1: (lib.IN, False), 2: (lib.IN, True), 3: (lib.FLAGGED, False)}
    assert [c for c in stub.calls if c[2] == V] == [(1, 1, V), (2, 1, V), (3, 1, V)]


def test_verifier_refuses_an_incomplete_primary_phase():
    with pytest.raises(ValueError, match="incomplete"):
        lib.verifier_targets(_papers(1, 2), {1: {"outcome": lib.IN}})


# ── resume ───────────────────────────────────────────────────────────


def test_pause_and_resume_process_every_paper_exactly_once(tmp_path):
    P = lib.PRIMARY
    script = {(i, n, P): "include" for i in range(1, 6) for n in (1, 2)}
    stub = StubScreen(script)
    started = []

    def blocked_after_two():
        started.append(1)
        return len(started) > 2

    assert _run_primary(tmp_path, _papers(1, 2, 3, 4, 5), stub, blocked_after_two) == "paused"
    assert _run_primary(tmp_path, _papers(1, 2, 3, 4, 5), stub) == "done"
    results, interrupted = lib.Checkpoint(tmp_path / "primary.jsonl").load()
    assert sorted(results) == [1, 2, 3, 4, 5] and not interrupted
    assert len(stub.calls) == 10 and len(set(stub.calls)) == 10
    assert not any(r["reprocessed_after_interrupt"] for r in results.values())


def test_a_claim_without_a_result_is_reprocessed_and_says_so(tmp_path):
    ck = lib.Checkpoint(tmp_path / "primary.jsonl")
    ck.claim(1)  # a process died mid-paper
    stub = StubScreen({(1, 1, lib.PRIMARY): "exclude", (1, 2, lib.PRIMARY): "exclude"})
    _run_primary(tmp_path, _papers(1), stub)
    results, interrupted = ck.load()
    assert results[1]["reprocessed_after_interrupt"] is True and not interrupted


def test_two_results_for_one_paper_are_refused(tmp_path):
    ck = lib.Checkpoint(tmp_path / "p.jsonl")
    ck.result({"paper_id": 1, "outcome": lib.IN})
    ck.result({"paper_id": 1, "outcome": lib.OUT})
    with pytest.raises(ValueError, match="two results"):
        ck.load()


# ── quiet window (R5) ────────────────────────────────────────────────


@pytest.mark.parametrize("hhmm,blocked", [
    ("06:19", False), ("06:20", True), ("07:00", True), ("09:29", True), ("09:30", False), ("03:00", False),
])
def test_quiet_window_with_a_ten_minute_lead(hhmm, blocked):
    h, m = map(int, hhmm.split(":"))
    assert lib.in_quiet_window(datetime(2026, 9, 13, h, m, tzinfo=timezone.utc), 10) is blocked


# ── gate 1 (R4) ──────────────────────────────────────────────────────


def _identity(hash_, user="PICO\n\nEXCLUSION CRITERIA:\n  - x\n\nSPECIALTY", kwargs=None):
    return {"hash": hash_, "format": {"f": 1}, "call_kwargs": kwargs or {"options": {"temperature": 0}, "think": False},
            "messages": [{"role": "system", "content": "s"}, {"role": "user", "content": user}]}


def _good_identities():
    E = lib.EXPECTED_PLACEHOLDER_HASHES
    c_user = "PICO\n\nEXCLUSION CRITERIA:\n  - x\n  - a\n  - b\n  - c\n  - d\n\nSPECIALTY"
    return {
        "A": {lib.PRIMARY: _identity(E["A"][lib.PRIMARY]), lib.VERIFIER: _identity(E["A"][lib.VERIFIER])},
        "B": {lib.PRIMARY: _identity(E["B"][lib.PRIMARY]), lib.VERIFIER: _identity(E["B"][lib.VERIFIER])},
        "C": {lib.PRIMARY: _identity("c", user=c_user), lib.VERIFIER: _identity(E["B"][lib.VERIFIER])},
    }


def test_identity_gate_passes_on_the_expected_shape():
    assert lib.check_identity(_good_identities())["ok"]


def test_identity_gate_fails_when_c_differs_outside_the_exclusion_block():
    ids = _good_identities()
    ids["C"][lib.PRIMARY]["messages"][1]["content"] += "\nextra"
    failed = [c["check"] for c in lib.check_identity(ids)["checks"] if not c["ok"]]
    assert failed == ["C.primary differs from B only inside the exclusion block"]


def test_identity_gate_fails_on_a_drifted_hash_or_option():
    ids = _good_identities()
    ids["A"][lib.PRIMARY]["hash"] = "0" * 64
    ids["B"][lib.VERIFIER]["call_kwargs"] = {"options": {"temperature": 0.1}, "think": False}
    failed = {c["check"] for c in lib.check_identity(ids)["checks"] if not c["ok"]}
    assert "A.primary placeholder hash" in failed
    assert "verifier call options identical across arms" in failed


def test_the_real_head_placeholder_requests_match_frozen_r1_r2():
    """The capture definition reproduces the frozen pins from HEAD's own screener."""
    from unittest import mock

    from engine.agents import screener

    spec = load_review_spec(LIVE_SPEC)
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
    assert got == lib.EXPECTED_PLACEHOLDER_HASHES["B"]


# ── the harness never names the database ─────────────────────────────


def _database_references(source: str) -> list[str]:
    hits = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and "review.db" in node.value:
            hits.append(f"string:{node.value[:60]}")
        elif isinstance(node, ast.Name) and node.id == "ReviewDatabase":
            hits.append("name:ReviewDatabase")
        elif isinstance(node, ast.Attribute) and node.attr == "ReviewDatabase":
            hits.append("attr:ReviewDatabase")
        elif isinstance(node, ast.alias) and "ReviewDatabase" in node.name:
            hits.append("import:ReviewDatabase")
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            mod = getattr(node, "module", None) or ""
            names = [a.name for a in node.names]
            if mod == "engine.core.database" or "engine.core.database" in names:
                hits.append("import:engine.core.database")
    return hits


@pytest.mark.parametrize("path", HARNESS_SOURCES, ids=lambda p: p.name)
def test_harness_source_has_no_database_reference(path):
    assert _database_references(path.read_text()) == []


def test_the_database_reference_scan_can_fail():
    assert _database_references('P = "data/x/review.db"\nfrom engine.core.database import ReviewDatabase\n')


# ── worker, end to end, against a fake engine tree ───────────────────


_FAKE_SCREENER = '''
import json
from pydantic import BaseModel

def ollama_chat(**kw):
    raise AssertionError("the worker must interpose on this name")

class D(BaseModel):
    decision: str
    rationale: str
    confidence: float

def screen_paper(paper, spec, pass_number, model=None, role="primary"):
    resp = ollama_chat(model=model, messages=[{"role": "user", "content": paper["title"] + role}],
                       format={"schema": 1}, options={"temperature": 0}, think=False)
    return D.model_validate_json(resp.message.content)
'''

_FAKE_SPEC = '''
class _M:
    primary = "fake-8b"
    verification = "fake-27b"

class _S:
    screening_models = _M()
    def screening_hash(self):
        return "fakehash"

def load_review_spec(path):
    return _S()
'''

_FAKE_CLIENT_PATCH = '''
import engine.agents.screener as s
from types import SimpleNamespace

TABLE = {"t1primary": "include", "t2primary": "exclude", "t1verifier": "include"}

def fake_chat(**kw):
    key = kw["messages"][0]["content"]
    v = TABLE[key]
    return SimpleNamespace(message=SimpleNamespace(content=json.dumps({"decision": v, "rationale": "r", "confidence": 1.0})),
                           total_duration=1, load_duration=0, prompt_eval_count=1, prompt_eval_duration=1,
                           eval_count=1, eval_duration=1, done_reason="stop")
'''


def _fake_tree(root: Path) -> None:
    for pkg in ("engine", "engine/agents", "engine/core", "engine/utils"):
        (root / pkg).mkdir(parents=True, exist_ok=True)
        (root / pkg / "__init__.py").write_text("")
    # The fake client answers from a table; the worker wraps it as it wraps the real one.
    (root / "engine/agents/screener.py").write_text(
        _FAKE_SCREENER.replace('raise AssertionError("the worker must interpose on this name")',
                               "return fake_chat(**kw)")
        + textwrap.dedent(_FAKE_CLIENT_PATCH).replace("import engine.agents.screener as s\n", ""))
    (root / "engine/core/review_spec.py").write_text(_FAKE_SPEC)
    (root / "review_specs").mkdir(exist_ok=True)
    (root / "review_specs/surgical_autonomy.yaml").write_text("fake: true\n")


def _worker(root: Path, out: Path, papers: Path, phase: str, pythonpath: Path | None = None, extra=()):
    env = {"PYTHONPATH": str(pythonpath or root), "PATH": "/usr/bin:/bin", lib.NOW_ENV: "2026-09-13T03:00:00+00:00"}
    return subprocess.run(
        [sys.executable, "-P", str(WORKER), "--arm", "B", "--root", str(root),
         "--spec", str(root / "review_specs/surgical_autonomy.yaml"), "--phase", phase,
         "--papers", str(papers), "--out-dir", str(out), *extra],
        env=env, capture_output=True, text=True, timeout=60)


def test_worker_runs_both_phases_and_resumes_without_recalling(tmp_path):
    root, out = tmp_path / "tree", tmp_path / "out"
    _fake_tree(root)
    papers = tmp_path / "papers.jsonl"
    papers.write_text("\n".join(json.dumps(p) for p in _papers(1, 2)) + "\n")

    r = _worker(root, out, papers, "primary")
    assert r.returncode == 0, r.stderr
    primary, _ = lib.Checkpoint(out / "primary.jsonl").load()
    assert {k: v["outcome"] for k, v in primary.items()} == {1: lib.IN, 2: lib.OUT}
    call = primary[1]["calls"][0]["call"]
    assert call["call_kwargs"] == {"model": "fake-8b", "options": {"temperature": 0}, "think": False}
    assert len(call["request_hash"]) == 64 and call["raw"]

    r = _worker(root, out, papers, "verifier")
    assert r.returncode == 0, r.stderr
    ver, _ = lib.Checkpoint(out / "verifier.jsonl").load()
    assert {k: v["outcome"] for k, v in ver.items()} == {1: lib.IN}
    assert ver[1]["calls"][0]["call"]["model"] == "fake-27b"

    before = (out / "primary.jsonl").read_text()
    assert _worker(root, out, papers, "primary").returncode == 0
    assert (out / "primary.jsonl").read_text() == before  # resume: nothing re-called


def test_worker_refuses_an_engine_from_outside_its_root(tmp_path):
    root, other, out = tmp_path / "tree", tmp_path / "other", tmp_path / "out"
    _fake_tree(root)
    _fake_tree(other)
    papers = tmp_path / "papers.jsonl"
    papers.write_text(json.dumps(_papers(1)[0]) + "\n")
    r = _worker(root, out, papers, "primary", pythonpath=other)
    assert r.returncode == 4 and "REFUSED" in r.stderr
    assert not (out / "primary.jsonl").exists()


def test_worker_pauses_inside_the_quiet_window(tmp_path):
    root, out = tmp_path / "tree", tmp_path / "out"
    _fake_tree(root)
    papers = tmp_path / "papers.jsonl"
    papers.write_text(json.dumps(_papers(1)[0]) + "\n")
    env_now = "2026-09-13T07:00:00+00:00"
    r = subprocess.run(
        [sys.executable, "-P", str(WORKER), "--arm", "B", "--root", str(root),
         "--spec", str(root / "review_specs/surgical_autonomy.yaml"), "--phase", "primary",
         "--papers", str(papers), "--out-dir", str(out)],
        env={"PYTHONPATH": str(root), "PATH": "/usr/bin:/bin", lib.NOW_ENV: env_now},
        capture_output=True, text=True, timeout=60)
    assert r.returncode == 3, r.stderr
    assert not (out / "primary.jsonl").exists()
