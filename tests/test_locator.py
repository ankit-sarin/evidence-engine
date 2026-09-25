"""The shared deterministic locator (R17; WRITE-PATH-01 9b-2d T1, T2, G3, R6)."""

from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path

import pytest

from engine.core import locator as L

REPO = Path(__file__).resolve().parent.parent
TEXT = ("The trial enrolled forty patients at two centres. The robot performed "
        "autonomous suturing on porcine tissue in ten trials. Outcomes were "
        "measured at six months.")


# ── T1 ────────────────────────────────────────────────────────────────
def test_t1_exact_match():
    r = L.locate(TEXT, "the  ROBOT performed autonomous suturing")
    assert (r.located, r.kind, r.score, r.snippet_supplied) == (True, L.EXACT, 1.0, True)


def test_t1_fuzzy_above_the_threshold():
    r = L.locate(TEXT, "The robot performd autonomous suturing on porcine tisue in ten trials.")
    assert r.located and r.kind == L.FUZZY
    assert L.FUZZY_THRESHOLD < r.score < 1.0


def test_t1_fuzzy_below_the_threshold():
    r = L.locate(TEXT, "Conducted in Norway by the authors of this study.")
    assert (r.located, r.kind) == (False, L.NONE)
    assert r.score is not None and r.score <= L.FUZZY_THRESHOLD


@pytest.mark.parametrize("snippet", [None, "", "   "])
def test_t1_no_snippet_is_not_located_and_says_so(snippet):
    r = L.locate(TEXT, snippet)
    assert (r.located, r.kind, r.score, r.snippet_supplied) == (False, L.NONE, None, False)


@pytest.mark.parametrize("snippet", ["   ", "\t\n", "\u00a0\u2003", " \r\n "])
def test_r8_a_snippet_that_normalises_to_nothing_is_not_located(snippet):
    """R8 (9b-2d): whitespace-only or empty after normalisation — never an
    'exact' match of the empty string, which the legacy grep reported."""
    assert L.normalize(snippet) == ""
    r = L.locate(TEXT, snippet)
    assert (r.located, r.kind, r.score, r.snippet_supplied, r.bridged) == \
        (False, L.NONE, None, False, False)


def test_t1_a_sentinel_claim_with_no_snippet_takes_the_identical_test():
    """R17: the locator never sees the value, so a sentinel is not special."""
    assert L.locate(TEXT, "") == L.locate(TEXT, None)


def test_the_threshold_is_strict():
    r = L.locate(TEXT, "The robot performd autonomous suturing on porcine tisue in ten trials.")
    at = L.locate(TEXT, "The robot performd autonomous suturing on porcine tisue in ten trials.",
                  threshold=r.score)
    assert at.located is False       # equal to the score is not above it


def test_a_bridged_snippet_is_marked():
    r = L.locate(TEXT, "The trial enrolled forty patients ... in ten trials.")
    assert r.bridged is True and r.snippet_supplied


def test_the_payload_carries_what_r17_names():
    r = L.locate(TEXT, "forty patients")
    p = L.locate_payload(r, threshold=L.FUZZY_THRESHOLD, parsed_text_sha256="a" * 64,
                         parsed_text_uid="u")
    assert set(p) == {"located", "kind", "score", "threshold", "locator_version",
                      "snippet_supplied", "bridged", "parsed_text_sha256", "parsed_text_uid"}
    assert p["locator_version"] == L.LOCATOR_VERSION == "locator-1"


# ── R6: verdict-identical to the legacy grep, and the names stay importable ─
CASES = [
    "the robot performed autonomous suturing", "The robot performd autonomous suturing on "
    "porcine tisue in ten trials.", "Conducted in Norway.", "", "Outcomes were measured",
    "outcomes were measured at six month", "Table.I shows", "enrolled forty",
]


@pytest.mark.parametrize("snippet", CASES)
def test_located_equals_the_legacy_grep_verdict(snippet):
    from analysis.provenance.legacy import grep_verify_fast
    from engine.agents.auditor import grep_verify
    assert L.locate(TEXT, snippet).located == grep_verify_fast(snippet, TEXT) \
        == grep_verify(snippet, TEXT)


def test_the_auditors_normalize_is_the_locators():
    from engine.agents import auditor
    assert auditor._normalize is L.normalize


# ── T2 / G3 ───────────────────────────────────────────────────────────
def _as_tuple(r):
    return [r.located, r.kind, r.score, r.snippet_supplied, r.bridged]


def test_t2_identical_output_across_calls_and_processes():
    here = [_as_tuple(L.locate(TEXT, s)) for s in CASES]
    assert here == [_as_tuple(L.locate(TEXT, s)) for s in CASES]
    code = ("import json,sys; from engine.core import locator as L; "
            "t, cs = json.loads(sys.stdin.read()); "
            "print(json.dumps([[r.located, r.kind, r.score, r.snippet_supplied, r.bridged] "
            "for r in (L.locate(t, s) for s in cs)]))")
    out = subprocess.run([sys.executable, "-c", code], input=json.dumps([TEXT, CASES]),
                         capture_output=True, text=True, cwd=REPO, check=True).stdout
    assert json.loads(out) == here


def test_g3_the_locator_imports_no_agent_and_no_database():
    tree = ast.parse((REPO / "engine" / "core" / "locator.py").read_text())
    mods = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)} | \
        {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    assert not any(m and (m.startswith("engine.agents") or m == "sqlite3"
                          or m.startswith("engine.core.database")) for m in mods), mods
