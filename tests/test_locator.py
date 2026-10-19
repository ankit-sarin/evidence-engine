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
    assert p["locator_version"] == L.LOCATOR_VERSION == "locator-1.1"   # U14, R675(f)


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


# ── U14 (R675): INT-i12-7 (autojunk) and INT-i12-8 (bridged) ──────────
# The fixtures are 12s_U14A's reproducer (docs/session-reports/session-12/12s_U14A/repro_u14a.py),
# copied: a 70-word passage of 382 characters inside a surrounding text.
import random  # noqa: E402

U14_PASSAGE = (
    "We used the robotic platform to suture the small bowel in twelve adult pigs. The arm held a "
    "curved needle with braided nylon thread. Each knot was tied by the arm, then trimmed by a nurse "
    "after every run. We observed no leakage at either test pressure in any animal. The mean time "
    "per stitch was between six and nine seconds, and no procedure was stopped early or finished manually.")
U14_TEXT = ("The authors describe the experimental platform in detail and report its calibration "
            "procedure before the main experiments. " + U14_PASSAGE +
            " The remaining results are presented in the following tables and are discussed with "
            "reference to previous reports.")
U14_EDIT = {"twelve": "eleven", "braided": "plaited", "nurse": "nurses"}
U14_LOST_BEFORE = (70, 108, 128, 200, 288, 298)   # 12s_U14B variant_measurement.md §2


def _edit(s, edits):
    return " ".join(edits.get(w, w) for w in s.split())


def _sweep():
    """12s_U14A's 300 seeded 3-word edits ("zq" for each changed word's first letter)."""
    rng, words, out = random.Random(20261019), U14_PASSAGE.split(), []
    for _ in range(300):
        w = list(words)
        for i in rng.sample(range(len(w)), 3):
            w[i] = "zq" + w[i][1:]
        out.append(" ".join(w))
    return out


def test_u14_the_worst_draw_of_the_sweep_is_located():
    """§11 test 1: the draw that scored 0.8136 under autojunk is located, 0.9883."""
    r = L.locate(U14_TEXT, _sweep()[288])
    assert (r.located, r.kind, round(r.score, 4)) == (True, L.FUZZY, 0.9883)


def test_u14_a_fuzzy_fixture_of_300_or_more_characters_is_located():
    """§11 test 2 and R675(h)(1): a 383-character, 3-word edit, at the threshold of record."""
    snippet = _edit(U14_PASSAGE, U14_EDIT)
    assert len(snippet) >= 300 and L.FUZZY_THRESHOLD == 0.85
    r = L.locate(U14_TEXT, snippet)
    assert (r.located, r.kind, round(r.score, 4), r.bridged) == (True, L.FUZZY, 0.9856, False)


def test_u14_a_long_unchanged_quote_is_still_exact():
    """§11 test 3."""
    r = L.locate(U14_TEXT, U14_PASSAGE)
    assert (r.located, r.kind, r.score) == (True, L.EXACT, 1.0)


@pytest.mark.parametrize("snippet, score", [
    (_edit(U14_PASSAGE[:150].rsplit(" ", 1)[0], U14_EDIT), 0.9658),
    (" ".join(_sweep()[0].split()[:27]), None),
])
def test_u14_below_200_characters_the_score_is_unchanged(snippet, score):
    """§11 test 4: under 200 characters the heuristic never applied, so the re-scored ratio is the
    ratio the heuristic gave."""
    from difflib import SequenceMatcher
    assert len(snippet) < 200
    r = L.locate(U14_TEXT, snippet)
    ns = L.normalize(snippet); tw = L.normalize(U14_TEXT).split(); n = len(ns.split())
    before = max(SequenceMatcher(None, ns, " ".join(tw[i:i + n])).ratio()
                 for i in range(len(tw) - n + 1))
    assert r.score == before
    if score is not None:
        assert round(r.score, 4) == score


def test_u14_a_long_snippet_with_no_counterpart_stays_not_located():
    """§11 test 5: re-scoring without the heuristic does not lift a true miss over the threshold."""
    snippet = ("Patients were randomised to laparoscopic or open repair and followed for two years "
               "with clinical examination and ultrasound at each visit, and the primary outcome was "
               "hernia recurrence assessed by a blinded surgeon at the final visit, while secondary "
               "outcomes included pain scores, return to work and the cost of the index admission.")
    assert len(snippet) >= 300
    r = L.locate(U14_TEXT, snippet)
    assert (r.located, r.kind) == (False, L.NONE) and r.score <= L.FUZZY_THRESHOLD


def test_u14_a_seeded_30_edit_subset_of_the_sweep_is_all_located():
    """R675(h)(2): the six edits the heuristic lost, and 24 more drawn with a fixed seed."""
    sweep = _sweep()
    rest = [i for i in range(300) if i not in U14_LOST_BEFORE]
    subset = sorted(U14_LOST_BEFORE + tuple(random.Random(30).sample(rest, 24)))
    assert len(subset) == 30
    assert [i for i in subset if not L.locate(U14_TEXT, sweep[i]).located] == []


U14_ELLIPSIS_TEXTS = {
    "…": ("Patients were followed for twelve months… Two were lost to follow-up. "
          "The device was removed in three cases. Complications were minor."),
    "...": ("The protocol was revised twice... Recruitment closed in March. "
            "The device was removed in three cases. Complications were minor."),
}
U14_PLAIN_TEXT = ("Patients were followed for twelve months. Two were lost to follow-up. "
                  "The device was removed in three cases. Complications were minor.")


@pytest.mark.parametrize("mark, quote", [
    ("…", "Patients were followed for twelve months… Two were lost to follow-up."),
    ("...", "The protocol was revised twice... Recruitment closed in March."),
])
def test_u14_a_verbatim_quote_containing_an_ellipsis_is_not_bridged(mark, quote):
    """§11 test 6 and R675(h)(3): the ellipsis is the paper's, not the extractor's."""
    r = L.locate(U14_ELLIPSIS_TEXTS[mark], quote)
    assert (r.located, r.kind, r.score, r.bridged) == (True, L.EXACT, 1.0, False)


def test_u14_a_snippet_bridging_passages_with_an_ellipsis_is_still_bridged():
    """§11 test 7 and R675(h)(4): the case the regex exists for."""
    r = L.locate(U14_PLAIN_TEXT, "Two were lost to follow-up ... Complications were minor.")
    assert (r.located, r.bridged) == (False, True)


def test_u14_a_quote_without_an_ellipsis_is_unchanged():
    """§11 test 8: the control."""
    r = L.locate(U14_ELLIPSIS_TEXTS["…"], "Two were lost to follow-up.")
    assert (r.located, r.kind, r.score, r.bridged) == (True, L.EXACT, 1.0, False)


def test_u14_a_fuzzy_match_keeps_bridged_only_when_the_window_lacks_the_ellipsis():
    """INT-i12-8 on the fuzzy path: a lightly misspelt quote of a passage with an ellipsis."""
    text = U14_ELLIPSIS_TEXTS["..."]
    r = L.locate(text, "The protocol was revisd twice... Recruitment closed in March.")
    assert (r.located, r.kind, r.bridged) == (True, L.FUZZY, False)


# The pre-existing inexact fixtures (12s_U14A §4), each with the outcome and score it had at
# locator-1: every one is under 200 characters, so nothing about it may move.
U14_AUDITOR_TEXT = (
    "This study evaluates the Smart Tissue Autonomous Robot (STAR) system "
    "for autonomous suturing. A randomized controlled trial was conducted "
    "with 20 trials on porcine tissue. The system achieved Level 3 autonomy "
    "per the Yang et al. taxonomy. Suture placement accuracy was 95.2% "
    "compared to 91.0% for expert surgeons. No tissue damage events were "
    "reported during autonomous operation.")


@pytest.mark.parametrize("text, snippet, located, score", [
    (TEXT, "The robot performd autonomous suturing on porcine tisue in ten trials.", True, 0.9859),
    (TEXT, "Conducted in Norway by the authors of this study.", False, 0.3551),
    (TEXT, "Conducted in Norway.", False, 0.4242),
    (TEXT, "outcomes were measured at six month", True, 1.0),
    (TEXT, "Table.I shows", False, 0.3846),
    (U14_AUDITOR_TEXT, "20 trials on porcme tissue", True, 0.9259),
    (U14_AUDITOR_TEXT, "500 experiments on synthetic phantoms with laser guidance", False, 0.3717),
    (U14_AUDITOR_TEXT, "The experiment was performed on cadaveric specimens", False, 0.4211),
    (U14_AUDITOR_TEXT, "Cooking temperature affects protein denaturation", False, 0.3678),
    (U14_AUDITOR_TEXT, "This snippet does not exist in the paper text at all.", False, 0.4),
])
def test_u14_every_pre_existing_fuzzy_fixture_is_unchanged(text, snippet, located, score):
    """R675(h): F3's ten fixtures, outcome and score as 12s_U14A measured them."""
    r = L.locate(text, snippet)
    assert (r.located, round(r.score, 4)) == (located, score)
