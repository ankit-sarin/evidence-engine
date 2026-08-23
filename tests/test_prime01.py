"""PRIME-01: the verbatim-window measure and the rank correlation.

The measure's whole value is comparability with QUALGAP-01 — its numbers get
tabulated next to that task's published 0.3% / 37.7%. So the walk is pinned
here: if someone "tidies" the asymmetric advance or the denominator, these tests
go red rather than the two studies silently drifting apart.
"""

from __future__ import annotations

import pytest

from analysis.eval.prime01 import (
    SNIPPET_RE,
    WINDOW_WORDS,
    measure,
    spearman,
    verbatim_window_rate,
)
from analysis.provenance.normalize import normalize


PAPER = normalize(
    "The robotic system achieved autonomous anastomosis in twelve consecutive "
    "porcine subjects with a mean operative time of forty one minutes. "
    "Complications were observed in two animals and both recovered fully."
)


# ── the window walk ──────────────────────────────────────────────────────


def test_window_size_is_eight_words():
    assert WINDOW_WORDS == 8


def test_verbatim_text_scores_near_total():
    """A draft that copies the paper should score close to every window."""
    hits, windows = verbatim_window_rate(
        "The robotic system achieved autonomous anastomosis in twelve consecutive "
        "porcine subjects with a mean operative time of forty one minutes.", PAPER)
    assert windows >= 2
    assert hits == windows


def test_paraphrase_scores_zero():
    """Reasoning prose about the paper is not the paper."""
    hits, _ = verbatim_window_rate(
        "I should consider whether the authors reported how long the procedure "
        "took and how many subjects were involved before deciding the value.",
        PAPER)
    assert hits == 0


def test_short_text_yields_no_windows_but_never_divides_by_zero():
    hits, windows = verbatim_window_rate("too short", PAPER)
    assert hits == 0
    assert windows == 1  # floored to 1 so the rate is 0.0, not a ZeroDivisionError


def test_empty_and_none_are_safe():
    for text in ("", None):
        hits, windows = verbatim_window_rate(text, PAPER)
        assert (hits, windows) == (0, 1)


def test_hit_advances_a_full_window_and_miss_advances_one_word():
    """Pinned: a matched run is counted once per window it fills, not per offset.

    Sliding by one after a hit would count a 16-word verbatim run ~9 times
    instead of 2 and inflate every rate in the table.
    """
    sixteen = " ".join(PAPER.split()[:16])
    hits, _ = verbatim_window_rate(sixteen, PAPER)
    assert hits == 2


def test_rate_is_hits_over_floored_window_count():
    stats = measure(1, "c", " ".join(PAPER.split()[:16]), PAPER)
    assert stats.windows == 2
    assert stats.rate == pytest.approx(100.0)


# ── character detection ──────────────────────────────────────────────────


def test_fenced_json_detected():
    assert measure(1, "c", '```json\n{"a": 1}\n```', PAPER).fenced_json is True
    assert measure(1, "c", "no fences here", PAPER).fenced_json is False


@pytest.mark.parametrize("draft", [
    '"source_snippet": "the robotic system achieved autonomous anastomosis"',
    "**source_snippet**: \"the robotic system achieved autonomous anastomosis\"",
])
def test_snippet_labels_counted_in_both_draft_shapes(draft):
    assert measure(1, "c", draft, PAPER).snippet_labels == 1


def test_narrative_text_has_no_snippet_labels():
    assert measure(1, "c", "I think the sample size was twelve.", PAPER).snippet_labels == 0


def test_snippet_regex_matches_analyze_qualgap01():
    """Both reports must answer 'does this enumerate fields' identically."""
    from analysis.eval.analyze_qualgap01 import _SNIPPET_RE as QG_RE
    assert SNIPPET_RE.pattern == QG_RE.pattern


# ── rank correlation ─────────────────────────────────────────────────────


def test_perfect_monotonic_is_one():
    assert spearman([1, 2, 3, 4], [10, 20, 30, 40]) == pytest.approx(1.0)


def test_perfect_inverse_is_minus_one():
    assert spearman([1, 2, 3, 4], [40, 30, 20, 10]) == pytest.approx(-1.0)


def test_monotonic_but_nonlinear_still_one():
    """Spearman is on ranks — that is why it is the right choice here."""
    assert spearman([1, 2, 3, 4], [1, 4, 9, 16]) == pytest.approx(1.0)


def test_ties_share_average_rank():
    assert spearman([1, 1, 2, 2], [1, 1, 2, 2]) == pytest.approx(1.0)


def test_constant_input_is_none_not_zero():
    """No variance means no correlation is defined; must not report 0.0."""
    assert spearman([5, 5, 5, 5], [1, 2, 3, 4]) is None


def test_too_few_pairs_is_none():
    assert spearman([1, 2], [1, 2]) is None


def test_mismatched_lengths_is_none():
    assert spearman([1, 2, 3], [1, 2]) is None
