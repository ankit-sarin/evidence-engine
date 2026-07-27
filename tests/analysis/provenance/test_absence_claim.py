"""Fixtures for ABSENCE_CLAIM (DEFINITIONS.md v1.1 §A).

Covers the six pinned patterns, the negatives they must not fire on, and both
directions of the precedence rule in §A3 — including the near-miss the amendment
was written around: a paper that itself says "no comparison was performed",
quoted verbatim, is ANCHORED, not ABSENCE_CLAIM.
"""

import pytest

from analysis.provenance import classifier as C
from analysis.provenance.absence import (
    ABSENCE_PATTERNS,
    BARE_SENTINEL_PATTERN,
    PATTERN_NAMES,
    is_absence_claim,
    match_absence_pattern,
)

# The paper deliberately contains its own absence statement, so the precedence
# rule can be exercised against real text rather than a contrived string.
PAPER = """
## METHODS

A pair of industrial CMOS cameras is used for online visual feedback data
acquisition with 640 x 480 resolution and 30 frames per second. The system was
evaluated on a tissue phantom in a bench setup.

## RESULTS

No comparison to a human operator was performed in this study. There was no
statistically significant difference between the two autonomous conditions.
No patients were enrolled, as the work is confined to the bench.
"""
# NB: "enrolled" contains the substring "nr" — that accident is the point of
# test_bare_sentinel_beats_anchoring, and it is why P4 overrides ANCHORED.


@pytest.fixture(scope="module")
def paper():
    return C.PaperIndex.build(1, PAPER)


# ── pattern detection ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "snippet,expected",
    [
        ("The paper does not explicitly state the sample size.", "P1_referent_negation"),
        ("This study does not report a comparison to human performance.", "P1_referent_negation"),
        ("The authors do not describe the selection process.", "P1_referent_negation"),
        ("No comparison reported.", "P2_bare_no_np"),
        ("No explicit selection process described.", "P2_bare_no_np"),
        ("No explicit comparison to human performance", "P2_bare_no_np"),
        ("Not explicitly stated.", "P3_not_explicitly"),
        ("Not explicitly mentioned in the paper.", "P3_not_explicitly"),
        (
            "The paper focuses on autonomous performance without comparing to human operators.",
            "P5_referent_without_report",
        ),
        ("Only one primary outcome was reported.", "P6_only_x_reported"),
        ("NR", "P4_bare_sentinel"),
        ("Not reported.", "P3_not_explicitly"),  # P3 precedes P4 in evaluation order
    ],
)
def test_positive_patterns(snippet, expected):
    assert match_absence_pattern(snippet) == expected


@pytest.mark.parametrize(
    "snippet",
    [
        # Rationales — assertions about the world, not about the reporting.
        "The robot uses sensors and cameras to monitor the fetoscope tip.",
        "The system generates a plan based on detected mental workload.",
        "The AFE has been tested in a bench setup and simulation environment.",
        # A reported result that happens to be negative — the rejected
        # `there is no` candidate (DEFINITIONS v1.1 §A4).
        "There was no statistically significant difference (all p > 0.05) between groups.",
        # Real paper prose containing a negation of a non-reporting verb.
        "In this study, the surgeon does not participate in the control process.",
        "This article does not contain any studies with human participants.",
        # `without` unbound to a paper referent — the rejected form in §A4.
        "The experiments were conducted on a skin phantom without specific clinical context.",
        # Ordinary quotation.
        "A pair of industrial CMOS cameras is used for online visual feedback.",
    ],
)
def test_negative_patterns(snippet):
    assert match_absence_pattern(snippet) is None


def test_every_pattern_is_named_and_documented():
    assert len(ABSENCE_PATTERNS) == 6
    assert len(set(PATTERN_NAMES)) == 6
    for name, rx, doc in ABSENCE_PATTERNS:
        assert name.startswith("P")
        assert rx.pattern
        assert len(doc) > 40, f"{name} lacks a documented rationale"


# ── precedence (v1.1 §A3) ────────────────────────────────────────────────


def test_anchored_beats_absence_patterns(paper):
    """The near-miss: the paper's OWN absence sentence, quoted verbatim.

    This is a real quotation and must classify ANCHORED, not ABSENCE_CLAIM,
    even though it matches P2.
    """
    snippet = "No comparison to a human operator was performed in this study."
    assert match_absence_pattern(snippet) == "P2_bare_no_np"
    res = C.classify_span(snippet, "No comparison reported", paper)
    assert res.taxonomy_class == C.ANCHORED
    # the pattern is still recorded, so these cases stay countable
    assert res.absence_pattern == "P2_bare_no_np"


def test_same_sentence_not_in_paper_is_an_absence_claim(paper):
    snippet = "No comparison to a human operator was reported anywhere in this work."
    res = C.classify_span(snippet, "No comparison reported", paper)
    assert res.taxonomy_class == C.ABSENCE_CLAIM
    assert res.absence_pattern == "P2_bare_no_np"


def test_bare_sentinel_beats_anchoring(paper):
    """P4 is the one pattern that overrides ANCHORED.

    'nr' is a substring of some word in nearly every paper, so the v1.0 rule
    would call a bare 'NR' snippet a verbatim quotation.
    """
    assert "nr" in paper.norm_text  # the accidental substring really is there
    verdict, pattern = is_absence_claim("NR", anchored=True)
    assert verdict is True
    assert pattern == BARE_SENTINEL_PATTERN
    res = C.classify_span("NR", "NR", paper)
    assert res.taxonomy_class == C.ABSENCE_CLAIM


def test_absence_claim_is_terminal_and_threshold_independent(paper):
    res = C.classify_span("The paper does not report the sample size.", "NR", paper)
    assert res.taxonomy_class == C.ABSENCE_CLAIM
    assert res.terminal is True
    assert all(res.classes_at(t) == C.ABSENCE_CLAIM for t in (0.0, 0.85, 0.90, 0.95, 1.0))


def test_empty_snippet_still_routes_to_the_empty_classes(paper):
    """ABSENCE_CLAIM is about snippet content; an empty snippet is a different case."""
    assert C.classify_span("", "NR", paper).taxonomy_class == C.ABSENCE_DECLARED
    assert C.classify_span("", "Shared", paper).taxonomy_class == C.MISSING_SNIPPET


def test_rationale_still_lands_in_no_basis(paper):
    """The separation must not drain the class it was carved out of."""
    res = C.classify_span("The robot uses sensors and cameras to monitor the tip.", "R", paper)
    assert res.taxonomy_class == C.UNTRACEABLE_NO_BASIS
    assert res.absence_pattern is None


def test_absence_claim_is_outside_the_traceability_ladder():
    assert C.ABSENCE_CLAIM in C.NON_TAXONOMY_CLASSES
    assert C.ABSENCE_CLAIM not in C.TAXONOMY_CLASSES
    assert C.ABSENCE_CLAIM in C.ALL_CLASSES
