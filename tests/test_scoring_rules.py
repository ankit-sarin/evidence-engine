"""The three corrected scoring rules (INSTRUMENTS-01, S1-S4).

Each test names the rule it exercises. The five pairs DISCOVERY-01 Part A used
to demonstrate the defect are first; the rest are the cases a fix has to get
right to be worth making.
"""

import pytest

from engine.analysis.scoring import NEGATION_CUES, parse_number, score_pair

# Fields, by the codebook's declared `type`:
NUMERIC = "sample_size"        # type: numeric
FREE_TEXT = "key_limitation"   # type: free_text
CATEGORICAL = "study_type"     # type: categorical
PLATFORM = "robot_platform"    # type: free_text


# ── The five Part A pairs (G3) ──────────────────────────────────────


@pytest.mark.parametrize("field,a,b,expected,rule", [
    (NUMERIC, "5", "50", "MISMATCH", "numeric: parsed and compared as numbers"),
    (FREE_TEXT, "5", "50", "MISMATCH", "numeric: both parse even on a free-text field"),
    (FREE_TEXT, "the intervention improved survival",
     "the intervention did not improve survival", "AMBIGUOUS", "polarity"),
    (FREE_TEXT, "reduced complications",
     "significantly reduced complications overall", "MATCH", "token-boundary containment"),
    (FREE_TEXT, "benefit", "no benefit", "AMBIGUOUS", "polarity"),
])
def test_the_five_part_a_pairs(field, a, b, expected, rule):
    assert score_pair(field, a, b).result == expected, rule


def test_only_the_da_vinci_pair_still_matches(record_property):
    """G3 in one assertion: of Part A's demonstration set, one MATCH remains."""
    part_a = [
        (NUMERIC, "5", "50"),
        (FREE_TEXT, "5", "50"),
        (FREE_TEXT, "the intervention improved survival",
         "the intervention did not improve survival"),
        (FREE_TEXT, "reduced complications",
         "significantly reduced complications overall"),
        (FREE_TEXT, "benefit", "no benefit"),
    ]
    matches = [p for p in part_a if score_pair(*p).result == "MATCH"]
    assert [p[1] for p in matches] == ["reduced complications"], (
        "only the genuine whole-token containment survives"
    )
    assert score_pair(PLATFORM, "da Vinci Xi",
                      "da Vinci Xi (Intuitive Surgical)").result == "MATCH"


# ── Rule: numeric comparison (S1) ───────────────────────────────────


@pytest.mark.parametrize("a,b,expected,why", [
    ("50", "50", "MATCH", "equal integers"),
    ("50 patients", "n=50", "MATCH", "unit word and n= qualifier are noise on a count"),
    ("1,000", "1000", "MATCH", "thousands separator"),
    ("~50", "50", "MATCH", "approximation qualifier"),
    ("2.5", "2.50", "MATCH", "decimal, trailing zero"),
    ("2.5", "25", "MISMATCH", "OLD: both became '25' and MATCHED"),
    ("-5", "5", "MISMATCH", "OLD: the sign was deleted and they MATCHED"),
    ("49", "50", "MISMATCH", "unequal integers — exact equality, no tolerance"),
    ("50-60", "50", "AMBIGUOUS", "a range is not a number; exactly one side parses"),
    ("fifty", "50", "AMBIGUOUS", "a word is not a number; exactly one side parses"),
])
def test_numeric_rule(a, b, expected, why):
    assert score_pair(NUMERIC, a, b).result == expected, why


@pytest.mark.parametrize("a,b,expected,why", [
    ("12.5%", "12.5", "AMBIGUOUS", "unit markers differ — percent is not noise"),
    ("12.5%", "12.5 %", "MATCH", "the same marker, spaced differently"),
    ("30%", "40%", "MISMATCH", "same marker, different number"),
])
def test_unit_marker_rule(a, b, expected, why):
    """Ruling 4b: a parsed number carries its % marker; differing markers never MATCH."""
    assert score_pair("primary_outcome_metric", a, b).result == expected, why


def test_parse_number_grammar():
    assert parse_number("50") == (pytest.approx(50), "")
    assert parse_number("n = 42 patients")[0] == 42
    assert parse_number("12.5%")[1] == "%"
    assert parse_number("1,234") [0] == 1234
    assert parse_number("-5")[0] == -5
    assert parse_number("50-60") is None
    assert parse_number("fifty") is None
    assert parse_number("") is None
    assert parse_number("2.5 mm")[0] == pytest.approx(2.5)


# ── Rule: token-boundary containment (S2) ───────────────────────────


@pytest.mark.parametrize("a,b,expected,why", [
    ("da Vinci Xi", "da Vinci Xi (Intuitive Surgical)", "MATCH",
     "the case the rule was written for — must survive"),
    ("cid", "acidosis", "MISMATCH", "OLD: raw substring, so this MATCHED"),
    ("Raven", "Ravensbourne", "MISMATCH", "OLD: raw substring prefix"),
    ("reduced complications.", "reduced complications", "MATCH",
     "punctuation is a boundary, not a character"),
    ("Reduced Complications", "reduced complications", "MATCH",
     "case is normalized away before comparison"),
    ("complications reduced", "significantly reduced complications overall", "AMBIGUOUS",
     "the tokens are present but not as a contiguous run"),
])
def test_token_boundary_rule(a, b, expected, why):
    assert score_pair(FREE_TEXT, a, b).result == expected, why


# ── Rule: polarity (S3) ─────────────────────────────────────────────


@pytest.mark.parametrize("a,b,why", [
    ("benefit", "no benefit", "negation at the start"),
    ("no significant difference", "significant difference", "negation at the start, reversed"),
    ("the effect was absent", "the effect was present", "negation mid-sentence"),
    ("improved survival", "did not improve survival", "auxiliary negation"),
    ("randomized", "non-randomized", "prefix negation, hyphen is a boundary"),
    ("the study reported complications", "the study reported none", "'none' is a cue"),
])
def test_a_cue_on_one_side_only_is_never_a_match(a, b, why):
    score = score_pair(FREE_TEXT, a, b)
    assert score.result != "MATCH", why
    if score.result == "AMBIGUOUS":
        assert "negation" in score.detail


@pytest.mark.parametrize("a,b,why", [
    ("no benefit", "no clear benefit", "cues on BOTH sides — the rule does not fire"),
    ("not reported by the authors", "not stated by the authors", "both negated"),
])
def test_cues_on_both_sides_do_not_block_a_match(a, b, why):
    assert score_pair(FREE_TEXT, a, b).result in ("MATCH", "AMBIGUOUS"), why


def test_the_negation_cue_list_is_one_declared_place():
    assert isinstance(NEGATION_CUES, frozenset)
    for expected in ("no", "not", "non", "never", "without", "absent",
                     "lack", "neither", "none", "failed"):
        assert expected in NEGATION_CUES
    assert all(c == c.lower() for c in NEGATION_CUES), "cues are matched lowercased"


# ── Unchanged behaviour that must stay unchanged ────────────────────


def test_absence_and_categorical_rules_are_untouched():
    assert score_pair(CATEGORICAL, None, None).result == "MATCH"
    assert score_pair(CATEGORICAL, "NR", "NOT_FOUND").result == "MATCH"
    assert score_pair(CATEGORICAL, "Review", None).result == "MISMATCH"
    assert score_pair(CATEGORICAL, "Review", "Original Research").result == "MISMATCH"
    assert score_pair(CATEGORICAL, "Review", "review").result == "MATCH"


def test_the_score_carries_the_labels_it_judged():
    """Kappa reads these; if they were absent it would have to normalize again."""
    s = score_pair(NUMERIC, "n=50", "50 patients")
    assert s.norm_a == "n=50" and s.norm_b == "50 patients"
    absent = score_pair(CATEGORICAL, "NR", None)
    assert absent.norm_a is None and absent.norm_b is None
