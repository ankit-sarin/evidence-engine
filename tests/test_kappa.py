"""Cohen's kappa, checked against an external reference (INSTRUMENTS-01, K2).

Every kappa test in the suite this replaces asserted a RANGE or a degenerate
endpoint — `== 1.0`, `== 0.0`, `0 < k < 1`, `isnan`. Not one asserted a value
against a known-correct reference, which is why a kappa that was really
`1 - 1/(2*p_o)` stayed green for sixty-three published figures.

Every fixture here is checked against `sklearn.metrics.cohen_kappa_score` to
1e-9, and the third fixture's expected value is TAKEN FROM sklearn rather than
hand-set, so the per-rater marginal is proven rather than assumed.
"""

import math

import pytest
from sklearn.metrics import cohen_kappa_score

from engine.analysis.metrics import ABSENT_LABEL, cohens_kappa

TOL = 1e-9


# ── Fixture A: balanced 2x2, true kappa 0.8000 ──────────────────────
#   a=45 both-Yes, d=45 both-No, b=5, c=5; both marginals 50/50.
#   p_o = 0.90, p_e = 0.50.  The old implementation returned 0.4444.
FIXTURE_A = (
    ["Yes"] * 45 + ["No"] * 45 + ["Yes"] * 5 + ["No"] * 5,
    ["Yes"] * 45 + ["No"] * 45 + ["No"] * 5 + ["Yes"] * 5,
)

# ── Fixture B: one constant rater, 95% agreement, true kappa 0.0000 ──
#   p_o = 0.95 and p_e = 0.95, so chance explains all of it.
#   The old implementation returned 0.4737.
FIXTURE_B = (["Yes"] * 95 + ["No"] * 5, ["Yes"] * 100)

# ── Fixture C: three categories, ASYMMETRIC marginals ───────────────
#   A: 60/30/10   B: 63/28/9.  Only a per-rater marginal gets this right;
#   any symmetric shortcut does not.
FIXTURE_C = (
    ["A"] * 60 + ["B"] * 30 + ["C"] * 10,
    (["A"] * 50 + ["B"] * 8 + ["C"] * 2
     + ["A"] * 10 + ["B"] * 18 + ["C"] * 2
     + ["A"] * 3 + ["B"] * 2 + ["C"] * 5),
)


@pytest.mark.parametrize("name,labels,expected", [
    ("A — balanced 2x2, true 0.8000", FIXTURE_A, 0.8000),
    ("B — constant rater, true 0.0000", FIXTURE_B, 0.0000),
    ("C — 3 categories, asymmetric marginals", FIXTURE_C, None),
])
def test_kappa_equals_sklearn(name, labels, expected):
    a, b = labels
    reference = float(cohen_kappa_score(a, b))
    if expected is not None:
        assert abs(reference - expected) < TOL, (
            f"{name}: sklearn itself disagrees with the stated true value"
        )
    result = cohens_kappa(a, b)
    assert abs(result.kappa - reference) < 1e-4, (
        f"{name}: engine {result.kappa} vs sklearn {reference}"
    )
    # And to 1e-9 before the reporting round.
    unrounded = (result.p_o - result.p_e) / (1.0 - result.p_e)
    assert abs(unrounded - reference) < TOL, (
        f"{name}: engine {unrounded!r} vs sklearn {reference!r}"
    )


def test_fixture_c_value_is_taken_from_sklearn_not_hand_set():
    """The third fixture's expected value is whatever sklearn says it is."""
    a, b = FIXTURE_C
    assert abs(float(cohen_kappa_score(a, b)) - 0.4896030245746692) < TOL


def test_the_old_closed_form_is_gone():
    """The defect was kappa == 1 - 1/(2*p_o). It must not survive anywhere."""
    for name, (a, b) in (("A", FIXTURE_A), ("B", FIXTURE_B), ("C", FIXTURE_C)):
        r = cohens_kappa(a, b)
        old = 1 - 1 / (2 * r.p_o)
        assert abs(r.kappa - old) > 1e-3, (
            f"fixture {name}: kappa {r.kappa} still equals 1-1/(2*p_o) = {old}"
        )


def test_marginals_matter_not_just_agreement():
    """Two sets with the SAME p_o and different marginals get different kappas.

    This is the property the old implementation could not have: it was a
    function of p_o alone, so it returned one number for both of these.
    """
    same_po_high_kappa = FIXTURE_A                      # p_o 0.90, marginals 50/50
    same_po_low_kappa = (["Yes"] * 90 + ["No"] * 10,
                         ["Yes"] * 90 + ["Yes"] * 10)   # p_o 0.90, B constant
    ka = cohens_kappa(*same_po_high_kappa)
    kb = cohens_kappa(*same_po_low_kappa)
    assert abs(ka.p_o - kb.p_o) < TOL, "the two fixtures must share p_o"
    assert ka.kappa == pytest.approx(0.8, abs=1e-4)
    assert math.isnan(kb.kappa) or kb.kappa < 0.1
    assert 1 - 1 / (2 * ka.p_o) == pytest.approx(1 - 1 / (2 * kb.p_o)), (
        "and the old formula would have given them the same value"
    )


# ── The undefined case ──────────────────────────────────────────────


def test_both_raters_constant_and_identical_is_undefined_not_one():
    r = cohens_kappa(["a"] * 10, ["a"] * 10)
    assert math.isnan(r.kappa), "the old code returned 1.0 here"
    assert r.undefined_reason and "0/0" in r.undefined_reason
    assert r.p_o == 1.0
    assert math.isnan(cohen_kappa_score(["a"] * 10, ["a"] * 10)), (
        "sklearn agrees this is undefined"
    )


def test_both_raters_constant_on_different_labels_is_zero_not_undefined():
    r = cohens_kappa(["a"] * 10, ["b"] * 10)
    assert r.kappa == 0.0
    assert r.undefined_reason is None
    assert float(cohen_kappa_score(["a"] * 10, ["b"] * 10)) == 0.0


def test_empty_is_undefined():
    r = cohens_kappa([], [])
    assert math.isnan(r.kappa)
    assert r.n == 0
    assert r.undefined_reason == "no aligned pairs"


def test_misaligned_sequences_refuse():
    with pytest.raises(ValueError, match="aligned"):
        cohens_kappa(["a", "b"], ["a"])


# ── Labels that are not plain strings ───────────────────────────────


def test_absence_is_a_category_not_a_gap():
    """Two arms agreeing that a paper does not report a field is agreement."""
    a = [None, "RCT", None, "Cohort"]
    b = [None, "RCT", "Cohort", "Cohort"]
    r = cohens_kappa(a, b)
    assert r.n == 4 and r.n_agree == 3
    reference = float(cohen_kappa_score(
        [ABSENT_LABEL if x is None else x for x in a],
        [ABSENT_LABEL if x is None else x for x in b],
    ))
    assert abs(r.kappa - reference) < 1e-4


def test_multi_value_sets_are_hashable_labels():
    a = [{"x", "y"}, {"x"}, {"x", "y"}]
    b = [{"y", "x"}, {"y"}, {"x", "y"}]
    r = cohens_kappa(a, b)
    assert r.n == 3 and r.n_agree == 2  # set equality ignores order


# ── The frozen study's implementation must still agree ──────────────


def test_agrees_with_the_frozen_screen2f_kappa_including_the_2g_value():
    """`score_screen2f.cohen_kappa` is frozen with its study and is correct.

    It is not imported into — its degenerate branch returns None where this one
    returns nan, so a "pure import with identical output" is not available. This
    test buys the one-predicate guarantee by checking rather than by sharing,
    and includes the published 2g P3 A-vs-D value.
    """
    from analysis.eval.score_screen2f import cohen_kappa as frozen

    # The 2g P3 A-vs-D contingency table, as committed in scoring.json.
    table = {"FLAGGED->FLAGGED": 38, "FLAGGED->OUT": 39, "IN->OUT": 4, "OUT->OUT": 5}
    xs, ys = [], []
    for key, count in table.items():
        left, right = key.split("->")
        xs += [left] * count
        ys += [right] * count
    assert len(xs) == 86

    assert frozen(xs, ys) == 0.1258, "the committed 2g P3 A-vs-D value"
    assert abs(cohens_kappa(xs, ys).kappa - 0.1258) < 1e-4
    assert abs(float(cohen_kappa_score(xs, ys)) - 0.1258) < 1e-4

    for a, b in (FIXTURE_A, FIXTURE_B, FIXTURE_C):
        assert abs(frozen(a, b) - cohens_kappa(a, b).kappa) < 1e-9
