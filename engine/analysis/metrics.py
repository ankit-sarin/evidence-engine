"""Agreement statistics for concordance analysis.

INSTRUMENTS-01. `cohens_kappa` used to take the scorer's collapsed verdicts —
a list of MATCH/MISMATCH — and compute chance agreement from *their* frequencies:

    p_e = p_match**2 + p_mismatch**2

The raters' own label distributions were gone before the function was entered,
so that is not Cohen's kappa. It reduces to

    kappa = 1 - 1 / (2 * p_o)

a function of the observed agreement alone, carrying no information the
percent-agreement column did not already carry. On a balanced 2x2 table with
true kappa 0.8000 it returned 0.4444; on 95% agreement against a constant rater,
true kappa 0.0000, it returned 0.4737. Sixty-three published values were that
number.

**Kappa needs each rater's marginals, so it needs each rater's labels.** The
signature takes two aligned label sequences and nothing else. Every value is
checked against `sklearn.metrics.cohen_kappa_score` in the test suite — the
suite could not see the old defect because every kappa test asserted a *range*
or a degenerate endpoint, never a value against a reference.

**The undefined case returns `nan`, not `1.0`.** When both raters used a single
label and it was the same label, `p_e` is 1 and kappa is 0/0: there is no
variance for chance to explain. sklearn returns `nan` there. The old code
returned `1.0`, which reads as perfect reliability when what actually happened
is that nobody made a choice. `KappaResult.undefined_reason` says which case it
was, so a reader is told rather than left with a bare `nan`.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field

from engine.analysis.scoring import FieldScore

#: The label a missing value carries into kappa. Absence is a rater's answer —
#: "this paper does not report it" — so it is a category, not a gap, and two
#: arms agreeing on absence is agreement.
ABSENT_LABEL = "\x00ABSENT"


@dataclass
class KappaResult:
    """Cohen's kappa with the material needed to check it."""

    kappa: float
    n: int  # aligned pairs entering the calculation
    n_agree: int
    n_disagree: int
    n_ambiguous: int  # scorer verdicts excluded from percent agreement, not from kappa
    ci_lower: float
    ci_upper: float
    p_o: float = float("nan")
    p_e: float = float("nan")
    n_categories: int = 0
    undefined_reason: str | None = None


@dataclass
class FieldSummary:
    """Per-field concordance summary.

    Carries **two** agreement numbers on purpose. `percent_agreement` is the
    scorer's verdict rate — the share of decisive pairs it called MATCH, under
    rules that treat "da Vinci Xi" and "da Vinci Xi (Intuitive Surgical)" as the
    same answer. `kappa_p_o` is verbatim label agreement, which is what kappa's
    chance correction is applied to. They are different questions and were
    previously conflated into one column.
    """

    field_name: str
    kappa: float
    percent_agreement: float
    n: int  # MATCH + MISMATCH (the scorer's decisive denominator)
    n_match: int
    n_mismatch: int
    n_ambiguous: int
    ci_lower: float
    ci_upper: float
    kappa_n: int = 0
    kappa_p_o: float = float("nan")
    kappa_p_e: float = float("nan")
    n_categories: int = 0
    undefined_reason: str | None = None


def _hashable(label):
    """A label usable as a dict key.

    `normalize_for_concordance` returns `None` for absence and a `set` for a
    multi-value categorical, neither of which can key a Counter.
    """
    if label is None:
        return ABSENT_LABEL
    if isinstance(label, (set, frozenset)):
        return frozenset(label)
    return label


def cohens_kappa(labels_a, labels_b) -> KappaResult:
    """Cohen's kappa over two aligned label sequences.

    `p_o` is the share of positions where the two raters gave the same label.
    `p_e` is `sum over c of p_A(c) * p_B(c)` across the union of labels either
    rater used — **each rater's own marginal**, which is the whole point.

    Undefined cases return `nan` with `undefined_reason` set:
      - no aligned pairs;
      - both raters used one label and it was the same label (`p_e == 1`), so
        kappa is 0/0. `sklearn` returns `nan` here too.

    Both raters constant on *different* labels is NOT undefined: `p_e` is 0,
    `p_o` is 0, and kappa is 0.0.
    """
    if len(labels_a) != len(labels_b):
        raise ValueError(
            f"label sequences must be aligned: {len(labels_a)} vs {len(labels_b)}"
        )

    a = [_hashable(x) for x in labels_a]
    b = [_hashable(x) for x in labels_b]
    n = len(a)

    if n == 0:
        return KappaResult(
            kappa=float("nan"), n=0, n_agree=0, n_disagree=0, n_ambiguous=0,
            ci_lower=float("nan"), ci_upper=float("nan"),
            undefined_reason="no aligned pairs",
        )

    n_agree = sum(1 for x, y in zip(a, b) if x == y)
    n_disagree = n - n_agree
    p_o = n_agree / n

    count_a, count_b = Counter(a), Counter(b)
    categories = set(count_a) | set(count_b)
    p_e = sum((count_a[c] / n) * (count_b[c] / n) for c in categories)

    if p_e >= 1.0:
        return KappaResult(
            kappa=float("nan"), n=n, n_agree=n_agree, n_disagree=n_disagree,
            n_ambiguous=0, ci_lower=float("nan"), ci_upper=float("nan"),
            p_o=round(p_o, 6), p_e=round(p_e, 6), n_categories=len(categories),
            undefined_reason=(
                "both raters used a single, identical label; kappa is 0/0 — "
                "there is no variance for chance to explain"
            ),
        )

    kappa = (p_o - p_e) / (1.0 - p_e)

    # Analytical SE (Fleiss, 1981) — unchanged from the previous implementation,
    # so the CI method of the superseded figures is the CI method of these.
    if n <= 1:
        ci_lower = ci_upper = kappa
    else:
        se = math.sqrt(p_o * (1.0 - p_o) / (n * (1.0 - p_e) ** 2))
        ci_lower = kappa - 1.96 * se
        ci_upper = kappa + 1.96 * se

    return KappaResult(
        kappa=round(kappa, 4),
        n=n,
        n_agree=n_agree,
        n_disagree=n_disagree,
        n_ambiguous=0,
        ci_lower=round(ci_lower, 4),
        ci_upper=round(ci_upper, 4),
        p_o=round(p_o, 6),
        p_e=round(p_e, 6),
        n_categories=len(categories),
    )


def percent_agreement(scores: list[FieldScore]) -> float:
    """The scorer's verdict rate. AMBIGUOUS excluded from the denominator.

    Deliberately unchanged: this is a statement about the scorer's rules, not
    about verbatim agreement, and conflating the two is what made the old kappa
    look plausible.
    """
    decisive = [s for s in scores if s.result != "AMBIGUOUS"]
    if not decisive:
        return float("nan")
    return sum(1 for s in decisive if s.result == "MATCH") / len(decisive)


def labels_from_scores(scores: list[FieldScore]) -> tuple[list, list]:
    """The two raters' label sequences, as `score_pair` normalized them.

    Taken off the `FieldScore` rather than re-normalized here: normalization has
    one site, and a second one would be free to drift from it.
    """
    return [s.norm_a for s in scores], [s.norm_b for s in scores]


def field_summary(field_name: str, scores: list[FieldScore]) -> FieldSummary:
    """Combine kappa, percent agreement, and counts for a single field.

    Kappa is computed over **every** aligned pair, including the ones the scorer
    called AMBIGUOUS: an ambiguous verdict is a statement about the scorer's
    confidence, not about what the two raters wrote, and the labels are known
    either way.
    """
    labels_a, labels_b = labels_from_scores(scores)
    kr = cohens_kappa(labels_a, labels_b)
    pa = percent_agreement(scores)

    n_match = sum(1 for s in scores if s.result == "MATCH")
    n_mismatch = sum(1 for s in scores if s.result == "MISMATCH")
    n_ambiguous = sum(1 for s in scores if s.result == "AMBIGUOUS")

    return FieldSummary(
        field_name=field_name,
        kappa=kr.kappa,
        percent_agreement=round(pa, 4) if not math.isnan(pa) else pa,
        n=n_match + n_mismatch,
        n_match=n_match,
        n_mismatch=n_mismatch,
        n_ambiguous=n_ambiguous,
        ci_lower=kr.ci_lower,
        ci_upper=kr.ci_upper,
        kappa_n=kr.n,
        kappa_p_o=kr.p_o,
        kappa_p_e=kr.p_e,
        n_categories=kr.n_categories,
        undefined_reason=kr.undefined_reason,
    )
