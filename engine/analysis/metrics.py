"""Agreement statistics for concordance analysis."""

import math
from dataclasses import dataclass, field

from engine.analysis.scoring import FieldScore


@dataclass
class KappaResult:
    """Cohen's kappa with metadata."""

    kappa: float
    n: int  # total scored (MATCH + MISMATCH, excluding AMBIGUOUS)
    n_agree: int
    n_disagree: int
    n_ambiguous: int
    ci_lower: float
    ci_upper: float


@dataclass
class FieldSummary:
    """Per-field concordance summary."""

    field_name: str
    kappa: float
    percent_agreement: float
    n: int  # MATCH + MISMATCH (denominator)
    n_match: int
    n_mismatch: int
    n_ambiguous: int
    ci_lower: float
    ci_upper: float


def cohens_kappa(scores: list[FieldScore]) -> KappaResult:
    """Compute Cohen's kappa from scored pairs.

    AMBIGUOUS pairs are excluded from the kappa calculation but counted
    separately.  Uses analytical standard error for 95% CI.
    """
    n_ambiguous = sum(1 for s in scores if s.result == "AMBIGUOUS")
    decisive = [s for s in scores if s.result != "AMBIGUOUS"]
    n = len(decisive)
    n_agree = sum(1 for s in decisive if s.result == "MATCH")
    n_disagree = n - n_agree

    if n == 0:
        return KappaResult(
            kappa=float("nan"), n=0,
            n_agree=0, n_disagree=0, n_ambiguous=n_ambiguous,
            ci_lower=float("nan"), ci_upper=float("nan"),
        )

    p_o = n_agree / n  # observed agreement

    # For a binary agree/disagree classification with two raters rating the
    # same items, chance agreement is computed from marginal proportions.
    # With only agree/disagree outcomes, each rater's "agree" rate is p_o
    # and "disagree" rate is 1 - p_o.  But since both raters see the same
    # items and produce the same agree/disagree table, the standard
    # formulation uses the marginals of the 2x2 table.
    #
    # In concordance scoring, each pair produces one of {MATCH, MISMATCH}.
    # We treat this as: rater A says "match" with probability p_a, rater B
    # says "match" with probability p_b.  Since both raters contribute to
    # the same outcome, p_a = p_b = p_o, giving p_e = p_o^2 + (1-p_o)^2.
    #
    # However, for a more meaningful kappa that doesn't collapse to trivial
    # values, we use the prevalence-adjusted formulation where p_e accounts
    # for the base rate of agreement by chance = 0.5 for binary outcomes
    # when no category information is available.  But the standard approach
    # for inter-rater reliability with binary categories uses the actual
    # marginals.
    #
    # Standard 2-category kappa:
    # p_e = sum over categories c of (proportion_raterA_c * proportion_raterB_c)
    # For our binary case: each "rater" (arm) produces a MATCH/MISMATCH
    # with the same frequencies, so:
    p_match = n_agree / n
    p_mismatch = n_disagree / n
    p_e = p_match ** 2 + p_mismatch ** 2

    if p_e == 1.0:
        kappa = 1.0 if p_o == 1.0 else 0.0
    else:
        kappa = (p_o - p_e) / (1.0 - p_e)

    # Analytical SE (Fleiss, 1981)
    if p_e == 1.0 or n <= 1:
        ci_lower, ci_upper = kappa, kappa
    else:
        se = math.sqrt(p_o * (1.0 - p_o) / (n * (1.0 - p_e) ** 2))
        ci_lower = kappa - 1.96 * se
        ci_upper = kappa + 1.96 * se

    return KappaResult(
        kappa=round(kappa, 4),
        n=n,
        n_agree=n_agree,
        n_disagree=n_disagree,
        n_ambiguous=n_ambiguous,
        ci_lower=round(ci_lower, 4),
        ci_upper=round(ci_upper, 4),
    )


def percent_agreement(scores: list[FieldScore]) -> float:
    """Simple percent agreement. AMBIGUOUS excluded from denominator."""
    decisive = [s for s in scores if s.result != "AMBIGUOUS"]
    if not decisive:
        return float("nan")
    return sum(1 for s in decisive if s.result == "MATCH") / len(decisive)


def field_summary(field_name: str, scores: list[FieldScore]) -> FieldSummary:
    """Combine kappa, percent agreement, and counts for a single field."""
    kr = cohens_kappa(scores)
    pa = percent_agreement(scores)
    return FieldSummary(
        field_name=field_name,
        kappa=kr.kappa,
        percent_agreement=round(pa, 4),
        n=kr.n,
        n_match=kr.n_agree,
        n_mismatch=kr.n_disagree,
        n_ambiguous=kr.n_ambiguous,
        ci_lower=kr.ci_lower,
        ci_upper=kr.ci_upper,
    )
