"""ELICIT-DESIGN-01 test 5 + the ported unit machinery.

The estimator's composition is the thing under test, not just its output: a
worst-observed chars->tokens ratio times a MEASURED marker inflation, against the
enforced 131,072 ceiling. See `engine/elicitation/sizing.py` for why the two do
not compose cleanly and why the ~4% over-count is the accepted direction.
"""

from __future__ import annotations

import pytest

from engine.elicitation import sizing as S
from engine.elicitation.units import (
    MIN_UNIT_TOKENS, build_unit_map, merge_short, strip_comments,
)


def test_units_exclude_docling_comment_artifacts():
    um = build_unit_map(1, "Alpha beta gamma delta. <!-- image --> Epsilon zeta eta theta.")
    assert um.n == 2
    assert all("<!--" not in u for u in um.units)


def test_short_units_are_merged_never_discarded():
    merged = merge_short(["H.", "One two three four five.", "2 (c)."], MIN_UNIT_TOKENS)
    assert "".join(merged).replace(" ", "") == "H.Onetwothreefourfive.2(c)."
    assert all(len(u.split()) >= MIN_UNIT_TOKENS for u in merged)


def test_bijection_nothing_is_dropped():
    text = "Alpha beta gamma. Delta epsilon zeta. H. Eta theta iota kappa."
    um = build_unit_map(1, text)
    assert " ".join(um.units) == strip_comments(text).strip()


def test_min_unit_tokens_is_the_frozen_elicit01_value():
    """A retuned threshold would make the index space incomparable with the
    measurement that justified it. Re-derivation belongs to the parse-quality gate."""
    assert MIN_UNIT_TOKENS == 3


# ══ Test 5 — sizing ═══════════════════════════════════════════════════


def test_estimator_applies_the_measured_marker_inflation():
    prompt = "x" * 100_000
    assert S.estimate_tokens(prompt) == int(100_000 * S.WORST_RATIO * S.INDEX_MARKER_INFLATION)
    assert S.INDEX_MARKER_INFLATION == 1.141
    assert S.WORST_RATIO == 0.4288
    assert S.CEILING_TOKENS == 131_072


def test_overflow_fails_hard_before_any_call():
    over = "x" * int(S.CEILING_TOKENS / (S.WORST_RATIO * S.INDEX_MARKER_INFLATION) + 10)
    with pytest.raises(S.PromptTooLargeError) as exc:
        S.enforce_fit(over, label="pass1_elicitation", paper_id=498)
    assert "ceiling" in str(exc.value)
    assert str(S.CEILING_TOKENS) in str(exc.value).replace(",", "")


def test_a_fitting_prompt_returns_its_estimate():
    assert S.enforce_fit("x" * 1000, label="pass1_elicitation") == S.estimate_tokens("x" * 1000)


def test_tripwire_fires_only_at_the_ceiling():
    assert S.truncation_tripwire(S.CEILING_TOKENS)
    assert S.truncation_tripwire(S.CEILING_TOKENS + 1)
    assert not S.truncation_tripwire(S.CEILING_TOKENS - 1)
    assert not S.truncation_tripwire(None)
