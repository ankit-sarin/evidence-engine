"""ELICIT-DESIGN-02 Rulings 1 and 4 — terminal states and conservative acceptance.

Ruling 1 moved the unit of refusal from the paper to the field. These tests
assert the three things that move with it: every field carries exactly one state
from a closed, codebook-derived vocabulary; the two states that carry no value
are non-value TOKENS that no consumer may read as values; and a paper is written
whole or not at all.

Ruling 4 replaced "re-issue the identical request" with one feedback-carrying
retry under a strict-inequality acceptance rule. The boundary case — an equal
number of unmet fields — is the one that matters, because F7 measured a retry
that regressed and an accept-on-ties rule would let a different-but-no-better
answer displace the first one.
"""

from __future__ import annotations

import pytest

from engine.core.completeness import TerminalStateError, enforce_terminal_states
from engine.elicitation import classes as C
from engine.elicitation import terminal as T
from engine.elicitation.contracts import (
    FieldRecord, Pass1Result, STEPS_MISSING, VALUE_WITHOUT_CITATION,
)

CB = {
    "escape_token": "NO_EVIDENCE_LOCATABLE",
    "contract_unmet_token": "CONTRACT_UNMET",
    "absence_sentinels": ["NR", "NOT_FOUND", "NOT REPORTED"],
    "fields": [
        {"name": "a", "field_class": "stated", "type": "free_text"},
        {"name": "b", "field_class": "inferable", "type": "free_text"},
        {"name": "c", "field_class": "judgment", "type": "free_text"},
    ],
}


def _rec(name, cls, value, *, indices=(), escape=False, violations=()):
    return FieldRecord(field_name=name, field_class=cls, value=value,
                       is_escape=escape, indices=indices, violations=violations)


def _result(*recs, n_units=50):
    return Pass1Result(records={r.field_name: r for r in recs},
                       parse_path="direct", n_units=n_units)


# ══ Terminal-state representation ═════════════════════════════════════


def test_every_field_gets_exactly_one_state_from_the_closed_set():
    res = _result(
        _rec("a", "stated", "12 patients", indices=(4,)),
        _rec("b", "inferable", "NO_EVIDENCE_LOCATABLE", escape=True),
        _rec("c", "judgment", "Task autonomy", violations=(STEPS_MISSING,)),
    )
    states = T.terminal_states(res, CB)
    assert states == {"a": "EVIDENCED_VALUE",
                      "b": "NO_EVIDENCE_LOCATABLE",
                      "c": "CONTRACT_UNMET"}
    assert set(states.values()) <= T.state_vocabulary(CB)


def test_the_vocabulary_is_codebook_derived_not_hardcoded():
    custom = {**CB, "contract_unmet_token": "REFUSED", "escape_token": "NOTHING_HERE"}
    assert T.state_vocabulary(custom) == {"EVIDENCED_VALUE", "REFUSED", "NOTHING_HERE"}
    res = _result(_rec("c", "judgment", "x", violations=(STEPS_MISSING,)))
    assert T.terminal_states(res, custom)["c"] == "REFUSED"


def test_the_two_token_states_do_not_collide_with_sentinels_or_real_values():
    """A sentinel is a VALUE and must never be mistaken for a terminal state."""
    tokens = C.non_value_tokens(CB)
    assert tokens == {"NO_EVIDENCE_LOCATABLE", "CONTRACT_UNMET"}
    for sentinel in CB["absence_sentinels"]:
        assert sentinel.upper() not in tokens
        assert not C.is_non_value_token(sentinel, CB)
    for real in ("General Surgery", "Task autonomy", "12", "", "Canada"):
        assert not C.is_non_value_token(real, CB)


def test_a_cited_sentinel_is_an_evidenced_value_not_a_terminal_state():
    """§4.1's distinction survives Ruling 1: 'NR' with a citation is a VALUE."""
    res = _result(_rec("a", "stated", "NR", indices=(9,)))
    assert T.terminal_states(res, CB)["a"] == "EVIDENCED_VALUE"


def test_an_uncited_sentinel_is_refused_exactly_like_any_uncited_value():
    res = _result(_rec("a", "stated", "NR", violations=(VALUE_WITHOUT_CITATION,)))
    assert T.terminal_states(res, CB)["a"] == "CONTRACT_UNMET"


def test_counts_are_enumerable():
    res = _result(
        _rec("a", "stated", "x", indices=(1,)),
        _rec("b", "inferable", "y", violations=(VALUE_WITHOUT_CITATION,)),
        _rec("c", "judgment", "z", violations=(STEPS_MISSING,)),
    )
    states = T.terminal_states(res, CB)
    assert T.n_contract_unmet(states, CB) == 2
    assert T.n_evidenced(states) == 1


# ══ The completeness predicate, redefined (Ruling 1) ══════════════════


VOCAB = frozenset({"EVIDENCED_VALUE", "NO_EVIDENCE_LOCATABLE", "CONTRACT_UNMET"})


def test_a_complete_state_map_passes():
    states = {"a": "EVIDENCED_VALUE", "b": "CONTRACT_UNMET", "c": "NO_EVIDENCE_LOCATABLE"}
    assert enforce_terminal_states(
        states, ("a", "b", "c"), VOCAB, paper_id=1, arm="test") == states


def test_a_field_with_no_state_fails():
    with pytest.raises(TerminalStateError) as exc:
        enforce_terminal_states({"a": "EVIDENCED_VALUE"}, ("a", "b"), VOCAB,
                                paper_id=1, arm="test")
    assert "b" in str(exc.value)


def test_a_state_outside_the_vocabulary_fails():
    """An unratified state would reach evidence_spans.value as a live token."""
    with pytest.raises(TerminalStateError) as exc:
        enforce_terminal_states({"a": "EVIDENCED_VALUE", "b": "PROBABLY_FINE"},
                                ("a", "b"), VOCAB, paper_id=1, arm="test")
    assert "PROBABLY_FINE" in str(exc.value)


def test_a_state_for_an_unrequested_field_fails():
    with pytest.raises(TerminalStateError):
        enforce_terminal_states({"a": "EVIDENCED_VALUE", "zz": "CONTRACT_UNMET"},
                                ("a",), VOCAB, paper_id=1, arm="test")


def test_terminal_state_error_shares_the_completeness_retry_budget():
    """It subclasses IncompleteExtractionError so the one budget still covers it."""
    from engine.core.completeness import IncompleteExtractionError

    assert issubclass(TerminalStateError, IncompleteExtractionError)


# ══ Conservative acceptance (Ruling 4) ════════════════════════════════


def _attempt(n_unmet: int, tag: str = "") -> Pass1Result:
    recs = []
    for i in range(3):
        failing = i < n_unmet
        recs.append(_rec(
            "abc"[i], ("stated", "inferable", "judgment")[i], f"v{i}{tag}",
            indices=() if failing else (i + 1,),
            violations=(VALUE_WITHOUT_CITATION,) if failing else (),
        ))
    return _result(*recs)


def test_attempt_2_wins_on_strictly_fewer_unmet_fields():
    accepted, n = T.accept_attempt(_attempt(3), _attempt(1, "b"), CB)
    assert n == 2 and accepted.records["a"].value.endswith("b")


def test_a_tie_keeps_attempt_1():
    """The boundary case. F7 measured p604 going 2 -> 5; an accept-on-ties rule
    would let an equal-but-different second answer displace the first for no
    gain, and the first is the one the engine already reasoned about."""
    accepted, n = T.accept_attempt(_attempt(2), _attempt(2, "b"), CB)
    assert n == 1 and not accepted.records["a"].value.endswith("b")


def test_a_regressing_attempt_2_is_discarded():
    accepted, n = T.accept_attempt(_attempt(1), _attempt(3, "b"), CB)
    assert n == 1 and T.n_contract_unmet(T.terminal_states(accepted, CB), CB) == 1


def test_no_second_attempt_means_attempt_1():
    accepted, n = T.accept_attempt(_attempt(2), None, CB)
    assert n == 1 and accepted is not None


def test_acceptance_never_mixes_fields_across_attempts():
    """A composite extraction's evidence set is internally inconsistent and
    corresponds to no answer any model gave. Whole attempts only."""
    first, second = _attempt(2, "1st"), _attempt(1, "2nd")
    accepted, _ = T.accept_attempt(first, second, CB)
    assert accepted is second
    assert all(r.value.endswith("2nd") for r in accepted.records.values())
