"""Per-field terminal states and the conservative retry-acceptance rule.

**Ruling 1 — the unit of refusal is the field, not the paper.** Before this,
one uncited field out of twenty failed the whole extraction: the
ELICIT-DESIGN-01 smoke stored nothing for p604, whose first attempt was two
fields short of clean, and nothing for p121 and p498 either. Three papers, nine
32B calls, zero rows. The strictness was right and the granularity was not.

A storable paper now carries EXACTLY ONE terminal state per codebook field:

  EVIDENCED_VALUE       the field met its class contract. Pass 2 supplies the
                        value; the engine supplies the snippet.
  NO_EVIDENCE_LOCATABLE the model declared it could locate nothing to cite.
                        A statement about its search. No value, no snippet.
  CONTRACT_UNMET        the field's contract was not met. The ENGINE's record
                        that it declined to write. No value, no snippet.

**What did NOT get softer.** An uncited value is still never stored as a value.
It is stored as CONTRACT_UNMET instead — a refusal on the record, enumerable by
`WHERE value = 'CONTRACT_UNMET'`, rather than a value nobody evidenced. The
write-boundary fail-fast is unchanged in strictness; only its unit moved. The
two states that carry no value are the two non-value tokens, and every consumer
that scores, audits, normalises or counts values excludes them through
`classes.non_value_tokens()` (D1/D2).

**Ruling 4 — conservative acceptance.** ELICIT-DESIGN-01's F7 measured that
re-issuing an identical request at temperature 0 does not help and sometimes
hurts: p604 went 2 -> 5 -> 5 failing fields, so the retry policy actively cost a
paper that was nearly clean on its first answer. Two things follow, and both are
implemented here.

  The retry is no longer identical. Attempt 2 carries a typed feedback block
  naming each failing field, its violation code, what it returned and what its
  contract requires (`prompts.build_feedback_block`).

  Acceptance is strict-inequality, whole-attempt. Attempt 2 replaces attempt 1
  ONLY if it has strictly FEWER CONTRACT_UNMET fields. Equal counts keep
  attempt 1. There is deliberately no per-field mixing: taking each field's best
  attempt would assemble an extraction that no single Pass-1 response ever
  produced, whose evidence set is internally inconsistent -- field A's citations
  reasoned about a paper reading that field B's citations contradict. A stored
  paper is one model's answer, or it is a composite nobody can audit.
"""

from __future__ import annotations

from engine.elicitation import classes as C
from engine.elicitation.contracts import Pass1Result


def terminal_states(result: Pass1Result, codebook: dict) -> dict[str, str]:
    """{field_name: terminal state} for every field the response was checked for.

    Exactly one state per field, drawn from the closed set. The two non-value
    states are the codebook's own tokens; nothing here spells them.
    """
    escape = C.escape_token(codebook)
    unmet = C.contract_unmet_token(codebook)
    out: dict[str, str] = {}
    for name, rec in result.records.items():
        if not rec.ok:
            out[name] = unmet
        elif rec.is_escape:
            out[name] = escape
        else:
            out[name] = C.EVIDENCED_VALUE
    return out


def state_vocabulary(codebook: dict) -> frozenset[str]:
    """The closed set of legal terminal states, codebook-derived."""
    return frozenset({C.EVIDENCED_VALUE,
                      C.escape_token(codebook),
                      C.contract_unmet_token(codebook)})


def n_contract_unmet(states: dict[str, str], codebook: dict) -> int:
    """How many fields the engine refused to write a value for."""
    unmet = C.contract_unmet_token(codebook)
    return sum(1 for s in states.values() if s == unmet)


def n_evidenced(states: dict[str, str]) -> int:
    """How many fields carry a value Pass 2 must supply."""
    return sum(1 for s in states.values() if s == C.EVIDENCED_VALUE)


def accept_attempt(
    first: Pass1Result,
    second: Pass1Result | None,
    codebook: dict,
) -> tuple[Pass1Result, int]:
    """Ruling 4's acceptance rule. Returns (accepted result, attempt number).

    Attempt 2 wins only on STRICTLY fewer CONTRACT_UNMET fields. A tie keeps
    attempt 1, which is the whole point: F7 measured a retry that regressed, and
    a rule that accepted ties would let an equal-but-different second answer
    displace a first one for no gain.
    """
    if second is None:
        return first, 1
    n1 = n_contract_unmet(terminal_states(first, codebook), codebook)
    n2 = n_contract_unmet(terminal_states(second, codebook), codebook)
    return (second, 2) if n2 < n1 else (first, 1)
