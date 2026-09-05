"""Field classes and value tokens, read from the codebook (ELICIT-DESIGN-01 C1).

The extraction codebook is the authoritative source consulted by prompt
construction. Nothing here, and nothing downstream, hand-lists a field: a class
is looked up, a class's field set is derived. Gate 2 is met by construction --
adding a field to the codebook adds it to its class's contract, and no code
changes.

The assignments are not authored in the codebook either. They are reproduced
from `analysis/provenance/FIELD_CLASSES.md` section 2 (`prov-fieldclass-1`), a
pre-registration artifact whose criteria were written and hashed before any
field was examined, mirrored machine-readably in
`analysis/provenance/field_class3.py`. A three-way pin test asserts codebook ==
module == document; on disagreement the codebook is the copy that is wrong.

Two value tokens, and the distinction between them is the whole point:

  **escape token** (`escape_token`, `NO_EVIDENCE_LOCATABLE`) -- "no evidence
  locatable for this field in this paper". A statement about the extractor's
  search, not about the paper. Carries ZERO citations *by definition*; a
  citation alongside it is itself a violation.

  **absence sentinel** (`absence_sentinels`, six members incl. `NOT_FOUND`) --
  "the paper does not report this". A claim about the body text, therefore a
  VALUE, therefore evidenced like any other value: at least one citation
  required. A sentinel with no citation fails the write contract and enters
  bounded retry. That is intended, not an edge case (ELICIT-DESIGN-01 section 4.1).

A THIRD token joins them in ELICIT-DESIGN-02 (Ruling 1):

  **contract-unmet token** (`contract_unmet_token`, `CONTRACT_UNMET`) -- "this
  field's Pass-1 evidence contract was not met, so the engine refused to store a
  value." Not a claim about the paper and not a claim about the search: the
  ENGINE's record that it declined to write. Carries no value, no citation, no
  snippet.

**Non-value tokens, and why they need one authority.** The escape token and the
contract-unmet token are both terminal STATES occupying the value column; a
sentinel is a VALUE. STEP 0 of ELICIT-DESIGN-02 read five consumers that take
`evidence_spans.value` at face value -- the auditor's audit call and its
LOW_YIELD count, the categorical normaliser's rewrite path, the distribution
monitor's observation set, and cross-arm concordance scoring -- and found every
one of them would score, audit, rewrite or count a terminal state as if it were
a value. `NO_EVIDENCE_LOCATABLE` was already exposed to all five before this task
added a second token.

`non_value_tokens()` is the single authority those consumers read. It is derived
from the codebook, so a sixth consumer inherits it and no site grows a hand-list
of its own (ELICIT-DESIGN-02 D1/D2). The hand-lists that already exist -- two
divergent `_ABSENCE_VALUES` in `auditor.py`, the normaliser's
`("NOT_FOUND", "NR")`, the monitor's `_NULL_SYNONYMS` -- are a recorded
fix-phase item (N2) and are deliberately NOT touched here.
"""

from __future__ import annotations

from pathlib import Path

from engine.agents.extractor import _find_codebook_path, _load_codebook

STATED = "stated"
INFERABLE = "inferable"
JUDGMENT = "judgment"
CLASSES = (STATED, INFERABLE, JUDGMENT)

ELICITATION_VERSION = "elicit-design-02"

# Terminal states (ELICIT-DESIGN-02 Ruling 1). Every codebook field carries
# EXACTLY ONE of these on a stored paper. Two of the three ARE the non-value
# tokens, read from the codebook; only `EVIDENCED_VALUE` is named in code,
# because it is the state that carries an actual value rather than a token.
EVIDENCED_VALUE = "EVIDENCED_VALUE"


class CodebookContractError(RuntimeError):
    """The codebook cannot support the elicitation contract.

    Raised at build time, never worked around: a missing or unknown
    `field_class` means the prompt cannot state a contract for that field, and
    guessing one would silently hold the model to a standard nobody chose.
    """


def load(codebook_path: str | Path | None = None) -> dict:
    """Load the codebook. Shares `extractor`'s cache, so one parse per path."""
    path = Path(codebook_path) if codebook_path else _find_codebook_path()
    return _load_codebook(str(path))


def escape_token(codebook: dict) -> str:
    tok = codebook.get("escape_token")
    if not tok or not str(tok).strip():
        raise CodebookContractError(
            "codebook declares no `escape_token`; the per-class contracts have no "
            "way to say 'no evidence locatable' (ELICIT-DESIGN-01 section 4.1)"
        )
    return str(tok).strip()


def absence_sentinels(codebook: dict) -> frozenset[str]:
    """Sentinels, upper-cased and stripped for comparison."""
    return frozenset(str(s).strip().upper() for s in codebook.get("absence_sentinels", ()))


def contract_unmet_token(codebook: dict) -> str:
    """The token marking a field whose evidence contract was not met.

    Raises rather than defaulting, exactly like `escape_token`: a pipeline that
    can refuse a field but cannot name the refusal would have to fall back on
    storing something, and storing something is what the refusal exists to
    prevent.
    """
    tok = codebook.get("contract_unmet_token")
    if not tok or not str(tok).strip():
        raise CodebookContractError(
            "codebook declares no `contract_unmet_token`; a field whose contract "
            "is unmet has no terminal state to be written as "
            "(ELICIT-DESIGN-02 Ruling 1)"
        )
    return str(tok).strip()


def non_value_tokens(codebook: dict) -> frozenset[str]:
    """Every token that occupies the value column WITHOUT being a value.

    Upper-cased for comparison. This is the one authority every downstream
    consumer reads (D1/D2); see the module docstring for the five sites and why
    a per-site list was refused.
    """
    return frozenset({escape_token(codebook).upper(),
                      contract_unmet_token(codebook).upper()})


def is_non_value_token(value: object, codebook: dict) -> bool:
    """True for the escape token or the contract-unmet token. Never a sentinel."""
    return str(value or "").strip().upper() in non_value_tokens(codebook)


def is_escape(value: object, codebook: dict) -> bool:
    return str(value or "").strip().upper() == escape_token(codebook).upper()


def is_absence_sentinel(value: object, codebook: dict) -> bool:
    """True for a sentinel VALUE. The escape token is never a sentinel."""
    v = str(value or "").strip().upper()
    return bool(v) and v in absence_sentinels(codebook) and not is_escape(value, codebook)


def classes_by_field(codebook: dict) -> dict[str, str]:
    """{field_name: class} for every codebook field, in codebook order.

    Raises rather than defaulting. A field with no declared class is a codebook
    defect, and the only safe response is to refuse to build a prompt at all.
    """
    out: dict[str, str] = {}
    bad: list[str] = []
    for f in codebook.get("fields", []):
        name = f.get("name")
        cls = str(f.get("field_class", "")).strip().lower()
        if not name:
            continue
        if cls not in CLASSES:
            bad.append(f"{name}={f.get('field_class')!r}")
            continue
        out[name] = cls
    if bad:
        raise CodebookContractError(
            f"codebook fields with missing or unknown `field_class`: {bad}. "
            f"Valid classes: {list(CLASSES)}."
        )
    return out


def field_class(name: str, codebook: dict) -> str:
    try:
        return classes_by_field(codebook)[name]
    except KeyError:
        raise CodebookContractError(f"{name!r} is not a codebook field") from None


def fields_by_class(cls: str, codebook: dict) -> list[dict]:
    """Codebook entries of one class, in codebook order. Derived, never listed."""
    if cls not in CLASSES:
        raise ValueError(f"unknown class {cls!r}")
    known = classes_by_field(codebook)
    return [f for f in codebook["fields"] if known.get(f["name"]) == cls]


def non_value_tokens_for(codebook_path: str | Path | None) -> frozenset[str]:
    """`non_value_tokens()` for a consumer that only has a path, never raising.

    The five downstream consumers (D1) run against reviews whose codebooks may
    predate either token — the whole surgical_autonomy corpus before Run 7 does.
    A hard failure there would take out the auditor, the validators and
    concordance on every legacy review to protect a token those reviews cannot
    contain. An empty set restores the exact pre-ELICIT-DESIGN-02 behaviour,
    which is the correct behaviour for data that has no terminal states in it.

    This tolerance is for the READ side only. `extract_paper_elicited` calls
    `contract_unmet_token()` directly and still refuses to run without it: a
    pipeline that can refuse a field must be able to name the refusal.
    """
    try:
        return non_value_tokens(load(codebook_path))
    except Exception:                       # missing file, missing key, bad YAML
        return frozenset()


# ── Evidence-modality lint (ELICIT-DESIGN-02 Ruling 3(b)) ────────────

# Codebook keys that tell the model HOW its evidence must be shaped, as opposed
# to what the field means. Only one exists today; the set is named so a second
# one inherits the rule instead of being added to a check by hand.
EVIDENCE_MODALITY_FLAGS = ("source_quote_required",)

# Which classes each flag can honestly coexist with, derived from what the class
# contract asks for -- not from which fields happen to carry the flag.
#
#   STATED     the paper asserts the value, so the passage carrying it IS
#              quotable. "Quote it" and "cite the unit asserting it" ask for the
#              same evidence in two notations. Compatible.
#   INFERABLE  the paper FIXES the value without stating it. There is no passage
#              that states the value to quote; that is the class definition.
#   JUDGMENT   the value is a synthesis no single passage states, and the
#              contract asks for reasoning STEPS. ELICIT-DESIGN-01's F5 measured
#              the collision directly: `key_limitation` carried this flag and
#              returned STEPS_MISSING on 5 of 6 attempts across all three papers,
#              because "the codebook asks for a quote, and the class contract
#              asks for steps, and the model does the first."
_MODALITY_COMPATIBLE: dict[str, frozenset[str]] = {
    "source_quote_required": frozenset({STATED}),
}


def check_evidence_modality(codebook: dict) -> list[str]:
    """Fields whose evidence-modality flag contradicts their class contract.

    Returns a list of human-readable violations, empty when the codebook is
    clean. A LINT rather than a load-time raise: it describes a codebook that
    will elicit worse answers, not one the engine cannot run against.
    """
    known = classes_by_field(codebook)
    out: list[str] = []
    for f in codebook.get("fields", []):
        name = f.get("name")
        cls = known.get(name)
        if not cls:
            continue
        for flag in EVIDENCE_MODALITY_FLAGS:
            if not f.get(flag):
                continue
            if cls not in _MODALITY_COMPATIBLE.get(flag, frozenset()):
                out.append(
                    f"{name}: `{flag}` contradicts its {cls.upper()} contract — "
                    f"the flag asks for a quotable passage, the contract asks for "
                    f"{'a declared inference' if cls == INFERABLE else 'reasoning steps'}"
                )
    return out
