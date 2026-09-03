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
"""

from __future__ import annotations

from pathlib import Path

from engine.agents.extractor import _find_codebook_path, _load_codebook

STATED = "stated"
INFERABLE = "inferable"
JUDGMENT = "judgment"
CLASSES = (STATED, INFERABLE, JUDGMENT)

ELICITATION_VERSION = "elicit-design-01"


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
