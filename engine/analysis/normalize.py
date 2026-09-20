"""Canonical normalization for concordance comparison across extraction arms."""

import re
from functools import lru_cache

from engine.core.review_paths import load_spec_for
from engine.core.review_spec import ReviewSpec
from engine.core.codebook import load_codebook_for

# Fields that allow semicolon-separated multi-values (per extraction prompt).
_MULTI_VALUE_FIELDS = {"validation_setting", "surgical_domain", "secondary_outcomes"}

#: The empty string is absence in any vocabulary; everything else comes from the
#: codebook (INSTRUMENTS-01). The hardcoded set this replaced diverged from the
#: codebook in both directions — it lacked `NA` and `NOT FOUND`, which the
#: codebook declares as absence sentinels, and carried `none`, which it does
#: not. `none` is a VALUE: "none" can mean no complications, and reading it as
#: "the paper does not report this" silently turns a finding into a gap.
_ALWAYS_NULL = {""}

#: OUT OF SCOPE here, recorded for S5b. Two dispatch lists that are not the
#: codebook's and should be:
#:   * `_PASSTHROUGH_NUMERIC_FIELDS` names `primary_outcome_value`, which the
#:     codebook types `free_text`; the branch skips lowercasing, so it is a
#:     third normalization behaviour with no declaration behind it.
#:   * `_MULTI_VALUE_FIELDS` has no codebook source at all, and its
#:     `secondary_outcomes` entry is DEAD — the multi-value branch runs only
#:     for `type == "categorical"` and that field is `free_text`.
_PASSTHROUGH_NUMERIC_FIELDS = {"primary_outcome_value"}


def _build_prefix_map(enum_values: list[str]) -> dict[str, str]:
    """Build a map from unique prefixes to canonical enum values.

    For each enum value, extract its leading token(s) and check whether that
    prefix uniquely identifies exactly one enum value.  E.g. "2" uniquely
    matches "2 (Task autonomy)" but "Research" might match multiple entries.

    Only prefixes that are strict substrings (not the full value) are included
    — full values already match exactly.
    """
    prefix_map: dict[str, str] = {}
    for ev in enum_values:
        # Extract the leading prefix before any parenthetical
        paren_match = re.match(r"^(.+?)\s*\(", ev)
        if paren_match:
            prefix = paren_match.group(1).strip()
            # Ensure this prefix uniquely maps to one enum value
            matches = [v for v in enum_values if v.startswith(prefix + " ") or v == prefix]
            if len(matches) == 1:
                prefix_map[prefix] = ev
    return prefix_map


#: GENERALIZE B2, still open. Concordance normalisation falls back to ONE
#: review's enum set when no spec is passed, which resolves another review's
#: categorical values against the wrong vocabulary — silently, with no
#: exception, just mismatches. SPEC-AUTH-01 only stops this module building
#: the path itself; the hardcoded review is PATH-AUTH-01's to remove.
_FALLBACK_REVIEW_ID = "surgical_autonomy"


@lru_cache(maxsize=1)
def _default_codebook():
    return load_codebook_for(_FALLBACK_REVIEW_ID)


def _get_field_def(field_name: str, spec=None):
    """The field's type and allowed values, from the codebook.

    `spec` is accepted and ignored: callers pass one, and the schema it used
    to carry is gone (SCHEMA-DERIVE-01).
    """
    cb = _default_codebook()
    try:
        return cb.view(field_name)
    except Exception:
        return None


@lru_cache(maxsize=1)
def _absence_sentinels() -> frozenset[str]:
    """The absence vocabulary, lowercased, from the codebook.

    One source. The codebook declares `absence_sentinels`, the extraction
    prompt instructs the model with them, and the write-boundary guard enforces
    them; normalization reading a different list meant a value the engine had
    told the model to use for absence could arrive here as a free-text answer.
    """
    cb = _default_codebook()
    declared = getattr(cb, "absence_sentinels", None) or ()
    return frozenset(_ALWAYS_NULL | {str(s).strip().lower() for s in declared})


def _normalize_null(raw: str | None) -> str | None:
    """Return None if the value represents absence."""
    if raw is None:
        return None
    if raw.strip().lower() in _absence_sentinels():
        return None
    return raw


def _normalize_categorical(raw: str, field_def) -> str:
    """Normalize a single categorical value against enum_values."""
    stripped = raw.strip()
    if not field_def.enum_values:
        return stripped

    # Exact match
    if stripped in field_def.enum_values:
        return stripped

    # Case-insensitive exact match
    lower_map = {v.lower(): v for v in field_def.enum_values}
    if stripped.lower() in lower_map:
        return lower_map[stripped.lower()]

    # Prefix match (e.g. "2" → "2 (Task autonomy)")
    prefix_map = _build_prefix_map(field_def.enum_values)
    if stripped in prefix_map:
        return prefix_map[stripped]

    # No match — return as-is (cross-field bleed, novel values)
    return stripped


def _normalize_free_text(raw: str) -> str:
    """Lowercase, strip, collapse whitespace."""
    return re.sub(r"\s+", " ", raw.strip().lower())


def normalize_for_concordance(
    field_name: str,
    raw_value: str | None,
    spec: ReviewSpec | None = None,
) -> str | set[str] | None:
    """Normalize a raw extracted value for concordance comparison.

    Returns:
        None — value represents absence (NR, empty, etc.)
        str — normalized single value
        set[str] — normalized set of values (for multi-value fields)
    """
    checked = _normalize_null(raw_value)
    if checked is None:
        return None

    field_def = _get_field_def(field_name, spec)

    # Numeric fields. Dispatch is the CODEBOOK's `type`, not a second list
    # (INSTRUMENTS-01): the list it replaced happened to agree with the
    # codebook, which is not the same as being derived from it.
    #
    # Normalization no longer interprets the number. It used to delete every
    # non-digit — turning "2.5" into "25", "-5" into "5" and "50-60" into
    # "5060" — and return a string, so "2.5" and "25" compared equal. Text is
    # canonicalized here; numbers are parsed and compared in
    # `engine.analysis.scoring.parse_number`, which is the one place that knows
    # what a number is.
    if field_def is not None and getattr(field_def, "type", None) == "numeric":
        return _normalize_free_text(checked)
    if field_name in _PASSTHROUGH_NUMERIC_FIELDS:
        return re.sub(r"\s+", " ", checked.strip())

    # Categorical fields
    if field_def and field_def.type == "categorical":
        # Multi-value categorical: split on semicolons, return set
        if field_name in _MULTI_VALUE_FIELDS and ";" in checked:
            parts = [p.strip() for p in checked.split(";") if p.strip()]
            normalized = set()
            for part in parts:
                null_check = _normalize_null(part)
                if null_check is not None:
                    normalized.add(_normalize_categorical(null_check, field_def))
            return normalized if normalized else None

        return _normalize_categorical(checked, field_def)

    # Free-text fields
    return _normalize_free_text(checked)
