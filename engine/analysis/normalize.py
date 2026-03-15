"""Canonical normalization for concordance comparison across extraction arms."""

import re
from functools import lru_cache

from engine.core.review_spec import ExtractionField, ReviewSpec, load_review_spec

# Fields that allow semicolon-separated multi-values (per extraction prompt).
_MULTI_VALUE_FIELDS = {"validation_setting", "surgical_domain", "secondary_outcomes"}

# Values that all normalize to None (absence).
_NULL_SYNONYMS = {"", "nr", "n/r", "not reported", "not_found", "none", "n/a"}

# Numeric fields with special handling.
_NUMERIC_FIELDS = {"sample_size"}
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


@lru_cache(maxsize=1)
def _default_spec() -> ReviewSpec:
    return load_review_spec("review_specs/surgical_autonomy_v1.yaml")


def _get_field_def(field_name: str, spec: ReviewSpec | None = None) -> ExtractionField | None:
    s = spec or _default_spec()
    for f in s.extraction_schema.fields:
        if f.name == field_name:
            return f
    return None


def _normalize_null(raw: str | None) -> str | None:
    """Return None if the value represents absence."""
    if raw is None:
        return None
    if raw.strip().lower() in _NULL_SYNONYMS:
        return None
    return raw


def _normalize_categorical(raw: str, field_def: ExtractionField) -> str:
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


def _normalize_numeric(raw: str) -> str:
    """Strip non-numeric characters, return integer string."""
    digits = re.sub(r"[^\d]", "", raw)
    if not digits:
        return raw.strip()
    return str(int(digits))


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

    # Numeric fields
    if field_name in _NUMERIC_FIELDS:
        return _normalize_numeric(checked)
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
