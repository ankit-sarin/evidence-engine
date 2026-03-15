"""Field-pair scoring for concordance analysis across extraction arms."""

from dataclasses import dataclass

from engine.analysis.normalize import normalize_for_concordance
from engine.core.review_spec import ReviewSpec


@dataclass
class FieldScore:
    """Result of comparing two extracted values for a single field."""

    result: str  # "MATCH", "MISMATCH", or "AMBIGUOUS"
    detail: str  # Human-readable explanation


def _jaccard(a: set, b: set) -> float:
    """Jaccard similarity coefficient."""
    if not a and not b:
        return 1.0
    union = a | b
    if not union:
        return 1.0
    return len(a & b) / len(union)


def _tokenize(text: str) -> set[str]:
    """Split text into lowercase word tokens."""
    return set(text.lower().split())


def _score_free_text(a: str, b: str) -> FieldScore:
    """Score two free-text values."""
    if a == b:
        return FieldScore("MATCH", "exact match")

    # Substring containment (either direction)
    if a in b:
        return FieldScore("MATCH", f"substring: '{a}' contained in '{b}'")
    if b in a:
        return FieldScore("MATCH", f"substring: '{b}' contained in '{a}'")

    # Token-set overlap
    tokens_a = _tokenize(a)
    tokens_b = _tokenize(b)
    if not tokens_a or not tokens_b:
        return FieldScore("MISMATCH", f"'{a}' vs '{b}'")

    j = _jaccard(tokens_a, tokens_b)
    if j == 0:
        return FieldScore("MISMATCH", f"no token overlap: '{a}' vs '{b}'")
    if j > 0.7:
        return FieldScore("AMBIGUOUS", f"token Jaccard={j:.2f}: '{a}' vs '{b}'")
    return FieldScore("AMBIGUOUS", f"partial token overlap Jaccard={j:.2f}: '{a}' vs '{b}'")


def score_pair(
    field_name: str,
    value_a: str | None,
    value_b: str | None,
    spec: ReviewSpec | None = None,
) -> FieldScore:
    """Score a pair of extracted values for concordance.

    Values are normalized before comparison. Returns a FieldScore with
    result (MATCH/MISMATCH/AMBIGUOUS) and a detail string.
    """
    norm_a = normalize_for_concordance(field_name, value_a, spec)
    norm_b = normalize_for_concordance(field_name, value_b, spec)

    # Both None → both correctly identified absence
    if norm_a is None and norm_b is None:
        return FieldScore("MATCH", "both absent")

    # One None, one has value
    if norm_a is None or norm_b is None:
        present = norm_a if norm_b is None else norm_b
        return FieldScore("MISMATCH", f"one absent, one '{present}'")

    # Set comparison (multi-value categorical)
    if isinstance(norm_a, set) or isinstance(norm_b, set):
        set_a = norm_a if isinstance(norm_a, set) else {norm_a}
        set_b = norm_b if isinstance(norm_b, set) else {norm_b}
        j = _jaccard(set_a, set_b)
        if j == 1.0:
            return FieldScore("MATCH", f"identical sets: {set_a}")
        if j == 0.0:
            return FieldScore("MISMATCH", f"disjoint sets: {set_a} vs {set_b}")
        diff_a = set_a - set_b
        diff_b = set_b - set_a
        return FieldScore(
            "AMBIGUOUS",
            f"Jaccard={j:.2f}, only in A: {diff_a}, only in B: {diff_b}",
        )

    # String comparison
    if isinstance(norm_a, str) and isinstance(norm_b, str):
        # Exact match (covers categorical after normalization, numeric, free-text)
        if norm_a == norm_b:
            return FieldScore("MATCH", f"exact: '{norm_a}'")

        # For free-text fields, apply fuzzy matching
        from engine.analysis.normalize import _get_field_def
        field_def = _get_field_def(field_name, spec)
        is_categorical = field_def and field_def.type == "categorical"

        if is_categorical:
            return FieldScore("MISMATCH", f"'{norm_a}' vs '{norm_b}'")

        return _score_free_text(norm_a, norm_b)

    # Fallback
    return FieldScore("MISMATCH", f"type mismatch: {type(norm_a)} vs {type(norm_b)}")
