"""Deterministic pre-judge flags for Paper 1 concordance pairs.

Pure computation: no DB I/O, no LLM calls, no logging.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal, Optional

FieldType = Literal["categorical", "numeric", "free_text"]

SpanMatchMethod = Literal["exact_substring", "jaccard_fallback", "none"]
ValueMatchMethod = Literal[
    "categorical_exact",
    "numeric_tolerance",
    "freetext_jaccard",
    "none",
]

STOP_WORDS: frozenset[str] = frozenset(
    {
        "a", "an", "and", "are", "as", "at", "be", "been", "being", "but",
        "by", "can", "did", "do", "does", "for", "from", "had", "has",
        "have", "in", "is", "it", "its", "of", "on", "or", "our", "that",
        "the", "their", "these", "this", "those", "to", "was", "were",
        "will", "with", "within", "without",
    }
)

_WS_RUN = re.compile(r"\s+")
_NUMERIC_TOKEN = re.compile(r"-?\d+(?:,\d{3})*(?:\.\d+)?")
_LEADING_NUMERIC = re.compile(r"^-?\d+(?:\.\d+)?")
_ALPHA_TOKEN = re.compile(r"[a-z]+")


@dataclass(frozen=True)
class PreCheckFlags:
    span_present: bool
    span_in_source: bool
    value_in_span: bool
    span_length: int
    span_match_method: SpanMatchMethod
    value_match_method: ValueMatchMethod


def _normalize_ws(text: str) -> str:
    return _WS_RUN.sub(" ", text).strip().lower()


def _parse_float(raw: str) -> Optional[float]:
    s = raw.strip().replace(",", "")
    m = _LEADING_NUMERIC.match(s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def _check_span_in_source(span: str, source_text: str) -> tuple[bool, SpanMatchMethod]:
    if not source_text:
        return False, "none"

    norm_span = _normalize_ws(span)
    norm_source = _normalize_ws(source_text)

    if not norm_span:
        return False, "none"

    if norm_span in norm_source:
        return True, "exact_substring"

    span_tokens = norm_span.split()
    source_tokens = norm_source.split()

    if not span_tokens or len(span_tokens) > len(source_tokens):
        return False, "none"

    if not any(_ALPHA_TOKEN.search(t) for t in span_tokens):
        return False, "none"

    span_set = set(span_tokens)
    source_set = set(source_tokens)
    if not (span_set & source_set):
        return False, "none"

    n = len(span_tokens)
    max_j = 0.0
    for i in range(len(source_tokens) - n + 1):
        window_set = set(source_tokens[i : i + n])
        union = len(span_set | window_set)
        if union == 0:
            continue
        inter = len(span_set & window_set)
        j = inter / union
        if j > max_j:
            max_j = j
            if max_j >= 0.9:
                return True, "jaccard_fallback"

    return False, "none"


def _check_categorical(value: str, span: str) -> bool:
    nv = _normalize_ws(value)
    ns = _normalize_ws(span)
    if not nv:
        return False
    return nv in ns


def _check_numeric(value: str, span: str, tolerance: float) -> bool:
    parsed = _parse_float(value)
    if parsed is None:
        return False

    candidates: list[float] = []
    for m in _NUMERIC_TOKEN.finditer(span):
        raw = m.group(0).replace(",", "")
        try:
            candidates.append(float(raw))
        except ValueError:
            continue

    if not candidates:
        return False

    if tolerance == 0.0 and parsed == int(parsed):
        target = int(parsed)
        return any(c == int(c) and int(c) == target for c in candidates)

    return any(abs(c - parsed) <= tolerance for c in candidates)


def _tokenize_freetext(text: str) -> list[str]:
    toks = _ALPHA_TOKEN.findall(text.lower())
    return [t for t in toks if len(t) >= 2 and t not in STOP_WORDS]


def _check_freetext(value: str, span: str) -> bool:
    v_tokens = set(_tokenize_freetext(value))
    if not v_tokens:
        return False
    s_tokens = set(_tokenize_freetext(span))
    if not s_tokens:
        return False
    union = v_tokens | s_tokens
    if not union:
        return False
    jaccard = len(v_tokens & s_tokens) / len(union)
    return jaccard >= 0.3


def compute_precheck_flags(
    value: Optional[str],
    span: Optional[str],
    source_text: str,
    field_type: FieldType,
    numeric_tolerance: float = 0.0,
) -> PreCheckFlags:
    """Compute deterministic pre-check flags for a single (value, span, source) triple.

    Returns all-False flags with match methods "none" when value or span is
    missing/empty (covers NR / absent-field cases).
    """
    if value is None or span is None:
        return PreCheckFlags(
            span_present=False,
            span_in_source=False,
            value_in_span=False,
            span_length=0,
            span_match_method="none",
            value_match_method="none",
        )

    stripped_span = span.strip()
    span_length = len(stripped_span)

    if span_length == 0 or value.strip() == "":
        return PreCheckFlags(
            span_present=False,
            span_in_source=False,
            value_in_span=False,
            span_length=span_length,
            span_match_method="none",
            value_match_method="none",
        )

    span_present = span_length >= 10

    span_in_source, span_match_method = _check_span_in_source(span, source_text)

    value_match_method: ValueMatchMethod
    if field_type == "categorical":
        value_in_span = _check_categorical(value, span)
        value_match_method = "categorical_exact"
    elif field_type == "numeric":
        parsed = _parse_float(value)
        if parsed is None:
            value_in_span = False
            value_match_method = "none"
        else:
            value_in_span = _check_numeric(value, span, numeric_tolerance)
            value_match_method = "numeric_tolerance"
    elif field_type == "free_text":
        v_tokens = _tokenize_freetext(value)
        if not v_tokens:
            value_in_span = False
            value_match_method = "none"
        else:
            value_in_span = _check_freetext(value, span)
            value_match_method = "freetext_jaccard"
    else:
        raise ValueError(f"Unknown field_type: {field_type!r}")

    return PreCheckFlags(
        span_present=span_present,
        span_in_source=span_in_source,
        value_in_span=value_in_span,
        span_length=span_length,
        span_match_method=span_match_method,
        value_match_method=value_match_method,
    )


__all__ = ["FieldType", "PreCheckFlags", "compute_precheck_flags", "STOP_WORDS"]
