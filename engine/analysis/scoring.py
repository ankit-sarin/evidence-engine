"""Field-pair scoring for concordance analysis across extraction arms.

INSTRUMENTS-01 fixed three rules that made false matches:

1. **Numbers were never compared as numbers.** `sample_size` was normalized by
   deleting every non-digit and comparing the result as a string, so "2.5" and
   "25" were the same value, "-5" and "5" were the same value, and "12.5%"
   became "125". Numeric fields are now parsed and compared as numbers.
2. **Containment ignored word boundaries.** The rule was raw substring
   containment in either direction, so "5" matched "50" and "cid" would match
   "acidosis". Containment now counts only as a contiguous run of whole tokens.
   The case the rule was written for — "da Vinci Xi" inside "da Vinci Xi
   (Intuitive Surgical)" — still matches.
3. **Negation was invisible.** "benefit" is contained in "no benefit", so they
   matched. A value carrying a negation cue when the other does not can no
   longer be a MATCH.

The rules are ordered: absence, then set comparison, then exact equality, then
the numeric rule for numeric fields, then the categorical rule, then free text.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from engine.analysis.normalize import normalize_for_concordance
from engine.core.review_spec import ReviewSpec


@dataclass
class FieldScore:
    """Result of comparing two extracted values for a single field.

    `norm_a` / `norm_b` are the normalized labels the verdict was reached on.
    They travel with the score so that kappa can be computed from the raters'
    own marginals without a second normalization site (INSTRUMENTS-01).
    """

    result: str  # "MATCH", "MISMATCH", or "AMBIGUOUS"
    detail: str  # Human-readable explanation
    norm_a: object = None
    norm_b: object = None


# ── Negation ─────────────────────────────────────────────────────────
#
# One list, one place, tested. Cues are matched as whole TOKENS, which is why
# multi-word negations do not need their own entries: "did not" is caught by
# "not", "failed to" by "failed", "lack of" by "lack". A contraction tokenizes
# with its stem separated ("didn't" -> "didn", "t"), so the stems are listed.

NEGATION_CUES = frozenset({
    "no", "not", "non", "never", "without", "absent", "absence",
    "lack", "lacks", "lacking", "neither", "nor", "none",
    "cannot", "fail", "fails", "failed", "unable", "unchanged",
    "didn", "doesn", "wasn", "weren", "isn", "aren", "won", "couldn", "hasn",
})


def _tokens(text: str) -> list[str]:
    """Alphanumeric tokens. Punctuation is a boundary, not a character."""
    return re.findall(r"[0-9a-z]+", text.lower())


def _has_negation(tokens: list[str]) -> bool:
    return any(t in NEGATION_CUES for t in tokens)


def _contiguous_run(haystack: list[str], needle: list[str]) -> bool:
    """True if `needle` appears in `haystack` as consecutive whole tokens."""
    if not needle or len(needle) > len(haystack):
        return False
    return any(
        haystack[i:i + len(needle)] == needle
        for i in range(len(haystack) - len(needle) + 1)
    )


# ── Numbers ──────────────────────────────────────────────────────────
#
# A real grammar rather than "delete every non-digit". Leading qualifiers and a
# trailing unit WORD are tolerated because they are noise on a count ("n=50",
# "50 patients"); a percent sign is not noise and is kept as a marker, so
# "12.5%" and "12.5" cannot match. A range ("50-60") parses as nothing and
# falls through to the text rules rather than becoming 5060.

_NUMBER_RE = re.compile(
    r"""^\s*
        (?:n\s*=\s*|~|≈|approx(?:imately)?\s+|about\s+|ca\.?\s+)?   # qualifier
        (?P<sign>[+-]?)
        (?P<int>\d{1,3}(?:,\d{3})+|\d+)                             # 1,000 or 1000
        (?:\.(?P<frac>\d+))?
        \s*
        (?P<pct>%)?
        (?:\s*(?P<unit>[a-z][a-z/\-]*))?                            # unit word
        \s*$""",
    re.VERBOSE | re.IGNORECASE,
)


def parse_number(text: str) -> tuple[Decimal, str] | None:
    """`(value, marker)` for a numeric string, or None if it is not one.

    `marker` is `"%"` or `""`. A trailing unit WORD is discarded — on a count it
    is noise, and treating "50 patients" and "50 cases" as different answers
    would manufacture disagreement. A percent marker is kept.
    """
    m = _NUMBER_RE.match(text or "")
    if not m:
        return None
    digits = m.group("int").replace(",", "")
    frac = m.group("frac")
    raw = f"{m.group('sign') or ''}{digits}" + (f".{frac}" if frac else "")
    try:
        value = Decimal(raw)
    except InvalidOperation:  # pragma: no cover - the grammar precludes it
        return None
    return value, ("%" if m.group("pct") else "")


def _score_numeric(a: str, b: str) -> tuple[str, str]:
    """Numeric-field comparison. Exact equality; no tolerance."""
    pa, pb = parse_number(a), parse_number(b)
    if pa is None and pb is None:
        # Neither is a number — a range, a word, a sentence. The text rules
        # answer it. (Reached only from the numeric-FIELD path: the free-text
        # path calls this only when both sides parse.)
        return _score_free_text(a, b)
    if pa is None or pb is None:
        unparsed = a if pa is None else b
        return "AMBIGUOUS", f"only one value is numeric: '{unparsed}' is not a number"
    (va, ma), (vb, mb) = pa, pb
    if ma != mb:
        return "AMBIGUOUS", f"unit markers differ: '{a}' ({ma or 'none'}) vs '{b}' ({mb or 'none'})"
    if va == vb:
        return "MATCH", f"numeric equal: {va}{ma}"
    return "MISMATCH", f"numeric differ: {va}{ma} vs {vb}{mb}"


# ── Free text ────────────────────────────────────────────────────────


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


def _score_free_text(a: str, b: str) -> tuple[str, str]:
    """Score two free-text values."""
    if a == b:
        return "MATCH", "exact match"

    # Numbers before tokens, even on a free-text field. Tokenization drops the
    # decimal point and the percent sign — "12.5%" and "12.5" both tokenize to
    # ["12", "5"] — so the text path would call them the same answer. When both
    # sides are numbers, the numeric rule is the one that knows what they mean.
    if parse_number(a) is not None and parse_number(b) is not None:
        return _score_numeric(a, b)

    tok_a, tok_b = _tokens(a), _tokens(b)

    # Polarity. Checked BEFORE containment, because containment is exactly how
    # "benefit" and "no benefit" used to match: the negation makes the shorter
    # value a substring of its own contradiction.
    polarity_conflict = _has_negation(tok_a) != _has_negation(tok_b)

    if not polarity_conflict:
        # Containment at token boundaries, either direction.
        if _contiguous_run(tok_b, tok_a):
            return "MATCH", f"whole-token containment: '{a}' within '{b}'"
        if _contiguous_run(tok_a, tok_b):
            return "MATCH", f"whole-token containment: '{b}' within '{a}'"

    set_a, set_b = _tokenize(a), _tokenize(b)
    if not set_a or not set_b:
        return "MISMATCH", f"'{a}' vs '{b}'"

    j = _jaccard(set_a, set_b)
    if j == 0:
        return "MISMATCH", f"no token overlap: '{a}' vs '{b}'"
    if polarity_conflict:
        return "AMBIGUOUS", (
            f"negation cue on one side only (Jaccard={j:.2f}): '{a}' vs '{b}'"
        )
    if j > 0.7:
        return "AMBIGUOUS", f"token Jaccard={j:.2f}: '{a}' vs '{b}'"
    return "AMBIGUOUS", f"partial token overlap Jaccard={j:.2f}: '{a}' vs '{b}'"


# ── Entry point ──────────────────────────────────────────────────────


def _verdict(field_name, norm_a, norm_b, spec) -> tuple[str, str]:
    """The verdict and its detail, on already-normalized values."""
    if norm_a is None and norm_b is None:
        return "MATCH", "both absent"

    if norm_a is None or norm_b is None:
        present = norm_a if norm_b is None else norm_b
        return "MISMATCH", f"one absent, one '{present}'"

    if isinstance(norm_a, (set, frozenset)) or isinstance(norm_b, (set, frozenset)):
        set_a = norm_a if isinstance(norm_a, (set, frozenset)) else {norm_a}
        set_b = norm_b if isinstance(norm_b, (set, frozenset)) else {norm_b}
        j = _jaccard(set(set_a), set(set_b))
        if j == 1.0:
            return "MATCH", f"identical sets: {set(set_a)}"
        if j == 0.0:
            return "MISMATCH", f"disjoint sets: {set(set_a)} vs {set(set_b)}"
        return "AMBIGUOUS", (
            f"Jaccard={j:.2f}, only in A: {set(set_a) - set(set_b)}, "
            f"only in B: {set(set_b) - set(set_a)}"
        )

    if isinstance(norm_a, str) and isinstance(norm_b, str):
        if norm_a == norm_b:
            return "MATCH", f"exact: '{norm_a}'"

        from engine.analysis.normalize import _get_field_def
        field_def = _get_field_def(field_name, spec)
        ftype = getattr(field_def, "type", None)

        if ftype == "numeric":
            return _score_numeric(norm_a, norm_b)
        if ftype == "categorical":
            return "MISMATCH", f"'{norm_a}' vs '{norm_b}'"
        return _score_free_text(norm_a, norm_b)

    return "MISMATCH", f"type mismatch: {type(norm_a)} vs {type(norm_b)}"


def score_pair(
    field_name: str,
    value_a: str | None,
    value_b: str | None,
    spec: ReviewSpec | None = None,
) -> FieldScore:
    """Score a pair of extracted values for concordance.

    Values are normalized once, here, and the normalized labels are returned on
    the `FieldScore` so that `metrics.cohens_kappa` can use the raters' own
    marginals without normalizing a second time.
    """
    norm_a = normalize_for_concordance(field_name, value_a, spec)
    norm_b = normalize_for_concordance(field_name, value_b, spec)
    result, detail = _verdict(field_name, norm_a, norm_b, spec)
    return FieldScore(result=result, detail=detail, norm_a=norm_a, norm_b=norm_b)
