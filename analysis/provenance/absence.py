"""ABSENCE_CLAIM detection — pinned pattern set (DEFINITIONS.md v1.1 §A).

An absence claim is a snippet that *asserts the paper does not report the item*
instead of quoting the paper. It is a different defect from an invented
quotation: the arm reached a defensible conclusion about the source and then
recorded that conclusion in the column reserved for copied text.

Detection is an enumerated set of six anchored regexes over the normalized
snippet. There is no fuzzy matching, no similarity threshold, and no unlogged
heuristic: a snippet either matches one of the six named patterns or it does
not, and the matching pattern name is persisted on every classified span so any
verdict can be traced to the rule that produced it.

Pattern derivation is empirical but closed: the surface forms were enumerated
from the 1,037 UNTRACEABLE_NO_BASIS spans of census
`provcensus_surgical_autonomy_20260727T183832Z`, and every candidate pattern was
measured for precision against that corpus before being admitted. Two candidates
were measured and REJECTED, and are recorded here so the exclusions are as
auditable as the inclusions:

  - `^there (is|are|was|were) no\\b` — 1 corpus match, and it was a false
    positive ("There was no statistically significant difference (all p > 0.05)
    between the targeting accuracy..."), which is a reported result, not a
    statement about reporting. Zero true positives. Rejected outright.
  - A bare `without <reporting verb>` rule — 6 matches, 3 of them false
    positives on a real results passage. Rejected in that form and re-admitted
    as P5 only when bound to a paper-referent subject, which scored 2/2.

The governing distinction throughout: a claim about *the reporting* is an
absence claim; a claim about *the world or the results* is not.
"""

from __future__ import annotations

import re

from analysis.provenance.normalize import normalize

ABSENCE_PATTERN_VERSION = "prov-absence-1"

# Subjects that refer to the source document itself.
_REFERENT = r"(?:the\s+|this\s+)?(?:paper|study|article|manuscript|authors?|text|work|document|source)"

# Negation forms. Quote folding (T4) has already straightened apostrophes.
_NEGATION = (
    r"(?:(?:does|do|did|is|are|was|were)\s+not"
    r"|(?:does|do|did|is|are|was|were)n't"
    r"|fails?\s+to|never)"
)

# Verbs of *reporting*. The object of the negation must be one of these for a
# sentence to count as a claim about the reporting rather than about the world.
_REPORT_VERB = (
    r"report(?:s|ed)?|state[sd]?|mention(?:s|ed)?|specif(?:y|ies|ied)|describe[sd]?"
    r"|provide[sd]?|compar(?:e|es|ed)|discuss(?:es|ed)?|present(?:s|ed)?|giv(?:e|es)"
    r"|given|includ(?:e|es|ed)|indicat(?:e|es|ed)|defin(?:e|es|ed)|quantif(?:y|ies|ied)"
    r"|list(?:s|ed)?|address(?:es|ed)?|detail(?:s|ed)?|nam(?:e|es|ed)"
    r"|identif(?:y|ies|ied)|explicitly"
)

# Predicates that mark a bare "no <noun phrase>" as being about the reporting.
_ABSENCE_PREDICATE = (
    r"report(?:s|ed)?|mention(?:s|ed)?|describ(?:e|es|ed)|provide[sd]?|state[sd]?"
    r"|specif(?:y|ies|ied)|giv(?:e|es|en)|list(?:s|ed)?|present(?:s|ed)?"
    r"|discuss(?:es|ed)?|indicat(?:e|es|ed)|available|explicit(?:ly)?|made"
    r"|performed|conducted|found|mention"
)

_WITHOUT_REPORT_VERB = (
    r"compar\w+|report\w+|specif\w+|mention\w+|describ\w+|provid\w+|stat\w+|discuss\w+"
)

# ── The pinned pattern set. Order is the evaluation order. ───────────────
#
# P4 is listed last but carries a precedence exception (see §A3 of
# DEFINITIONS.md and `is_absence_claim` below): a bare sentinel is an absence
# claim even when it "anchors", because a two-character string like "NR" occurs
# as a substring of nearly every paper by accident and cannot be a quotation.

ABSENCE_PATTERNS: tuple[tuple[str, re.Pattern, str], ...] = (
    (
        "P1_referent_negation",
        re.compile(
            rf"^{_REFERENT}\b[^.]{{0,60}}\b{_NEGATION}\b[^.]{{0,45}}\b(?:{_REPORT_VERB})\b"
        ),
        'Snippet opens by naming the source document and negates a reporting verb — '
        '"The paper does not explicitly state the sample size."',
    ),
    (
        "P2_bare_no_np",
        re.compile(rf"^no\b[^.]{{0,80}}\b(?:{_ABSENCE_PREDICATE})\b"),
        'Snippet opens with "no <noun phrase>" and carries a reporting predicate — '
        '"No comparison reported.", "No explicit selection process described."',
    ),
    (
        "P3_not_explicitly",
        re.compile(
            r"^not\s+(?:explicitly\s+|clearly\s+|directly\s+|specifically\s+)?"
            r"(?:stated|mentioned|reported|specified|described|provided|given"
            r"|discussed|addressed|available|applicable|found|compared|quantified)\b"
        ),
        'Elliptical absence assertion with the subject dropped — "Not explicitly stated."',
    ),
    (
        "P5_referent_without_report",
        re.compile(
            rf"^{_REFERENT}\b[^.]{{0,70}}\bwithout\s+(?:explicitly\s+|directly\s+|any\s+)?"
            rf"(?:{_WITHOUT_REPORT_VERB})\b"
        ),
        'Positively-phrased absence: the source is said to do something *without* '
        'reporting the item — "The paper focuses on autonomous performance without '
        'comparing to human operators." Bound to a paper-referent subject because the '
        'unbound form scored 3 false positives in 6 matches.',
    ),
    (
        "P6_only_x_reported",
        re.compile(
            r"^only\b[^.]{0,70}\b(?:reported|mentioned|provided|stated|described|available)\b"
        ),
        'Exhaustiveness assertion implying the remainder is absent — "Only one primary '
        'outcome was reported." Observed exclusively on secondary_outcomes.',
    ),
    (
        "P4_bare_sentinel",
        re.compile(
            r"^(?:nr|n/a|na|not_found|not found|not reported|none|none reported"
            r"|not applicable|no comparison reported)\.?$"
        ),
        'The whole snippet is an absence sentinel rather than a quotation — "NR". '
        'Carries a precedence exception: see is_absence_claim().',
    ),
)

BARE_SENTINEL_PATTERN = "P4_bare_sentinel"

PATTERN_NAMES = tuple(name for name, _, _ in ABSENCE_PATTERNS)


def match_absence_pattern(snippet: str | None) -> str | None:
    """Return the name of the first matching pinned pattern, or None.

    Operates on the normalized snippet (DEFINITIONS.md §2), so case, quote
    style, ligatures and whitespace are already folded.
    """
    text = normalize(snippet or "")
    if not text:
        return None
    for name, rx, _ in ABSENCE_PATTERNS:
        if rx.search(text):
            return name
    return None


def is_absence_claim(snippet: str | None, anchored: bool) -> tuple[bool, str | None]:
    """Decide ABSENCE_CLAIM, honouring the anchoring precedence rule.

    Returns (verdict, pattern_name). `anchored` is whether the normalized
    snippet occurs contiguously in the normalized paper text.

    Precedence (DEFINITIONS.md v1.1 §A3):
      - P4 (bare sentinel) wins unconditionally. A two-character snippet such as
        "NR" is a substring of nearly every paper by accident; treating that
        coincidence as a quotation would be a false ANCHORED verdict, and a bare
        sentinel cannot be evidence for anything under any reading.
      - Every other pattern loses to ANCHORED. If the paper itself contains the
        sentence "no comparison to a human operator was performed", an arm that
        quotes it verbatim has produced a real quotation, and the taxonomy must
        say ANCHORED rather than ABSENCE_CLAIM.
    """
    pattern = match_absence_pattern(snippet)
    if pattern is None:
        return False, None
    if pattern == BARE_SENTINEL_PATTERN:
        return True, pattern
    if anchored:
        return False, pattern
    return True, pattern
