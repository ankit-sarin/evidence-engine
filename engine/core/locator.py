"""The one deterministic locator (R17; WRITE-PATH-01 9b-2d).

"A single implementation of the auditor's normalize + exact/fuzzy match." It
answers one question — does this snippet occur in this parsed text — and
nothing else: no connection, no spec, no model, no clock. The test is identical
for a value and for an absence sentinel (R17); a claim with no snippet is simply
not located, and provenance says why (`snippet_supplied`), which is R22-U6's
point that "located = false" and "no snippet" are one state with two histories.

**Verdict-identical to the auditor's `grep_verify`** (9b-2d R3, R6): the same
`normalize`, moved here verbatim; exact substring on normalized text first;
else every word window of the snippet's length, `SequenceMatcher` ratio, located
when the best window's ratio is strictly greater than the threshold. The one
difference is that the whole text is scanned so the best ratio can be reported
as `score` — `grep_verify` stopped at the first window over the threshold, which
decides the same `located` but cannot say how close a miss was.

`LOCATOR_VERSION` names this algorithm. Changing the normalisation, the window
rule or the threshold is a new version, never an edit, because every
`citation_located` event records the version it was decided by.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher

from engine.core.constants import INVALID_SNIPPET_RE

LOCATOR_VERSION = "locator-1"
FUZZY_THRESHOLD = 0.85

EXACT, FUZZY, NONE = "exact", "fuzzy", "none"

_WS_RE = re.compile(r"\s+")
_PUNCT_GLUED_RE = re.compile(r"(?<=\w)\.(?=\w)")
_SMART_QUOTES = str.maketrans({
    "‘": "'", "’": "'",   # single curly quotes
    "“": '"', "”": '"',   # double curly quotes
    "–": "-", "—": "-",   # en/em dash
})


def normalize(text: str) -> str:
    """Normalize text for comparison: lowercase, collapse whitespace,
    fix glued punctuation (Table.I → Table I), straighten quotes."""
    text = text.translate(_SMART_QUOTES)
    text = unicodedata.normalize("NFKC", text)
    text = _PUNCT_GLUED_RE.sub(" ", text)
    return _WS_RE.sub(" ", text.lower()).strip()


@dataclass(frozen=True)
class LocateResult:
    located: bool
    kind: str                  # EXACT | FUZZY | NONE
    score: float | None        # 1.0 exact; best window ratio otherwise; None if not tried
    snippet_supplied: bool
    bridged: bool              # the snippet bridges passages with an ellipsis


def locate(parsed_text: str, snippet: str | None, *,
           threshold: float = FUZZY_THRESHOLD) -> LocateResult:
    """Where, if anywhere, `snippet` occurs in `parsed_text`. Pure and deterministic."""
    # R8 (9b-2d): a snippet that normalises to nothing is no snippet. The legacy
    # grep let "   " through and found "" in every text; that is not a location.
    norm_snippet = normalize(snippet) if snippet else ""
    supplied = bool(norm_snippet)
    bridged = bool(supplied and INVALID_SNIPPET_RE.search(snippet))
    if not supplied or not parsed_text:
        return LocateResult(False, NONE, None, supplied, bridged)

    norm_text = normalize(parsed_text)
    if norm_snippet in norm_text:
        return LocateResult(True, EXACT, 1.0, True, bridged)

    snippet_words = norm_snippet.split()
    text_words = norm_text.split()
    size = len(snippet_words)
    if size == 0:
        return LocateResult(False, NONE, None, True, bridged)

    best = 0.0
    for i in range(max(1, len(text_words) - size + 1)):
        window = " ".join(text_words[i: i + size])
        ratio = SequenceMatcher(None, norm_snippet, window).ratio()
        if ratio > best:
            best = ratio
    located = best > threshold
    return LocateResult(located, FUZZY if located else NONE, best, True, bridged)


def locate_payload(result: LocateResult, *, threshold: float, parsed_text_sha256: str,
                   parsed_text_uid: str) -> dict:
    """The `citation_located` event payload (R17): the verdict, the threshold, the
    locator version and the parsed-text identity that was checked."""
    return {"located": result.located, "kind": result.kind, "score": result.score,
            "threshold": threshold, "locator_version": LOCATOR_VERSION,
            "snippet_supplied": result.snippet_supplied, "bridged": result.bridged,
            "parsed_text_sha256": parsed_text_sha256, "parsed_text_uid": parsed_text_uid}
