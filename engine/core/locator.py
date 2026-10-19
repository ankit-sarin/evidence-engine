"""The one deterministic locator (R17; WRITE-PATH-01 9b-2d; U14, R675).

"A single implementation of the auditor's normalize + exact/fuzzy match." It
answers one question — does this snippet occur in this parsed text — and
nothing else: no connection, no spec, no model, no clock. The test is identical
for a value and for an absence sentinel (R17); a claim with no snippet is simply
not located, and provenance says why (`snippet_supplied`), which is R22-U6's
point that "located = false" and "no snippet" are one state with two histories.

The match: exact substring on normalized text first; else every word window of
the snippet's length is scored with `SequenceMatcher`, the `RESCORE_TOP_K` best
windows are re-scored with `autojunk=False`, and the claim is located when the
best re-scored ratio is strictly greater than the threshold. The whole text is
scanned so the best ratio can be reported as `score`.

Why the re-score (INT-i12-7, R675(c)): `SequenceMatcher`'s automatic junk
heuristic drops every character occurring in more than 1% of a sequence of 200
or more elements, which in an English window is nearly every letter, so a
snippet of 200 or more characters was under-scored and a lightly edited long
quote could fall below the threshold. Below 200 characters the two settings give
the same ratio. Scoring every window with `autojunk=False` costs about eight
times as much on a long text (12s_U14A F11); ranking with the heuristic and
re-scoring only the best windows recovered the same claims at the same windows
at the original cost (12s_U14B variant measurement).

`bridged` (INT-i12-8, R675(d)): the snippet carries an ellipsis
(`INVALID_SNIPPET_RE`) that the located text does not. A verbatim quote of a
passage that itself contains an ellipsis is not bridged; a snippet that joins
passages with an ellipsis the text does not have is, located or not.

`LOCATOR_VERSION` names this algorithm. Changing the normalisation, the window
rule, the scoring or the threshold is a new version, never an edit, because
every `citation_located` event records the version it was decided by.
"locator-1.1" is U14 (R675(f)); "locator-2" is the name R572 and R575 reserve.
"""

from __future__ import annotations

import heapq
import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher

from engine.core.constants import INVALID_SNIPPET_RE

LOCATOR_VERSION = "locator-1.1"
FUZZY_THRESHOLD = 0.85
RESCORE_TOP_K = 5

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
    score: float | None        # 1.0 exact; best re-scored window ratio otherwise; None if not tried
    snippet_supplied: bool
    bridged: bool              # the snippet has an ellipsis the located text does not


def locate(parsed_text: str, snippet: str | None, *,
           threshold: float = FUZZY_THRESHOLD) -> LocateResult:
    """Where, if anywhere, `snippet` occurs in `parsed_text`. Pure and deterministic."""
    # R8 (9b-2d): a snippet that normalises to nothing is no snippet. The legacy
    # grep let "   " through and found "" in every text; that is not a location.
    norm_snippet = normalize(snippet) if snippet else ""
    supplied = bool(norm_snippet)
    ellipsis = bool(supplied and INVALID_SNIPPET_RE.search(snippet))
    if not supplied or not parsed_text:
        return LocateResult(False, NONE, None, supplied, ellipsis)

    norm_text = normalize(parsed_text)
    if norm_snippet in norm_text:
        # An exact match contains the snippet's ellipsis, if it has one.
        return LocateResult(True, EXACT, 1.0, True, False)

    snippet_words = norm_snippet.split()
    text_words = norm_text.split()
    size = len(snippet_words)
    if size == 0:
        return LocateResult(False, NONE, None, True, ellipsis)

    def window(i: int) -> str:
        return " ".join(text_words[i: i + size])

    # Rank every window with the heuristic on, then re-score the best few
    # without it (INT-i12-7). Ties rank the earlier window first.
    ranked = heapq.nlargest(
        RESCORE_TOP_K,
        ((SequenceMatcher(None, norm_snippet, window(i)).ratio(), -i)
         for i in range(max(1, len(text_words) - size + 1))))
    best, best_window = 0.0, ""
    for i in sorted(-neg for _, neg in ranked):
        w = window(i)
        ratio = SequenceMatcher(None, norm_snippet, w, autojunk=False).ratio()
        if ratio > best:
            best, best_window = ratio, w
    located = best > threshold
    bridged = ellipsis and not (located and INVALID_SNIPPET_RE.search(best_window))
    return LocateResult(located, FUZZY if located else NONE, best, True, bridged)


def locate_payload(result: LocateResult, *, threshold: float, parsed_text_sha256: str,
                   parsed_text_uid: str) -> dict:
    """The `citation_located` event payload (R17): the verdict, the threshold, the
    locator version and the parsed-text identity that was checked."""
    return {"located": result.located, "kind": result.kind, "score": result.score,
            "threshold": threshold, "locator_version": LOCATOR_VERSION,
            "snippet_supplied": result.snippet_supplied, "bridged": result.bridged,
            "parsed_text_sha256": parsed_text_sha256, "parsed_text_uid": parsed_text_uid}
