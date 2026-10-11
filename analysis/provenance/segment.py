"""Sentence segmentation for the provenance taxonomy.

Tokenizer: pysbd (Pragmatic Sentence Boundary Disambiguation), pinned to
0.3.4, language "en", rule-based and fully deterministic — no model files,
no training data, no randomness, identical output on every machine.

Chosen over an ad-hoc regex split because scientific prose is dense with
non-terminal periods ("Fig. 3", "et al.", "e.g.", "1.04 mm", "vs.") that a
`(?<=[.!?])\\s+` split shreds into fragments. That shredding is not cosmetic:
a fragment like "3 for safety consideration." matches the paper trivially and
inflates the traceable-sentence count, while its parent sentence is lost.
"""

from __future__ import annotations

import re
from functools import lru_cache

import pysbd

TOKENIZER_NAME = "pysbd"
TOKENIZER_VERSION = pysbd.__version__
TOKENIZER_LANGUAGE = "en"

# Pre-segmentation split (T-seg-0). An ellipsis is an explicit bridge marker:
# the writer is stating that text was omitted between the two sides. pysbd
# treats "A... B" inconsistently — sometimes one unit, sometimes two, and it
# rewrites a trailing "..." to "." even with clean=False — so the split is done
# here, deterministically, before pysbd ever sees the text. The marker itself is
# discarded: it is punctuation about the quote, not content of the quote.
# Without this step an ellipsis-bridged snippet whose two halves are both
# verbatim paper text would be scored as a single unmatchable sentence and
# land in UNTRACEABLE_NO_BASIS — the single largest misclassification risk in
# the taxonomy, since 81% of one arm's non-anchored snippets carry an ellipsis.
_ELLIPSIS_SPLIT_RE = re.compile(r"\.{3,}|…")

# Sentences with fewer than this many whitespace tokens are excluded from the
# traceability test. Rationale: a 1–2 token fragment ("2 (c).", "H.") carries no
# discriminative content — it matches almost any paper by chance, so counting it
# as evidence would inflate every traceable class. Excluded fragments are
# counted and reported, never silently dropped.
MIN_SENTENCE_TOKENS = 3


@lru_cache(maxsize=8)
def _segmenter() -> pysbd.Segmenter:
    """One reusable segmenter. clean=False keeps the text byte-for-byte."""
    return pysbd.Segmenter(language=TOKENIZER_LANGUAGE, clean=False)


def sentences(text: str) -> list[str]:
    """Segment raw text into sentences (T-seg-0 then pysbd).

    Empty/blank input yields []. Deterministic: same input, same output, on
    every machine and every run.
    """
    if not text or not text.strip():
        return []
    out: list[str] = []
    for chunk in _ELLIPSIS_SPLIT_RE.split(text):
        if not chunk.strip():
            continue
        out.extend(s for s in (s.strip() for s in _segmenter().segment(chunk)) if s)
    return out


def _align(chunk: str, stats: dict | None) -> list[tuple[int, int]]:
    """(start, end) of each pysbd sentence in `chunk`, covering every
    non-whitespace character of it exactly once.

    pysbd returns strings, not offsets, and does not promise to return the text
    unchanged, so each sentence is found again in the chunk. A sentence that is
    not the next text in the chunk (pysbd rewrote a character) is placed by its
    count of non-whitespace characters instead, and counted in
    `stats["realigned"]`. Whatever pysbd did, the spans tile the chunk: text is
    never dropped, only the boundary between two sentences can be lost.
    """
    spans: list[tuple[int, int]] = []
    cursor, n = 0, len(chunk)
    for seg in _segmenter().segment(chunk):
        core = seg.strip()
        if not core:
            continue
        while cursor < n and chunk[cursor].isspace():
            cursor += 1
        if cursor >= n:
            break
        if chunk.startswith(core, cursor):
            end = cursor + len(core)
        else:
            if stats is not None:
                stats["realigned"] = stats.get("realigned", 0) + 1
            want = sum(1 for ch in core if not ch.isspace())
            end = cursor
            while end < n and want:
                if not chunk[end].isspace():
                    want -= 1
                end += 1
        spans.append((cursor, end))
        cursor = end
    rest = chunk[cursor:]
    if rest.strip():                       # pysbd returned less than the chunk holds
        if stats is not None:
            stats["tail_recovered"] = stats.get("tail_recovered", 0) + 1
        end = cursor + len(rest.rstrip())
        if spans:
            spans[-1] = (spans[-1][0], end)
        else:
            spans.append((cursor + len(rest) - len(rest.lstrip()), end))
    return spans


def sentence_spans(text: str, stats: dict | None = None) -> list[tuple[int, int]]:
    """`sentences()` with offsets: the (start, end) of each sentence in `text`.

    Same segmentation as `sentences` (T-seg-0, then pysbd on each chunk), with
    two differences that follow from returning positions instead of copies.
    `text[start:end]` is the sentence exactly as written, where `sentences`
    returns pysbd's string. And the ellipsis marker is not discarded: it is
    still where the split happens, and it belongs to the sentence before it
    (to the one after when nothing precedes it). Spans are ordered, disjoint
    and trimmed, and together hold every non-whitespace character of `text`.

    `sentences` is unchanged and remains what the provenance taxonomy scores.
    `stats`, when given, counts the alignments that needed the fallback.
    """
    spans: list[tuple[int, int]] = []
    carry: int | None = None               # start of an ellipsis with no sentence before it
    pos = 0
    pieces = [(m.start(), m.end()) for m in _ELLIPSIS_SPLIT_RE.finditer(text)]
    for ell_start, ell_end in pieces + [(len(text), len(text))]:
        found = [(pos + s, pos + e) for s, e in _align(text[pos:ell_start], stats)]
        if found and carry is not None:
            found[0] = (carry, found[0][1])
            carry = None
        spans.extend(found)
        if ell_end > ell_start:
            if spans and carry is None:
                spans[-1] = (spans[-1][0], ell_end)
            elif carry is None:
                carry = ell_start
        pos = ell_end
    if carry is not None:                  # the text is nothing but ellipses
        spans.append((carry, len(text.rstrip())))
    return spans
