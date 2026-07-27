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
