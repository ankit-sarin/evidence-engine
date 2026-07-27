"""Evidence-provenance taxonomy classifier.

Implements the four-way taxonomy pinned in DEFINITIONS.md:

    ANCHORED               snippet is one contiguous passage of the paper
    STITCHED               not contiguous, but every sentence is verbatim paper text
    DRIFTED                every sentence matches to >= THRESHOLD_PRIMARY, not all verbatim
    UNTRACEABLE_PARTIAL    some but not all sentences traceable
    UNTRACEABLE_NO_BASIS   no sentence traceable

plus three classes outside the taxonomy proper, so that a census covers 100%
of spans rather than 100% of spans-that-happen-to-have-a-snippet:

    ABSENCE_CLAIM          snippet asserts the paper does not report the item
    ABSENCE_DECLARED       empty snippet, value is a codebook absence sentinel
    MISSING_SNIPPET        empty snippet, value asserts something
    UNCLASSIFIABLE_SHORT   every sentence below MIN_SENTENCE_TOKENS

Similarity is difflib.SequenceMatcher(autojunk=False).ratio() over normalized
characters. autojunk MUST stay off: the heuristic treats characters appearing
in >1% of a >=200-element sequence as junk, which silently distorts ratios for
exactly the long snippets this taxonomy cares about.

No Ollama, no network, no model inference — pure Python string matching.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from difflib import SequenceMatcher

from analysis.provenance.absence import is_absence_claim
from analysis.provenance.normalize import normalize
from analysis.provenance.segment import MIN_SENTENCE_TOKENS, sentences

# ── Pinned constants (every one is documented in DEFINITIONS.md §5) ──────

THRESHOLD_PRIMARY = 0.90
THRESHOLD_BAND = (0.85, 0.90, 0.95)
RATIO_CEILING = 0.95  # window search stops here; see DEFINITIONS.md §5.4

ANCHORED = "ANCHORED"
STITCHED = "STITCHED"
DRIFTED = "DRIFTED"
UNTRACEABLE_PARTIAL = "UNTRACEABLE_PARTIAL"
UNTRACEABLE_NO_BASIS = "UNTRACEABLE_NO_BASIS"
ABSENCE_CLAIM = "ABSENCE_CLAIM"
ABSENCE_DECLARED = "ABSENCE_DECLARED"
MISSING_SNIPPET = "MISSING_SNIPPET"
UNCLASSIFIABLE_SHORT = "UNCLASSIFIABLE_SHORT"

TAXONOMY_CLASSES = (ANCHORED, STITCHED, DRIFTED, UNTRACEABLE_PARTIAL, UNTRACEABLE_NO_BASIS)
NON_TAXONOMY_CLASSES = (
    ABSENCE_CLAIM, ABSENCE_DECLARED, MISSING_SNIPPET, UNCLASSIFIABLE_SHORT,
)
ALL_CLASSES = TAXONOMY_CLASSES + NON_TAXONOMY_CLASSES

# Mirrors extraction_codebook.yaml `absence_sentinels` plus the empty string.
# Kept as a literal (not loaded from YAML) so the classifier is importable and
# testable without a review directory; the census asserts the two agree.
ABSENCE_SENTINELS = frozenset({
    "", "nr", "n/a", "na", "not_found", "not found", "not reported", "none",
})


@dataclass
class PaperIndex:
    """Normalized views of one paper, built once and reused across its spans."""

    paper_id: int
    norm_text: str
    norm_words: list[str] = field(default_factory=list)
    _norm_sentences: frozenset[str] | None = field(default=None, repr=False)

    @classmethod
    def build(cls, paper_id: int, raw_text: str, with_sentences: bool = False) -> "PaperIndex":
        norm_text = normalize(raw_text)
        idx = cls(paper_id=paper_id, norm_text=norm_text, norm_words=norm_text.split())
        if with_sentences:
            idx._norm_sentences = frozenset(
                n for n in (normalize(s) for s in sentences(raw_text)) if n
            )
        return idx

    @property
    def norm_sentences(self) -> frozenset[str]:
        """Paper sentence set — only populated when built with_sentences=True."""
        return self._norm_sentences or frozenset()


@dataclass
class SpanClassification:
    taxonomy_class: str
    n_sentences: int
    n_evaluated: int
    n_exact: int
    sentence_ratios: list[float]
    min_ratio: float | None
    strict_variant_class: str | None = None
    absence_pattern: str | None = None
    terminal: bool = False
    """True when the class was decided before the sentence stage (ANCHORED, the
    empty-snippet classes, UNCLASSIFIABLE_SHORT) and so cannot depend on the
    threshold."""

    def classes_at(self, threshold: float) -> str:
        """Reclassify at a different threshold from the stored ratios.

        Only meaningful for spans that reached the sentence-level stage; the
        non-taxonomy classes and ANCHORED are threshold-independent.
        """
        if self.terminal:
            return self.taxonomy_class
        if self.n_exact == self.n_evaluated:
            return STITCHED
        traceable = sum(1 for r in self.sentence_ratios if r >= threshold)
        if traceable == self.n_evaluated:
            return DRIFTED
        if traceable > 0:
            return UNTRACEABLE_PARTIAL
        return UNTRACEABLE_NO_BASIS


def best_window_ratio(norm_sentence: str, paper: PaperIndex) -> float:
    """Best SequenceMatcher ratio between a sentence and any same-length window.

    Window length is fixed at the sentence's word count; the scan is exhaustive
    over word offsets. quick_ratio/real_quick_ratio are cheap upper bounds on
    ratio, so skipping windows that cannot beat the incumbent is exact, not
    approximate. The scan stops early at RATIO_CEILING because every threshold
    in THRESHOLD_BAND is below it — the classification is already decided.
    """
    if not norm_sentence or not paper.norm_words:
        return 0.0
    n = len(norm_sentence.split())
    if n == 0:
        return 0.0

    matcher = SequenceMatcher(None, autojunk=False)
    matcher.set_seq1(norm_sentence)
    words = paper.norm_words
    best = 0.0
    for i in range(max(1, len(words) - n + 1)):
        matcher.set_seq2(" ".join(words[i:i + n]))
        if matcher.real_quick_ratio() <= best or matcher.quick_ratio() <= best:
            continue
        r = matcher.ratio()
        if r > best:
            best = r
            if best >= RATIO_CEILING:
                break
    return best


def classify_span(
    snippet: str | None,
    value: str | None,
    paper: PaperIndex,
    threshold: float = THRESHOLD_PRIMARY,
    strict_variant: bool = False,
) -> SpanClassification:
    """Classify one evidence span. See DEFINITIONS.md §3 for the decision order."""
    snippet = snippet or ""
    if not snippet.strip():
        cls = (
            ABSENCE_DECLARED
            if (value or "").strip().lower() in ABSENCE_SENTINELS
            else MISSING_SNIPPET
        )
        return SpanClassification(cls, 0, 0, 0, [], None, terminal=True)

    norm_snippet = normalize(snippet)
    anchored = bool(norm_snippet) and norm_snippet in paper.norm_text

    # Step 0d — absence claim, assessed before the ladder (DEFINITIONS v1.1 §A3).
    absent, pattern = is_absence_claim(snippet, anchored)
    if absent:
        return SpanClassification(
            ABSENCE_CLAIM, 1, 0, 0, [], None, absence_pattern=pattern, terminal=True
        )

    # Step 1 — contiguity. One passage, verbatim, in order.
    if anchored:
        return SpanClassification(
            ANCHORED, 1, 1, 1, [1.0], 1.0, absence_pattern=pattern, terminal=True
        )

    # Step 2 — sentence decomposition.
    raw_sents = sentences(snippet)
    norm_sents = [normalize(s) for s in raw_sents]
    evaluated = [s for s in norm_sents if len(s.split()) >= MIN_SENTENCE_TOKENS]
    if not evaluated:
        return SpanClassification(
            UNCLASSIFIABLE_SHORT, len(norm_sents), 0, 0, [], None, terminal=True
        )

    # Step 3 — per-sentence exact containment, then fuzzy for the misses.
    ratios: list[float] = []
    n_exact = 0
    for s in evaluated:
        if s in paper.norm_text:
            n_exact += 1
            ratios.append(1.0)
        else:
            ratios.append(best_window_ratio(s, paper))

    strict_cls = None
    if strict_variant:
        strict_cls = _strict_variant_class(evaluated, paper, ratios, threshold)

    result = SpanClassification(
        taxonomy_class="",  # decided by classes_at() immediately below
        n_sentences=len(norm_sents),
        n_evaluated=len(evaluated),
        n_exact=n_exact,
        sentence_ratios=ratios,
        min_ratio=min(ratios) if ratios else None,
        strict_variant_class=strict_cls,
        absence_pattern=pattern,
    )
    result.taxonomy_class = result.classes_at(threshold)
    return result


def _strict_variant_class(
    evaluated: list[str], paper: PaperIndex, ratios: list[float], threshold: float
) -> str:
    """Sensitivity variant: a sentence counts as verbatim only if it falls
    inside a single paper sentence, never across a paper sentence boundary.

    Reported alongside the primary result because the primary rule (containment
    in the whole normalized paper text) can call a snippet STITCHED when its
    "sentence" actually straddles two adjacent paper sentences.
    """
    paper_sents = paper.norm_sentences
    if not paper_sents:
        return ""
    n_exact = sum(1 for s in evaluated if any(s in ps for ps in paper_sents))
    if n_exact == len(evaluated):
        return STITCHED
    traceable = n_exact + sum(
        1 for s, r in zip(evaluated, ratios)
        if r >= threshold and not any(s in ps for ps in paper_sents)
    )
    if traceable == len(evaluated):
        return DRIFTED
    return UNTRACEABLE_PARTIAL if traceable > 0 else UNTRACEABLE_NO_BASIS
