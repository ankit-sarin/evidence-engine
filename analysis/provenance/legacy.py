"""Verdict-equivalent, fast reimplementation of the legacy anchoring test.

The legacy figure quoted in earlier diagnostics came from
`engine.agents.auditor.grep_verify()`: normalize, exact substring, else slide a
word window the width of the whole snippet and accept if any window scores
`SequenceMatcher(...).ratio() > 0.85`.

That implementation scores *every* window with a full `ratio()` call and no
prefilter, which costs tens of seconds per span on a 10,000-word paper — far too
slow to re-run across the corpus. This module reproduces its verdicts exactly
while being usable at census scale. Two optimizations, both verdict-preserving:

  1. `real_quick_ratio()` / `quick_ratio()` are documented upper bounds on
     `ratio()`, so a window whose upper bound is <= the acceptance threshold
     cannot be accepted and may be skipped.
  2. The scan returns True at the first accepted window; the legacy function
     does the same.

Everything that could change a verdict is held identical to the original: the
same normalization (imported from the auditor, not re-implemented), the same
window width, the same strict `> 0.85` comparison, and crucially the same
`SequenceMatcher` construction — **autojunk left at its default True**, unlike
the provenance taxonomy which turns it off. `tests/analysis/provenance/
test_legacy_equivalence.py` asserts agreement against the real function.

Used only to restate the legacy figure for the manuscript's revision history.
"""

from __future__ import annotations

from difflib import SequenceMatcher

from engine.agents.auditor import _normalize as legacy_normalize

LEGACY_THRESHOLD = 0.85
LEGACY_SOURCE = "engine.agents.auditor.grep_verify"


def grep_verify_fast(source_snippet: str, paper_text: str) -> bool:
    """Verdict-identical to engine.agents.auditor.grep_verify()."""
    if not source_snippet or not paper_text:
        return False

    norm_snippet = legacy_normalize(source_snippet)
    norm_text = legacy_normalize(paper_text)
    # Inherited quirk, reproduced deliberately: a whitespace-only snippet
    # normalizes to "" and "" is a substring of everything, so the legacy
    # function reports it as anchored. Not corrected here — this module's
    # contract is verdict-equivalence, not correctness. The census never
    # reaches this branch because it guards on snippet.strip() first, and the
    # taxonomy routes blank snippets to ABSENCE_DECLARED / MISSING_SNIPPET.
    if norm_snippet in norm_text:
        return True

    text_words = norm_text.split()
    snippet_words = norm_snippet.split()
    window = len(snippet_words)
    if window == 0:
        return False

    # autojunk default (True) deliberately retained — see module docstring.
    matcher = SequenceMatcher(None)
    matcher.set_seq1(norm_snippet)
    for i in range(max(1, len(text_words) - window + 1)):
        matcher.set_seq2(" ".join(text_words[i:i + window]))
        if matcher.real_quick_ratio() <= LEGACY_THRESHOLD:
            continue
        if matcher.quick_ratio() <= LEGACY_THRESHOLD:
            continue
        if matcher.ratio() > LEGACY_THRESHOLD:
            return True
    return False
