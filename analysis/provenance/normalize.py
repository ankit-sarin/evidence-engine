"""Pinned text normalization for the evidence-provenance taxonomy.

Every transform applied before any string comparison is enumerated here and in
DEFINITIONS.md. Nothing else touches the text. The transform list is versioned:
bump NORMALIZATION_VERSION if any transform is added, removed, or reordered —
census rows carry the version so results stay interpretable across revisions.

Order is load-bearing: de-hyphenation (T7) needs the line breaks that whitespace
collapse (T9) destroys, so T7 must precede T9.
"""

from __future__ import annotations

import html
import re
import unicodedata

NORMALIZATION_VERSION = "prov-norm-1"

# T3 — zero-width and soft-hyphen code points removed outright.
_ZERO_WIDTH = dict.fromkeys(
    [
        0x00AD,  # SOFT HYPHEN
        0x200B,  # ZERO WIDTH SPACE
        0x200C,  # ZERO WIDTH NON-JOINER
        0x200D,  # ZERO WIDTH JOINER
        0xFEFF,  # ZERO WIDTH NO-BREAK SPACE / BOM
    ],
    None,
)

# T4 — quote folding. Typographic quotes are a rendering choice, not content.
_QUOTES = str.maketrans({
    "‘": "'", "’": "'", "‚": "'", "‛": "'",
    "“": '"', "”": '"', "„": '"', "‟": '"',
    "«": '"', "»": '"', "‹": "'", "›": "'",
    "`": "'", "´": "'",
})

# T5 — dash folding. En/em/figure/minus all render as "-" in some parsers.
_DASHES = str.maketrans({
    "‐": "-", "‑": "-", "‒": "-", "–": "-",
    "—": "-", "―": "-", "−": "-",
})

# T6 — ellipsis folding. Deliberately preserved (not deleted): an ellipsis is
# the model's own signal that it bridged passages, and it must keep failing to
# match paper text so the bridge is detected rather than silently repaired.
_ELLIPSIS_RE = re.compile("…")

# T7 — de-hyphenation across a line break. Restricted to letter-hyphen-newline-
# letter so numeric ranges ("1990-\n1995") and compound IDs are never merged.
_HYPHEN_BREAK_RE = re.compile(r"([A-Za-z])-[ \t]*\r?\n[ \t]*([A-Za-z])")

# T9 — whitespace collapse.
_WS_RE = re.compile(r"\s+")


def normalize(text: str) -> str:
    """Apply the pinned normalization pipeline T1–T9. See DEFINITIONS.md §2."""
    if not text:
        return ""
    text = html.unescape(text)                      # T1 HTML entities
    text = unicodedata.normalize("NFKC", text)      # T2 NFKC (folds ligatures)
    text = text.translate(_ZERO_WIDTH)              # T3 zero-width / soft hyphen
    text = text.translate(_QUOTES)                  # T4 quote folding
    text = text.translate(_DASHES)                  # T5 dash folding
    text = _ELLIPSIS_RE.sub("...", text)            # T6 ellipsis folding
    text = _HYPHEN_BREAK_RE.sub(r"\1\2", text)      # T7 de-hyphenation
    text = text.lower()                             # T8 case folding
    return _WS_RE.sub(" ", text).strip()            # T9 whitespace collapse
