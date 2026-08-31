"""ELICIT-01 — deterministic numbered-unit rendering for the INDEX condition.

Starts from `analysis/provenance/segment.py` (pysbd 0.3.4, clean=False) unmodified,
then applies one post-pass. The post-pass exists because raw pysbd output is not
usable as an index space:

  * Docling comment artifacts (`<!-- image -->`, `<!-- formula-not-decoded -->`)
    are shredded into fragments such as `<!` and `-- image -->`. They carry no
    quotable content but would occupy index slots the model could select.
  * 12-29% of raw units fall below 3 whitespace tokens ("2 (c).", "H."). A model
    selecting one of those yields a quote with no discriminative content.

Contract of the post-pass, all three properties enforced by test:

  1. **Comment artifacts are excluded from numbering.** Any `<!-- ... -->` block
     is stripped before segmentation, so no index can point at one.
  2. **Bijection over the remaining text.** Every character of the paper that
     survives step 1 belongs to exactly one numbered unit. Concatenating the
     units in order, with single spaces where the source had whitespace,
     reproduces the stripped source. Nothing is dropped.
  3. **No unit below MIN_UNIT_TOKENS survives.** Short units are MERGED into a
     neighbour, never discarded -- merge forward into the following unit, or
     backward into the previous one when the short unit is last. Merging
     preserves property 2 by construction; discarding would break it.

`MIN_UNIT_TOKENS` is chosen from the smoke papers' distributions and frozen at
smoke sign-off; see the ELICIT-01 report for the derivation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from analysis.provenance.segment import sentences

# Matches Docling's comment artifacts, including the unterminated tails pysbd
# would otherwise shred. Non-greedy so adjacent comments do not merge.
COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)

MIN_UNIT_TOKENS = 3  # frozen at smoke sign-off; see report

UNITS_VERSION = "elicit01-units-1"


@dataclass(frozen=True)
class UnitMap:
    """The index space handed to the model, plus what it was built from."""

    paper_id: int
    units: tuple[str, ...]
    source_stripped: str
    min_unit_tokens: int
    version: str = UNITS_VERSION

    @property
    def n(self) -> int:
        return len(self.units)

    def render(self) -> str:
        """The numbered text as the model sees it."""
        return "\n".join(f"[S{i}] {u}" for i, u in enumerate(self.units, start=1))

    def resolve(self, index: int) -> str | None:
        """1-based index -> unit text. None for any out-of-range index.

        Never clamps, never nearest-matches: an invalid index is a measurement,
        not something to repair.
        """
        if not isinstance(index, int) or index < 1 or index > len(self.units):
            return None
        return self.units[index - 1]

    def to_json(self) -> dict:
        return {
            "paper_id": self.paper_id,
            "version": self.version,
            "min_unit_tokens": self.min_unit_tokens,
            "n_units": len(self.units),
            "units": list(self.units),
        }


def strip_comments(text: str) -> str:
    return COMMENT_RE.sub(" ", text)


def merge_short(units: list[str], min_tokens: int) -> list[str]:
    """Merge sub-threshold units into a neighbour. Never discards."""
    if not units:
        return []
    out: list[str] = []
    for u in units:
        if out and len(out[-1].split()) < min_tokens:
            out[-1] = f"{out[-1]} {u}"        # previous was short: absorb forward
        else:
            out.append(u)
    # A trailing short unit has no successor; fold it backward.
    while len(out) > 1 and len(out[-1].split()) < min_tokens:
        tail = out.pop()
        out[-1] = f"{out[-1]} {tail}"
    return out


def build_unit_map(paper_id: int, raw_text: str,
                   min_tokens: int = MIN_UNIT_TOKENS) -> UnitMap:
    stripped = strip_comments(raw_text)
    units = merge_short([s for s in sentences(stripped) if s.strip()], min_tokens)
    return UnitMap(paper_id=paper_id, units=tuple(units),
                   source_stripped=stripped, min_unit_tokens=min_tokens)
