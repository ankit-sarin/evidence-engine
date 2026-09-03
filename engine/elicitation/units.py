"""Deterministic numbered-unit rendering: the index space handed to the model.

Ported unchanged from `analysis/eval/elicit01/units.py`, which ELICIT-01
measured at 505/505 valid indices and 505/505 round-trips over 38 papers and 9
STATED fields. Behaviour is byte-identical; only the docstrings below are new.

**Provenance disclosure.** ELICIT-01's own report (section 8) states: "The unit
post-pass is a study artifact, not a production segmenter. Its 3-token merge and
comment stripping were frozen from three papers' distributions." That remains
true of this module. `MIN_UNIT_TOKENS = 3` is adopted **provisionally** on the
strength of the measurement above, not re-derived; re-derivation belongs to the
queued parse-quality-gate task, which is where a threshold over the whole corpus
can be argued. Porting it unchanged is deliberate: a silently retuned threshold
would make the production index space incomparable with the measurement that
justified it.

**Layering note.** `analysis.provenance.segment` is imported rather than forked.
It is the segmenter whose output the frozen v1.1 taxonomy scores, and a
production copy of it would be free to drift from the thing that grades it. The
dependency is read-only and one-way.

Contract of the post-pass, all three properties enforced by test:

  1. **Comment artifacts are excluded from numbering.** Any `<!-- ... -->` block
     is stripped before segmentation, so no index can point at one. Docling
     emits these (`<!-- image -->`, `<!-- formula-not-decoded -->`) and pysbd
     shreds them into fragments such as `<!` that would occupy index slots
     carrying no quotable content.
  2. **Bijection over the remaining text.** Every character of the paper that
     survives step 1 belongs to exactly one numbered unit. Nothing is dropped.
  3. **No unit below MIN_UNIT_TOKENS survives.** Short units are MERGED into a
     neighbour, never discarded -- merge forward into the following unit, or
     backward into the previous one when the short unit is last. Merging
     preserves property 2 by construction; discarding would break it.

Headings remain citable. ELICIT-01 observed a title-header citation for
`study_type` (report section 6, p764) and the architect ruling is that heading
units stay in the index space: whether a cited unit is *good* evidence is a
judge-scoring question, not a filtering one.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from analysis.provenance.segment import sentences

# Matches Docling's comment artifacts, including the unterminated tails pysbd
# would otherwise shred. Non-greedy so adjacent comments do not merge.
COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)

MIN_UNIT_TOKENS = 3  # ELICIT-01 study artifact, adopted provisionally -- see module docstring

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

        One deliberate deviation from the ELICIT-01 original, recorded because
        "ported unchanged" should mean it: the original guard was
        `not isinstance(index, int)`, which accepts `True`/`False` (bool is a
        subclass of int) and would resolve `True` to unit 1. ELICIT-01 never hit
        it because its analyzer classified bools as malformed before calling
        resolve. Here resolve is the only gate, so it rejects bools itself --
        the same verdict the study reached, reached one layer earlier.
        """
        if not isinstance(index, int) or isinstance(index, bool):
            return None
        if index < 1 or index > len(self.units):
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
