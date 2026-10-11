"""Deterministic numbered-unit rendering: the index space handed to the model.

Ported from `analysis/eval/elicit01/units.py`, which ELICIT-01 measured at
505/505 valid indices and 505/505 round-trips over 38 papers and 9 STATED
fields. **No longer byte-identical to that port (P1 unit U01, R534, R572,
`UNITS_VERSION` "units-2").** A unit used to be pysbd's string, and a run of
units was their strings joined by one space — text that was not in the paper
wherever the source had a line wrap, a double space, a blank line, an ellipsis
or a Docling comment between them (INT-g6-1, A-10). A unit is now a
`(start, end)` slice of the raw parsed text, and everything stored is cut from
the paper by those offsets.

**Provenance disclosure.** ELICIT-01's own report (section 8) states: "The unit
post-pass is a study artifact, not a production segmenter. Its 3-token merge and
comment stripping were frozen from three papers' distributions." That remains
true of this module. `MIN_UNIT_TOKENS = 3` is adopted **provisionally** on the
strength of the measurement above, not re-derived; re-derivation belongs to the
queued parse-quality-gate task, which is where a threshold over the whole corpus
can be argued.

**Layering note.** `analysis.provenance.segment` is imported rather than forked.
It is the segmenter whose output the frozen v1.1 taxonomy scores, and a
production copy of it would be free to drift from the thing that grades it. The
dependency is read-only and one-way. `sentence_spans` is that segmenter with
offsets; `sentences`, which the taxonomy uses, is unchanged.

Contract, each property enforced by test and by the corpus check
(`docs/session-reports/session-12/12l_U01/corpus_check.py`):

  1. **A unit is an exact slice.** `source_raw[start:end]` is the unit's stored
     text. Units are ordered and do not overlap.
  2. **Comment artifacts are excluded by span arithmetic.** A `<!-- ... -->`
     block is a boundary no unit crosses: units are built inside the stretches
     of text between comments, and nothing is substituted for a comment. An
     opener with no closer of its own is not a comment and stays body text.
  3. **Coverage.** Every character outside comments and outside the whitespace
     between units belongs to exactly one unit. An ellipsis belongs to the unit
     before it. Nothing is dropped.
  4. **Short units are merged, never discarded.** A unit below
     `MIN_UNIT_TOKENS` is merged into a neighbour — forward, or backward when it
     is last — by joining the two spans (start of the first, end of the last).
     Merging never crosses a comment, because the merged slice would then hold
     the comment; a stretch between two comments that is shorter than the
     threshold therefore stays a short unit.
  5. **Presented and stored differ only in whitespace.** The model is shown
     each unit with every whitespace run collapsed to one space (`presented`);
     what is stored is the exact slice.

Headings remain citable. ELICIT-01 observed a title-header citation for
`study_type` (report section 6, p764) and the architect ruling is that heading
units stay in the index space: whether a cited unit is *good* evidence is a
judge-scoring question, not a filtering one.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from analysis.provenance.segment import sentence_spans

# Docling's comment artifacts. Non-greedy so adjacent comments do not merge, and
# no opener may sit inside a match: an unterminated `<!--` followed later by a
# real comment is body text up to that comment, not one long comment swallowing
# the body between them.
COMMENT_RE = re.compile(r"<!--(?:(?!<!--).)*?-->", re.DOTALL)

MIN_UNIT_TOKENS = 3  # ELICIT-01 study artifact, adopted provisionally -- see module docstring

UNITS_VERSION = "units-2"

Span = tuple[int, int]


def presented(piece: str) -> str:
    """A unit as the model is shown it: the slice with every whitespace run
    collapsed to one space and the ends stripped. The one place the presented
    form is derived from the stored one."""
    return " ".join(piece.split())


@dataclass(frozen=True)
class UnitMap:
    """The index space handed to the model, plus what it was built from.

    `units[i]` is the presented form of `source_raw[spans[i][0]:spans[i][1]]`.
    """

    paper_id: int
    units: tuple[str, ...]
    spans: tuple[Span, ...]
    source_raw: str
    min_unit_tokens: int
    version: str = UNITS_VERSION

    @property
    def n(self) -> int:
        return len(self.units)

    def render(self) -> str:
        """The numbered text as the model sees it."""
        return "\n".join(f"[S{i}] {u}" for i, u in enumerate(self.units, start=1))

    def resolve(self, index: int) -> str | None:
        """1-based index -> the unit as presented. None for any out-of-range index.

        Never clamps, never nearest-matches: an invalid index is a measurement,
        not something to repair.

        One deliberate deviation from the ELICIT-01 original: the original guard
        was `not isinstance(index, int)`, which accepts `True`/`False` (bool is a
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

    def span(self, index: int) -> Span | None:
        """1-based index -> (start, end) into `source_raw`. None when out of range."""
        if self.resolve(index) is None:
            return None
        return self.spans[index - 1]

    def to_json(self) -> dict:
        return {
            "paper_id": self.paper_id,
            "version": self.version,
            "min_unit_tokens": self.min_unit_tokens,
            "n_units": len(self.units),
            "units": list(self.units),
            "spans": [list(s) for s in self.spans],
        }


def comment_spans(text: str) -> list[Span]:
    return [m.span() for m in COMMENT_RE.finditer(text)]


def merge_short(spans: list[Span], text: str, min_tokens: int) -> list[Span]:
    """Merge sub-threshold units into a neighbour by joining their spans.
    Never discards. The caller passes the units of one comment-free stretch."""
    def short(s: Span) -> bool:
        return len(text[s[0]:s[1]].split()) < min_tokens
    out: list[Span] = []
    for s in spans:
        if out and short(out[-1]):
            out[-1] = (out[-1][0], s[1])        # previous was short: absorb forward
        else:
            out.append(s)
    # A trailing short unit has no successor; fold it backward.
    while len(out) > 1 and short(out[-1]):
        tail = out.pop()
        out[-1] = (out[-1][0], tail[1])
    return out


def unit_spans(raw_text: str, min_tokens: int = MIN_UNIT_TOKENS,
               stats: dict | None = None) -> list[Span]:
    """Every unit's (start, end) into `raw_text`, in order."""
    spans: list[Span] = []
    pos = 0
    for c_start, c_end in comment_spans(raw_text) + [(len(raw_text), len(raw_text))]:
        stretch = raw_text[pos:c_start]
        found = [(pos + s, pos + e) for s, e in sentence_spans(stretch, stats)]
        spans.extend(merge_short(found, raw_text, min_tokens))
        pos = c_end
    return spans


def build_unit_map(paper_id: int, raw_text: str,
                   min_tokens: int = MIN_UNIT_TOKENS, *, stats: dict | None = None) -> UnitMap:
    """`stats`, when given, receives the segmenter's count of sentences it had
    to re-align (`sentence_spans`); the corpus check reads it."""
    spans = unit_spans(raw_text, min_tokens, stats)
    return UnitMap(paper_id=paper_id,
                   units=tuple(presented(raw_text[s:e]) for s, e in spans),
                   spans=tuple(spans), source_raw=raw_text, min_unit_tokens=min_tokens)
