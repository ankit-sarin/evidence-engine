"""Turn cited unit indices into verbatim text.

The model never copies text; it names units. The engine resolves those names
against the unit map it built and rendered. Every materialized quote is
therefore verbatim by construction -- ELICIT-01's 505/505 round-trip is a
property of this step, not a hope about the model.

**One continuous passage per span.** `evidence_spans.source_snippet` holds a
single quote, and the existing contract on it is one continuous passage with no
ellipsis bridging. Cited units are frequently adjacent ([12, 13]) and sometimes
not ([12, 47]). Joining disjoint units would manufacture a quote that appears
nowhere in the paper -- the frozen v1.1 taxonomy would score it STITCHED or
DRIFTED, and the design would be penalized for the materializer's choice rather
than measured. So the span carries the FIRST CONTIGUOUS RUN of cited units,
which is verbatim-contiguous and ANCHORED by construction.

That is a real narrowing at the database boundary, and it is a consequence of
the no-migration ruling (ELICIT-DESIGN-01 C5), not of the elicitation design:
`evidence_spans` has no column for a citation set. Nothing is lost from the
record -- every cited index and every materialized unit reaches
`record_call(extra=...)` and the per-run unit-map file, and Pass 2 is primed with
ALL of them, not just the first run. Only the single-quote column is narrowed.
"""

from __future__ import annotations

from dataclasses import dataclass

from engine.elicitation.contracts import FieldRecord
from engine.elicitation.units import UnitMap


@dataclass(frozen=True)
class Citation:
    index: int
    text: str


def citations(record: FieldRecord, unit_map: UnitMap) -> tuple[Citation, ...]:
    """Materialize every valid cited index, in the order the model cited them."""
    out = []
    for ix in record.indices:
        text = unit_map.resolve(ix)
        if text is not None:          # cannot be None: indices were validated
            out.append(Citation(index=ix, text=text))
    return tuple(out)


def contiguous_runs(indices: tuple[int, ...]) -> list[list[int]]:
    """Split cited indices into maximal runs of consecutive units.

    Sorted first: [13, 12] and [12, 13] name the same passage, and which one the
    model wrote down should not change what the passage is.
    """
    runs: list[list[int]] = []
    for ix in sorted(set(indices)):
        if runs and ix == runs[-1][-1] + 1:
            runs[-1].append(ix)
        else:
            runs.append([ix])
    return runs


def source_snippet(record: FieldRecord, unit_map: UnitMap) -> str:
    """The single continuous verbatim passage stored on the span.

    Empty string when nothing was cited -- which, for a non-escape value, is
    exactly the condition the write-boundary fail-fast refuses.
    """
    runs = contiguous_runs(record.indices)
    if not runs:
        return ""
    return " ".join(unit_map.resolve(i) or "" for i in runs[0]).strip()


def evidence_block(record: FieldRecord, unit_map: UnitMap) -> str:
    """The per-field priming text handed to Pass 2.

    Carries the materialized evidence and, for INFERABLE and JUDGMENT, the
    declared inference or the reasoning steps -- the accompaniment is what the
    class contract exists to elicit, so discarding it before Pass 2 would throw
    away the half of the contract that is not the citation.
    """
    lines = [f"### {record.field_name}  [{record.field_class}]"]
    if record.is_escape:
        lines.append(f"  (declared: {record.value} -- no evidence was locatable)")
        return "\n".join(lines)

    for c in citations(record, unit_map):
        lines.append(f'  [S{c.index}] "{c.text}"')
    if record.inference:
        lines.append(f"  Declared inference: {record.inference}")
    for i, s in enumerate(record.steps, start=1):
        basis = (", ".join(f"S{x}" for x in s.indices) if s.indices
                 else "criteria application, no textual basis claimed")
        lines.append(f"  Step {i} ({basis}): {s.text}")
    lines.append(f"  Pass-1 value: {record.value}")
    return "\n".join(lines)


def priming_block(records: dict[str, FieldRecord], unit_map: UnitMap,
                  order: tuple[str, ...]) -> str:
    """The whole materialized-evidence context, in prompt field order."""
    blocks = [evidence_block(records[n], unit_map) for n in order if n in records]
    return "\n\n".join(blocks)
