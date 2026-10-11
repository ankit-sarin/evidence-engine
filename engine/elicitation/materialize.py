"""Turn cited unit indices into verbatim text.

The model never copies text; it names units. The engine resolves those names
against the unit map it built and rendered, and cuts what it stores out of the
paper by the units' offsets.

**One continuous passage per claim.** A claim's `source_snippet` holds a single
quote, and the contract on it is one continuous passage with no ellipsis
bridging. Cited units are frequently adjacent ([12, 13]) and sometimes not
([12, 47]). Joining disjoint units would manufacture a quote that appears
nowhere in the paper, so the claim carries the FIRST CONTIGUOUS RUN of cited
units — the lowest-numbered run, since the indices are sorted first.

**A run is one slice, not a join (R572).** The run is materialised as
`source_raw[start of its first unit : end of its last unit]`: whatever lies
between the units in the paper — a line wrap, a blank line, a Docling comment —
is in the snippet, because it is in the paper. `source_snippet` then confirms
the snippet is the text at those offsets and a substring of the text it is
checked against, and raises `SnippetNotExact` otherwise (R534's
materialisation-time EXACT check): a snippet that is not in the paper is a
fault of the run, never something to store.

Only the first run is on the claim. Every cited index reaches
`record_call(extra=...)` and the per-run unit-map file, and Pass 2 is primed
with ALL cited units, not just the first run.
"""

from __future__ import annotations

from dataclasses import dataclass

from engine.elicitation.contracts import FieldRecord
from engine.elicitation.units import UnitMap


class PrimingInvariantViolated(RuntimeError):
    """Pass 2's priming input carries a record the routing upstream guarantees it cannot."""


class SnippetNotExact(RuntimeError):
    """R534: a materialised snippet is not the paper's text at its offsets.

    A run fault, not a paper outcome: the unit map and the text it is checked
    against disagree, which no retry on this paper can repair."""


@dataclass(frozen=True)
class Citation:
    index: int
    text: str


def citations(record: FieldRecord, unit_map: UnitMap) -> tuple[Citation, ...]:
    """Materialize every valid cited index, in the order the model cited them."""
    out = []
    for ix in record.indices:
        text = unit_map.resolve(ix)
        if text is None:
            raise PrimingInvariantViolated(
                f"priming invariant violated: field {record.field_name!r} cites unit {ix}, "
                f"which this unit map ({unit_map.n} units) cannot resolve — indices are "
                "validated in contracts.check_entry before priming")
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


def source_snippet(record: FieldRecord, unit_map: UnitMap,
                   source_text: str | None = None) -> str:
    """The single continuous verbatim passage stored on the claim.

    Empty string when nothing was cited -- which, for a non-escape value, is
    exactly the condition the write-boundary fail-fast refuses.

    `source_text` is the text the snippet must be an exact slice of; it
    defaults to the text the unit map was built from. The pipeline passes the
    parsed text the claim names, so a unit map built from any other text is
    refused here.
    """
    runs = contiguous_runs(record.indices)
    if not runs:
        return ""
    first, last = unit_map.span(runs[0][0]), unit_map.span(runs[0][-1])
    if first is None or last is None:
        raise PrimingInvariantViolated(
            f"priming invariant violated: field {record.field_name!r} cites units "
            f"{runs[0]}, which this unit map ({unit_map.n} units) cannot resolve")
    start, end = first[0], last[1]
    snippet = unit_map.source_raw[start:end]
    text = unit_map.source_raw if source_text is None else source_text
    if not snippet or text[start:end] != snippet or snippet not in text:
        raise SnippetNotExact(
            f"field {record.field_name!r}: the snippet materialised for units {runs[0]} "
            f"(offsets {start}–{end}, unit map {unit_map.version}) is not the source "
            "text at those offsets. The unit map was built from a different text than "
            "the one the claim names; nothing was stored.")
    return snippet


def evidence_block(record: FieldRecord, unit_map: UnitMap) -> str:
    """The per-field priming text handed to Pass 2.

    Carries the materialized evidence and, for INFERABLE and JUDGMENT, the
    declared inference or the reasoning steps -- the accompaniment is what the
    class contract exists to elicit, so discarding it before Pass 2 would throw
    away the half of the contract that is not the citation.
    """
    lines = [f"### {record.field_name}  [{record.field_class}]"]
    if record.is_escape:
        raise PrimingInvariantViolated(
            f"priming invariant violated: field {record.field_name!r} is an escape record "
            f"({record.value!r}) yet reached the priming input — terminal.terminal_states "
            "routes every escape to the escape token (NO_EVIDENCE_LOCATABLE) and "
            "pass2_priming is given EVIDENCED_VALUE records only")

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
