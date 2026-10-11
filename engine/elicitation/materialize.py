"""Turn cited unit indices into verbatim text.

The model never copies text; it names units. The engine resolves those names
against the unit map it built and rendered, and cuts what it stores out of the
paper by the units' offsets.

**One continuous passage per claim.** A claim's `source_snippet` holds a single
quote, and the contract on it is one continuous passage with no ellipsis
bridging. Cited units are frequently adjacent ([12, 13]) and sometimes not
([12, 47]). Joining disjoint units would manufacture a quote that appears
nowhere in the paper, so the claim's `source_snippet` is ONE contiguous run of
cited units: the LOWEST-NUMBERED run. The indices are sorted before they are
split into runs, so this is not the run the model cited first — cited
[47, 48, 12], the stored snippet is unit 12 (Wave 1 row 57).

**A run is one slice, not a join (R572).** The run is materialised as
`source_raw[start of its first unit : end of its last unit]`: whatever lies
between the units in the paper — a line wrap, a blank line, a Docling comment —
is in the snippet, because it is in the paper. `source_snippet` then confirms
the snippet is the text at those offsets and a substring of the text it is
checked against, and raises `SnippetNotExact` otherwise (R534's
materialisation-time EXACT check): a snippet that is not in the paper is a
fault of the run, never something to store.

**Every cited unit is on the claim (INT-g12-1).** The snippet is one run; the
claim's payload also carries `citation_record`: the indices as the contract
check counted them, in the model's order, every run derived from them with its
offsets into the parsed text, and each JUDGMENT step's own citations. It
resolves from the parsed text the claim names by sha256 and needs no unit-map
file. Pass 2 is primed with ALL cited units, not just the stored run.
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


def _run_slice(field_name: str, run: list[int], unit_map: UnitMap,
               source_text: str | None) -> tuple[int, int, str]:
    """(start, end, text) of one run: one slice of the source, checked EXACT (R534)."""
    first, last = unit_map.span(run[0]), unit_map.span(run[-1])
    if first is None or last is None:
        raise PrimingInvariantViolated(
            f"priming invariant violated: field {field_name!r} cites units "
            f"{run}, which this unit map ({unit_map.n} units) cannot resolve")
    start, end = first[0], last[1]
    snippet = unit_map.source_raw[start:end]
    text = unit_map.source_raw if source_text is None else source_text
    if not snippet or text[start:end] != snippet or snippet not in text:
        raise SnippetNotExact(
            f"field {field_name!r}: the snippet materialised for units {run} "
            f"(offsets {start}–{end}, unit map {unit_map.version}) is not the source "
            "text at those offsets. The unit map was built from a different text than "
            "the one the claim names; nothing was stored.")
    return start, end, snippet


def source_snippet(record: FieldRecord, unit_map: UnitMap,
                   source_text: str | None = None) -> str:
    """The single continuous verbatim passage stored on the claim: the
    lowest-numbered run of the cited units, not the run cited first.

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
    return _run_slice(record.field_name, runs[0], unit_map, source_text)[2]


def _runs_with_offsets(field_name: str, indices: tuple[int, ...], unit_map: UnitMap,
                       source_text: str | None) -> list[dict]:
    out = []
    for run in contiguous_runs(indices):
        start, end, _ = _run_slice(field_name, run, unit_map, source_text)
        out.append({"first": run[0], "last": run[-1], "start": start, "end": end})
    return out


def citation_record(record: FieldRecord, unit_map: UnitMap,
                    source_text: str | None = None) -> dict | None:
    """Every unit the claim cites, resolvable from the parsed text alone (INT-g12-1).

    * `units_version` — the segmentation the indices belong to.
    * `cited` — `record.indices`: every index the contract check counted as a
      citation for the field, in the model's order. For a JUDGMENT field that is
      its field-level indices followed by its steps' (`contracts.check_entry`).
    * `runs` — the runs derived from `cited` (sorted, de-duplicated), each with
      its `first` and `last` unit and the `start` and `end` character offsets of
      its slice in the parsed text. `runs[0]` is the stored `source_snippet`.
    * `steps` — JUDGMENT only, one entry per reasoning step in order: the step's
      own `cited` and `runs`, and whether it is a criteria application.

    Each run is checked EXACT against `source_text` exactly as `source_snippet`
    checks its one (`SnippetNotExact` otherwise). `None` when nothing was cited.
    Plain JSON types: it is stored as one key of the claim's payload.
    """
    if not record.indices:
        return None
    name = record.field_name
    out: dict = {"units_version": unit_map.version,
                 "cited": list(record.indices),
                 "runs": _runs_with_offsets(name, record.indices, unit_map, source_text)}
    if record.steps:
        out["steps"] = [{"cited": list(s.indices),
                         "runs": _runs_with_offsets(name, s.indices, unit_map, source_text),
                         "criteria_application": bool(s.criteria_application)}
                        for s in record.steps]
    return out


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
