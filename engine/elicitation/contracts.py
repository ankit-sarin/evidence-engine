"""Per-class Pass-1 contracts: what the model must return, and how it is checked.

One citation mechanism for all three classes -- the model names sentence-unit
indices and never copies text (ELICIT-01 measured 505/505 valid indices and
505/505 round-trips against a COPY comparator at 36.1% ANCHORED). The classes
differ only in what must ACCOMPANY the citations:

  STATED     cite >=1 unit, then state the value.
  INFERABLE  cite >=1 unit, then a declared inference (1-3 sentences naming the
             step from cited evidence to value), then the value.
  JUDGMENT   stepwise reasoning where EACH step either cites >=1 unit or is
             explicitly marked as criteria application (no textual basis
             claimed); then the value.

Any class may instead return the escape token with zero citations.

**Detection, not absorption** (acceptance gate 3). ELICIT-01's container parser
was "deliberately shallow: it recovers the JSON container and nothing more.
Per-field validity ... is measured by the analysis, not repaired here." That was
right for a study, whose analyzer counted the defects afterwards. It is wrong for
production, where nothing runs afterwards. Validation moves here, and a
contract-violating response is a recorded failure rather than a silently thinner
result.

**No silent repair anywhere.** An out-of-range or malformed index is recorded and
FAILS the field's evidence contract; it is never dropped so the remaining indices
can carry the field. ELICIT-01 observed zero of these in 505 indices, which is a
reason to expect them to be rare, not a reason to handle them loosely.

**Severity.** Two kinds of finding, deliberately separated:

  FATAL     the field's evidence contract is not met. Drives the write-boundary
            fail-fast and bounded retry.
  ADVISORY  recorded in telemetry, does not fail the field. Two members, and the
            reasoning for each is that the evidence itself is intact:
            DUPLICATE_INDICES (the same unit cited twice supports the value
            exactly as well as citing it once) and VALUE_BEFORE_EVIDENCE (the
            contract's ordering was not honoured in the serialization, but the
            citations are present and valid). Both are prompt-compliance signals
            worth watching; neither is an evidence defect, and failing a paper
            over either would discard a sound extraction.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field as dc_field

from analysis.provenance.segment import sentences
from engine.elicitation import classes as C
from engine.elicitation.units import UnitMap

# ── Violation vocabulary (closed set) ────────────────────────────────

FIELD_MISSING = "FIELD_MISSING"
VALUE_MISSING = "VALUE_MISSING"
VALUE_WITHOUT_CITATION = "VALUE_WITHOUT_CITATION"
ESCAPE_WITH_CITATION = "ESCAPE_WITH_CITATION"
INDEX_MALFORMED = "INDEX_MALFORMED"
INDEX_OUT_OF_RANGE = "INDEX_OUT_OF_RANGE"
INFERENCE_MISSING = "INFERENCE_MISSING"
INFERENCE_MALFORMED = "INFERENCE_MALFORMED"
STEPS_MISSING = "STEPS_MISSING"
STEP_WITHOUT_BASIS = "STEP_WITHOUT_BASIS"

DUPLICATE_INDICES = "DUPLICATE_INDICES"
VALUE_BEFORE_EVIDENCE = "VALUE_BEFORE_EVIDENCE"

ADVISORY: frozenset[str] = frozenset({DUPLICATE_INDICES, VALUE_BEFORE_EVIDENCE})

FATAL: frozenset[str] = frozenset({
    FIELD_MISSING, VALUE_MISSING, VALUE_WITHOUT_CITATION, ESCAPE_WITH_CITATION,
    INDEX_MALFORMED, INDEX_OUT_OF_RANGE, INFERENCE_MISSING, INFERENCE_MALFORMED,
    STEPS_MISSING, STEP_WITHOUT_BASIS,
})

# An inference is one to three sentences. Below one it declares nothing; above
# three it is reasoning, which is the JUDGMENT contract, not this one.
INFERENCE_MIN_SENTENCES = 1
INFERENCE_MAX_SENTENCES = 3

# Response keys, in the order the contract asks for them.
KEY_FIELD = "field_name"
KEY_INDICES = "unit_indices"
KEY_INFERENCE = "inference"
KEY_STEPS = "reasoning_steps"
KEY_VALUE = "value"
KEY_STEP_TEXT = "step"
KEY_STEP_CRITERIA = "criteria_application"


@dataclass(frozen=True)
class Step:
    """One JUDGMENT reasoning step."""

    text: str
    indices: tuple[int, ...] = ()
    criteria_application: bool = False

    @property
    def has_basis(self) -> bool:
        return bool(self.indices) or self.criteria_application


@dataclass(frozen=True)
class FieldRecord:
    """One field's Pass-1 result, parsed and checked but not yet materialized."""

    field_name: str
    field_class: str
    value: str
    is_escape: bool
    indices: tuple[int, ...] = ()          # valid, deduped, first-appearance order
    inference: str = ""
    steps: tuple[Step, ...] = ()
    violations: tuple[str, ...] = ()
    bad_indices: tuple[object, ...] = ()   # every index that did not resolve, verbatim
    duplicate_indices: tuple[int, ...] = ()

    @property
    def fatal(self) -> tuple[str, ...]:
        return tuple(v for v in self.violations if v in FATAL)

    @property
    def advisories(self) -> tuple[str, ...]:
        return tuple(v for v in self.violations if v in ADVISORY)

    @property
    def ok(self) -> bool:
        """The field met its evidence contract."""
        return not self.fatal


@dataclass
class Pass1Result:
    """Everything one Pass-1 response yielded, defects included."""

    records: dict[str, FieldRecord] = dc_field(default_factory=dict)
    parse_path: str = "unparseable"
    unknown_fields: tuple[str, ...] = ()
    n_units: int = 0

    @property
    def failed_fields(self) -> tuple[str, ...]:
        return tuple(n for n, r in self.records.items() if not r.ok)

    @property
    def ok(self) -> bool:
        return self.parse_path != "unparseable" and not self.failed_fields

    def telemetry(self) -> dict:
        """Flat, JSON-safe summary for `record_call(extra=...)`.

        Carries the per-field citation record C5 rules must live outside the
        database: indices, class, escape usage and every violation, so a stored
        span's evidence is auditable after the fact without a migration.
        """
        return {
            "elicitation_version": C.ELICITATION_VERSION,
            "parse_path": self.parse_path,
            "n_units": self.n_units,
            "unknown_fields": list(self.unknown_fields),
            "failed_fields": list(self.failed_fields),
            "fields": {
                n: {
                    "class": r.field_class,
                    "indices": list(r.indices),
                    "escape": r.is_escape,
                    "n_steps": len(r.steps),
                    "has_inference": bool(r.inference),
                    "violations": list(r.violations),
                    "bad_indices": [repr(b) for b in r.bad_indices],
                    "duplicate_indices": list(r.duplicate_indices),
                }
                for n, r in self.records.items()
            },
        }


# ── Container recovery ───────────────────────────────────────────────


def parse_container(raw: str | None) -> tuple[list[dict], str]:
    """Recover the JSON array of field entries. Returns (entries, parse_path).

    Ported from ELICIT-01's `parse_fields`, which measured a `direct` parse on
    every INDEX call. Recovery is limited to locating the container; it never
    invents or edits an entry.
    """
    text = (raw or "").strip()
    if "```" in text:
        for part in text.split("```"):
            p = part.strip()
            if p.startswith("json"):
                p = p[4:].strip()
            if p.startswith("{") or p.startswith("["):
                text = p
                break
    try:
        return _entries(json.loads(text)), "direct"
    except Exception:
        pass
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        return [], "unparseable"
    try:
        return _entries(json.loads(text[start:end + 1])), "recovered_braces"
    except Exception:
        return [], "unparseable"


def _entries(obj) -> list[dict]:
    if isinstance(obj, dict):
        for key in ("fields", "entries", "extractions", "results", "data"):
            v = obj.get(key)
            if isinstance(v, list):
                return [e for e in v if isinstance(e, dict)]
        if KEY_FIELD in obj:
            return [obj]
        return []
    if isinstance(obj, list):
        return [e for e in obj if isinstance(e, dict)]
    return []


# ── Index handling ───────────────────────────────────────────────────


def _resolve_indices(raw, unit_map: UnitMap
                     ) -> tuple[tuple[int, ...], tuple[object, ...], tuple[int, ...], list[str]]:
    """(valid, bad, duplicates, violations) for one field's `unit_indices`.

    `bad` holds every index that did not resolve, verbatim, so telemetry can
    show what the model actually emitted rather than a normalized shadow of it.
    """
    violations: list[str] = []
    if raw is None:
        return (), (), (), violations
    if not isinstance(raw, list):
        raw = [raw]

    valid: list[int] = []
    bad: list[object] = []
    dupes: list[int] = []
    for ix in raw:
        if not isinstance(ix, int) or isinstance(ix, bool):
            bad.append(ix)
            if INDEX_MALFORMED not in violations:
                violations.append(INDEX_MALFORMED)
            continue
        if unit_map.resolve(ix) is None:
            bad.append(ix)
            if INDEX_OUT_OF_RANGE not in violations:
                violations.append(INDEX_OUT_OF_RANGE)
            continue
        if ix in valid:
            dupes.append(ix)
            if DUPLICATE_INDICES not in violations:
                violations.append(DUPLICATE_INDICES)
            continue
        valid.append(ix)
    return tuple(valid), tuple(bad), tuple(dupes), violations


def _parse_steps(raw, unit_map: UnitMap
                 ) -> tuple[tuple[Step, ...], tuple[object, ...], tuple[int, ...], list[str]]:
    steps: list[Step] = []
    bad: list[object] = []
    dupes: list[int] = []
    violations: list[str] = []
    if not isinstance(raw, list):
        return (), (), (), violations
    for s in raw:
        if not isinstance(s, dict):
            continue
        idx, b, d, v = _resolve_indices(s.get(KEY_INDICES), unit_map)
        bad.extend(b)
        dupes.extend(d)
        for name in v:
            if name not in violations:
                violations.append(name)
        steps.append(Step(
            text=str(s.get(KEY_STEP_TEXT) or "").strip(),
            indices=idx,
            criteria_application=bool(s.get(KEY_STEP_CRITERIA)),
        ))
    return tuple(steps), tuple(bad), tuple(dupes), violations


# ── The per-class contracts ──────────────────────────────────────────


def check_entry(entry: dict, cb_field: dict, cls: str, unit_map: UnitMap,
                codebook: dict) -> FieldRecord:
    """Apply one field's class contract to one response entry."""
    name = cb_field["name"]
    value = str(entry.get(KEY_VALUE) if entry.get(KEY_VALUE) is not None else "").strip()
    escape = C.is_escape(value, codebook)
    violations: list[str] = []

    idx, bad, dupes, iv = _resolve_indices(entry.get(KEY_INDICES), unit_map)
    violations.extend(iv)

    steps: tuple[Step, ...] = ()
    inference = ""
    if cls == C.JUDGMENT:
        steps, sbad, sdupes, sv = _parse_steps(entry.get(KEY_STEPS), unit_map)
        bad = bad + sbad
        dupes = dupes + sdupes
        for v in sv:
            if v not in violations:
                violations.append(v)
    elif cls == C.INFERABLE:
        inference = str(entry.get(KEY_INFERENCE) or "").strip()

    # Citations backing the value: a JUDGMENT field's evidence is the union of
    # its steps' citations, plus any it cited at field level.
    cited: list[int] = list(idx)
    for s in steps:
        for i in s.indices:
            if i not in cited:
                cited.append(i)
    cited_t = tuple(cited)

    if not value:
        violations.append(VALUE_MISSING)
    elif escape:
        # The escape token is a statement about the search, not the paper. A
        # citation alongside it contradicts the token itself.
        if cited_t:
            violations.append(ESCAPE_WITH_CITATION)
    else:
        if not cited_t:
            violations.append(VALUE_WITHOUT_CITATION)
        if cls == C.INFERABLE:
            if not inference:
                violations.append(INFERENCE_MISSING)
            elif not (INFERENCE_MIN_SENTENCES
                      <= len(sentences(inference)) <= INFERENCE_MAX_SENTENCES):
                violations.append(INFERENCE_MALFORMED)
        elif cls == C.JUDGMENT:
            if not steps:
                violations.append(STEPS_MISSING)
            elif any(not s.has_basis for s in steps):
                violations.append(STEP_WITHOUT_BASIS)

    if not escape and cited_t and _value_precedes_evidence(entry, cls):
        violations.append(VALUE_BEFORE_EVIDENCE)

    return FieldRecord(
        field_name=name, field_class=cls, value=value, is_escape=escape,
        indices=cited_t, inference=inference, steps=steps,
        violations=tuple(dict.fromkeys(violations)),
        bad_indices=tuple(bad), duplicate_indices=tuple(dict.fromkeys(dupes)),
    )


def _value_precedes_evidence(entry: dict, cls: str) -> bool:
    """Did the response serialize `value` before its evidence keys?

    The contract is evidence-BEFORE-value, and JSON object key order survives
    `json.loads`, so the ordering is observable rather than merely requested.
    Advisory only -- see the module docstring.
    """
    keys = [k for k in entry if k in (KEY_INDICES, KEY_INFERENCE, KEY_STEPS, KEY_VALUE)]
    if KEY_VALUE not in keys:
        return False
    evidence_keys = {KEY_INDICES, KEY_STEPS} if cls == C.JUDGMENT else \
        ({KEY_INDICES, KEY_INFERENCE} if cls == C.INFERABLE else {KEY_INDICES})
    vpos = keys.index(KEY_VALUE)
    return any(k in evidence_keys and keys.index(k) > vpos for k in keys)


def check_response(raw: str | None, unit_map: UnitMap,
                   codebook: dict, expected: tuple[str, ...]) -> Pass1Result:
    """Parse and check a whole Pass-1 response against every expected field."""
    entries, path = parse_container(raw)
    by_name = {str(e.get(KEY_FIELD)): e for e in entries if e.get(KEY_FIELD)}
    cb_by_name = {f["name"]: f for f in codebook["fields"]}
    known = C.classes_by_field(codebook)

    records: dict[str, FieldRecord] = {}
    for name in expected:
        cb_field = cb_by_name.get(name)
        cls = known.get(name, "")
        if cb_field is None or not cls:
            raise C.CodebookContractError(
                f"{name!r} was asked for but has no codebook entry or class"
            )
        entry = by_name.get(name)
        if entry is None:
            records[name] = FieldRecord(
                field_name=name, field_class=cls, value="", is_escape=False,
                violations=(FIELD_MISSING,),
            )
            continue
        records[name] = check_entry(entry, cb_field, cls, unit_map, codebook)

    unknown = tuple(sorted(n for n in by_name if n not in set(expected)))
    return Pass1Result(records=records, parse_path=path,
                       unknown_fields=unknown, n_units=unit_map.n)
