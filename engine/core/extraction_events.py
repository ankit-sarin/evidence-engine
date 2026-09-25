"""An extraction attempt's outcome as events (WRITE-PATH-01 9b-2c; S5a, S5b).

Built and tested here; the extractor is wired to it at the flip, after 2(d), so
that R111 (no dual write) and R112 (readers before writers) both hold. Until
then nothing in the run path calls this module.

**Two outcomes, one per paper.**

* `ExtractionRecord` — the arm produced something. Each asked-for field has a
  `FieldOutcome` (a value, `contract_unmet`, or `declined`) or is listed in
  `incomplete_fields` (R140: missing after the budget, no event). Built at the
  extractor's write site, or at a refusal's raising site and carried on the
  exception (9b-2c R3), so an exhausted budget still stores what was produced.
* `PaperFailure` — the arm produced nothing usable. One paper event, on the
  processing axis, with a code from `paper_state.EXTRACTION_REASON_CODES` (F9).

**`plan_extraction_events` is pure**: it takes the outcome, the live claims per
field and the paper's processing state, and returns the events as keyword
arguments for `events.write_field_event` / `write_paper_event`. Rules:

* a value -> `asserted` (an absence sentinel is a value; rows 12/13 derive it);
* a class-contract failure, an uncited value or a duplicate after the budget ->
  `contract_unmet` with `violation_codes` and `attempts` (R139, R118);
* the elicited escape token -> `declined` (row 14);
* a live claim on the cell under a different reuse key -> `superseded` against
  it, before the new claim; under the SAME key the plan refuses, because
  selection should have skipped the paper (R96);
* no `citation_located` event — the locator is 2(d).

Every claim-bearing event carries the three input-identity payload keys named
in `engine.core.events`; the writer refuses one that does not (R1).

**`write_extraction_events` is the only thing that touches a connection**: one
savepoint, every event written with `commit=False`, one commit — a refusal
anywhere in a paper writes nothing for it. The R130 telemetry row for an input
that does not fit is JSONL and is written after the commit, outside it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from engine.core import paper_state as PS
from engine.core.completeness import DUPLICATE_FIELD, IncompleteExtractionError
from engine.core.citation_guard import UncitedValueError
from engine.core.effective import (
    NO_RECORDED_STATE,
    LiveClaimEvent,
    effective_state,
    live_claim_events,
)
from engine.core.events import (
    PAYLOAD_PARSED_TEXT_SHA256,
    PAYLOAD_PARSED_TEXT_UID,
    PAYLOAD_REUSE_KEY,
    EventRefused,
    write_field_event,
    write_paper_event,
)
from engine.core.parsed_text import ParsedTextError, ParsedTextRef
from engine.core.reuse_key import reuse_key

logger = logging.getLogger(__name__)

VALUE, CONTRACT_UNMET, DECLINED = "value", "contract_unmet", "declined"
_FIELD_KINDS = (VALUE, CONTRACT_UNMET, DECLINED)
_EVENT_TYPE = {VALUE: "asserted", CONTRACT_UNMET: "contract_unmet", DECLINED: "declined"}

#: The telemetry outcome of the R130 row (`extraction_telemetry.record_call`).
TELEMETRY_OUTCOME_INPUT_EXCEEDS_CONTEXT = "input_exceeds_context"


class ReuseKeyAlreadyClaimed(EventRefused):
    """The arm already holds a live claim on this cell under this input (R96)."""


class ExhaustedWithoutRecord(RuntimeError):
    """A refusal exhausted the budget carrying no record (9b-2c R3). A programming
    invariant, not a paper outcome: the run stops rather than guess."""


@dataclass(frozen=True)
class FieldOutcome:
    field_name: str
    kind: str                                  # VALUE | CONTRACT_UNMET | DECLINED
    value: str | None = None
    source_snippet: str | None = None
    confidence: float | None = None
    violation_codes: tuple[str, ...] = ()
    attempts: int = 1

    def __post_init__(self):
        if self.kind not in _FIELD_KINDS:
            raise ValueError(f"unknown field outcome kind {self.kind!r}")


@dataclass(frozen=True)
class ExtractionRecord:
    paper_id: int
    arm: str
    run_id: int
    extraction_uid: str
    parsed_text: ParsedTextRef | None
    model: str
    model_digest: str | None
    fields: tuple[FieldOutcome, ...]
    incomplete_fields: tuple[str, ...]
    attempts: int
    stage_name: str
    #: 9b-2c R6: request_hash is not returned to the caller; None until it is.
    presented_context_sha256: str | None = None


@dataclass(frozen=True)
class PaperFailure:
    paper_id: int
    arm: str
    run_id: int
    reason_code: str
    stage_name: str
    detail: Mapping = field(default_factory=dict)
    #: R130: the truncated-attempt telemetry row's fields, for an input-fit failure.
    telemetry: Mapping | None = None

    @property
    def to_state(self) -> str:
        return PS.EXTRACTION_REASONS[self.reason_code]


@dataclass(frozen=True)
class EventPlan:
    field_events: tuple[dict, ...]
    paper_event: dict


# ── Records, built at a write site or a raising site ────────────────────
def _index_offenders(offenders) -> dict[str, tuple[str, ...]]:
    out: dict[str, list[str]] = {}
    for name, why in offenders or ():
        out.setdefault(name, []).append(why)
    return {k: tuple(v) for k, v in out.items()}


def _span_by_name(spans) -> dict[str, Mapping]:
    out = {}
    for s in spans or []:
        d = s if isinstance(s, Mapping) else s.model_dump()
        out.setdefault(d.get("field_name"), d)
    return out


def _value(name, span, attempts) -> FieldOutcome:
    return FieldOutcome(name, VALUE, value=span.get("value"),
                        source_snippet=span.get("source_snippet") or None,
                        confidence=span.get("confidence"), attempts=attempts)


def legacy_record(*, paper_id: int, arm: str, run_id: int, extraction_uid: str,
                  parsed_text: ParsedTextRef | None, model: str, model_digest: str | None,
                  expected: tuple[str, ...], spans, offenders=(),
                  duplicated: tuple[str, ...] = (), attempts: int = 1,
                  stage_name: str = "extract_pass2") -> ExtractionRecord:
    """The legacy prompt's attempt, per field: a duplicate or an uncited value is
    `contract_unmet`, a span is a value, anything else is incomplete."""
    by_name, bad = _span_by_name(spans), _index_offenders(offenders)
    fields, incomplete = [], []
    for name in expected:
        if name in duplicated:
            fields.append(FieldOutcome(name, CONTRACT_UNMET, violation_codes=(DUPLICATE_FIELD,),
                                       attempts=attempts))
        elif name in bad:
            fields.append(FieldOutcome(name, CONTRACT_UNMET, violation_codes=bad[name],
                                       attempts=attempts))
        elif name in by_name:
            fields.append(_value(name, by_name[name], attempts))
        else:
            incomplete.append(name)
    return ExtractionRecord(paper_id, arm, run_id, extraction_uid, parsed_text, model,
                            model_digest, tuple(fields), tuple(incomplete), attempts,
                            stage_name)


def elicited_record(*, paper_id: int, arm: str, run_id: int, extraction_uid: str,
                    parsed_text: ParsedTextRef | None, model: str, model_digest: str | None,
                    expected: tuple[str, ...], states: Mapping[str, str], spans,
                    violations: Mapping[str, tuple[str, ...]], unmet_token: str,
                    escape_token: str, evidenced_token: str, offenders=(),
                    duplicated: tuple[str, ...] = (), attempts: int = 1,
                    stage_name: str = "elicitation_pass1") -> ExtractionRecord:
    """The elicited path's attempt, from its terminal states: CONTRACT_UNMET ->
    `contract_unmet` with the class-contract violations, the escape token ->
    `declined`, an evidenced field with its span -> a value (unless uncited)."""
    by_name, bad = _span_by_name(spans), _index_offenders(offenders)
    fields, incomplete = [], []
    for name in expected:
        state = states.get(name)
        if name in duplicated:
            fields.append(FieldOutcome(name, CONTRACT_UNMET, violation_codes=(DUPLICATE_FIELD,),
                                       attempts=attempts))
        elif state == unmet_token:
            fields.append(FieldOutcome(name, CONTRACT_UNMET,
                                       violation_codes=tuple(violations.get(name, ())),
                                       attempts=attempts))
        elif state == escape_token:
            fields.append(FieldOutcome(name, DECLINED, attempts=attempts))
        elif state == evidenced_token and name in bad:
            fields.append(FieldOutcome(name, CONTRACT_UNMET, violation_codes=bad[name],
                                       attempts=attempts))
        elif state == evidenced_token and name in by_name:
            fields.append(_value(name, by_name[name], attempts))
        else:
            incomplete.append(name)
    return ExtractionRecord(paper_id, arm, run_id, extraction_uid, parsed_text, model,
                            model_digest, tuple(fields), tuple(incomplete), attempts,
                            stage_name)


# ── Exceptions -> outcomes ───────────────────────────────────────────
def outcome_for_exception(exc: BaseException, *, paper_id: int, arm: str, run_id: int,
                          stage_name: str):
    """What the extractor's failure branch records for `exc`: the exhausted
    attempt's `ExtractionRecord`, or a `PaperFailure` with its F9 code.

    Propagated as run faults, never mapped (R5): `CodebookContractError` and
    `ExhaustedWithoutRecord`.
    """
    import httpx
    from pydantic import ValidationError

    from engine.elicitation.classes import CodebookContractError
    from engine.utils.ollama_client import (
        CeilingUnavailable, InputDropped, InputOverflow, InputTruncated,
    )
    # A late import: the error class lives with the Pass-1 reader.
    from engine.agents.extractor import MissingThinkingChannelError

    if isinstance(exc, (CodebookContractError, ExhaustedWithoutRecord)):
        raise exc
    if isinstance(exc, (IncompleteExtractionError, UncitedValueError)):
        if exc.record is None:
            raise ExhaustedWithoutRecord(
                f"paper {paper_id}: {type(exc).__name__} exhausted the budget with no "
                "record attached at its raising site (9b-2c R3)") from exc
        return exc.record

    def fail(code, telemetry=None):
        return PaperFailure(paper_id, arm, run_id, code, stage_name,
                            detail={"exception": type(exc).__name__, "message": str(exc)[:500]},
                            telemetry=telemetry)

    if isinstance(exc, ParsedTextError):
        return fail(exc.reason_code)
    if isinstance(exc, CeilingUnavailable):
        return fail(PS.REASON_CONTEXT_CEILING_UNAVAILABLE)
    fit = {InputOverflow: PS.REASON_INPUT_OVERFLOW_ESTIMATED,
           InputTruncated: PS.REASON_INPUT_TRUNCATED_AT_CEILING,
           InputDropped: PS.REASON_INPUT_DROPPED_BELOW_FLOOR}
    for cls, code in fit.items():
        if isinstance(exc, cls):
            return fail(code, telemetry={"model": exc.model, "done_reason": None,
                                         "prompt_eval_count": getattr(exc, "count", None),
                                         "fields": dict(exc.fields)})
    if isinstance(exc, ValidationError):
        return fail(PS.REASON_RESPONSE_UNPARSEABLE)
    if isinstance(exc, MissingThinkingChannelError):
        return fail(PS.REASON_THINKING_CHANNEL_MISSING)
    if isinstance(exc, (TimeoutError, httpx.HTTPError)) or \
            type(exc).__module__.startswith("ollama"):
        return fail(PS.REASON_MODEL_CALL_FAILED)
    if isinstance(exc, KeyboardInterrupt):
        return fail(PS.REASON_RUN_INTERRUPTED)
    return fail(PS.REASON_UNCLASSIFIED_ERROR)


# ── The plan (pure) ─────────────────────────────────────────────────
def _identity(arm: str, paper_id: int, ref: ParsedTextRef | None) -> dict:
    """The three input-identity payload keys (F2), or nothing if the text is
    unknown — which the writer then refuses (ClaimWithoutInputIdentity)."""
    if ref is None:
        return {}
    return {PAYLOAD_REUSE_KEY: reuse_key(arm, paper_id, ref.sha256),
            PAYLOAD_PARSED_TEXT_SHA256: ref.sha256,
            PAYLOAD_PARSED_TEXT_UID: ref.parsed_text_uid}


def _paper_event(outcome, *, event_type, to_state, from_state, reason_code, payload):
    return dict(event_type=event_type, paper_id=outcome.paper_id, to_state=to_state,
                from_state=from_state, actor_kind="engine", actor_role="system",
                actor_name="extractor", run_id=outcome.run_id, reason_code=reason_code,
                stage_name=outcome.stage_name, payload=payload)


def plan_extraction_events(outcome, *, live: Mapping[str, tuple[LiveClaimEvent, ...]],
                           from_state: str | None, sentinels=frozenset()) -> EventPlan:
    """The events one outcome becomes. Pure: no connection, no clock."""
    if isinstance(outcome, PaperFailure):
        if outcome.reason_code not in PS.EXTRACTION_REASON_CODES:
            raise ValueError(f"reason {outcome.reason_code!r} is not in the closed set (F9)")
        return EventPlan((), _paper_event(
            outcome, event_type="extraction_failed", to_state=outcome.to_state,
            from_state=from_state, reason_code=outcome.reason_code,
            payload={**dict(outcome.detail), "arm": outcome.arm}))

    rec: ExtractionRecord = outcome
    if not rec.fields:
        return plan_extraction_events(
            PaperFailure(rec.paper_id, rec.arm, rec.run_id, PS.REASON_NO_FIELDS_RETURNED,
                         rec.stage_name, detail={"incomplete_fields": list(rec.incomplete_fields)}),
            live=live, from_state=from_state, sentinels=sentinels)

    ident = _identity(rec.arm, rec.paper_id, rec.parsed_text)
    key = ident.get(PAYLOAD_REUSE_KEY)
    base = dict(paper_id=rec.paper_id, arm=rec.arm, run_id=rec.run_id,
                extraction_uid=rec.extraction_uid, sentinels=sentinels)
    out: list[dict] = []
    for fo in rec.fields:
        on_cell = live.get(fo.field_name, ())
        if key is not None and any(ev.payload.get(PAYLOAD_REUSE_KEY) == key for ev in on_cell):
            raise ReuseKeyAlreadyClaimed(
                f"paper {rec.paper_id} field {fo.field_name!r}: arm {rec.arm!r} already "
                f"holds a live claim under reuse key {key}; selection should have skipped "
                "this paper (R96). Nothing is written.")
        stale = {ev.claim_id for ev in on_cell}
        if stale:
            out.append(dict(base, event_type="superseded", field_name=fo.field_name,
                            actor_kind="engine", actor_role="system", actor_name="extractor",
                            against_claims=stale, reason="input identity changed",
                            payload=dict(ident)))
        payload = dict(ident)
        if fo.kind == CONTRACT_UNMET:
            payload.update(violation_codes=list(fo.violation_codes), attempts=fo.attempts)
        elif fo.kind == VALUE and fo.confidence is not None:
            payload["confidence"] = fo.confidence
        out.append(dict(base, event_type=_EVENT_TYPE[fo.kind], field_name=fo.field_name,
                        value=fo.value if fo.kind == VALUE else None,
                        source_snippet=fo.source_snippet if fo.kind == VALUE else None,
                        actor_kind="model", actor_role="extractor", actor_name=rec.model,
                        actor_digest=rec.model_digest,
                        presented_context_sha256=rec.presented_context_sha256,
                        payload=payload))

    counts = {k: sum(1 for f in rec.fields if f.kind == k) for k in _FIELD_KINDS}
    return EventPlan(tuple(out), _paper_event(
        rec, event_type="extracted", to_state="extracted", from_state=from_state,
        reason_code=None,
        payload={"asserted": counts[VALUE], "contract_unmet": counts[CONTRACT_UNMET],
                 "declined": counts[DECLINED], "incomplete_fields": list(rec.incomplete_fields),
                 "extraction_uid": rec.extraction_uid, "arm": rec.arm, **ident}))


# ── The writer ──────────────────────────────────────────────────────
def write_extraction_events(conn, outcome, *, sentinels=frozenset(),
                            review_dir: str | Path | None = None) -> EventPlan:
    """Plan `outcome` against the database's live claims and write it: every event
    in one savepoint, one commit, nothing on any refusal."""
    by_field: dict[str, list[LiveClaimEvent]] = {}
    for ev in live_claim_events(conn, outcome.paper_id, outcome.arm):
        by_field.setdefault(ev.field_name, []).append(ev)
    processing = effective_state(conn, outcome.paper_id).processing
    plan = plan_extraction_events(
        outcome, live={k: tuple(v) for k, v in by_field.items()},
        from_state=None if processing == NO_RECORDED_STATE else processing,
        sentinels=sentinels)

    conn.execute("SAVEPOINT extraction_events")
    try:
        for fe in plan.field_events:
            write_field_event(conn, **fe, commit=False)
        write_paper_event(conn, **plan.paper_event, commit=False)
        conn.execute("RELEASE extraction_events")
        if conn.in_transaction:
            conn.commit()
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK TO extraction_events")
            conn.execute("RELEASE extraction_events")
        raise

    if isinstance(outcome, PaperFailure) and outcome.telemetry and review_dir is not None:
        from engine.core.extraction_telemetry import record_call
        t = outcome.telemetry
        record_call(review_dir, arm=t.get("model") or outcome.arm, paper_id=outcome.paper_id,
                    attempt=1, outcome=TELEMETRY_OUTCOME_INPUT_EXCEEDS_CONTEXT,
                    model=t.get("model"), finish_reason=t.get("done_reason"),
                    pass1_done_reason=t.get("done_reason"),
                    pass1_prompt_eval_count=t.get("prompt_eval_count"),
                    error=outcome.reason_code, extra={"input_fit": dict(t.get("fields", {}))})
    return plan
