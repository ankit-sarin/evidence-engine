"""The paper-level state vocabulary, on two axes — R29 as corrected by R39.

Migration 016 introduced a single `to_state` CHECK over five tokens, and
READERS-01's Phase 1 read-out measured what that collapsed together: `eligible`,
`abstract_out` and `full_text_out` are *eligibility* facts, `audited_ai` is a
*processing* fact, and `full_text_not_obtainable` is S3h reason #1 wearing an
eligibility column's clothes. The same read-out found `PAPER_EVENT_TYPES` already
carrying `extraction_failed`, `parsed` and `extracted` with **no `to_state` token
any of them could write** — so A9 was not merely latent in the corpus predicate,
it was unwritable in the store.

R39 separates them. Eligibility answers *should this paper be in the review*;
processing answers *how far did the machinery get, and why did it stop*. A paper
whose extraction failed is still eligible — that is the whole of ruling 4, and it
is what row 6's gate means by "`EXTRACT_FAILED` papers stay eligible with a
reason".

**There is no stored `axis` column, deliberately.** The two token sets are
disjoint, so an event's axis is a function of its `to_state`, and a column
carrying a value derivable from another column in the same row is the second
source of truth that session 5 deleted `field_state` to avoid ("the reader
derives every state and never reads a stored one"). The axis is enforced instead
by a CHECK pairing `event_type` with its axis's tokens, which is a constraint the
database can hold rather than a value a writer can get wrong.

**`analysis_ready` is derived, never stored** (R39). It is eligibility ==
`eligible` AND processing in `COMPLETED_PROCESSING_STATES`. The set is declared
here, once.

**Migration modules do not import this file** (R35): from 018 onward a migration
declares its own DDL and token lists, so that editing a constant can never change
what an already-applied migration meant. `tests/test_paper_state_vocabulary.py`
asserts the migration's re-declaration and this module agree — one predicate, two
programs, checked rather than copied.
"""

from __future__ import annotations

#: Eligibility axis — "should this paper be in the review". R39.
ELIGIBILITY_STATES: tuple[str, ...] = (
    "eligible",
    "abstract_out",
    "full_text_out",
)

#: Processing axis — "how far did the machinery get". R39.
PROCESSING_STATES: tuple[str, ...] = (
    "parsed",
    "extracted",
    "extraction_failed",
    "full_text_not_obtainable",
    "parse_failed",
    "input_exceeds_context",
    "audited_ai",
)

#: The processing tokens that are failures. Each one REQUIRES `reason_code`;
#: every other token forbids it. Enforced by CHECK in migration 019, not by the
#: writer — S3h's "failure states carry a reason" is an invariant of the record,
#: and an invariant enforced only in a writer is one the next writer does not
#: inherit (the same argument that made append-only a trigger).
FAILURE_STATES: tuple[str, ...] = (
    "extraction_failed",
    "full_text_not_obtainable",
    "parse_failed",
    "input_exceeds_context",
)

#: Processing tokens that mean the machinery finished. `analysis_ready` is
#: eligibility == 'eligible' AND processing in this set (R39, S3h).
COMPLETED_PROCESSING_STATES: tuple[str, ...] = (
    "extracted",
    "audited_ai",
)

#: Returned on either axis when the paper has no event on that axis. Distinct
#: from every token above: "we have no record" is not "we recorded nothing
#: happened", and an export that cannot tell them apart will report the first as
#: the second.
NO_RECORDED_STATE = "no_recorded_state"

#: `event_type` → the axis its `to_state` must belong to (R39; step 2 of the
#: Phase 2a contract, "PAPER_EVENT_TYPES mapped to an axis"). Three types are
#: `both`: `state_at_migration` records whatever was true at the seed,
#: `manual_advance` and `bypass` are administrative acts that can land on either
#: axis. Every other type names one.
#:
#: A type mapped to an axis that has no token for its outcome stays unwritable —
#: which is the state 016 was already in, now made explicit instead of accidental.
EVENT_TYPE_AXIS: dict[str, str] = {
    "identified":         "eligibility",
    "duplicate_of":       "eligibility",
    "screened":           "eligibility",
    "verified":           "eligibility",
    "adjudicated":        "eligibility",
    "acquired":           "processing",
    "not_obtainable":     "processing",
    "parsed":             "processing",
    "extracted":          "processing",
    "extraction_failed":  "processing",
    "audited":            "processing",
    "manual_advance":     "both",
    "bypass":             "both",
    "state_at_migration": "both",
}


def axis_of(to_state: str) -> str | None:
    """The axis a token belongs to, or None if it belongs to neither.

    The sets are disjoint by construction and a test pins that; this function is
    the one place that fact is used, so a future overlap becomes one failure
    rather than a silently wrong axis at every reader.
    """
    if to_state in ELIGIBILITY_STATES:
        return "eligibility"
    if to_state in PROCESSING_STATES:
        return "processing"
    return None


def is_failure(to_state: str) -> bool:
    """True if `to_state` is a processing failure and therefore needs a reason."""
    return to_state in FAILURE_STATES


# ── F9: the closed reason vocabulary for extraction outcomes (9b-2c R7) ──
#
# `reason_code` on a processing-axis failure. Every code the extractor's event
# mapping can emit is here and spelled nowhere else in the engine (T12's pin).
# The exhaustion outcomes — an incomplete, uncited or duplicated field after the
# retry budget — are deliberately ABSENT: they store (R139 contract_unmet, R140
# incomplete_fields), so they are not failures. `extraction_failed` is reserved
# for "the arm produced nothing usable; select the paper again next run".

#: A14 (R122): the parsed text cannot be handed out. `engine.core.parsed_text`'s
#: refusals carry these as their `reason_code`.
REASON_PARSED_TEXT_NOT_RECORDED = "parsed_text_not_recorded"
REASON_PARSED_TEXT_MISSING = "parsed_text_missing"
REASON_PARSED_TEXT_MODIFIED = "parsed_text_modified"
#: R120: the input does not fit the model's context — one code per cause.
REASON_INPUT_OVERFLOW_ESTIMATED = "input_overflow_estimated"
REASON_INPUT_TRUNCATED_AT_CEILING = "input_truncated_at_ceiling"
REASON_INPUT_DROPPED_BELOW_FLOOR = "input_dropped_below_floor"
#: The model's context ceiling could not be read (`/api/show` failed).
REASON_CONTEXT_CEILING_UNAVAILABLE = "context_ceiling_unavailable"
#: Timeout, transport or server error after the client's retries and restart.
REASON_MODEL_CALL_FAILED = "model_call_failed"
#: A think-enabled Pass 1 returned no reasoning channel (REGRESSION-01).
REASON_THINKING_CHANNEL_MISSING = "thinking_channel_missing"
#: Pass 2 could not be parsed after the outer budget (9b-2c R4).
REASON_RESPONSE_UNPARSEABLE = "response_unparseable"
#: The arm returned no field with any outcome.
REASON_NO_FIELDS_RETURNED = "no_fields_returned"
#: The run stopped mid-paper.
REASON_RUN_INTERRUPTED = "run_interrupted"
#: Anything else; the exception's type and message ride in the payload (R5).
REASON_UNCLASSIFIED_ERROR = "unclassified_error"

#: Each reason code -> the processing token it is recorded under.
EXTRACTION_REASONS: dict[str, str] = {
    REASON_PARSED_TEXT_NOT_RECORDED: "extraction_failed",
    REASON_PARSED_TEXT_MISSING: "extraction_failed",
    REASON_PARSED_TEXT_MODIFIED: "extraction_failed",
    REASON_INPUT_OVERFLOW_ESTIMATED: "input_exceeds_context",
    REASON_INPUT_TRUNCATED_AT_CEILING: "input_exceeds_context",
    REASON_INPUT_DROPPED_BELOW_FLOOR: "input_exceeds_context",
    REASON_CONTEXT_CEILING_UNAVAILABLE: "extraction_failed",
    REASON_MODEL_CALL_FAILED: "extraction_failed",
    REASON_THINKING_CHANNEL_MISSING: "extraction_failed",
    REASON_RESPONSE_UNPARSEABLE: "extraction_failed",
    REASON_NO_FIELDS_RETURNED: "extraction_failed",
    REASON_RUN_INTERRUPTED: "extraction_failed",
    REASON_UNCLASSIFIED_ERROR: "extraction_failed",
}

EXTRACTION_REASON_CODES: frozenset[str] = frozenset(EXTRACTION_REASONS)
