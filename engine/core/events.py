"""The event writer: append-only, with every refusal v2.1 needs at write time.

EFFECTIVE-RESULT-02 (session 5, S2 core). The reader in
`engine.core.effective` says what a cell currently means; this module is the
only supported way to add to the history it reads. Refusals live here because
v2.1's "unresolved" rows each name an exit, and an exit that cannot be
represented is not an exit: R20 and R24 are enforced *before* the row is
written, so the store never holds a decision that resolves nothing.

**One predicate, two programs.** Every refusal that turns on "what is live on
this cell" calls `effective.live_claims` — the same function the resolution rule
uses. A second copy of that query here is the divergence this whole slice exists
to end.

**Atomic with its against-set.** An event and its junction rows are written in
one transaction, so a crash leaves neither. If a partial set ever did become
visible it would read as a proper subset, which addendum 3 §A calls "inert by
construction" — the safe direction to fail in.

**No refusal on a pre-manifest arm.** An earlier draft of this session refused
`asserted` on an arm marked `not recorded (pre-manifest)`. That was reversed:
v2.1 row 7 *is* two claims on such an arm, and addendum 2 §C.4 registers the
three existing arms pre-manifest precisely "to make row 7 reachable", so the
refusal would have deleted the row it was meant to protect. R19 and R22-U4 are
the operational control until session 7 builds R10's configuration-mismatch
refusal with the spec arms block.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone

from engine.core.effective import (
    PRE_MANIFEST, RULE_VERSION, REVIEWER_EVENT_TYPES,
    classify_field_state, is_assigned, live_claims, UnknownArm,
)

__all__ = [
    "EventRefused", "ReviewerDecisionAmbiguous", "AgainstReferenceIncomplete",
    "AcceptAgainstMultipleClaims", "CellNotAssigned", "ArmConfigurationFrozen",
    "UnknownArm", "PRE_MANIFEST",
    "mint_extraction_uid", "make_claim_id", "register_arm", "retire_arm",
    "write_field_event", "write_paper_event",
]


class EventRefused(Exception):
    """A write that v2.1 or a PI ruling forbids. Never a silent no-op."""


class ReviewerDecisionAmbiguous(EventRefused):
    """R20: a reviewer decision that names nothing cannot name its exit."""


class AgainstReferenceIncomplete(EventRefused):
    """R24: a proper-subset against-reference, refused naming what it omitted."""


class AcceptAgainstMultipleClaims(EventRefused):
    """R20: ACCEPT cannot say which of several live values it endorses."""


class CellNotAssigned(EventRefused):
    """R22/U2: a reviewer decision on a cell outside the arm's assignment."""


class ArmConfigurationFrozen(EventRefused):
    """R21: an arm holding a claim cannot be re-pinned; a new configuration is a new arm."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def mint_extraction_uid() -> str:
    """A uuid4 per extraction. Nothing is derived from content: a re-extraction
    that reproduces identical text still gets a new uid, which is the required
    behaviour (read-out §2.3)."""
    return str(uuid.uuid4())


def make_claim_id(arm: str, extraction_uid: str, field_name: str) -> str:
    return f"{arm}:{extraction_uid}:{field_name}"


# ── the arm registry ──────────────────────────────────────────────────
def register_arm(conn, arm_name, arm_kind, *, configuration=None,
                 configuration_marker=None, registered_at=None) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO arms (arm_name, arm_kind, configuration_json, "
        "configuration_marker, registered_at, retired_at) VALUES (?, ?, ?, ?, ?, NULL)",
        (arm_name, arm_kind, json.dumps(configuration or {}),
         configuration_marker, registered_at or _now()))


def retire_arm(conn, arm_name, *, at=None) -> None:
    """R21: retiring means the arm accepts no new claims; its claims stand."""
    conn.execute("UPDATE arms SET retired_at = ? WHERE arm_name = ?",
                 (at or _now(), arm_name))


def repin_arm_configuration(conn, arm_name, configuration) -> None:
    """Refused by trigger once the arm holds a claim (R21)."""
    try:
        conn.execute("UPDATE arms SET configuration_json = ? WHERE arm_name = ?",
                     (json.dumps(configuration), arm_name))
    except sqlite3.IntegrityError as exc:
        raise ArmConfigurationFrozen(str(exc)) from exc


# ── the write path ────────────────────────────────────────────────────
def write_field_event(conn, *, event_type, paper_id, field_name, arm,
                      claim_id=None, extraction_uid=None, value=None,
                      source_snippet=None, actor_kind, actor_role, actor_name,
                      actor_digest=None, occurred_at=None, run_id=None,
                      run_marker="pre-manifest", prior_event_id=None,
                      presented_context_sha256=None, reason=None, payload=None,
                      against_claims=(), against_decisions=(), sentinels=frozenset(),
                      commit=True) -> int:
    """Append one `field_events` row and its against-set, or refuse."""
    against_claims = set(against_claims)
    against_decisions = set(against_decisions)
    is_reviewer = event_type in REVIEWER_EVENT_TYPES and actor_role == "reviewer"

    if is_reviewer:
        if not is_assigned(conn, arm, paper_id):
            raise CellNotAssigned(
                f"reviewer decision refused: paper {paper_id} field {field_name!r} is "
                f"outside arm {arm!r}'s assignment, so the cell is out of scope and a "
                f"decision on it resolves nothing (R22/U2). The assignment table arrives "
                f"with human arm loading in session 12.")
        if not against_claims and not against_decisions:
            raise ReviewerDecisionAmbiguous(
                f"reviewer decision refused: {event_type} on paper {paper_id} field "
                f"{field_name!r} names no claim and no competing decision, so it cannot "
                f"name its exit (R20). Every 'unresolved' outcome names its exit.")
        live = set(live_claims(conn, paper_id, field_name, arm))
        if event_type == "human_accepted" and len(live) > 1:
            raise AcceptAgainstMultipleClaims(
                f"ACCEPT refused: {len(live)} live claims on paper {paper_id} field "
                f"{field_name!r} in arm {arm!r} ({', '.join(sorted(live))}) — an ACCEPT "
                f"cannot say which value it endorses (R20). Exit this cell with CORRECT "
                f"or WITHDRAW naming every competing claim.")
        if against_claims and against_claims < live:
            omitted = sorted(live - against_claims)
            raise AgainstReferenceIncomplete(
                f"reviewer decision refused: its against-reference is a proper subset of "
                f"the live claims — it omits {', '.join(omitted)} (R24). A partial "
                f"reference cannot say which value it endorses or replaces.")

    payload = dict(payload or {})
    payload.setdefault("state_at_write",
                       classify_field_state(value, [], event_type, sentinels=sentinels))
    payload.setdefault("rule_version", RULE_VERSION)
    if presented_context_sha256 is not None:
        payload.setdefault("presented_context_sha256", presented_context_sha256)
    if reason is not None:
        payload.setdefault("reason", reason)

    if claim_id is None:
        if extraction_uid is None:
            extraction_uid = mint_extraction_uid()
        claim_id = make_claim_id(arm, extraction_uid, field_name)

    try:
        cur = conn.execute(
            "INSERT INTO field_events (event_uid, event_type, occurred_at, recorded_at, "
            "actor_kind, actor_role, actor_name, actor_digest, run_id, run_marker, "
            "prior_event_id, presented_context_sha256, reason, payload_json, claim_id, "
            "extraction_uid, paper_id, field_name, arm, value, source_snippet) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (str(uuid.uuid4()), event_type, occurred_at or _now(), _now(),
             actor_kind, actor_role, actor_name, actor_digest, run_id, run_marker,
             prior_event_id, presented_context_sha256, reason, json.dumps(payload),
             claim_id, extraction_uid, paper_id, field_name, arm, value, source_snippet))
        event_id = cur.lastrowid
        for cid in sorted(against_claims):
            conn.execute("INSERT INTO field_event_against (event_id, against_claim_id) "
                         "VALUES (?, ?)", (event_id, cid))
        for aid in sorted(against_decisions):
            conn.execute("INSERT INTO field_event_against_decisions "
                         "(event_id, against_event_id) VALUES (?, ?)", (event_id, aid))
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    return event_id


def write_paper_event(conn, *, event_type, paper_id, to_state, from_state=None,
                      actor_kind, actor_role, actor_name, actor_digest=None,
                      occurred_at=None, run_id=None, run_marker="pre-manifest",
                      prior_event_id=None, presented_context_sha256=None,
                      reason=None, reason_code=None, stage_name=None,
                      payload=None, commit=True) -> int:
    cur = conn.execute(
        "INSERT INTO paper_events (event_uid, event_type, occurred_at, recorded_at, "
        "actor_kind, actor_role, actor_name, actor_digest, run_id, run_marker, "
        "prior_event_id, presented_context_sha256, reason, payload_json, paper_id, "
        "to_state, from_state, reason_code, stage_name) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (str(uuid.uuid4()), event_type, occurred_at or _now(), _now(),
         actor_kind, actor_role, actor_name, actor_digest, run_id, run_marker,
         prior_event_id, presented_context_sha256, reason,
         json.dumps(payload or {}), paper_id, to_state, from_state,
         reason_code, stage_name))
    if commit:
        conn.commit()
    return cur.lastrowid
