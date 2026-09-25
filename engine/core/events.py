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

**The refusals, in full** (R74 — this list replaces the session-5 claim that
no refusal applies to a pre-manifest arm; R59 reinstated one, for *claims*):

* **R20** `ReviewerDecisionAmbiguous` — a reviewer decision naming nothing.
* **R20** `AcceptAgainstMultipleClaims` — ACCEPT with more than one live claim.
* **R24** `AgainstReferenceIncomplete` — a proper-subset against-reference.
* **R22-U2** `CellNotAssigned` — a reviewer decision outside the arm's assignment.
* **R21** `ArmConfigurationFrozen` — re-pinning a claimed, pinned or pre-manifest arm.
* **R68** `RunLinkRefused` — an event with no `run_id`, or a `run_marker` from a
  caller that is not a migration. Only a migration writes `'pre-manifest'`.
* **R59** `ClaimOnPreManifestArm` — a claim on an arm registered pre-manifest.
* **R21** `ClaimOnRetiredArm` — a claim on a retired arm ("accepts no new claims").
* **R10** `ArmNotInRun` — a claim on a model arm the run's manifest did not pin.
* **9b-2c R1** `ClaimWithoutInputIdentity` — an extractor's claim-bearing event
  without the three input-identity payload keys.

**Row 7 stays reachable.** v2.1 row 7 is two live claims on a pre-manifest
arm. After R59 no new claim can land on such an arm, so on live data row 7 is
reachable only through claims seeded by a migration — there are none, and R25
seeded no field history. Its *exits* are reviewer events (R20), and reviewer
events on pre-manifest claims stay allowed, each carrying its reviewer session's
`run_id` (R68). Fixtures construct row 7 the way a migration would, below the
writer.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import uuid
from pathlib import Path
from datetime import datetime, timezone

from engine.core.effective import (
    CLAIM_EVENT_TYPES, PRE_MANIFEST, RULE_VERSION, REVIEWER_EVENT_TYPES,
    classify_field_state, is_assigned, live_claims, UnknownArm,
)

__all__ = [
    "EventRefused", "ReviewerDecisionAmbiguous", "AgainstReferenceIncomplete",
    "AcceptAgainstMultipleClaims", "CellNotAssigned", "ArmConfigurationFrozen",
    "RunLinkRefused", "ClaimOnPreManifestArm", "ClaimOnRetiredArm", "ArmNotInRun",
    "ClaimWithoutInputIdentity",
    "UnknownArm", "PRE_MANIFEST", "PRE_MANIFEST_MARKER",
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
    """R21/R59: a claimed, pinned or pre-manifest arm cannot be re-pinned; a new
    configuration is a new arm."""


class RunLinkRefused(EventRefused):
    """R68: every event carries a run; only a migration writes 'pre-manifest'."""


class ClaimOnPreManifestArm(EventRefused):
    """R59: a pre-manifest arm never pins and accepts no new claims."""


class ClaimOnRetiredArm(EventRefused):
    """R21: a retired arm accepts no new claims; its claims stand."""


class ArmNotInRun(EventRefused):
    """R10: a claim on a model arm whose configuration the run did not pin."""


class ClaimWithoutInputIdentity(EventRefused):
    """9b-2c R1: an extractor's claim must say which input it was made from —
    the reuse key, the parsed-text hash and uid — or selection could never skip
    it and a new text version could never supersede it (F2, R96)."""


#: The event-row marker for seeded, pre-manifest rows (R68). Migration 020
#: re-declares it; a test asserts the two agree.
PRE_MANIFEST_MARKER = "pre-manifest"

#: F2 (9b-2a): the claim's input identity rides in `payload_json`, no migration.
#: Every claim-bearing event the extractor writes carries all three from slice
#: 2(c) (R139, 9b-2a R3); selection (`engine.core.selection`) reads the first.
#: These names are spelled here and nowhere else a payload is read.
PAYLOAD_REUSE_KEY = "reuse_key"
PAYLOAD_PARSED_TEXT_SHA256 = "parsed_text_sha256"
PAYLOAD_PARSED_TEXT_UID = "parsed_text_uid"

_MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
_MISSING = object()


def _called_from_migration(depth: int = 3) -> bool:
    """True when the writer's caller is a numbered migration module.

    Applied migration 017 calls `write_paper_event(..., run_marker="pre-manifest")`
    and its text is checksummed, so it cannot be changed to pass anything else.
    The caller's FILE is the only identity it has; a keyword a migration would
    have to pass would be one 017 does not pass.
    """
    frame = sys._getframe(depth)
    try:
        return Path(frame.f_code.co_filename).resolve().parent == _MIGRATIONS_DIR
    finally:
        del frame


def _run_link(conn, run_id, run_marker, *, migration: bool) -> tuple:
    """R68: resolve (run_id, run_marker) or refuse."""
    if run_marker is not None and not migration:
        raise RunLinkRefused(
            f"event refused: run_marker={run_marker!r} may be written only by a "
            "migration (R68). A new event carries its run's run_id instead.")
    if run_id is _MISSING or run_id is None:
        if migration and run_marker == PRE_MANIFEST_MARKER:
            return None, run_marker
        raise RunLinkRefused(
            "event refused: run_id is required (R68). Every event a run writes "
            "carries that run's manifest id; a reviewer's session is a run of kind "
            "'review_session'. Only seeded rows carry no run, marked 'pre-manifest'.")
    if not conn.execute("SELECT 1 FROM run_manifests WHERE run_id = ?", (run_id,)).fetchone():
        raise RunLinkRefused(f"event refused: run_id {run_id} names no run manifest (R68)")
    return run_id, None


def _refuse_claim_on_arm(conn, arm: str, run_id) -> None:
    """R59, R21, R10 — a claim may land only on a live arm its run pinned."""
    row = conn.execute(
        "SELECT arm_kind, configuration_marker, retired_at FROM arms WHERE arm_name = ?",
        (arm,)).fetchone()
    if row is None:
        return  # the FK refuses an unknown arm, naming it
    kind, marker, retired = row
    if marker == PRE_MANIFEST:
        raise ClaimOnPreManifestArm(
            f"claim refused: arm {arm!r} was registered pre-manifest; it never pins "
            "and accepts no new claims (R59). A new configuration is a new arm (R10).")
    if retired is not None:
        raise ClaimOnRetiredArm(
            f"claim refused: arm {arm!r} was retired at {retired}; a retired arm "
            "accepts no new claims, its existing claims stand (R21).")
    if kind == "model" and run_id is not None and not conn.execute(
            "SELECT 1 FROM run_stage_configs WHERE run_id = ? AND arm_name = ?",
            (run_id, arm)).fetchone():
        raise ArmNotInRun(
            f"claim refused: run {run_id} did not pin arm {arm!r}, so this write's "
            "configuration is not its declared arm's (R10).")


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
                      actor_digest=None, occurred_at=None, run_id=_MISSING,
                      run_marker=None, prior_event_id=None,
                      presented_context_sha256=None, reason=None, payload=None,
                      against_claims=(), against_decisions=(), sentinels=frozenset(),
                      commit=True) -> int:
    """Append one `field_events` row and its against-set, or refuse.

    `run_id` is required (R68). `run_marker` is accepted only from a migration.
    """
    run_id, run_marker = _run_link(conn, run_id, run_marker,
                                   migration=_called_from_migration(2))
    against_claims = set(against_claims)
    against_decisions = set(against_decisions)
    is_reviewer = event_type in REVIEWER_EVENT_TYPES and actor_role == "reviewer"

    # 9b-2d R1: R59/R21/R10 govern CLAIMS. A system event about an existing
    # claim — citation_located, superseded — is not a claim and is not refused
    # for the arm's state, as reviewer and state_at_migration events never were.
    if not is_reviewer and event_type in CLAIM_EVENT_TYPES:
        _refuse_claim_on_arm(conn, arm, run_id)

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
    if actor_role == "extractor" and event_type in CLAIM_EVENT_TYPES:
        absent = [k for k in (PAYLOAD_REUSE_KEY, PAYLOAD_PARSED_TEXT_SHA256,
                              PAYLOAD_PARSED_TEXT_UID) if not payload.get(k)]
        if absent:
            raise ClaimWithoutInputIdentity(
                f"{event_type} refused: paper {paper_id} field {field_name!r} arm {arm!r} "
                f"carries no {', '.join(absent)} in its payload. An extractor's claim "
                "names the input it was made from (F2, 9b-2c R1).")
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
                      occurred_at=None, run_id=_MISSING, run_marker=None,
                      prior_event_id=None, presented_context_sha256=None,
                      reason=None, reason_code=None, stage_name=None,
                      payload=None, commit=True) -> int:
    """Append one `paper_events` row. `run_id` is required (R68); `run_marker`
    is accepted only from a migration (017's seed is the one caller that passes it)."""
    run_id, run_marker = _run_link(conn, run_id, run_marker,
                                   migration=_called_from_migration(2))
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
