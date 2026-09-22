"""One reader of "the current value" — resolution rule v2.1 (R23).

EFFECTIVE-RESULT-02 (session 5, S2 core). Inventory row A1 is that there is no
such reader: thirteen stores, fifteen readers, each with its own rule, three
pairs disagreeing structurally. This module is the one rule, written down once
and tested on constructed histories.

`effective_value(paper, field, arm)` → `(value, state, provenance)`.
`effective_state(paper)` → the paper's lifecycle state with who and why.

**`arm` is required** (R18/Q4). **Arms are data** (R12): there is no per-arm
branch anywhere below. **First matching row wins**, rows 0 through 17 in order;
the row number is returned in the provenance so a disagreement about a result is
a disagreement about a numbered row, not about an opinion.

**The reader derives every state and never reads a stored one** (read-out §5.5).
`classify_field_state` is the one derivation, and session 9's write path calls
*this* function rather than reimplementing it — one predicate, two programs,
shared by import and never copied. The writer records its own verdict in
`payload_json.state_at_write` as telemetry a test asserts the derivation
against; the reader does not consult it.

**Two rows are not returnable here, by design.** Row 16 (arms compared, never
merged) is a property of the caller: `effective_value` is per-arm by
construction, so comparing arms is something a caller does with several of its
results, and a cross-arm judgment is not an event shape (R22/U3). Row 17 (state
at migration) is `effective_state`'s, not a field's: under R25 the event store
begins empty of field-level history, so row 17 has no field-level population at
all (addendum 4 §C).

**`papers.status` is never read**, not even into provenance. R13's "model arms
default to the full corpus" is implemented as `is_assigned` returning True
unconditionally for a model arm — addendum 2 §C.5: "model arms default to the
full corpus and row 0 never fires" — which is the same answer without consulting
a status column the event store is replacing.

**Absence sentinels are a required argument, not a constant here.** The codebook
is their only source; a copy in this module would be the divergence it exists to
prevent. `load_absence_sentinels` reads them from the codebook that the caller
names, through `engine.core.codebook` — the one loader for that file type.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field as _dc_field
from pathlib import Path

RULE_VERSION = "v2.1"

# The paper-state vocabulary lives in one module and is imported, never
# re-spelled here (R35 binds migrations, not the reader: the reader is the
# consumer of the constants, migration 019 is the re-declaration a test pins
# against them).
from engine.core.paper_state import (  # noqa: E402
    COMPLETED_PROCESSING_STATES,
    ELIGIBILITY_STATES,
    NO_RECORDED_STATE as _NO_RECORDED_STATE,
    PROCESSING_STATES,
    is_failure,
)

#: The marker R10 requires on an arm registered before manifests existed. It
#: lives here, in the lowest module of the three, so the writer, migration 016
#: and this reader share one spelling rather than three copies of a string whose
#: equality is load-bearing for v2.1 row 7.
PRE_MANIFEST = "not recorded (pre-manifest)"

# ── the states this reader emits ──────────────────────────────────────
OUT_OF_SCOPE = "out of scope"            # row 0 — NOT an S5b state (R13)
MISSING = "missing"                       # row 1
UNRESOLVED_REREVIEW = "unresolved (needs re-review)"    # rows 2, 3, 7
WITHDRAWN = "withdrawn"                   # row 4
CORRECTED_BY_HUMAN = "corrected by human" # row 5
UNRESOLVED_DUPLICATE = "unresolved (duplicate values)"  # row 6
ASSERTED_WITH_EVIDENCE = "asserted with evidence"       # rows 10, 12
ASSERTED_WITHOUT_EVIDENCE = "asserted without locatable evidence"  # rows 11, 13
DECLINED = "declined"                     # row 14
CONTRACT_UNMET = "contract unmet"         # row 15
NO_RECORDED_STATE = _NO_RECORDED_STATE    # effective_state, no event on that axis

CLAIM_EVENT_TYPES = ("asserted", "declined", "contract_unmet")
REVIEWER_EVENT_TYPES = ("human_accepted", "human_corrected", "human_withdrew")


@dataclass(frozen=True)
class EffectiveValue:
    value: str | None
    state: str
    rule_row: int
    provenance: dict = _dc_field(default_factory=dict)


@dataclass(frozen=True)
class EffectiveState:
    """A paper's lifecycle on TWO axes (R29, corrected by R39).

    Eligibility answers *should this paper be in the review*; processing answers
    *how far did the machinery get, and why did it stop*. They are separate
    values because they are separate facts: a paper whose extraction failed is
    still eligible, which is the whole of ruling 4 and what row 6's gate means by
    "`EXTRACT_FAILED` papers stay eligible with a reason".

    `analysis_ready` is DERIVED here and stored nowhere (R39): eligibility is
    `eligible` and processing is in `COMPLETED_PROCESSING_STATES`.

    No caller receives a bare token. There was a single `state` field until
    Phase 2a; it is gone rather than kept beside the axes (R30), because a reader
    that can still ask the old question will.
    """
    eligibility: str
    processing: str
    processing_reason: str | None = None
    eligibility_provenance: dict = _dc_field(default_factory=dict)
    processing_provenance: dict = _dc_field(default_factory=dict)

    @property
    def analysis_ready(self) -> bool:
        return (self.eligibility == "eligible"
                and self.processing in COMPLETED_PROCESSING_STATES)

    @property
    def in_corpus(self) -> bool:
        """S3h/A9: corpus membership is the ELIGIBILITY axis and nothing else."""
        return self.eligibility == "eligible"


def load_absence_sentinels(codebook_path: str | Path) -> frozenset[str]:
    """The codebook's `absence_sentinels`, from the codebook and nowhere else.

    Through `engine.core.codebook`, never a bare `yaml.safe_load`: C8's guard
    leaves exactly three raw-YAML sites in the repository, each the sole
    authority for its own file type, and eleven readers that each reached for
    the keys they happened to need is the defect that guard exists to prevent.
    """
    from engine.core.codebook import load_codebook
    return frozenset(load_codebook(codebook_path).absence_sentinels)


# ── the derivation, shared with session 9's write path ────────────────
def classify_field_state(value, citations, event_type, *, sentinels=frozenset()):
    """The one derivation of a claim-bearing event's field state.

    `citations` is the sequence of that claim's `citation_located` payloads.
    Returns None for an event type that carries no claim, so a caller can tell
    "this event says nothing about the value" from "this event says the value
    has no evidence".
    """
    if event_type == "declined":
        return DECLINED
    if event_type == "contract_unmet":
        return CONTRACT_UNMET
    if event_type != "asserted":
        return None
    located = any(bool(c.get("located")) for c in citations)
    return ASSERTED_WITH_EVIDENCE if located else ASSERTED_WITHOUT_EVIDENCE


def is_assigned(conn: sqlite3.Connection, arm: str, paper_id: int) -> bool:
    """R13. Model arms default to the full corpus, so row 0 never fires for one.

    A `human_extractor` arm is assigned nothing until the assignment table
    arrives with human arm loading (session 12, addendum 2 §C.5).
    """
    row = conn.execute("SELECT arm_kind FROM arms WHERE arm_name = ?", (arm,)).fetchone()
    if row is None:
        raise UnknownArm(f"arm {arm!r} is not in the registry — a claim id naming "
                         f"an unregistered arm is not a claim id (addendum 2 §D.1)")
    return row[0] == "model"


class UnknownArm(KeyError):
    """An arm that does not resolve in the review's registry (addendum 2 §D.1)."""


# ── internals ─────────────────────────────────────────────────────────
class _Ev:
    __slots__ = ("event_id", "event_type", "claim_id", "value", "actor_role",
                 "actor_kind", "actor_name", "occurred_at", "payload",
                 "source_snippet", "against_claims", "against_decisions")

    def __init__(self, row, cols):
        d = dict(zip(cols, row))
        self.event_id = d["event_id"]
        self.event_type = d["event_type"]
        self.claim_id = d["claim_id"]
        self.value = d["value"]
        self.actor_role = d["actor_role"]
        self.actor_kind = d["actor_kind"]
        self.actor_name = d["actor_name"]
        self.occurred_at = d["occurred_at"]
        self.source_snippet = d["source_snippet"]
        self.payload = json.loads(d["payload_json"] or "{}")
        self.against_claims = set()
        self.against_decisions = set()


_COLS = ("event_id", "event_type", "claim_id", "value", "actor_role", "actor_kind",
         "actor_name", "occurred_at", "source_snippet", "payload_json")


def _cell_events(conn, paper_id, field_name, arm) -> list[_Ev]:
    rows = conn.execute(
        f"SELECT {', '.join(_COLS)} FROM field_events "
        "WHERE paper_id = ? AND field_name = ? AND arm = ? ORDER BY event_id",
        (paper_id, field_name, arm)).fetchall()
    evs = [_Ev(r, _COLS) for r in rows]
    if not evs:
        return evs
    ids = tuple(e.event_id for e in evs)
    qs = ", ".join("?" * len(ids))
    by_id = {e.event_id: e for e in evs}
    for eid, cid in conn.execute(
            f"SELECT event_id, against_claim_id FROM field_event_against "
            f"WHERE event_id IN ({qs})", ids):
        by_id[eid].against_claims.add(cid)
    for eid, aid in conn.execute(
            f"SELECT event_id, against_event_id FROM field_event_against_decisions "
            f"WHERE event_id IN ({qs})", ids):
        by_id[eid].against_decisions.add(aid)
    return evs


def live_claims(conn, paper_id, field_name, arm) -> list[str]:
    """The claims on this cell in this arm that row 9 has not superseded."""
    return _live(_cell_events(conn, paper_id, field_name, arm))


def _live(evs) -> list[str]:
    order, seen = [], set()
    for e in evs:
        if e.event_type in CLAIM_EVENT_TYPES and e.claim_id not in seen:
            seen.add(e.claim_id)
            order.append(e.claim_id)
    superseded = set()
    for e in evs:
        if e.event_type == "superseded":
            superseded |= e.against_claims
    return [c for c in order if c not in superseded]


def _governing_reviewer(reviewer_events):
    """(governing_event, conflict) — v2.1 row 2's test, written once."""
    if not reviewer_events:
        return None, False
    if len(reviewer_events) == 1:
        return reviewer_events[0], False
    effects = {(e.event_type, e.value) for e in reviewer_events}
    if len(effects) == 1:
        return reviewer_events[-1], False
    for e in reversed(reviewer_events):
        others = {o.event_id for o in reviewer_events if o.event_id != e.event_id}
        if others and others <= e.against_decisions:
            return e, False
    return None, True


def _citations_for(evs, claim_id):
    return [e.payload for e in evs
            if e.event_type == "citation_located" and e.claim_id == claim_id]


def _arm_is_pre_manifest(conn, arm) -> bool:
    row = conn.execute(
        "SELECT configuration_marker FROM arms WHERE arm_name = ?", (arm,)).fetchone()
    return bool(row) and row[0] == PRE_MANIFEST


# ── the rule ──────────────────────────────────────────────────────────
def effective_value(conn, paper_id, field_name, arm, *, sentinels) -> EffectiveValue:
    """Resolution rule v2.1, first matching row wins."""
    if not is_assigned(conn, arm, paper_id):                       # row 0
        return EffectiveValue(None, OUT_OF_SCOPE, 0,
                              {"arm": arm, "assigned": False})

    evs = _cell_events(conn, paper_id, field_name, arm)
    if not evs:                                                    # row 1
        return EffectiveValue(None, MISSING, 1,
                              {"arm": arm, "reason": "assigned, no claim"})

    live = _live(evs)
    live_set = set(live)
    reviewer_events = [e for e in evs
                       if e.event_type in REVIEWER_EVENT_TYPES
                       and e.actor_role == "reviewer"]
    governing, conflict = _governing_reviewer(reviewer_events)

    if conflict:                                                   # row 2
        return EffectiveValue(
            None, UNRESOLVED_REREVIEW, 2,
            {"decisions": [{"event_id": e.event_id, "type": e.event_type,
                            "value": e.value, "by": e.actor_name}
                           for e in reviewer_events],
             "exit": "a reviewer event referencing all competing decisions (R20)"})

    if governing is not None:
        against = governing.against_claims
        if against - live_set:                                     # row 3
            newest = _newest_claim_event(evs, live)
            return EffectiveValue(
                None, UNRESOLVED_REREVIEW, 3,
                {"reviewer": {"event_id": governing.event_id,
                              "type": governing.event_type,
                              "against": sorted(against)},
                 "current": sorted(live),
                 "value_unchanged": bool(newest) and newest.value == governing.value,
                 "exit": "a new decision against the current claim"})
        if against == live_set:
            if governing.event_type == "human_withdrew":           # row 4
                return EffectiveValue(
                    None, WITHDRAWN, 4,
                    {"withdrawn_by": governing.actor_name, "at": governing.occurred_at,
                     "reason": governing.payload.get("reason"),
                     "original_values": [e.value for e in evs
                                         if e.event_type in CLAIM_EVENT_TYPES
                                         and e.claim_id in live_set],
                     "original_claim_ids": sorted(live_set)})
            if governing.event_type == "human_corrected":          # row 5
                return EffectiveValue(
                    governing.value, CORRECTED_BY_HUMAN, 5,
                    {"corrected_by": governing.actor_name, "at": governing.occurred_at,
                     "original_values": [e.value for e in evs
                                         if e.event_type in CLAIM_EVENT_TYPES
                                         and e.claim_id in live_set],
                     "original_claim_ids": sorted(live_set),
                     "presented_context_sha256":
                         governing.payload.get("presented_context_sha256")})

    #: rows 6 and 7 sit below the reviewer exits (R20): in v2 a CORRECT naming
    #: both duplicated values was shadowed here and never fired.
    has_full_reviewer = governing is not None and governing.against_claims == live_set

    if not has_full_reviewer:
        for cid in live:                                           # row 6
            vals = [e.value for e in evs
                    if e.event_type == "asserted" and e.claim_id == cid]
            if len(set(vals)) > 1:
                return EffectiveValue(
                    None, UNRESOLVED_DUPLICATE, 6,
                    {"candidates": sorted(set(vals)), "claim_id": cid,
                     "exit": "CORRECT or WITHDRAW naming every competing claim (R20); "
                             "ACCEPT is refused at write"})
        if len(live) > 1 and _arm_is_pre_manifest(conn, arm):      # row 7
            return EffectiveValue(
                None, UNRESOLVED_REREVIEW, 7,
                {"claims": sorted(live),
                 "reason": "configurations indistinguishable — pre-manifest",
                 "exit": "CORRECT or WITHDRAW naming every competing claim (R20); "
                         "ACCEPT is refused at write"})

    endorsed = None
    if (governing is not None and governing.event_type == "human_accepted"
            and governing.against_claims == live_set and len(live_set) == 1):
        endorsed = governing                                       # row 8

    newest = _newest_claim_event(evs, live)
    if newest is None:
        return EffectiveValue(None, MISSING, 1,
                              {"arm": arm, "reason": "assigned, no live claim"})

    superseded = sorted({c for e in evs if e.event_type == "superseded"
                         for c in e.against_claims})

    base = _classify_claim(evs, newest, sentinels)
    prov = dict(base.provenance)
    prov.update({"arm": arm, "claim_id": newest.claim_id,
                 "actor": newest.actor_name, "role": newest.actor_role})
    row = base.rule_row
    if superseded:                                                 # row 9
        prov["supersedes"] = superseded
        prov["superseded_because"] = "input identity changed"
    if endorsed is not None:
        prov["endorsed"] = {"accepted_by": endorsed.actor_name,
                            "at": endorsed.occurred_at,
                            "presented_context_sha256":
                                endorsed.payload.get("presented_context_sha256")}
        prov["rule_row_endorsement"] = 8
    return EffectiveValue(base.value, base.state, row, prov)


def _newest_claim_event(evs, live):
    live_set = set(live)
    for e in reversed(evs):
        if e.event_type in CLAIM_EVENT_TYPES and e.claim_id in live_set:
            return e
    return None


def _classify_claim(evs, ev, sentinels) -> EffectiveValue:
    """Rows 10–15 on one claim-bearing event."""
    if ev.event_type == "declined":                                # row 14
        return EffectiveValue(None, DECLINED, 14, {})
    if ev.event_type == "contract_unmet":                          # row 15
        return EffectiveValue(None, CONTRACT_UNMET, 15,
                              {"violation_codes": ev.payload.get("violation_codes"),
                               "attempts": ev.payload.get("attempts")})
    cites = _citations_for(evs, ev.claim_id)
    state = classify_field_state(ev.value, cites, ev.event_type, sentinels=sentinels)
    is_sentinel = ev.value in sentinels
    located = next((c for c in reversed(cites) if c.get("located")), None)
    if located is not None:                                        # rows 10 / 12
        return EffectiveValue(ev.value, state, 12 if is_sentinel else 10,
                              {"located": located})
    last = cites[-1] if cites else None                            # rows 11 / 13
    return EffectiveValue(
        ev.value, state, 13 if is_sentinel else 11,
        {"snippet_supplied": bool(ev.source_snippet), "located": last})


def effective_state(conn, paper_id) -> EffectiveState:
    """The paper's lifecycle on two axes, from `paper_events` and nothing else.

    **Not "the last row wins".** That is what made one axis impossible: a
    processing event would have erased the eligibility fact and vice versa. Each
    axis independently takes the last event whose `to_state` belongs to *that*
    axis's closed token set, the two sets being disjoint by construction
    (`engine/core/paper_state.py`).

    An axis with no event reads `no_recorded_state`, which is distinct from every
    token on it: "we have no record" is not "we recorded that nothing happened",
    and an export that cannot tell them apart will report the first as the
    second.

    `papers.status` is never read, not even into provenance.
    """
    rows = conn.execute(
        "SELECT event_id, event_type, to_state, from_state, actor_name, "
        "occurred_at, payload_json, reason_code, stage_name FROM paper_events "
        "WHERE paper_id = ? ORDER BY event_id", (paper_id,)).fetchall()

    eligibility, processing = NO_RECORDED_STATE, NO_RECORDED_STATE
    reason = None
    elig_prov: dict = {}
    proc_prov: dict = {}

    for (eid, etype, to_state, from_state, actor, at,
         payload_json, reason_code, stage_name) in rows:
        payload = json.loads(payload_json or "{}")
        prov = {"event_id": eid, "event_type": etype, "from_state": from_state,
                "by": actor, "at": at, "stage_name": stage_name}
        if etype == "state_at_migration":                          # row 17
            prov.update({"source": payload.get("source"), "migrated_at": at,
                         "note": payload.get("note"), "rule_row": 17})
        if to_state in ELIGIBILITY_STATES:
            eligibility, elig_prov = to_state, prov
        elif to_state in PROCESSING_STATES:
            processing, proc_prov = to_state, prov
            reason = reason_code if is_failure(to_state) else None
        else:  # pragma: no cover - the CHECK in 019 makes this unreachable
            raise ValueError(
                f"paper_events.event_id={eid} carries to_state={to_state!r}, "
                f"which is on neither axis. The 019 CHECK should have refused "
                f"it; the vocabulary and the database have diverged."
            )

    return EffectiveState(eligibility, processing, reason, elig_prov, proc_prov)


# ── the registry, and the grid every migrated reader enumerates ───────
def registered_arms(conn, *, kind: str | None = None,
                    include_retired: bool = False) -> tuple[str, ...]:
    """Every arm in the review's registry, in `arm_name` order.

    R12: arms are data. This is the single routing predicate A12 asks for — the
    thing `concordance.load_arm`'s `if arm == "local"` and
    `distribution_monitor._query_values`'s `arm.startswith("human_")` were each
    a private, divergent copy of. Ordered so a report built from it is
    reproducible.

    A retired arm is excluded by default and its claims still resolve through
    `effective_value` (R21: "it accepts no new claims; its claims and decisions
    stand").
    """
    sql = "SELECT arm_name FROM arms WHERE 1=1"
    params: list = []
    if kind is not None:
        sql += " AND arm_kind = ?"
        params.append(kind)
    if not include_retired:
        sql += " AND retired_at IS NULL"
    sql += " ORDER BY arm_name"
    return tuple(r[0] for r in conn.execute(sql, params))


def eligible_paper_ids(conn) -> tuple[int, ...]:
    """The corpus: every paper whose ELIGIBILITY axis reads `eligible` (S3h/A9).

    This replaces `engine.core.corpus.corpus_status_sql`, which returned SQL over
    a `papers.status` column the event store is replacing. Processing outcome is
    deliberately not consulted: a paper whose extraction failed is in the corpus
    and is reported by its reason, which is what A9 said the status allowlist got
    wrong by excluding `EXTRACT_FAILED`.
    """
    placeholders = ", ".join("?" * len(ELIGIBILITY_STATES))
    rows = conn.execute(
        f"""SELECT paper_id, to_state FROM paper_events
            WHERE to_state IN ({placeholders})
              AND event_id = (
                  SELECT MAX(e2.event_id) FROM paper_events e2
                  WHERE e2.paper_id = paper_events.paper_id
                    AND e2.to_state IN ({placeholders}))
            ORDER BY paper_id""",
        ELIGIBILITY_STATES + ELIGIBILITY_STATES,
    ).fetchall()
    return tuple(pid for pid, state in rows if state == "eligible")


def corpus_id_sql(conn, column: str = "paper_id") -> tuple[str, tuple[int, ...]]:
    """An ``<column> IN (?, ?, ...)`` fragment over the corpus, and its params.

    The shape `engine.core.corpus.corpus_status_sql` had, so a call site swaps
    one import for another and nothing else moves. What changed underneath is
    the question being asked: that function spliced a `papers.status` allowlist,
    and this one names the papers whose ELIGIBILITY axis reads `eligible` — so a
    paper whose extraction failed is included, which is A9.

    The id list is materialised rather than expressed as a sub-select because the
    axis derivation is Python, not SQL. That bounds the caller by SQLite's
    variable limit; at 190 corpus papers against a limit of 32,766 there is room,
    and a review large enough to matter should be joining against a temp table,
    which is a change to make when a review needs it rather than now.
    """
    ids = eligible_paper_ids(conn)
    placeholders = ", ".join("?" * len(ids)) if ids else "NULL"
    return f"{column} IN ({placeholders})", ids


def grid_cells(conn, *, codebook, papers=None, arms=None) -> tuple[tuple, ...]:
    """Every (paper_id, field_name, arm) the review has, as a closed set.

    The universe R28 requires: paper set x codebook fields x registered arms,
    enumerated once rather than rebuilt from a different source by each consumer
    (a table scan in concordance, `_STATUS_ORDER` in the exporter, a CSV in the
    judge loader, a per-field query in the distribution monitor).

    `papers` defaults to the corpus, `arms` to the registry, fields always to the
    codebook — which is their sole source (R26).
    """
    paper_ids = tuple(papers) if papers is not None else eligible_paper_ids(conn)
    arm_names = tuple(arms) if arms is not None else registered_arms(conn)
    fields = codebook.field_names
    return tuple((p, f, a) for p in paper_ids for f in fields for a in arm_names)


def iter_grid(conn, *, codebook, sentinels=None, papers=None, arms=None):
    """`(paper_id, field_name, arm, EffectiveValue)` for every cell in the grid.

    Yields the `missing` cells too — the scorer's verdict is a FEATURE of a cell,
    never the thing that decides whether the cell exists (R28). That distinction
    is B3: the judge's universe was the scorer's disagreement set, so 1,535 of
    3,802 cells were never judged, one-directionally.

    No batch form. Measured 2026-09-22 on the live database: 11,400 cells at
    about 5 microseconds a call, ~0.1 s for the whole grid — an optimisation
    without a measured problem. The measurement was taken on an EMPTY field-event
    store, where every cell resolves at rule row 1, the cheapest path, so it is
    an upper bound on speed and must be re-measured after the freshman smoke run.
    """
    if sentinels is None:
        sentinels = frozenset(codebook.absence_sentinels)
    for paper_id, field_name, arm in grid_cells(
            conn, codebook=codebook, papers=papers, arms=arms):
        yield paper_id, field_name, arm, effective_value(
            conn, paper_id, field_name, arm, sentinels=sentinels)
