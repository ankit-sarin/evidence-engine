"""Resolution rule v2.1 (R23), one constructed history per row, plus D1-1…D1-4.

EFFECTIVE-RESULT-02. Every fixture here is a history **built through the
writer**, never found in live data — the revised session-5 gate (addendum 4 §B)
requires exactly that, and it is what makes rows with zero live population
testable at all.

Test names carry the row number or the reproducer id, so a failure names the
rule row it broke.
"""

from __future__ import annotations

import importlib
import json
import sqlite3

import pytest

from engine.core import events
from engine.core.effective import (
    ASSERTED_WITHOUT_EVIDENCE, ASSERTED_WITH_EVIDENCE, CONTRACT_UNMET,
    CORRECTED_BY_HUMAN, DECLINED, MISSING, NO_RECORDED_STATE, OUT_OF_SCOPE,
    PRE_MANIFEST, UNRESOLVED_DUPLICATE, UNRESOLVED_REREVIEW, WITHDRAWN,
    effective_state, effective_value, live_claims,
)

_016 = importlib.import_module("engine.migrations.016_event_store")

#: The six the codebook declares. Passed in, never imported into the reader:
#: the codebook is the only source, and a copy inside the reader would be the
#: divergence the argument exists to prevent.
SENTINELS = frozenset({"NR", "N/A", "NA", "NOT_FOUND", "NOT FOUND", "NOT REPORTED"})

FIELD = "primary_outcome_value"


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE papers (id INTEGER PRIMARY KEY, status TEXT)")
    conn.executemany("INSERT INTO papers (id, status) VALUES (?, 'AI_AUDIT_COMPLETE')",
                     [(i,) for i in range(1, 11)])
    _016.create_schema(conn)
    events.register_arm(conn, "local", "model",
                        configuration={"model": "deepseek-r1:32b", "temperature": 0})
    events.register_arm(conn, "cloud", "model", configuration={"model": "o4-mini"})
    events.register_arm(conn, "premanifest_a", "model",
                        configuration_marker=PRE_MANIFEST)
    events.register_arm(conn, "human_A", "human_extractor",
                        configuration={"extractor": "A"})
    conn.commit()
    yield conn
    conn.close()


# ── helpers ───────────────────────────────────────────────────────────
def assert_claim(db, paper, value, *, arm="local", claim_id=None, uid=None,
                 snippet="a quote", etype="asserted"):
    return events.write_field_event(
        db, event_type=etype, paper_id=paper, field_name=FIELD, arm=arm,
        claim_id=claim_id, extraction_uid=uid, value=value, source_snippet=snippet,
        actor_kind="model", actor_role="extractor", actor_name="deepseek-r1:32b",
        sentinels=SENTINELS)


def locate(db, paper, claim_id, located, *, arm="local"):
    return events.write_field_event(
        db, event_type="citation_located", paper_id=paper, field_name=FIELD, arm=arm,
        claim_id=claim_id, actor_kind="engine", actor_role="system",
        actor_name="locator", payload={"located": located, "parsed_text_id": "pt-1",
                                       "threshold": 0.85, "locator_version": "0"},
        sentinels=SENTINELS)


def supersede(db, paper, superseded_claim_ids, *, arm="local"):
    return events.write_field_event(
        db, event_type="superseded", paper_id=paper, field_name=FIELD, arm=arm,
        claim_id=sorted(superseded_claim_ids)[0], actor_kind="engine",
        actor_role="system", actor_name="write-path",
        against_claims=superseded_claim_ids, sentinels=SENTINELS)


def review(db, paper, etype, *, against=(), against_decisions=(), value=None,
           arm="local", who="PI"):
    return events.write_field_event(
        db, event_type=etype, paper_id=paper, field_name=FIELD, arm=arm,
        claim_id=sorted(against)[0] if against else "n/a", value=value,
        actor_kind="human", actor_role="reviewer", actor_name=who,
        against_claims=against, against_decisions=against_decisions,
        presented_context_sha256="ctx-" + etype, sentinels=SENTINELS)


def read(db, paper, *, arm="local"):
    return effective_value(db, paper, FIELD, arm, sentinels=SENTINELS)


def one_claim(db, paper, value, **kw):
    uid = events.mint_extraction_uid()
    cid = events.make_claim_id(kw.get("arm", "local"), uid, FIELD)
    assert_claim(db, paper, value, claim_id=cid, **kw)
    return cid


# ── rows 0 and 1 ──────────────────────────────────────────────────────
def test_row0_unassigned_cell_is_out_of_scope_not_a_state(db):
    r = read(db, 1, arm="human_A")
    assert r.state == OUT_OF_SCOPE and r.rule_row == 0
    assert r.provenance["assigned"] is False and r.value is None


def test_row0_a_model_arm_defaults_to_the_full_corpus_so_row0_never_fires(db):
    assert read(db, 7).rule_row == 1  # missing, not out of scope


def test_row1_assigned_with_no_claim_is_missing(db):
    r = read(db, 1)
    assert r.state == MISSING and r.rule_row == 1
    assert r.provenance["reason"] == "assigned, no claim"


# ── rows 2 and 3 — the reviewer conflicts ─────────────────────────────
def test_row2_conflicting_reviewer_decisions_are_unresolved_and_newest_does_not_win(db):
    c = one_claim(db, 1, "5")
    review(db, 1, "human_corrected", against={c}, value="6", who="PI-a")
    review(db, 1, "human_withdrew", against={c}, who="PI-b")
    r = read(db, 1)
    assert r.state == UNRESOLVED_REREVIEW and r.rule_row == 2
    assert r.value is None
    assert len(r.provenance["decisions"]) == 2
    assert "R20" in r.provenance["exit"]


def test_row2_exit_a_decision_referencing_all_competing_decisions(db):
    c = one_claim(db, 1, "5")
    a = review(db, 1, "human_corrected", against={c}, value="6", who="PI-a")
    b = review(db, 1, "human_withdrew", against={c}, who="PI-b")
    review(db, 1, "human_corrected", against={c}, against_decisions={a, b},
           value="7", who="PI-c")
    r = read(db, 1)
    assert r.state == CORRECTED_BY_HUMAN and r.value == "7"


def test_row3_reviewer_decision_against_a_superseded_claim_is_unresolved(db):
    c1 = one_claim(db, 1, "5")
    c2 = one_claim(db, 1, "6")
    supersede(db, 1, {c1})
    review(db, 1, "human_corrected", against={c1}, value="9")
    r = read(db, 1)
    assert r.state == UNRESOLVED_REREVIEW and r.rule_row == 3
    assert r.provenance["current"] == [c2]
    assert r.provenance["value_unchanged"] is False


# ── rows 4, 5, 8 — the reviewer exits ─────────────────────────────────
def test_row4_withdrawn_is_no_value_and_the_original_lives_in_history(db):
    c = one_claim(db, 1, "5")
    review(db, 1, "human_withdrew", against={c})
    r = read(db, 1)
    assert r.state == WITHDRAWN and r.rule_row == 4 and r.value is None
    assert r.provenance["original_values"] == ["5"]
    assert r.provenance["original_claim_ids"] == [c]


def test_row5_corrected_by_human_returns_the_reviewers_value(db):
    c = one_claim(db, 1, "5")
    review(db, 1, "human_corrected", against={c}, value="42")
    r = read(db, 1)
    assert r.state == CORRECTED_BY_HUMAN and r.rule_row == 5 and r.value == "42"
    assert r.provenance["original_values"] == ["5"]
    assert r.provenance["presented_context_sha256"] == "ctx-human_corrected"


def test_row8_accepted_keeps_the_extractor_state_and_adds_endorsement(db):
    c = one_claim(db, 1, "5")
    locate(db, 1, c, True)
    review(db, 1, "human_accepted", against={c})
    r = read(db, 1)
    assert r.state == ASSERTED_WITH_EVIDENCE and r.value == "5"
    assert r.provenance["rule_row_endorsement"] == 8
    assert r.provenance["endorsed"]["accepted_by"] == "PI"


# ── rows 6 and 7 — the two "more than one claim" rows ─────────────────
def test_row6_duplicate_values_within_one_claim_are_unresolved(db):
    uid = events.mint_extraction_uid()
    cid = events.make_claim_id("local", uid, FIELD)
    assert_claim(db, 1, "5", claim_id=cid)
    assert_claim(db, 1, "50", claim_id=cid)      # same claim_id, differing values
    r = read(db, 1)
    assert r.state == UNRESOLVED_DUPLICATE and r.rule_row == 6
    assert sorted(r.provenance["candidates"]) == ["5", "50"]


def test_row6_exit_correct_naming_every_competing_claim(db):
    uid = events.mint_extraction_uid()
    cid = events.make_claim_id("local", uid, FIELD)
    assert_claim(db, 1, "5", claim_id=cid)
    assert_claim(db, 1, "50", claim_id=cid)
    review(db, 1, "human_corrected", against={cid}, value="5")
    r = read(db, 1)
    assert r.state == CORRECTED_BY_HUMAN and r.value == "5"


def test_row7_indistinguishable_pre_manifest_claims_are_unresolved(db):
    one_claim(db, 1, "5", arm="premanifest_a")
    one_claim(db, 1, "6", arm="premanifest_a")
    r = read(db, 1, arm="premanifest_a")
    assert r.state == UNRESOLVED_REREVIEW and r.rule_row == 7
    assert "pre-manifest" in r.provenance["reason"]
    assert len(r.provenance["claims"]) == 2


def test_row7_exit_withdraw_naming_every_competing_claim(db):
    a = one_claim(db, 1, "5", arm="premanifest_a")
    b = one_claim(db, 1, "6", arm="premanifest_a")
    review(db, 1, "human_withdrew", against={a, b}, arm="premanifest_a")
    r = read(db, 1, arm="premanifest_a")
    assert r.state == WITHDRAWN and r.rule_row == 4


# ── row 9 — supersession within one arm ───────────────────────────────
def test_row9_superseded_within_one_arm_returns_the_newest_claim(db):
    c1 = one_claim(db, 1, "5")
    c2 = one_claim(db, 1, "6")
    supersede(db, 1, {c1})
    r = read(db, 1)
    assert r.value == "6"
    assert r.provenance["supersedes"] == [c1]
    assert r.provenance["superseded_because"] == "input identity changed"
    assert live_claims(db, 1, FIELD, "local") == [c2]


# ── rows 10–13 — the citation rows, and R22/U6 ────────────────────────
def test_row10_value_with_a_located_citation_is_asserted_with_evidence(db):
    c = one_claim(db, 1, "12.5")
    locate(db, 1, c, True)
    r = read(db, 1)
    assert r.state == ASSERTED_WITH_EVIDENCE and r.rule_row == 10
    assert r.provenance["located"]["locator_version"] == "0"


def test_row11_value_with_no_located_event_is_asserted_without_evidence(db):
    one_claim(db, 1, "12.5")
    r = read(db, 1)
    assert r.state == ASSERTED_WITHOUT_EVIDENCE and r.rule_row == 11
    assert r.provenance["located"] is None
    assert r.provenance["snippet_supplied"] is True


def test_row11_u6_located_false_is_the_same_state_with_a_different_provenance(db):
    c = one_claim(db, 2, "12.5")
    locate(db, 2, c, False)
    r = read(db, 2)
    assert r.state == ASSERTED_WITHOUT_EVIDENCE and r.rule_row == 11
    assert r.provenance["located"]["located"] is False   # R22/U6: two provenances


def test_row12_sentinel_with_a_located_citation_takes_the_identical_test(db):
    c = one_claim(db, 1, "NR")
    locate(db, 1, c, True)
    r = read(db, 1)
    assert r.state == ASSERTED_WITH_EVIDENCE and r.rule_row == 12
    assert r.value == "NR"          # a sentinel IS a value


def test_row13_sentinel_with_no_located_event(db):
    one_claim(db, 1, "NOT REPORTED")
    r = read(db, 1)
    assert r.state == ASSERTED_WITHOUT_EVIDENCE and r.rule_row == 13


def test_v21_no_comparison_reported_is_an_ordinary_value_not_a_sentinel(db):
    one_claim(db, 1, "No comparison reported")
    assert read(db, 1).rule_row == 11       # rows 10/11, never 12/13 (R22)


# ── rows 14 and 15 ────────────────────────────────────────────────────
def test_row14_declined_is_a_state_never_a_skip(db):
    one_claim(db, 1, None, etype="declined", snippet=None)
    r = read(db, 1)
    assert r.state == DECLINED and r.rule_row == 14 and r.value is None


def test_row15_contract_unmet_is_the_engines_failure(db):
    uid = events.mint_extraction_uid()
    cid = events.make_claim_id("local", uid, FIELD)
    events.write_field_event(
        db, event_type="contract_unmet", paper_id=1, field_name=FIELD, arm="local",
        claim_id=cid, actor_kind="engine", actor_role="system", actor_name="pipeline",
        payload={"violation_codes": ["INDEX_MALFORMED"], "attempts": 2},
        sentinels=SENTINELS)
    r = read(db, 1)
    assert r.state == CONTRACT_UNMET and r.rule_row == 15
    assert r.provenance["violation_codes"] == ["INDEX_MALFORMED"]


# ── rows 16 and 17 ────────────────────────────────────────────────────
def test_row16_arms_are_compared_never_merged_the_reader_is_per_arm(db):
    one_claim(db, 1, "5", arm="local")
    one_claim(db, 1, "6", arm="cloud")
    assert read(db, 1, arm="local").value == "5"
    assert read(db, 1, arm="cloud").value == "6"
    # there is no call that merges them: arm is required, so a cross-arm
    # judgment is the caller's act and not an event shape (R22/U3)
    with pytest.raises(TypeError):
        effective_value(db, 1, FIELD, sentinels=SENTINELS)


def test_row17_state_at_migration_is_effective_states_row_not_a_fields(db):
    events.write_paper_event(
        db, event_type="state_at_migration", paper_id=3, to_state="eligible",
        actor_kind="engine", actor_role="system", actor_name="017_seed_event_store",
        payload={"source": "state at migration",
                 "note": "history not reconstructable from the record"})
    s = effective_state(db, 3)
    assert s.state == "eligible"
    assert s.provenance["rule_row"] == 17
    assert s.provenance["from_state"] is None
    assert s.provenance["source"] == "state at migration"


def test_q2_a_paper_with_no_paper_events_has_no_recorded_state(db):
    s = effective_state(db, 9)
    assert s.state == NO_RECORDED_STATE and s.provenance == {}


def test_q2_the_reader_never_reads_papers_status(db):
    """A paper whose status says AI_AUDIT_COMPLETE still has no recorded state."""
    assert db.execute("SELECT status FROM papers WHERE id = 9").fetchone()[0] \
        == "AI_AUDIT_COMPLETE"
    assert effective_state(db, 9).state == NO_RECORDED_STATE


# ── D1-1 … D1-4, as constructed event histories ───────────────────────
def test_d1_1_reject_means_withdraw_so_no_route_can_export_it_as_verified(db):
    """Two importers, two meanings of REJECT (A3). Under R1 there is one."""
    c = one_claim(db, 1, "5")
    review(db, 1, "human_withdrew", against={c})
    r = read(db, 1)
    assert r.value is None and r.state == WITHDRAWN
    assert "5" in r.provenance["original_values"]   # kept in history, not in the value


def test_d1_2_a_stale_decision_cannot_silently_overwrite_a_newer_run(db):
    """A4: the workbook bound to the extraction it was NOT built from."""
    c1 = one_claim(db, 1, "5")
    one_claim(db, 1, "6")
    supersede(db, 1, {c1})
    review(db, 1, "human_corrected", against={c1}, value="99")
    r = read(db, 1)
    assert r.rule_row == 3 and r.state == UNRESOLVED_REREVIEW
    assert r.value is None                       # neither value silently wins


def test_d1_3_an_auditor_verdict_is_provenance_never_a_field_state(db):
    """A5: flagged and contested count as work owed, so a stage is not complete."""
    uid = events.mint_extraction_uid()
    cid = events.make_claim_id("local", uid, FIELD)
    events.write_field_event(
        db, event_type="asserted", paper_id=1, field_name=FIELD, arm="local",
        claim_id=cid, value="5", source_snippet="q", actor_kind="model",
        actor_role="extractor", actor_name="deepseek-r1:32b",
        payload={"auditor_verdict": "flagged"}, sentinels=SENTINELS)
    r = read(db, 1)
    assert r.state == ASSERTED_WITHOUT_EVIDENCE      # R18/Q7: not a field state
    assert "endorsed" not in r.provenance            # so the work is still owed


def test_d1_4_a_newer_abstention_is_not_dropped_by_a_non_value_guard(db):
    """A7: load_arm folded every extraction and the older claim stood."""
    c1 = one_claim(db, 1, "5")
    one_claim(db, 1, None, etype="declined", snippet=None)
    supersede(db, 1, {c1})
    r = read(db, 1)
    assert r.state == DECLINED and r.value is None   # the abstention wins, as it must


# ── the writer's telemetry against the reader's derivation (Q6) ───────
@pytest.mark.parametrize("value,etype,expected", [
    ("12.5", "asserted", ASSERTED_WITHOUT_EVIDENCE),
    ("NR", "asserted", ASSERTED_WITHOUT_EVIDENCE),
    (None, "declined", DECLINED),
])
def test_rule_v21_reader_state_equals_state_at_write_when_no_citation_follows(
        db, value, etype, expected):
    one_claim(db, 1, value, etype=etype, snippet="q" if value else None)
    row = db.execute("SELECT payload_json FROM field_events ORDER BY event_id DESC "
                     "LIMIT 1").fetchone()
    payload = json.loads(row[0])
    assert payload["rule_version"] == "v2.1"
    assert read(db, 1).state == payload["state_at_write"] == expected


def test_rule_v21_a_later_located_event_moves_the_state_off_state_at_write(db):
    """Not a divergence: the writer's verdict is true at write time, and a
    citation_located arriving afterwards is new evidence, not a disagreement."""
    c = one_claim(db, 1, "12.5")
    written = json.loads(db.execute(
        "SELECT payload_json FROM field_events WHERE claim_id = ? AND "
        "event_type = 'asserted'", (c,)).fetchone()[0])["state_at_write"]
    assert written == ASSERTED_WITHOUT_EVIDENCE
    locate(db, 1, c, True)
    assert read(db, 1).state == ASSERTED_WITH_EVIDENCE
