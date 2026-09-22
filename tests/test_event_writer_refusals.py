"""Write-time refusals: R20, R21, R22/U2 and R24, plus the actor_role CHECKs.

EFFECTIVE-RESULT-02. v2.1's "unresolved" rows each name an exit, and an exit
that cannot be represented is not an exit — so the refusals that keep the store
free of decisions that resolve nothing are enforced before the row is written,
not discovered when someone reads it back.
"""

from __future__ import annotations

import importlib
import sqlite3

import pytest

from engine.core import events
from engine.core.effective import PRE_MANIFEST, effective_value

from tests._event_store_fixture import (
    fixture_run, seed_claim, seed_pre_manifest_paper_event, upgrade_event_store,
)
SENTINELS = frozenset({"NR", "NOT_FOUND"})
FIELD = "primary_outcome_value"


@pytest.fixture
def db(tmp_path):
    # MANIFEST-01 Phase 2a: a file at the post-020 shape, events under a run (R68).
    path = tmp_path / "writer.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE papers (id INTEGER PRIMARY KEY, status TEXT)")
    conn.executemany("INSERT INTO papers (id, status) VALUES (?, 'FT_ELIGIBLE')",
                     [(i,) for i in range(1, 5)])
    conn.commit()
    conn.close()
    upgrade_event_store(path)
    conn = sqlite3.connect(path)
    events.register_arm(conn, "local", "model", configuration={"model": "m"})
    events.register_arm(conn, "human_A", "human_extractor", configuration={"x": 1})
    fixture_run(conn, "local")
    conn.commit()
    yield conn
    conn.close()


def _run(db):
    return fixture_run(db)


def claim(db, paper, value, *, arm="local"):
    uid = events.mint_extraction_uid()
    cid = events.make_claim_id(arm, uid, FIELD)
    events.write_field_event(
        db, event_type="asserted", paper_id=paper, field_name=FIELD, arm=arm,
        claim_id=cid, value=value, source_snippet="q", actor_kind="model",
        actor_role="extractor", actor_name="m", sentinels=SENTINELS, run_id=_run(db))
    return cid


def reviewer(db, paper, etype, *, against=(), against_decisions=(), **kw):
    kw.setdefault("run_id", _run(db))
    return events.write_field_event(
        db, event_type=etype, paper_id=paper, field_name=FIELD,
        arm=kw.pop("arm", "local"), claim_id="c", actor_kind="human",
        actor_role="reviewer", actor_name="PI", sentinels=SENTINELS,
        against_claims=against, against_decisions=against_decisions, **kw)


# ── R20 — a decision that names nothing cannot name its exit ──────────
def test_r20_refuses_a_reviewer_decision_with_an_empty_against_reference(db):
    claim(db, 1, "5")
    with pytest.raises(events.ReviewerDecisionAmbiguous) as e:
        reviewer(db, 1, "human_corrected", value="6")
    assert "names no claim" in str(e.value) and "R20" in str(e.value)
    assert db.execute("SELECT COUNT(*) FROM field_events WHERE actor_role='reviewer'"
                      ).fetchone()[0] == 0


# ── R20 — ACCEPT cannot say which of several values it endorses ───────
def test_r20_refuses_accept_when_more_than_one_claim_is_live(db):
    a, b = claim(db, 1, "5"), claim(db, 1, "6")
    with pytest.raises(events.AcceptAgainstMultipleClaims) as e:
        reviewer(db, 1, "human_accepted", against={a, b})
    assert "cannot say which value it endorses" in str(e.value)
    assert effective_value(db, 1, FIELD, "local", sentinels=SENTINELS).rule_row != 8


def test_r20_accept_is_allowed_when_exactly_one_claim_is_live(db):
    a = claim(db, 1, "5")
    reviewer(db, 1, "human_accepted", against={a})
    assert effective_value(db, 1, FIELD, "local",
                           sentinels=SENTINELS).provenance["rule_row_endorsement"] == 8


# ── R24 — a proper-subset against-reference is refused, naming what it omits ──
def test_r24_refuses_a_proper_subset_against_reference_and_names_the_omission(db):
    a, b = claim(db, 1, "5"), claim(db, 1, "6")
    with pytest.raises(events.AgainstReferenceIncomplete) as e:
        reviewer(db, 1, "human_corrected", against={a}, value="9")
    msg = str(e.value)
    assert "R24" in msg and b in msg and "proper subset" in msg


def test_r24_set_equality_is_accepted_it_is_r20s_exit(db):
    a, b = claim(db, 1, "5"), claim(db, 1, "6")
    reviewer(db, 1, "human_withdrew", against={a, b})
    assert effective_value(db, 1, FIELD, "local", sentinels=SENTINELS).rule_row == 4


def test_r24_naming_a_claim_outside_live_is_row_3_not_a_refusal(db):
    """Row 3 is a real situation with an exit, not an error at write time."""
    a = claim(db, 1, "5")
    claim(db, 1, "6")
    events.write_field_event(
        db, event_type="superseded", paper_id=1, field_name=FIELD, arm="local",
        claim_id=a, actor_kind="engine", actor_role="system", actor_name="w",
        against_claims={a}, sentinels=SENTINELS, run_id=_run(db))
    reviewer(db, 1, "human_corrected", against={a}, value="9")   # accepted
    assert effective_value(db, 1, FIELD, "local", sentinels=SENTINELS).rule_row == 3


# ── R22/U2 — a decision on a cell outside the arm's assignment ────────
def test_r22_u2_refuses_a_reviewer_decision_on_an_unassigned_cell(db):
    with pytest.raises(events.CellNotAssigned) as e:
        reviewer(db, 1, "human_corrected", against={"human_A:x:f"}, value="9",
                 arm="human_A")
    assert "R22/U2" in str(e.value) and "session 12" in str(e.value)


# ── R21 — an arm holding a claim cannot be re-pinned ──────────────────
def test_r21_refuses_a_configuration_repin_once_the_arm_holds_a_claim(db):
    claim(db, 1, "5")
    with pytest.raises(sqlite3.IntegrityError) as e:
        db.execute("UPDATE arms SET configuration_json = '{\"model\":\"other\"}' "
                   "WHERE arm_name = 'local'")
    assert "R21" in str(e.value) and "new arm" in str(e.value)


def test_r21_a_configuration_repin_is_allowed_before_any_claim(db):
    db.execute("UPDATE arms SET configuration_json = '{\"model\":\"other\"}' "
               "WHERE arm_name = 'local'")
    db.commit()
    assert db.execute("SELECT configuration_json FROM arms WHERE arm_name='local'"
                      ).fetchone()[0] == '{"model":"other"}'


def test_r21_retirement_stays_possible_on_an_arm_that_holds_claims(db):
    claim(db, 1, "5")
    events.retire_arm(db, "local")
    db.commit()
    assert db.execute("SELECT retired_at FROM arms WHERE arm_name='local'"
                      ).fetchone()[0] is not None


def test_arm_name_is_immutable_because_claim_ids_embed_it(db):
    with pytest.raises(sqlite3.IntegrityError) as e:
        db.execute("UPDATE arms SET arm_name = 'renamed' WHERE arm_name = 'local'")
    assert "orphans every claim" in str(e.value)


# ── actor_role, and the 'system' extension of R11 ─────────────────────
def test_actor_role_system_is_refused_unless_actor_kind_is_engine(db):
    for kind in ("human", "model"):
        with pytest.raises(sqlite3.IntegrityError):
            events.write_field_event(
                db, event_type="asserted", paper_id=1, field_name=FIELD, arm="local",
                value="5", actor_kind=kind, actor_role="system", actor_name="x",
                sentinels=SENTINELS, run_id=_run(db))


def test_a_system_event_never_triggers_an_override_row(db):
    """R11 is extended, not reversed: override rows fire only on 'reviewer'."""
    c = claim(db, 1, "5")
    events.write_field_event(
        db, event_type="human_withdrew", paper_id=1, field_name=FIELD, arm="local",
        claim_id=c, actor_kind="engine", actor_role="system", actor_name="engine",
        against_claims={c}, sentinels=SENTINELS, run_id=_run(db))
    r = effective_value(db, 1, FIELD, "local", sentinels=SENTINELS)
    assert r.rule_row == 11 and r.value == "5"     # not row 4


def test_an_unregistered_arm_is_not_a_claim_id(db):
    with pytest.raises(events.UnknownArm):
        effective_value(db, 1, FIELD, "nosucharm", sentinels=SENTINELS)


# ── the append-only triggers ──────────────────────────────────────────
@pytest.mark.parametrize("table", [
    "field_events", "paper_events", "field_event_against",
    "field_event_against_decisions", "parsed_text_refs", "review_identities"])
def test_every_event_table_is_append_only_by_trigger(db, table):
    c = claim(db, 1, "5")
    d = reviewer(db, 1, "human_withdrew", against={c})
    db.execute("INSERT INTO field_event_against_decisions VALUES (?, ?)", (d, d))
    db.commit()
    db.execute("INSERT INTO parsed_text_refs (parsed_text_uid, paper_id, "
               "parsed_text_path, parsed_text_version, recorded_at) "
               "VALUES ('u', 1, 'p.md', 1, 'now')")
    db.execute("INSERT INTO review_identities VALUES ('k', 'v', '{}', 'now')")
    seed_pre_manifest_paper_event(db, 1)
    for stmt in (f"DELETE FROM {table}", f"UPDATE {table} SET rowid = rowid"):
        with pytest.raises(sqlite3.IntegrityError) as e:
            db.execute(stmt)
        assert "append-only" in str(e.value)


# ── one predicate, two programs — shared, never copied ────────────────
def test_the_writer_and_the_reader_share_one_classifier_by_construction(db, monkeypatch):
    """Change the owner's rule; the consumer's answer follows.

    `classify_field_state` lives in the reader and session 9's write path calls
    it rather than reimplementing it. Asserting equivalent behaviour would pass
    just as well against a copy, so this changes the owner and watches the
    consumer move.
    """
    import json

    from engine.core import effective
    monkeypatch.setattr(effective, "classify_field_state",
                        lambda *a, **k: "SENTINEL-FROM-THE-OWNER")
    monkeypatch.setattr(events, "classify_field_state",
                        effective.classify_field_state)
    claim(db, 1, "5")
    written = json.loads(db.execute(
        "SELECT payload_json FROM field_events ORDER BY event_id DESC LIMIT 1"
    ).fetchone()[0])
    assert written["state_at_write"] == "SENTINEL-FROM-THE-OWNER"


def test_the_pre_manifest_marker_is_one_object_not_three_spellings():
    """v2.1 row 7 turns on string equality between what 017 writes and what the
    reader compares, so the marker is imported, never re-typed."""
    import importlib

    from engine.core import effective
    m016 = importlib.import_module("engine.migrations.016_event_store")
    m017 = importlib.import_module("engine.migrations.017_seed_event_store")
    assert m016.PRE_MANIFEST is effective.PRE_MANIFEST
    assert m017.PRE_MANIFEST is effective.PRE_MANIFEST
    assert events.PRE_MANIFEST is effective.PRE_MANIFEST


def test_the_writer_uses_the_readers_live_predicate_not_its_own_query(db, monkeypatch):
    """R24's refusal must see exactly what the rule sees."""
    seen = {}

    from engine.core import effective
    real = effective.live_claims

    def spy(conn, paper_id, field_name, arm):
        seen["called"] = (paper_id, field_name, arm)
        return real(conn, paper_id, field_name, arm)

    monkeypatch.setattr(events, "live_claims", spy)
    a, b = claim(db, 1, "5"), claim(db, 1, "6")
    with pytest.raises(events.AgainstReferenceIncomplete):
        reviewer(db, 1, "human_corrected", against={a}, value="9")
    assert seen["called"] == (1, FIELD, "local")



# ── R68 — every event carries a run; only a migration writes 'pre-manifest' ──
def test_r68_an_event_without_a_run_id_is_refused(db):
    with pytest.raises(events.RunLinkRefused, match="run_id is required"):
        events.write_field_event(
            db, event_type="asserted", paper_id=1, field_name=FIELD, arm="local",
            value="5", source_snippet="q", actor_kind="model", actor_role="extractor",
            actor_name="m", sentinels=SENTINELS)
    with pytest.raises(events.RunLinkRefused, match="run_id is required"):
        events.write_paper_event(db, event_type="extracted", paper_id=1,
                                 to_state="extracted", actor_kind="engine",
                                 actor_role="system", actor_name="m")


def test_r68_run_marker_from_a_caller_that_is_not_a_migration_is_refused(db):
    with pytest.raises(events.RunLinkRefused, match="only by a migration"):
        events.write_paper_event(db, event_type="state_at_migration", paper_id=1,
                                 to_state="eligible", actor_kind="engine",
                                 actor_role="system", actor_name="s",
                                 run_marker="pre-manifest")


def test_r68_a_run_id_that_names_no_manifest_is_refused(db):
    with pytest.raises(events.RunLinkRefused, match="names no run manifest"):
        events.write_paper_event(db, event_type="extracted", paper_id=1,
                                 to_state="extracted", actor_kind="engine",
                                 actor_role="system", actor_name="m", run_id=999)


def test_r68_the_check_refuses_a_runless_row_that_is_not_pre_manifest(db):
    """The CHECK holds below the writer too — the database, not the code, is
    where the invariant lives after 020."""
    with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
        db.execute(
            "INSERT INTO paper_events (event_uid, event_type, occurred_at, recorded_at, "
            "actor_kind, actor_role, actor_name, run_id, run_marker, paper_id, to_state) "
            "VALUES ('u', 'extracted', 'n', 'n', 'engine', 'system', 'x', NULL, NULL, 1, "
            "'extracted')")


def test_r68_migration_017_may_still_write_its_pre_manifest_seed(db, tmp_path):
    """017 is checksummed and passes run_marker='pre-manifest' with no run_id;
    the writer recognises it by its file, so the applied migration still means
    what it meant."""
    m017 = importlib.import_module("engine.migrations.017_seed_event_store")
    db.commit()
    m017.build_seed(db, corpus_ids=[3], parsed_rows=[], spec_identity={"s": 1},
                    codebook_identity={"c": 1}, arms=("seeded_arm",))
    row = db.execute("SELECT run_id, run_marker FROM paper_events WHERE paper_id = 3"
                     ).fetchone()
    assert row == (None, "pre-manifest")


# ── R59 / R21 / R10 — which arms accept a claim ──────────────────────
def test_r59_a_claim_on_a_pre_manifest_arm_is_refused(db):
    events.register_arm(db, "old_arm", "model", configuration_marker=PRE_MANIFEST)
    with pytest.raises(events.ClaimOnPreManifestArm, match="R59"):
        events.write_field_event(
            db, event_type="asserted", paper_id=1, field_name=FIELD, arm="old_arm",
            value="5", source_snippet="q", actor_kind="model", actor_role="extractor",
            actor_name="m", sentinels=SENTINELS, run_id=_run(db))


def test_r21_a_claim_on_a_retired_arm_is_refused(db):
    events.retire_arm(db, "local")
    with pytest.raises(events.ClaimOnRetiredArm, match="R21"):
        claim(db, 1, "5")


def test_r10_a_claim_on_a_model_arm_the_run_did_not_pin_is_refused(db):
    events.register_arm(db, "other_arm", "model")
    with pytest.raises(events.ArmNotInRun, match="R10"):
        events.write_field_event(
            db, event_type="asserted", paper_id=1, field_name=FIELD, arm="other_arm",
            value="5", source_snippet="q", actor_kind="model", actor_role="extractor",
            actor_name="m", sentinels=SENTINELS, run_id=_run(db))


def test_r68_a_reviewer_event_on_a_pre_manifest_claim_is_allowed_under_a_review_session(db):
    """Row 7's exit. The claims were seeded; the reviewer's decision is new and
    carries its review session's run_id."""
    events.register_arm(db, "old_arm", "model", configuration_marker=PRE_MANIFEST)
    a = seed_claim(db, arm="old_arm", paper_id=1, field_name=FIELD, value="5",
                   source_snippet="q")
    b = seed_claim(db, arm="old_arm", paper_id=1, field_name=FIELD, value="6",
                   source_snippet="q")
    session = db.execute(
        "INSERT INTO run_manifests (run_uid, review_id, run_kind, git_commit, git_dirty, "
        "spec_hash, codebook_hash, codebook_sha256, library_versions_json, host, "
        "started_at, manifest_json, manifest_sha256) VALUES ('rs', 'r', 'review_session', "
        "?, 0, 'h', 'h', 'h', '{}', 'h', 'n', '{}', 'h')", ("1" * 40,)).lastrowid
    assert effective_value(db, 1, FIELD, "old_arm", sentinels=SENTINELS).rule_row == 7
    events.write_field_event(
        db, event_type="human_withdrew", paper_id=1, field_name=FIELD, arm="old_arm",
        claim_id=a, actor_kind="human", actor_role="reviewer", actor_name="PI",
        against_claims={a, b}, sentinels=SENTINELS, run_id=session)
    r = effective_value(db, 1, FIELD, "old_arm", sentinels=SENTINELS)
    assert r.rule_row != 7 and r.value is None


# ── R59 — the widened freeze trigger ─────────────────────────────────
def test_r59_a_pre_manifest_arm_can_never_be_pinned(db):
    events.register_arm(db, "old_arm", "model", configuration_marker=PRE_MANIFEST)
    with pytest.raises(sqlite3.IntegrityError, match="R59"):
        db.execute("UPDATE arms SET configuration_json = '{}', configuration_marker = "
                   "'pinned' WHERE arm_name = 'old_arm'")


def test_r59_a_pinned_arm_is_frozen_before_it_holds_any_claim(db):
    db.execute("UPDATE arms SET configuration_marker = 'pinned', pinned_run_id = ?, "
               "pinned_sha256 = 'x' WHERE arm_name = 'local'", (_run(db),))
    with pytest.raises(sqlite3.IntegrityError, match="pinned by a manifest"):
        db.execute("UPDATE arms SET configuration_json = '{\"model\":\"other\"}' "
                   "WHERE arm_name = 'local'")
