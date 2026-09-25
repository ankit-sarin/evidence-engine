"""The event-side auditor (WRITE-PATH-01 9b-2d): T3–T14.

Every database is a scratch `ReviewDatabase` with a real manifest (the 2(b)
helper, which declares the `audit` stage); claims are written through the event
writer with `claim_identity`; `semantic_verify` is mocked — no model is called.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from engine.agents import audit_events as AE
from engine.agents.auditor import AuditVerdict, audit_span
from engine.core import audit_telemetry as AT
from engine.core import events
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.effective import PRE_MANIFEST, effective_state, effective_value
from engine.core.parsed_text import resolve_parsed_text
from engine.core.review_spec import load_review_spec
from _event_store_fixture import claim_identity, open_extraction_run, seed_claim, seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
TEXT = ("The trial enrolled forty patients at two centres. The robot performed "
        "autonomous suturing on porcine tissue in ten trials. Outcomes were measured "
        "at six months.\n")
P7, P8 = 7, 8


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("aud", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    for pid in (P7, P8):
        rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                          "updated_at) VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (pid,))
        seed_eligibility(rdb._conn, pid)
        write_parsed(rdb, pid, TEXT)
    rdb._conn.commit()
    yield rdb
    rdb.close()


@pytest.fixture
def run_id(db, spec):
    return open_extraction_run(db, spec)


@pytest.fixture
def review_dir(db):
    return Path(db.db_path).parent


@pytest.fixture
def sentinels(review_dir):
    return frozenset(load_codebook(review_dir / "extraction_codebook.yaml").absence_sentinels)


def claim(db, spec, run_id, pid, field, value, snippet):
    uid = events.mint_extraction_uid()
    ref = resolve_parsed_text(db._conn, pid)
    events.write_field_event(
        db._conn, event_type="asserted", paper_id=pid, field_name=field,
        arm=spec.extraction_models.arm, value=value, source_snippet=snippet,
        extraction_uid=uid, actor_kind="model", actor_role="extractor",
        actor_name="deepseek-r1:32b",
        payload=claim_identity(spec.extraction_models.arm, pid, sha=ref.sha256,
                               uid=ref.parsed_text_uid), run_id=run_id)
    return events.make_claim_id(spec.extraction_models.arm, uid, field)


def located_events(db, pid=None):
    sql = "SELECT claim_id, payload_json FROM field_events WHERE event_type = 'citation_located'"
    rows = db._conn.execute(sql + (" AND paper_id = ?" if pid else ""),
                            (pid,) if pid else ()).fetchall()
    return {r[0]: json.loads(r[1]) for r in rows}


def audited(db, pid):
    return db._conn.execute("SELECT COUNT(*) FROM paper_events WHERE paper_id = ? AND "
                            "to_state = 'audited_ai'", (pid,)).fetchone()[0]


@pytest.fixture
def verify():
    with patch.object(AE, "semantic_verify", return_value=AuditVerdict(
            status="flagged", grep_found=False, reasoning="not supported")) as m:
        yield m


def run(db, spec, run_id, review_dir):
    return AE.audit_run(db._conn, spec, run_id=run_id, arm=spec.extraction_models.arm,
                        review_dir=review_dir)


# ── T3 ────────────────────────────────────────────────────────────────
def test_t3_locate_every_claim_verify_only_the_unlocated_value(db, spec, run_id,
                                                              review_dir, sentinels, verify):
    arm = spec.extraction_models.arm
    exact = claim(db, spec, run_id, P7, "study_type", "RCT",
                  "The trial enrolled forty patients at two centres.")
    fuzzy = claim(db, spec, run_id, P7, "task_performed", "suturing",
                  "The robot performd autonomous suturing on porcine tisue in ten trials.")
    lost = claim(db, spec, run_id, P7, "country", "Norway", "Conducted in Norway by the authors.")
    sent = claim(db, spec, run_id, P7, "comparison_to_human", "NR",
                 "Outcomes were measured at six months.")
    rep = run(db, spec, run_id, review_dir)

    evs = located_events(db, P7)
    assert set(evs) == {exact, fuzzy, lost, sent}
    for p in evs.values():
        assert {"located", "kind", "score", "threshold", "locator_version", "snippet_supplied",
                "bridged", "parsed_text_sha256", "parsed_text_uid"} <= set(p)
    assert (evs[exact]["kind"], evs[fuzzy]["kind"], evs[lost]["located"]) == \
        ("exact", "fuzzy", False)
    assert verify.call_count == 1 and verify.call_args.args[0].field_name == "country"
    rows = AT.read_verdicts(review_dir)
    assert len(rows) == 1 and rows[0]["claim_id"] == lost and rows[0]["run_id"] == run_id
    assert audited(db, P7) == 1 and rep.papers_audited == 1
    assert effective_state(db._conn, P7).processing == "audited_ai"
    row = lambda f: effective_value(db._conn, P7, f, arm, sentinels=sentinels).rule_row
    assert (row("study_type"), row("task_performed"), row("country"),
            row("comparison_to_human")) == (10, 10, 11, 12)


# ── T4 ────────────────────────────────────────────────────────────────
def test_t4_a_second_audit_writes_nothing(db, spec, run_id, review_dir, verify):
    claim(db, spec, run_id, P7, "country", "Norway", "Conducted in Norway.")
    run(db, spec, run_id, review_dir)
    n = db._conn.execute("SELECT (SELECT COUNT(*) FROM field_events), "
                         "(SELECT COUNT(*) FROM paper_events)").fetchone()
    again = run(db, spec, run_id, review_dir)
    assert db._conn.execute("SELECT (SELECT COUNT(*) FROM field_events), "
                            "(SELECT COUNT(*) FROM paper_events)").fetchone() == n
    assert again.papers_audited == 0 and verify.call_count == 1


# ── T5 ────────────────────────────────────────────────────────────────
def test_t5_a_superseded_claim_is_ignored(db, spec, run_id, review_dir, verify):
    arm = spec.extraction_models.arm
    old = claim(db, spec, run_id, P7, "country", "Norway", "Conducted in Norway.")
    new = claim(db, spec, run_id, P7, "country", "Denmark", "The trial enrolled forty patients")
    events.write_field_event(
        db._conn, event_type="superseded", paper_id=P7, field_name="country", arm=arm,
        claim_id=new, actor_kind="engine", actor_role="system", actor_name="w",
        against_claims={old}, run_id=run_id)
    run(db, spec, run_id, review_dir)
    assert set(located_events(db, P7)) == {new}


# ── T6 ────────────────────────────────────────────────────────────────
def test_t6_a_refused_text_skips_its_paper_and_the_others_are_audited(db, spec, run_id,
                                                                     review_dir, verify):
    claim(db, spec, run_id, P7, "country", "Norway", "Conducted in Norway.")
    c8 = claim(db, spec, run_id, P8, "country", "Norway", "Conducted in Norway.")
    resolve_parsed_text(db._conn, P8).path.write_text("edited in place\n")
    rep = run(db, spec, run_id, review_dir)
    assert rep.skipped_refused == ((P8, "parsed_text_modified"),)
    assert located_events(db, P8) == {} and audited(db, P8) == 0
    assert audited(db, P7) == 1 and c8 not in located_events(db)


# ── T7 ────────────────────────────────────────────────────────────────
def test_t7_a_failure_mid_paper_leaves_no_located_event_and_no_audited_ai(
        db, spec, run_id, review_dir, verify):
    claim(db, spec, run_id, P7, "study_type", "RCT", "The trial enrolled forty patients")
    claim(db, spec, run_id, P7, "country", "Norway", "Conducted in Norway.")
    with patch.object(AE, "write_paper_event", side_effect=events.RunLinkRefused("boom")):
        with pytest.raises(events.RunLinkRefused):
            run(db, spec, run_id, review_dir)
    assert located_events(db, P7) == {} and audited(db, P7) == 0


# ── T8 (R124) ─────────────────────────────────────────────────────────
def test_t8_the_old_hand_list_values_are_not_auto_verified(db, spec, run_id, review_dir,
                                                          verify):
    nd = claim(db, spec, run_id, P7, "country", "Not discussed", "Conducted in Norway.")
    ncr = claim(db, spec, run_id, P7, "comparison_to_human", "No comparison reported", "")
    run(db, spec, run_id, review_dir)
    evs = located_events(db, P7)
    assert evs[nd]["located"] is False and evs[ncr]["snippet_supplied"] is False
    assert verify.call_count == 1                        # "Not discussed" is a value
    by_claim = {r["claim_id"]: r for r in AT.read_verdicts(review_dir)}
    assert by_claim[ncr]["verdict"] == "flagged"         # no snippet, no auto-verify
    # The legacy per-span audit no longer short-circuits them either.
    for value in ("Not discussed", "No comparison reported", "NR"):
        status, _ = audit_span({"field_name": "country", "value": value, "source_snippet": ""},
                               TEXT)
        assert status == "flagged", value


# ── T9 (R124) ─────────────────────────────────────────────────────────
def test_t9_a_tier_4_claim_is_located_like_any_other(db, spec, run_id, review_dir, verify):
    c = claim(db, spec, run_id, P7, "key_limitation", "small sample", "Not a sentence here.")
    run(db, spec, run_id, review_dir)
    assert located_events(db, P7)[c]["located"] is False and verify.call_count == 1
    with patch("engine.agents.auditor.semantic_verify", return_value=AuditVerdict(
            status="verified", grep_found=False, reasoning="ok")):
        status, _ = audit_span({"field_name": "key_limitation", "value": "small",
                                "source_snippet": "Not a sentence here."}, TEXT, field_tier=4)
    assert status == "contested"           # grep evaluated and failed; never passed unchecked


# ── T10 (R136) ────────────────────────────────────────────────────────
def test_t10_low_yield_reads_the_codebook(db, spec, run_id, review_dir):
    arm = spec.extraction_models.arm
    cb = load_codebook(review_dir / "extraction_codebook.yaml")
    claim(db, spec, run_id, P7, "country", "NR", "")
    claim(db, spec, run_id, P7, "sample_size", "NOT_FOUND", "")
    assert AE.low_yield(db._conn, P7, arm, codebook=cb, threshold=1) is True
    assert AE.low_yield(db._conn, P7, arm, codebook=cb, threshold=4) is True
    claim(db, spec, run_id, P8, "clinical_readiness_assessment", "Not assessable", "")
    assert AE.low_yield(db._conn, P8, arm, codebook=cb, threshold=1) is False
    assert AE.low_yield(db._conn, P8, arm, codebook=cb, threshold=4) is True


# ── T12 ───────────────────────────────────────────────────────────────
def test_t12_the_audit_telemetry_row_is_pinned(tmp_path):
    assert AT.SCHEMA_VERSION == "audit-telemetry-1"
    assert AT.telemetry_path(tmp_path) == tmp_path / "telemetry" / "audit_calls.jsonl"
    AT.record_verdict(tmp_path, run_id=1, paper_id=7, claim_id="c", field_name="f", arm="a",
                      auditor_model="gemma3:27b", auditor_digest="d" * 64, verdict="flagged",
                      rationale="r")
    (row,) = AT.read_verdicts(tmp_path)
    assert tuple(row) == AT.FIELDS == (
        "schema", "run_id", "paper_id", "claim_id", "field_name", "arm", "auditor_model",
        "auditor_digest", "verdict", "rationale", "occurred_at")
    assert row["schema"] == "audit-telemetry-1"


# ── T13 (R1) ──────────────────────────────────────────────────────────
def test_t13_a_located_event_on_a_pre_manifest_claim_is_admitted_a_claim_is_not(db, run_id):
    events.register_arm(db._conn, "premanifest_a", "model", configuration_marker=PRE_MANIFEST)
    cid = seed_claim(db._conn, arm="premanifest_a", paper_id=P7, field_name="country",
                     value="Norway", source_snippet="x")
    db._conn.commit()
    events.write_field_event(
        db._conn, event_type="citation_located", paper_id=P7, field_name="country",
        arm="premanifest_a", claim_id=cid, actor_kind="engine", actor_role="system",
        actor_name=AE.LOCATOR_ACTOR, payload={"located": False}, run_id=run_id)
    with pytest.raises(events.ClaimOnPreManifestArm):
        events.write_field_event(
            db._conn, event_type="asserted", paper_id=P7, field_name="country",
            arm="premanifest_a", value="v", source_snippet="v", actor_kind="model",
            actor_role="extractor", actor_name="m",
            payload=claim_identity("premanifest_a", P7), run_id=run_id)


# ── T14 (R4) ──────────────────────────────────────────────────────────
def test_t14_a_snippet_less_value_is_flagged_without_a_model_call(db, spec, run_id,
                                                                 review_dir, verify):
    c = claim(db, spec, run_id, P7, "country", "Norway", "")
    run(db, spec, run_id, review_dir)
    verify.assert_not_called()
    (row,) = AT.read_verdicts(review_dir)
    assert (row["claim_id"], row["verdict"], row["rationale"]) == \
        (c, "flagged", AE.NO_SNIPPET_RATIONALE)
    assert located_events(db, P7)[c]["snippet_supplied"] is False


def test_an_audit_run_needs_its_audit_stage(db, spec):
    from engine.core import run_manifest as rm
    with pytest.raises(rm.StageNotInRun):
        AE.audit_run(db._conn, spec, run_id=999, arm=spec.extraction_models.arm,
                     review_dir=Path(db.db_path).parent)
