"""The event-side auditor (WRITE-PATH-01 9b-2d): T3–T14.

Every database is a scratch `ReviewDatabase` with a real manifest (the 2(b)
helper, which declares the `audit` stage); claims are written through the event
writer with `claim_identity`; `semantic_verify` is mocked — no model is called.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
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
from engine.core.effective_config import stage_config
from _event_store_fixture import (
    FIXTURE_CONTEXT_SHA, claim_identity, open_extraction_run, seed_claim, seed_eligibility,
)
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
                               uid=ref.parsed_text_uid), run_id=run_id,
        presented_context_sha256=FIXTURE_CONTEXT_SHA)  # R224a
    return events.make_claim_id(spec.extraction_models.arm, uid, field)


def located_events(db, pid=None):
    sql = "SELECT claim_id, payload_json FROM field_events WHERE event_type = 'citation_located'"
    rows = db._conn.execute(sql + (" AND paper_id = ?" if pid else ""),
                            (pid,) if pid else ()).fetchall()
    return {r[0]: json.loads(r[1]) for r in rows}


def verdicts(db, pid=None):
    """R218: audit_verdicts is the authority now, not audit_calls.jsonl."""
    db._conn.row_factory = sqlite3.Row
    sql = "SELECT * FROM audit_verdicts"
    rows = db._conn.execute(sql + (" WHERE paper_id = ?" if pid else ""),
                            (pid,) if pid else ()).fetchall()
    return [dict(r) for r in rows]


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
    rows = verdicts(db, P7)  # R218: audit_verdicts, not audit_calls.jsonl
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
    by_claim = {r["claim_id"]: r for r in verdicts(db, P7)}  # R218
    assert by_claim[ncr]["verdict"] == "flagged"         # no snippet, no auto-verify
    # The legacy per-span audit no longer short-circuits them either.
    for value in ("Not discussed", "No comparison reported", "NR"):
        status, _ = audit_span({"field_name": "country", "value": value, "source_snippet": ""},
                               TEXT, cfg=stage_config("audit"))
        assert status == "flagged", value


# ── T9 (R124) ─────────────────────────────────────────────────────────
def test_t9_a_tier_4_claim_is_located_like_any_other(db, spec, run_id, review_dir, verify):
    c = claim(db, spec, run_id, P7, "key_limitation", "small sample", "Not a sentence here.")
    run(db, spec, run_id, review_dir)
    assert located_events(db, P7)[c]["located"] is False and verify.call_count == 1
    with patch("engine.agents.auditor.semantic_verify", return_value=AuditVerdict(
            status="verified", grep_found=False, reasoning="ok")):
        status, _ = audit_span({"field_name": "key_limitation", "value": "small",
                                "source_snippet": "Not a sentence here."}, TEXT, field_tier=4, cfg=stage_config("audit"))
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


# ── T12 RETIRED (R218, 10a-C5) ───────────────────────────────────────
# `test_t12_the_audit_telemetry_row_is_pinned` pinned audit_calls.jsonl file
# mechanics — `AT.telemetry_path`, a file-based record_verdict/read_verdicts
# round trip — none of which exist any more (audit_verdicts is the table).
# Its one surviving fact, SCHEMA_VERSION/FIELDS, is pinned by test_t15 below.
# Retention ledger: id retired, not rewritten — its premise (a JSONL file
# exists to round-trip) cannot survive R218.


# ── T15 (R218 B4-T1): one audit_verdicts row per verdict ──────────────
def test_t15_one_audit_verdicts_row_per_verdict_matching_the_calls_inputs(
        db, spec, run_id, review_dir, verify):
    """Two claims that both need a model call (no snippet, or unlocated) —
    each gets exactly one audit_verdicts row, fields equal to the call's
    inputs, run_id equal to the audited_ai event's run_id."""
    arm = spec.extraction_models.arm
    lost = claim(db, spec, run_id, P7, "country", "Norway",
                "Conducted in Norway by the authors.")
    no_snippet = claim(db, spec, run_id, P7, "task_performed", "suturing", "")
    rep = run(db, spec, run_id, review_dir)

    rows = {r["claim_id"]: r for r in verdicts(db, P7)}
    assert set(rows) == {lost, no_snippet}
    assert AT.SCHEMA_VERSION == "audit-telemetry-1"
    for cid, field_name in ((lost, "country"), (no_snippet, "task_performed")):
        r = rows[cid]
        assert r["schema"] == "audit-telemetry-1"
        assert r["field_name"] == field_name and r["arm"] == arm
        assert r["auditor_model"] == "gemma3:27b"
        row_audited_ai = db._conn.execute(
            "SELECT run_id FROM paper_events WHERE paper_id = ? AND to_state = "
            "'audited_ai'", (P7,)).fetchone()
        assert r["run_id"] == row_audited_ai[0] == run_id
    assert rep.papers_audited == 1


# ── T16 (R218 B4-T2): atomicity — verdicts and the paper event together ──
def test_t16_a_failure_after_verdicts_leaves_no_verdict_rows_and_no_audited_ai(
        db, spec, run_id, review_dir, verify):
    """A failure injected between the verdict-row writes and the paper
    event's write rolls back the whole savepoint — zero audit_verdicts rows
    for the paper, no audited_ai event. Same shape as T7, one layer deeper."""
    claim(db, spec, run_id, P7, "country", "Norway", "Conducted in Norway.")
    with patch.object(AE, "write_paper_event", side_effect=events.RunLinkRefused("boom")):
        with pytest.raises(events.RunLinkRefused):
            run(db, spec, run_id, review_dir)
    assert verdicts(db, P7) == []
    assert audited(db, P7) == 0


# ── T17 (R218 B4-T3): record_verdict refuses with no run_id ──────────
def test_t17_record_verdict_with_no_run_id_refuses_and_writes_nothing(db):
    with pytest.raises(AT.VerdictWithoutRun):
        AT.record_verdict(
            db._conn, run_id=None, paper_id=P7, claim_id="c", field_name="f",
            arm="a", auditor_model="gemma3:27b", auditor_digest="d" * 64,
            verdict="flagged", rationale="r", occurred_at="2026-01-01T00:00:00+00:00")
    assert db._conn.execute("SELECT COUNT(*) FROM audit_verdicts").fetchone()[0] == 0


# ── T18 (R218 B4-T4): no JSONL file is ever created ───────────────────
def test_t18_no_audit_calls_jsonl_is_created_by_a_run(db, spec, run_id, review_dir, verify):
    claim(db, spec, run_id, P7, "country", "Norway", "Conducted in Norway.")
    run(db, spec, run_id, review_dir)
    assert not (review_dir / "telemetry" / "audit_calls.jsonl").exists()
    assert not hasattr(AT, "telemetry_path")
    assert not hasattr(AT, "read_verdicts")


# ── T19 (R218 B4-T5): a claim needing no model call ────────────────────
def test_t19_an_unlocated_absence_sentinel_gets_no_verdict_row_at_all(
        db, spec, run_id, review_dir, verify):
    """R218 B4-T5, distinct from T14/T18's 'no snippet' case: an unlocated
    claim whose value is one of the codebook's absence_sentinels is not
    `is_populated`, so `audit_run` never appends anything to `verdicts` for
    it — `continue`s before the flagged-without-a-call branch even runs.
    Pinning what the code does today, unchanged: NO verdict row (not a
    'flagged' one) for a sentinel value, though its citation_located event
    is still written."""
    c = claim(db, spec, run_id, P7, "country", "NR", "unmatched text entirely")
    run(db, spec, run_id, review_dir)
    verify.assert_not_called()
    assert located_events(db, P7)[c]["located"] is False
    assert verdicts(db, P7) == []


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
    (row,) = verdicts(db, P7)  # R218
    assert (row["claim_id"], row["verdict"], row["rationale"]) == \
        (c, "flagged", AE.NO_SNIPPET_RATIONALE)
    assert located_events(db, P7)[c]["snippet_supplied"] is False


def test_an_audit_run_needs_its_audit_stage(db, spec):
    from engine.core import run_manifest as rm
    with pytest.raises(rm.StageNotInRun):
        AE.audit_run(db._conn, spec, run_id=999, arm=spec.extraction_models.arm,
                     review_dir=Path(db.db_path).parent)


# ── D23 (12d): audit reads each claim's own recorded text version ─────
OTHER_TEXT = "An entirely different parse of the same paper, sharing no sentence.\n"
SNIPPET = "The trial enrolled forty patients at two centres."


def test_d23_t1_claims_on_an_older_version_are_located_against_that_version(
        db, spec, run_id, review_dir, verify):
    old = resolve_parsed_text(db._conn, P7)
    c = claim(db, spec, run_id, P7, "study_type", "RCT", SNIPPET)
    write_parsed(db, P7, OTHER_TEXT)
    assert resolve_parsed_text(db._conn, P7).parsed_text_uid != old.parsed_text_uid
    rep = run(db, spec, run_id, review_dir)
    p = located_events(db, P7)[c]
    assert (p["located"], p["kind"]) == (True, "exact")
    assert (p["parsed_text_uid"], p["parsed_text_sha256"]) == (old.parsed_text_uid, old.sha256)
    payload = json.loads(db._conn.execute(
        "SELECT payload_json FROM paper_events WHERE paper_id = ? AND to_state = 'audited_ai'",
        (P7,)).fetchone()[0])
    assert payload["parsed_text_sha256"] == old.sha256
    assert rep.skipped_refused == () and rep.located == 1


def test_d23_t2_claims_spanning_two_text_versions_refuse_the_paper(
        db, spec, run_id, review_dir, verify, caplog):
    old = resolve_parsed_text(db._conn, P7)
    claim(db, spec, run_id, P7, "study_type", "RCT", SNIPPET)
    write_parsed(db, P7, OTHER_TEXT)
    new = resolve_parsed_text(db._conn, P7)
    claim(db, spec, run_id, P7, "country", "Norway", "An entirely different parse")
    claim(db, spec, run_id, P8, "country", "Norway", SNIPPET)
    with caplog.at_level("WARNING", logger=AE.logger.name):
        rep = run(db, spec, run_id, review_dir)
    assert rep.skipped_refused == ((P7, AE.REFUSAL_MIXED_TEXT_VERSIONS),)
    line = next(r.getMessage() for r in caplog.records if "audit skipped" in r.getMessage())
    assert f"Paper {P7}" in line and AE.REFUSAL_MIXED_TEXT_VERSIONS in line
    assert old.parsed_text_uid in line and new.parsed_text_uid in line
    assert located_events(db, P7) == {} and audited(db, P7) == 0
    assert audited(db, P8) == 1 and rep.papers_audited == 1


def test_d23_t3_an_altered_claim_version_refuses_the_paper_though_a_newer_one_is_intact(
        db, spec, run_id, review_dir, verify, caplog):
    old = resolve_parsed_text(db._conn, P7)
    claim(db, spec, run_id, P7, "study_type", "RCT", SNIPPET)
    write_parsed(db, P7, OTHER_TEXT)
    old.path.write_text("edited in place\n")
    claim(db, spec, run_id, P8, "country", "Norway", SNIPPET)
    with caplog.at_level("WARNING", logger=AE.logger.name):
        rep = run(db, spec, run_id, review_dir)
    assert rep.skipped_refused == ((P7, "parsed_text_modified"),)
    line = next(r.getMessage() for r in caplog.records if "audit skipped" in r.getMessage())
    assert f"Paper {P7}" in line and old.parsed_text_uid in line
    assert located_events(db, P7) == {} and audited(db, P7) == 0
    assert audited(db, P8) == 1


def test_d23_t4_the_writer_refuses_a_citation_against_another_text(db, spec, run_id):
    ref = resolve_parsed_text(db._conn, P7)
    c = claim(db, spec, run_id, P7, "study_type", "RCT", SNIPPET)
    n = db._conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0]
    res = AE.locate(TEXT, SNIPPET)
    other = "0" * 64
    with pytest.raises(events.CitationTextMismatch) as exc:
        events.write_field_event(
            db._conn, event_type="citation_located", paper_id=P7, field_name="study_type",
            arm=spec.extraction_models.arm, claim_id=c, actor_kind="engine",
            actor_role="system", actor_name=AE.LOCATOR_ACTOR,
            payload=AE.locate_payload(res, threshold=AE.FUZZY_THRESHOLD,
                                      parsed_text_sha256=other,
                                      parsed_text_uid=ref.parsed_text_uid), run_id=run_id)
    assert isinstance(exc.value, events.EventRefused)
    assert ref.sha256 in str(exc.value) and other in str(exc.value)
    assert db._conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0] == n
    assert located_events(db, P7) == {}


def test_d23_t5_a_byte_identical_reparse_is_not_a_mismatch(db, spec, run_id, review_dir,
                                                           verify):
    old = resolve_parsed_text(db._conn, P7)
    c = claim(db, spec, run_id, P7, "study_type", "RCT", SNIPPET)
    write_parsed(db, P7, TEXT)
    new = resolve_parsed_text(db._conn, P7)
    assert (new.sha256 == old.sha256) and new.parsed_text_uid != old.parsed_text_uid
    rep = run(db, spec, run_id, review_dir)
    p = located_events(db, P7)[c]
    assert p["located"] is True and p["parsed_text_sha256"] == old.sha256
    assert p["parsed_text_uid"] == old.parsed_text_uid
    assert rep.skipped_refused == () and audited(db, P7) == 1


def test_d23_a_claim_with_no_recorded_text_identity_refuses_the_paper(db, spec, run_id,
                                                                      review_dir, verify):
    events.register_arm(db._conn, "premanifest_a", "model", configuration_marker=PRE_MANIFEST)
    seed_claim(db._conn, arm="premanifest_a", paper_id=P7, field_name="country",
               value="Norway", source_snippet=SNIPPET)
    db._conn.commit()
    rep = AE.audit_run(db._conn, spec, run_id=run_id, arm="premanifest_a",
                       review_dir=review_dir)
    assert rep.skipped_refused == ((P7, "parsed_text_not_recorded"),)
    assert located_events(db, P7) == {} and audited(db, P7) == 0


# ── D23 strict form (12d-D23-R2 R-1): present AND equal ───────────────
def test_d23_t7_the_writer_refuses_a_citation_that_names_no_text(db, spec, run_id):
    c = claim(db, spec, run_id, P7, "study_type", "RCT", SNIPPET)
    assert db._conn.execute(
        "SELECT COUNT(*) FROM field_events fe JOIN claim_inputs ci "
        "ON fe.extraction_uid = ci.extraction_uid WHERE fe.claim_id = ?", (c,)).fetchone()[0] == 1
    n = db._conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0]
    with pytest.raises(events.CitationTextMismatch) as exc:
        events.write_field_event(
            db._conn, event_type="citation_located", paper_id=P7, field_name="study_type",
            arm=spec.extraction_models.arm, claim_id=c, actor_kind="engine",
            actor_role="system", actor_name=AE.LOCATOR_ACTOR,
            payload={"located": True}, run_id=run_id)
    assert "names no text" in str(exc.value)
    assert db._conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0] == n
    assert located_events(db, P7) == {}


def test_d23_t8_a_citation_on_a_claim_with_no_claim_inputs_row_is_admitted(db, run_id):
    events.register_arm(db._conn, "premanifest_a", "model", configuration_marker=PRE_MANIFEST)
    uid = events.mint_extraction_uid()
    cid = seed_claim(db._conn, arm="premanifest_a", paper_id=P7, field_name="country",
                     value="Norway", source_snippet="x", extraction_uid=uid)
    db._conn.commit()
    assert db._conn.execute("SELECT extraction_uid FROM field_events WHERE claim_id = ?",
                            (cid,)).fetchone()[0] == uid
    assert db._conn.execute("SELECT COUNT(*) FROM claim_inputs").fetchone()[0] == 0
    events.write_field_event(
        db._conn, event_type="citation_located", paper_id=P7, field_name="country",
        arm="premanifest_a", claim_id=cid, actor_kind="engine", actor_role="system",
        actor_name=AE.LOCATOR_ACTOR, payload={"located": True}, run_id=run_id)
    assert set(located_events(db, P7)) == {cid}
