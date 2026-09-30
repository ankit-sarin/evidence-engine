"""STAGED-ENTRY-01 11b — the FT adjudication import under an import manifest
(R247, R252, R257; 11b-ADJ-R1 D1–D9; closes C42).

A validated decision file is applied under one `import` manifest that records
the file's sha256 under `inputs`, in one transaction: per decision the
adjudication row, the status write and an `adjudicated` eligibility event (actor
human / reviewer, actor_name the file's stored path). A refused transition rolls
the file back and closes the manifest `aborted`. No test reaches git or a model
server: the manifest is opened with a fixed git state and a null digest.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from engine.adjudication import ft_screening_adjudicator as adj
from engine.adjudication.ft_screening_adjudicator import import_ft_adjudication_decisions
from engine.adjudication.workflow import complete_stage, ensure_workflow_table
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.effective import eligible_paper_ids
from engine.core.review_paths import load_spec_for
from engine.exporters.prisma import validate_prisma_counts
from engine.search.models import Citation

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")

CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
DIRTY = rm.GitState(commit="b" * 40, dirty=True, tag=None)
STAGE = "FULL_TEXT_ADJUDICATION_COMPLETE"


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


@pytest.fixture
def db(tmp_path):
    d = ReviewDatabase("adj_import", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(d.db_path).parent / "extraction_codebook.yaml")
    yield d
    d.close()


def _paper(db, n, *, via_verifier=True):
    """A paper at FT_FLAGGED — by the verifier's flag (PARSED → FT_ELIGIBLE →
    FT_FLAGGED, with a verification row) or by the primary's (PARSED → FT_FLAGGED)."""
    db.add_papers([Citation(title=f"Paper {n}", abstract="a", pmid=f"ADJ{n}",
                            source="pubmed", authors=["A"], journal="J", year=2024)])
    pid = db._conn.execute("SELECT id FROM papers WHERE pmid = ?", (f"ADJ{n}",)).fetchone()[0]
    for s in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED"):
        db.update_status(pid, s)
    if via_verifier:
        db.update_status(pid, "FT_ELIGIBLE")
        db.add_ft_verification_decision(pid, "gemma3:27b", "FT_FLAGGED", "r", 0.9)
    db.update_status(pid, "FT_FLAGGED")
    return pid


def _file(tmp_path, rows, name="decisions.json"):
    p = tmp_path / name
    p.write_text(json.dumps(rows))
    return p


def _import(db, spec, path, *, git=CLEAN):
    return import_ft_adjudication_decisions(db, path, spec=spec, git=git,
                                            digest_fn=lambda m: "")


def _one(db, sql, *args):
    return db._conn.execute(sql, args).fetchone()


def _count(db, table, where="1", *args):
    return _one(db, f"SELECT COUNT(*) FROM {table} WHERE {where}", *args)[0]


def _status(db, pid):
    return _one(db, "SELECT status FROM papers WHERE id = ?", pid)[0]


def _manifests(db):
    return db._conn.execute(
        "SELECT run_id, run_kind, end_status, end_reason, ended_at, manifest_json "
        "FROM run_manifests ORDER BY run_id").fetchall()


def _workflow(db):
    return [tuple(r) for r in db._conn.execute(
        "SELECT * FROM workflow_state ORDER BY stage_name")]


# ── 1, 2 — the manifest ─────────────────────────────────────────────


def test_1_one_import_manifest_with_zero_stage_rows_closed_completed(db, spec, tmp_path):
    pid = _paper(db, 1)
    _import(db, spec, _file(tmp_path, [{"paper_id": pid, "decision": "FT_ELIGIBLE"}]))
    (m,) = _manifests(db)
    assert m["run_kind"] == "import"
    assert m["end_status"] == "completed" and m["end_reason"] is None and m["ended_at"]
    assert _count(db, "run_stage_configs") == 0 and _count(db, "run_calls") == 0


def test_2_the_manifest_records_the_file_sha256_under_inputs_and_pins_stay_arms(
        db, spec, tmp_path):
    pid = _paper(db, 1)
    path = _file(tmp_path, [{"paper_id": pid, "decision": "FT_ELIGIBLE"}])
    _import(db, spec, path)
    body = json.loads(_manifests(db)[0]["manifest_json"])
    assert body["inputs"] == {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest()}
    assert body["pins"] == {}


def test_2_a_path_under_the_repo_is_stored_repo_relative():
    assert adj._stored_path(REPO / "data" / "x" / "d.json") == "data/x/d.json"
    assert adj._stored_path(Path("/tmp/elsewhere/d.json")) == str(Path("/tmp/elsewhere/d.json").resolve())


# ── 3, 4 — the two events ───────────────────────────────────────────


def _assert_event(db, pid, path, to_state):
    evs = db._conn.execute("SELECT * FROM paper_events WHERE paper_id = ?", (pid,)).fetchall()
    assert len(evs) == 1
    ev = evs[0]
    run_id = _manifests(db)[0]["run_id"]
    assert ev["event_type"] == "adjudicated" and ev["to_state"] == to_state
    assert ev["actor_kind"] == "human" and ev["actor_role"] == "reviewer"
    assert ev["actor_name"] == str(path.resolve()) and ev["actor_digest"] is None
    assert ev["run_id"] == run_id and ev["run_marker"] is None
    assert ev["stage_name"] == "ft_adjudication"
    assert ev["reason"] is None and ev["reason_code"] is None
    assert ev["payload_json"] == "{}"
    assert ev["from_state"] is None and ev["prior_event_id"] is None
    assert ev["presented_context_sha256"] is None


def test_3_adjudicated_eligible_writes_one_eligible_event(db, spec, tmp_path):
    pid = _paper(db, 1)
    path = _file(tmp_path, [{"paper_id": pid, "decision": "FT_ELIGIBLE", "note": "ok"}])
    result = _import(db, spec, path)
    _assert_event(db, pid, path, "eligible")
    assert _status(db, pid) == "FT_ELIGIBLE"
    assert _count(db, "ft_screening_adjudication", "paper_id = ?", pid) == 1
    assert result["stats"]["ft_eligible"] == 1 and "status_update_failed" not in result["stats"]


def test_4_adjudicated_screened_out_writes_one_full_text_out_event(db, spec, tmp_path):
    pid = _paper(db, 1)
    path = _file(tmp_path, [{"paper_id": pid, "decision": "FT_SCREENED_OUT"}])
    _import(db, spec, path)
    _assert_event(db, pid, path, "full_text_out")
    assert _status(db, pid) == "FT_SCREENED_OUT"
    assert _count(db, "ft_screening_adjudication", "paper_id = ?", pid) == 1


# ── 5, 6 — file-atomic ──────────────────────────────────────────────


def test_5_a_refused_second_row_rolls_back_the_whole_file(db, spec, tmp_path):
    ok, bad = _paper(db, 1), _paper(db, 2)
    db.update_status(bad, "FT_SCREENED_OUT")          # FT_SCREENED_OUT allows no transition
    path = _file(tmp_path, [{"paper_id": ok, "decision": "FT_ELIGIBLE"},
                            {"paper_id": bad, "decision": "FT_ELIGIBLE"}])
    with pytest.raises(ValueError, match="Invalid transition"):
        _import(db, spec, path)
    assert _count(db, "ft_screening_adjudication") == 0
    assert _count(db, "paper_events") == 0
    assert _status(db, ok) == "FT_FLAGGED" and _status(db, bad) == "FT_SCREENED_OUT"
    (m,) = _manifests(db)
    assert m["end_status"] == "aborted"
    assert f"paper {bad}" in m["end_reason"] and "FT_SCREENED_OUT → FT_ELIGIBLE" in m["end_reason"]
    assert not db._conn.in_transaction


def test_6_an_exception_at_the_event_write_writes_nothing_and_fails_the_run(
        db, spec, tmp_path):
    pid = _paper(db, 1)
    path = _file(tmp_path, [{"paper_id": pid, "decision": "FT_ELIGIBLE"}])
    with patch("engine.core.events.write_paper_event",
               side_effect=RuntimeError("event write failed")):
        with pytest.raises(RuntimeError, match="event write failed"):
            _import(db, spec, path)
    assert _count(db, "ft_screening_adjudication") == 0 and _count(db, "paper_events") == 0
    assert _status(db, pid) == "FT_FLAGGED"
    (m,) = _manifests(db)
    assert m["end_status"] == "failed" and m["end_reason"] == "event write failed"


# ── 7, 11 — validation rejects before any write (D3, D6) ────────────


def test_7_a_validation_rejection_writes_no_manifest(db, spec, tmp_path):
    pid = _paper(db, 1)
    result = _import(db, spec, _file(tmp_path, [{"paper_id": pid, "decision": "INCLUDE"}]))
    assert result["stats"]["invalid"] == 1
    assert _manifests(db) == [] and _count(db, "ft_screening_adjudication") == 0
    assert _status(db, pid) == "FT_FLAGGED"


@pytest.mark.parametrize("bad_id", [None, "7", 7.0, True, 0])
def test_11_a_missing_or_non_integer_paper_id_is_refused_in_json(db, spec, tmp_path, bad_id):
    _paper(db, 1)
    result = _import(db, spec, _file(tmp_path, [{"paper_id": bad_id, "decision": "FT_ELIGIBLE"}]))
    assert result["stats"]["invalid"] == 1 and _manifests(db) == []


@pytest.mark.parametrize("bad_id", [None, "not-a-number"])
def test_11_a_missing_or_non_integer_paper_id_is_refused_in_xlsx(db, spec, tmp_path, bad_id):
    from openpyxl import Workbook
    _paper(db, 1)
    wb = Workbook()
    ws = wb.active
    ws.title = "Review Queue"
    ws.append(["Row #", "Paper ID", "Title", "PI_decision"])
    ws.append([1, bad_id, "Paper 1", "FT_ELIGIBLE"])
    path = tmp_path / "decisions.xlsx"
    wb.save(path)
    result = _import(db, spec, path)
    assert result["stats"]["invalid"] == 1 and _manifests(db) == []
    assert _count(db, "ft_screening_adjudication") == 0


# ── 8 — the stage advances only when nothing is unresolved (D9) ─────


def test_8_a_file_resolving_every_flagged_paper_completes_the_stage(db, spec, tmp_path):
    ensure_workflow_table(db._conn)
    pid = _paper(db, 1)
    _import(db, spec, _file(tmp_path, [{"paper_id": pid, "decision": "FT_ELIGIBLE"}]))
    row = _one(db, "SELECT status, metadata FROM workflow_state WHERE stage_name = ?", STAGE)
    assert tuple(row) == ("complete", "1 eligible, 0 screened out (of 1 total)")


def test_8_a_file_leaving_one_unresolved_does_not_complete_the_stage(db, spec, tmp_path):
    ensure_workflow_table(db._conn)
    pid, _left = _paper(db, 1), _paper(db, 2)
    _import(db, spec, _file(tmp_path, [{"paper_id": pid, "decision": "FT_ELIGIBLE"}]))
    assert _one(db, "SELECT status FROM workflow_state WHERE stage_name = ?",
                STAGE)[0] == "pending"


def test_8_a_refused_import_leaves_workflow_state_byte_identical(db, spec, tmp_path):
    ensure_workflow_table(db._conn)
    bad = _paper(db, 1)
    db.update_status(bad, "FT_SCREENED_OUT")
    before = _workflow(db)
    with pytest.raises(ValueError):
        _import(db, spec, _file(tmp_path, [{"paper_id": bad, "decision": "FT_ELIGIBLE"}]))
    assert _workflow(db) == before


# ── 9 — a dirty tree ────────────────────────────────────────────────


def test_9_a_dirty_tree_refuses_before_any_write(db, spec, tmp_path):
    pid = _paper(db, 1)
    with pytest.raises(rm.DirtyTree):
        _import(db, spec, _file(tmp_path, [{"paper_id": pid, "decision": "FT_ELIGIBLE"}]),
                git=DIRTY)
    assert _manifests(db) == []
    assert _count(db, "ft_screening_adjudication") == 0 and _count(db, "paper_events") == 0
    assert _status(db, pid) == "FT_FLAGGED"


# ── 10 — the seam (closes R257's in-progress case) ──────────────────


def test_10_adjudicating_a_flagged_paper_eligible_keeps_the_seam_valid(db, spec, tmp_path):
    pid = _paper(db, 1)
    before = len(eligible_paper_ids(db._conn))
    _import(db, spec, _file(tmp_path, [{"paper_id": pid, "decision": "FT_ELIGIBLE"}]))
    v = validate_prisma_counts(db)
    assert v["valid"] and v["verification_pending"] == 0, v
    assert len(eligible_paper_ids(db._conn)) == before + 1


# ── 12 — complete_stage(commit=...) (D4) ────────────────────────────


def test_12_complete_stage_commits_by_default(db):
    complete_stage(db._conn, STAGE, metadata="m")
    assert not db._conn.in_transaction
    assert _one(db, "SELECT status FROM workflow_state WHERE stage_name = ?", STAGE)[0] == "complete"


def test_12_complete_stage_commit_false_leaves_the_update_uncommitted(db):
    ensure_workflow_table(db._conn)
    complete_stage(db._conn, STAGE, metadata="m", commit=False)
    assert db._conn.in_transaction
    db._conn.rollback()
    assert _one(db, "SELECT status FROM workflow_state WHERE stage_name = ?", STAGE)[0] == "pending"


def test_12_complete_stage_commit_false_raises_when_no_row_is_touched(db):
    ensure_workflow_table(db._conn)
    db._conn.execute("DELETE FROM workflow_state WHERE stage_name = ?", (STAGE,))
    db._conn.commit()
    with pytest.raises(RuntimeError, match="touched 0 rows"):
        complete_stage(db._conn, STAGE, commit=False)


# ── 13 — open_run's inputs keyword (D1) ─────────────────────────────

#: Every key a manifest body carries without `selection_bound` or `inputs` —
#: the shape before D1.
_BODY_KEYS = {"run_uid", "review_id", "run_kind", "git", "engine_state", "spec_hash",
              "codebook", "libraries", "host", "started_at", "cloud_arms",
              "payload_description", "stages", "pins"}


def _open(db, spec, **kw):
    cb = load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    h = rm.open_run(db._conn, spec, kind="import", stages=(), codebook=cb, git=CLEAN,
                    digest_fn=lambda m: "", **kw)
    return json.loads(_one(db, "SELECT manifest_json FROM run_manifests WHERE run_id = ?",
                           h.run_id)[0])


def test_13_open_run_without_inputs_writes_todays_body_shape(db, spec):
    assert set(_open(db, spec)) == _BODY_KEYS


def test_13_open_run_with_inputs_records_them(db, spec):
    body = _open(db, spec, inputs={"data/x/d.json": "e" * 64})
    assert set(body) == _BODY_KEYS | {"inputs"}
    assert body["inputs"] == {"data/x/d.json": "e" * 64}


# ── 14 — the R-V1 note: an adjudicated primary flag is not re-verified ─


def test_14_a_primary_flag_adjudicated_eligible_is_not_selected_by_the_verifier(
        db, spec, tmp_path):
    from engine.agents import ft_screener as ft
    pid = _paper(db, 1, via_verifier=False)
    assert _count(db, "ft_verification_decisions", "paper_id = ?", pid) == 0
    _import(db, spec, _file(tmp_path, [{"paper_id": pid, "decision": "FT_ELIGIBLE"}]))
    assert _status(db, pid) == "FT_ELIGIBLE" and pid in eligible_paper_ids(db._conn)
    run_id = rm.open_run(db._conn, spec, kind="screening",
                         stages=("ft_screen_primary", "ft_screen_verifier"),
                         codebook=load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml"),
                         git=CLEAN, digest_fn=lambda m: "a" * 64).run_id
    with patch.object(ft, "ft_verify_paper",
                      side_effect=AssertionError("the verifier must not be called")):
        stats = ft.run_ft_verification(db, spec, run_id=run_id)
    assert stats["total"] == 0
