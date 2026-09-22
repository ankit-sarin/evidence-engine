"""Run open, refusals, arm pinning, call recording and cloud opt-in (T4, T5, G4).

MANIFEST-01 Phase 2a. Every database here is a fresh `ReviewDatabase` under
tmp_path (so migration 020 is applied by the runner, as on any review), the git
state is injected, and the digest function is a stand-in — nothing reaches git,
a model server or a cloud provider (R45).
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from engine.core import events
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.effective import PRE_MANIFEST
from engine.core.review_paths import load_spec_for
from engine.core.review_spec import CloudConfig
from engine.utils import ollama_client as oc

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
SPEC_PATH = REPO / "review_specs" / "surgical_autonomy.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")

CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
DIGEST = "a" * 64
EXTRACTION = ("extract_pass1", "extract_pass2", "extract_retry_snippet")
LOCAL_ARM = "local_deepseek_r1_32b"


def _digest(model):
    return DIGEST


@pytest.fixture
def review(tmp_path):
    from engine.core.database import ReviewDatabase
    db = ReviewDatabase("rm", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    cb = load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    yield db, cb
    db.close()


@pytest.fixture
def spec():
    return load_spec_for("surgical_autonomy")


def _open(db, spec, cb, **kw):
    kw.setdefault("stages", EXTRACTION)
    kw.setdefault("git", CLEAN)
    kw.setdefault("digest_fn", _digest)
    return rm.open_run(db._conn, spec, kind=kw.pop("kind", "extraction"), codebook=cb, **kw)


def _count(conn, table):
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# ── the manifest ─────────────────────────────────────────────────────
def test_a_run_writes_its_manifest_and_one_row_per_stage(review, spec):
    db, cb = review
    h = _open(db, spec, cb, preflight_models=["deepseek-r1:32b"], stages=EXTRACTION + ("preflight",))
    row = db._conn.execute("SELECT * FROM run_manifests WHERE run_id = ?", (h.run_id,)).fetchone()
    assert row["git_commit"] == "b" * 40 and row["git_dirty"] == 0 and row["engine_state"] is None
    assert row["spec_hash"] == rm.spec_hash(spec) and row["codebook_hash"] == cb.semantic_hash
    libs = json.loads(row["library_versions_json"])
    assert set(libs) == {"ollama", "openai", "anthropic"}           # R75
    assert row["cloud_arms_json"] == "[]" and row["ended_at"] is None
    stages = {r["stage"]: r for r in db._conn.execute(
        "SELECT * FROM run_stage_configs WHERE run_id = ?", (h.run_id,))}
    assert set(stages) == set(EXTRACTION) | {"preflight:deepseek-r1:32b"}
    for r in stages.values():
        assert r["model_digest"] == DIGEST and r["keep_alive"] == "-1"
    assert stages["extract_pass1"]["arm_name"] == LOCAL_ARM
    assert stages["preflight:deepseek-r1:32b"]["arm_name"] is None


def test_the_manifest_exists_before_the_first_model_call_and_every_call_is_recorded(
        review, spec, monkeypatch):
    """G4: at the moment the first request reaches the client, the manifest and
    its stage rows are already committed; the call itself lands in run_calls."""
    db, cb = review
    seen = {}

    class Fake:
        _client = SimpleNamespace(base_url="http://capture.invalid")

        def show(self, model):
            return SimpleNamespace(modelinfo={"general.context_length": 131_072})

        def chat(self, **kw):
            probe = sqlite3.connect(db.db_path)
            seen["manifests"] = _count(probe, "run_manifests")
            seen["stages"] = _count(probe, "run_stage_configs")
            probe.close()
            seen["kw"] = kw
            n = max(1, int(oc.message_chars(kw["messages"]) * 0.3))
            return SimpleNamespace(message=SimpleNamespace(content="draft", thinking="t"),
                                   prompt_eval_count=n, done_reason="stop")
    monkeypatch.setattr(oc, "_client", Fake())
    oc.clear_ceiling_cache()

    from engine.agents.extractor import extract_pass1_reasoning
    from engine.core.effective_config import stage_config
    h = _open(db, spec, cb)
    with rm.active_run(db._conn, h.run_id):
        extract_pass1_reasoning("a prompt", cfg=stage_config("extract_pass1", spec))
    assert seen["manifests"] == 1 and seen["stages"] == len(EXTRACTION)
    call = db._conn.execute("SELECT * FROM run_calls").fetchone()
    assert call["run_id"] == h.run_id and call["stage"] == "extract_pass1"
    assert call["request_hash"] == rm.request_hash(seen["kw"])
    assert call["response_digest"] == rm.response_digest("draft", "t")


def test_run_id_is_on_every_event_row_a_run_writes(review, spec):
    db, cb = review
    h = _open(db, spec, cb)
    db._conn.execute("INSERT INTO papers (id, title, source, status, created_at, updated_at) "
                     "VALUES (1, 't', 's', 'EXTRACTED', 'x', 'x')")
    events.write_paper_event(db._conn, event_type="extracted", paper_id=1, to_state="extracted",
                             actor_kind="engine", actor_role="system", actor_name="e",
                             run_id=h.run_id)
    events.write_field_event(db._conn, event_type="asserted", paper_id=1, field_name="f",
                             arm=LOCAL_ARM, value="v", source_snippet="v", actor_kind="model",
                             actor_role="extractor", actor_name="m", run_id=h.run_id)
    for table in ("paper_events", "field_events"):
        assert db._conn.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id IS NOT ? ",
                                (h.run_id,)).fetchone()[0] == 0, table


def test_a_run_records_its_end_once(review, spec):
    db, cb = review
    h = _open(db, spec, cb)
    rm.close_run(db._conn, h.run_id, "completed")
    with pytest.raises(sqlite3.IntegrityError):
        rm.close_run(db._conn, h.run_id, "failed")


# ── the refusals, each before anything is written ────────────────────
def _nothing_written(conn):
    return _count(conn, "run_manifests") == 0 and _count(conn, "run_stage_configs") == 0


def test_a_dirty_tree_is_refused(review, spec):
    db, cb = review
    with pytest.raises(rm.DirtyTree, match="uncommitted"):
        _open(db, spec, cb, git=rm.GitState(commit="c" * 40, dirty=True, tag=None))
    assert _nothing_written(db._conn)


def test_an_untagged_head_is_allowed_with_engine_state_null(review, spec):
    db, cb = review
    h = _open(db, spec, cb)
    row = db._conn.execute("SELECT git_tag, engine_state FROM run_manifests WHERE run_id = ?",
                           (h.run_id,)).fetchone()
    assert tuple(row) == (None, None)


def test_a_pre_manifest_arm_is_refused(review, spec):
    db, cb = review
    events.register_arm(db._conn, LOCAL_ARM, "model", configuration_marker=PRE_MANIFEST)
    db._conn.commit()
    with pytest.raises(rm.PreManifestArm, match="R59"):
        _open(db, spec, cb)
    assert _nothing_written(db._conn)


def test_a_retired_arm_is_refused(review, spec):
    db, cb = review
    _open(db, spec, cb)
    events.retire_arm(db._conn, LOCAL_ARM)
    db._conn.commit()
    with pytest.raises(rm.RetiredArm):
        _open(db, spec, cb)


def test_a_declared_arm_is_pinned_at_its_first_manifest(review, spec):
    db, cb = review
    assert db._conn.execute("SELECT 1 FROM arms WHERE arm_name = ?", (LOCAL_ARM,)).fetchone() is None
    h = _open(db, spec, cb)
    row = db._conn.execute("SELECT * FROM arms WHERE arm_name = ?", (LOCAL_ARM,)).fetchone()
    assert row["configuration_marker"] == "pinned" and row["pinned_run_id"] == h.run_id
    tup = json.loads(row["configuration_json"])
    assert tup["model"] == "deepseek-r1:32b" and set(tup["stages"]) == set(EXTRACTION)
    assert row["pinned_sha256"] == rm.sha256_canonical(tup)


def test_an_unpinned_registered_arm_is_pinned_too(review, spec):
    db, cb = review
    events.register_arm(db._conn, LOCAL_ARM, "model")
    db._conn.commit()
    h = _open(db, spec, cb)
    assert tuple(db._conn.execute("SELECT configuration_marker, pinned_run_id FROM arms "
                                  "WHERE arm_name = ?", (LOCAL_ARM,)).fetchone()) == ("pinned", h.run_id)


def test_the_same_configuration_opens_again_and_keeps_the_first_pin(review, spec):
    db, cb = review
    first = _open(db, spec, cb)
    second = _open(db, spec, cb)
    assert second.run_id != first.run_id
    assert db._conn.execute("SELECT pinned_run_id FROM arms WHERE arm_name = ?",
                            (LOCAL_ARM,)).fetchone()[0] == first.run_id


def test_a_pinned_arm_resolving_differently_is_refused_and_the_difference_named(review, spec):
    db, cb = review
    _open(db, spec, cb)
    changed = spec.model_copy(update={"extraction_models":
                                      spec.extraction_models.model_copy(update={"temperature": 0.2})})
    with pytest.raises(rm.ArmPinMismatch, match="options_hash"):
        _open(db, changed, cb)
    assert _count(db._conn, "run_manifests") == 1


def test_a_changed_model_digest_is_a_pin_mismatch(review, spec):
    db, cb = review
    _open(db, spec, cb)
    with pytest.raises(rm.ArmPinMismatch, match="model_digest"):
        _open(db, spec, cb, digest_fn=lambda m: "e" * 64)


def test_a_digest_failure_refuses_the_run(review, spec):
    db, cb = review
    from engine.utils.ollama_client import ModelDigestError

    def fail(model):
        raise ModelDigestError("no digest")
    with pytest.raises(ModelDigestError):
        _open(db, spec, cb, digest_fn=fail)
    assert _nothing_written(db._conn)


def test_a_review_session_is_a_run_with_zero_stage_rows(review, spec):
    """R68/D-4."""
    db, cb = review
    h = rm.open_review_session(db._conn, spec, codebook=cb, git=CLEAN)
    assert db._conn.execute("SELECT run_kind FROM run_manifests WHERE run_id = ?",
                            (h.run_id,)).fetchone()[0] == "review_session"
    assert db._conn.execute("SELECT COUNT(*) FROM run_stage_configs WHERE run_id = ?",
                            (h.run_id,)).fetchone()[0] == 0


# ── cloud opt-in (S3g, T5) ───────────────────────────────────────────
def _enabled(spec, *names):
    return spec.model_copy(update={"cloud": CloudConfig(enabled_arms=list(names),
                                                        prices=spec.cloud.prices)})


def _cloud_arm(spec, provider):
    return next(a.name for a in spec.arms if a.provider == provider)


def test_a_cloud_arm_the_spec_does_not_enable_is_refused(review, spec):
    db, cb = review
    with pytest.raises(rm.CloudArmNotEnabled):
        _open(db, spec, cb, stages=(), cloud_arms=[_cloud_arm(spec, "openai")],
              payload_description="p")
    assert _nothing_written(db._conn)


def test_an_enabled_cloud_run_records_its_arms_and_payload_description(review, spec):
    from engine.cloud.base import PAYLOAD_DESCRIPTION
    db, cb = review
    arm = _cloud_arm(spec, "anthropic")
    h = _open(db, _enabled(spec, arm), cb, stages=(), cloud_arms=[arm],
              payload_description=PAYLOAD_DESCRIPTION)
    row = db._conn.execute("SELECT cloud_arms_json, payload_description FROM run_manifests "
                           "WHERE run_id = ?", (h.run_id,)).fetchone()
    assert json.loads(row[0]) == [arm] and row[1] == PAYLOAD_DESCRIPTION
    stage = db._conn.execute("SELECT provider, model_digest, arm_name FROM run_stage_configs "
                             "WHERE run_id = ?", (h.run_id,)).fetchone()
    assert tuple(stage) == ("anthropic", None, arm)


def test_the_cli_refuses_an_arm_outside_enabled_arms_before_any_request(tmp_path, spec):
    from scripts import run_cloud_extraction as cli
    with patch.dict(cli.EXTRACTORS, {"openai": _explode, "anthropic": _explode}):
        with pytest.raises(rm.CloudArmNotEnabled, match="nothing was sent"):
            cli.select_cloud_arms(spec, [_cloud_arm(spec, "openai")])
        with patch("sys.argv", ["x", "--review", "surgical_autonomy",
                                "--arm", _cloud_arm(spec, "openai"),
                                "--db", str(tmp_path / "none.db")]), \
             patch("engine.utils.background.maybe_background"):
            with pytest.raises(SystemExit) as e:
                cli.main()
            assert e.value.code == 2


def test_no_arm_and_nothing_enabled_proceeds_and_sends_nothing(tmp_path, spec, capsys):
    from scripts import run_cloud_extraction as cli
    assert cli.select_cloud_arms(spec, None) == []
    with patch.dict(cli.EXTRACTORS, {"openai": _explode, "anthropic": _explode}), \
         patch("sys.argv", ["x", "--review", "surgical_autonomy",
                            "--db", str(tmp_path / "none.db")]), \
         patch("engine.utils.background.maybe_background"):
        cli.main()
    assert "nothing leaves the machine" in capsys.readouterr().out


def test_an_enabled_arm_named_on_the_cli_is_selected(spec):
    from scripts import run_cloud_extraction as cli
    arm = _cloud_arm(spec, "openai")
    assert cli.select_cloud_arms(_enabled(spec, arm), [arm]) == [arm]


def test_a_cloud_call_records_the_hash_of_its_complete_payload(review, spec):
    """C16: run_calls.request_hash is over system message, user turn AND parameters."""
    from engine.cloud.openai_extractor import OpenAIExtractor
    db, cb = review
    arm = _cloud_arm(spec, "openai")
    h = _open(db, _enabled(spec, arm), cb, stages=(), cloud_arms=[arm],
              payload_description="p")
    sent = {}

    class Completions:
        def create(self, **kw):
            sent.update(kw)
            return "response"
    with patch("engine.cloud.openai_extractor.openai.OpenAI") as cls:
        cls.return_value = SimpleNamespace(chat=SimpleNamespace(completions=Completions()))
        ex = OpenAIExtractor(db.db_path, str(SPEC_PATH), api_key="k", arm_name=arm)
        ex.run_id = h.run_id
        ex.send(None, "prompt text")
        call = ex._conn.execute("SELECT stage, request_hash FROM run_calls").fetchone()
        ex.close()
    assert call[0] == f"cloud:{arm}"
    assert call[1] == rm.request_hash(sent) == ex.last_request_hash
    assert set(sent) == {"model", "messages", "reasoning_effort", "response_format"}


def _explode(*a, **kw):
    raise AssertionError("an extractor was constructed; the refusal must come first")
