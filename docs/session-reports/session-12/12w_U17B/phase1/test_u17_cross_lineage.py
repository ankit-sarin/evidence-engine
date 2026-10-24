"""U17 (INT-i12-45; R703): the cross-family rule as a base-lineage check.

R703(b) "family" is the lineage of the base model the served weights carry (deepseek-r1:32b is qwen);
R703(c) one top-level `model_lineage` map in the spec, tokens from a closed vocabulary in engine code;
R703(d) coverage refused at spec load and re-checked at open_run; R703(e) the extraction→audit pair
refused at open_run before any manifest row, the extractor read from the arm's pin in an audit-only
run when the arm is pinned, else from the spec; R703(f) model, lineage and server family recorded per
declared stage in manifest_json.

Phase 1 (12w-U17B): written under docs/session-reports/session-12/12w_U17B/phase1/ and run against
HEAD there; it enters tests/ with the fix (R695(b)). Fresh databases under tmp_path, git state and
digests injected; the one /api/tags read (T3) is answered by a patched `httpx.get`, so nothing reaches
a model server.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import yaml

from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.review_paths import load_spec_for
from engine.core.review_spec import CloudConfig, ReviewSpecError, load_review_spec

REPO = next(p for p in Path(__file__).resolve().parents if (p / "review_specs").is_dir())
LIVE_SPEC = REPO / "review_specs" / "surgical_autonomy.yaml"
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
PINS_CLOSE = REPO / "docs/session-reports/session-12/12v_close/pin_tuples_close.json"
SCREENING_HASH_BEFORE = "e8fa9719b1028ac16f9af72996bc807c8ad8b1071d2420fc5d98ed826f043596"
CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
EXTRACTION = ("extract_pass1", "extract_pass2", "extract_retry_snippet")
ARM = "local_deepseek_r1_32b"
DIGESTS = {"deepseek-r1:32b": "edba8017331d15236e57480eb45406c0d721db77a4cdcf234df500fc2ad3960c",
           "gemma3:27b": "a418f5838eaf7fe2cfe0a3046c8384b68ba43a4435542c942f9db00a5f342203"}

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")


@pytest.fixture
def review(tmp_path):
    from engine.core.database import ReviewDatabase
    db = ReviewDatabase("u17", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    cb = load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    yield db, cb
    db.close()


@pytest.fixture
def spec():
    return load_spec_for("surgical_autonomy")


def _open(db, spec, cb, stages=(*EXTRACTION, "audit"), **kw):
    kw.setdefault("git", CLEAN)
    kw.setdefault("digest_fn", lambda m: "a" * 64)
    return rm.open_run(db._conn, spec, kind=kw.pop("kind", "extraction"), stages=stages,
                       codebook=cb, **kw)


def _manifest_rows(conn):
    return conn.execute("SELECT COUNT(*) FROM run_manifests").fetchone()[0]


def _body(db, run_id):
    return json.loads(db._conn.execute(
        "SELECT manifest_json FROM run_manifests WHERE run_id = ?", (run_id,)).fetchone()[0])


def _with_auditor(spec, model):
    return spec.model_copy(update={"auditor_model": model})


def _write_spec(tmp_path, raw):
    p = tmp_path / "spec.yaml"
    p.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return p


# ── T1, T2: the pair check ────────────────────────────────────────────
def test_t1_a_qwen_lineage_auditor_of_the_deepseek_distillation_is_refused(review, spec):
    """The distillation case: deepseek-r1:32b is lineage qwen (R703(b)), so qwen3:32b auditing it is
    same-lineage although the vendors differ."""
    db, cb = review
    with pytest.raises(rm.SameLineageVerifier) as exc:
        _open(db, _with_auditor(spec, "qwen3:32b"), cb)
    assert issubclass(rm.SameLineageVerifier, rm.RunRefused)
    text = str(exc.value)
    for needle in ("extract_pass1", "audit", "deepseek-r1:32b", "qwen3:32b", "qwen", "spec"):
        assert needle in text, needle
    assert _manifest_rows(db._conn) == 0


def test_t2_an_auditor_identical_to_the_extractor_is_refused(review, spec):
    db, cb = review
    with pytest.raises(rm.SameLineageVerifier):
        _open(db, _with_auditor(spec, spec.extraction_models.extractor), cb)
    assert _manifest_rows(db._conn) == 0


# ── T3: the live pairing opens and is recorded ───────────────────────
def _tags_response(families):
    payload = {"models": [{"name": m, "digest": DIGESTS.get(m, "c" * 64),
                           "details": {"family": f, "families": [f]}} for m, f in families.items()]}
    return SimpleNamespace(status_code=200, json=lambda: payload, text=json.dumps(payload))


def test_t3_the_live_pairing_opens_and_records_model_lineage_and_server_family(review, spec):
    db, cb = review
    tags = _tags_response({"deepseek-r1:32b": "qwen2", "gemma3:27b": "gemma3"})
    with patch("engine.utils.ollama_client.httpx.get", return_value=tags):
        h = _open(db, spec, cb, digest_fn=None)
    stages = _body(db, h.run_id)["stages"]
    for s in EXTRACTION:
        assert (stages[s]["model"], stages[s]["lineage"], stages[s]["server_family"]) == \
            ("deepseek-r1:32b", "qwen", "qwen2"), s
    assert (stages["audit"]["model"], stages["audit"]["lineage"], stages["audit"]["server_family"]) == \
        ("gemma3:27b", "gemma", "gemma3")


# ── T4, T5: refused at spec load ─────────────────────────────────────
def test_t4_a_default_factory_model_without_an_entry_is_refused_at_load(tmp_path):
    """The live spec has no `audit:` block; `AuditModels`' default supplies gemma3:27b. A map without
    that model is refused, naming the model and the field (R703(d))."""
    raw = yaml.safe_load(LIVE_SPEC.read_text())
    assert "audit" not in raw
    raw["model_lineage"] = {m: t for m, t in raw["model_lineage"].items() if m != "gemma3:27b"}
    with pytest.raises(ReviewSpecError) as exc:
        load_review_spec(_write_spec(tmp_path, raw))
    assert "gemma3:27b" in str(exc.value) and "audit.model" in str(exc.value)


@pytest.mark.parametrize("token", ["Qwen", "qwen3", "alibaba", ""])
def test_t5_a_lineage_token_outside_the_vocabulary_is_refused_at_load(tmp_path, token):
    raw = yaml.safe_load(LIVE_SPEC.read_text())
    raw["model_lineage"] = {**raw["model_lineage"], "qwen3:8b": token}
    with pytest.raises(ReviewSpecError) as exc:
        load_review_spec(_write_spec(tmp_path, raw))
    assert "model_lineage" in str(exc.value)


# ── T6: coverage re-checked at open_run ──────────────────────────────
def test_t6_a_model_copy_auditor_with_no_entry_is_refused_at_open(review, spec):
    """`model_copy` skips validation (12w_U17A §6 finding 6), so open_run re-checks coverage."""
    db, cb = review
    with pytest.raises(rm.ModelLineageMissing) as exc:
        _open(db, _with_auditor(spec, "unmapped-auditor:1b"), cb)
    assert issubclass(rm.ModelLineageMissing, rm.RunRefused)
    assert "unmapped-auditor:1b" in str(exc.value) and "audit" in str(exc.value)
    assert _manifest_rows(db._conn) == 0


# ── T7, T8: the audit-only run ───────────────────────────────────────
def _pin_the_arm(db, spec, cb):
    h = _open(db, spec, cb, stages=EXTRACTION)
    rm.bind_extraction_arm(db._conn, h.run_id, arm=ARM)
    rm.close_run(db._conn, h.run_id, "completed")


def _respec_extractor(spec, model):
    return spec.model_copy(update={
        "extraction_models": spec.extraction_models.model_copy(update={"extractor": model}),
        "arms": [a.model_copy(update={"model": model}) if a.name == ARM else a for a in spec.arms]})


def test_t7_an_audit_only_run_on_a_pinned_arm_reads_the_extractor_from_the_pin(review, spec):
    """The pin says deepseek-r1:32b (qwen). The spec is then changed to say gemma3:27b: a gemma auditor
    opens (the pin, not the spec, is the extractor), recorded as source 'pin'; a qwen auditor is
    refused although the spec's extractor would have passed it."""
    db, cb = review
    _pin_the_arm(db, spec, cb)
    drifted = _respec_extractor(spec, "gemma3:27b")
    h = _open(db, _with_auditor(drifted, "gemma3:27b"), cb, stages=("audit",))
    assert _body(db, h.run_id)["lineage_check"]["extractor_source"] == "pin"
    before = _manifest_rows(db._conn)
    with pytest.raises(rm.SameLineageVerifier) as exc:
        _open(db, _with_auditor(drifted, "qwen3:32b"), cb, stages=("audit",))
    assert "pin" in str(exc.value) and "deepseek-r1:32b" in str(exc.value)
    assert _manifest_rows(db._conn) == before


def test_t8_an_audit_only_run_on_an_unpinned_arm_reads_the_spec(review, spec):
    db, cb = review
    h = _open(db, spec, cb, stages=("audit",))
    check = _body(db, h.run_id)["lineage_check"]
    assert check["extractor_source"] == "spec"
    assert check["extractor_models"] == {s: "deepseek-r1:32b" for s in EXTRACTION}
    assert (check["audit_model"], check["audit_lineage"]) == ("gemma3:27b", "gemma")


# ── T9: nothing that is sent moves ───────────────────────────────────
def test_t9_the_new_live_spec_moves_no_prompt_hash_options_hash_or_screening_hash():
    """R703(h), in R695(d)'s form: the hash components, not the whole tuple."""
    from engine.agents.extractor import extraction_stages
    spec0 = load_review_spec(LIVE_SPEC)
    assert spec0.model_lineage, "the live spec carries model_lineage"
    assert spec0.screening_hash() == SCREENING_HASH_BEFORE
    ref = json.loads(PINS_CLOSE.read_text())
    cb = load_codebook(LIVE_CODEBOOK)
    for label, elicit in (("non_elicited", False), ("elicited", True)):
        s = spec0.model_copy(update={"extraction_models": spec0.extraction_models.model_copy(
            update={"elicitation": elicit})})
        stages = [*extraction_stages(s), "audit", "preflight"]
        arms = {st: a for st in stages if (a := rm._stage_arm(s, st))}
        res = rm.resolve_run(s, stages, digest_fn=DIGESTS.__getitem__, preflight_models=sorted(DIGESTS),
                             codebook_path=cb.path, arms_by_stage=arms)
        assert {k: r.prompt_hash for k, r in sorted(res.items())} == ref[label]["prompt_hashes"], label
        assert {k: r.options_hash for k, r in res.items() if k in ref[label]["tuple"]["stages"]} == \
            {k: v["options_hash"] for k, v in ref[label]["tuple"]["stages"].items()}, label


# ── T10: a cloud model ───────────────────────────────────────────────
def test_t10_a_cloud_model_loads_with_its_provider_lineage_and_records_no_server_family(review, spec):
    db, cb = review
    arm = next(a for a in spec.arms if a.provider == "openai")
    assert spec.model_lineage[arm.model] == "openai"
    enabled = spec.model_copy(update={"cloud": CloudConfig(enabled_arms=[arm.name],
                                                           prices=spec.cloud.prices)})
    h = _open(db, enabled, cb, stages=(), cloud_arms=[arm.name], payload_description="p")
    row = _body(db, h.run_id)["stages"][f"cloud:{arm.name}"]
    assert (row["model"], row["lineage"], row["server_family"]) == (arm.model, "openai", None)
