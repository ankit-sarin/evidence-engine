"""P1 unit U04, commit 2 (R612, R616(a)): what the pin records.

INT-i12-4 — the elicited arm's pin records the segmenter library version,
`MIN_UNIT_TOKENS` and `UNITS_VERSION`, so a change to how a paper is cut into
units refuses the next run instead of passing unseen.
C34 — `canonical_absence_sentinel` is inside the codebook's semantic hash.
INT-l12-1 — an elicited run declares and pins `elicitation_pass1` and
`extract_pass2` only; `extract_retry_snippet`, which the elicited path never
calls, is not in its stage set, and a call on it under the run is refused (C54).

Scratch databases under `tmp_path`; the model client is a fake. Nothing here
reaches a model, a service or the live review.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from engine.agents import extractor as E
from engine.core import effective_config as ec
from engine.core import run_manifest as rm
from engine.core.codebook import compute_semantic_hash, load_codebook
from engine.core.database import ReviewDatabase
from engine.core.review_paths import load_spec_for
from engine.elicitation import units as U
from engine.utils import ollama_client as oc

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")

CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
DIGEST = "a" * 64
ARM = "local_deepseek_r1_32b"
ELICITED_STAGES = ("elicitation_pass1", "extract_pass2")


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


@pytest.fixture(scope="module")
def elicited(spec):
    em = spec.extraction_models.model_copy(update={"elicitation": True})
    return spec.model_copy(update={"extraction_models": em})


@pytest.fixture
def review(tmp_path):
    db = ReviewDatabase("pin", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    cb = load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    yield db, cb
    db.close()


def _open(db, spec, cb, stages=None):
    return rm.open_run(db._conn, spec, kind="extraction",
                       stages=stages or [*E.extraction_stages(spec), "audit"],
                       codebook=cb, git=CLEAN, digest_fn=lambda m: DIGEST)


def _pin(spec, cb) -> dict:
    stages = [*E.extraction_stages(spec), "audit"]
    arms = {s: a for s in stages if (a := rm._stage_arm(spec, s))}
    resolved = rm.resolve_run(spec, stages, digest_fn=lambda m: DIGEST,
                              codebook_path=cb.path, arms_by_stage=arms)
    return rm.pin_tuple(spec, ARM, resolved, cb.semantic_hash, rm.library_versions())


# ── INT-i12-4 — R589 (row 5(b)), N02 ──────────────────────────────────
def test_a_segmentation_change_moves_the_elicited_arm_pin(review, elicited, spec, monkeypatch):
    import importlib.metadata
    db, cb = review
    base = _pin(elicited, cb)
    assert base["segmentation"] == {"pysbd": importlib.metadata.version("pysbd"),
                                    "min_unit_tokens": U.MIN_UNIT_TOKENS,
                                    "units_version": U.UNITS_VERSION}
    assert rm.library_versions()["pysbd"] == importlib.metadata.version("pysbd")
    # an arm that cuts no units records no segmentation
    assert "segmentation" not in _pin(spec, cb)

    first = _open(db, elicited, cb)
    E.verify_extraction_run(db._conn, elicited, first.run_id)    # the arm is pinned here
    real_versions = rm.library_versions

    def changes():
        yield "segmentation.pysbd", lambda m: m.setattr(
            rm, "library_versions", lambda: {**real_versions(), "pysbd": "9.9.9"})
        yield "segmentation.min_unit_tokens", lambda m: m.setattr(
            U, "MIN_UNIT_TOKENS", U.MIN_UNIT_TOKENS + 1)
        yield "segmentation.units_version", lambda m: m.setattr(U, "UNITS_VERSION", "units-3")

    for key, change in changes():
        with monkeypatch.context() as m:
            change(m)
            assert _pin(elicited, cb) != base, key
            with pytest.raises(rm.ArmPinMismatch, match=key) as exc:
                _open(db, elicited, cb)
            assert "prompt_hash" not in str(exc.value)
    # unchanged, the same configuration still opens
    assert _pin(elicited, cb) == base
    _open(db, elicited, cb)


# ── C34 — R589 (row 14) ───────────────────────────────────────────────
def test_semantic_hash_moves_on_a_canonical_sentinel_edit(tmp_path):
    doc = yaml.safe_load(LIVE_CODEBOOK.read_text())
    assert doc["canonical_absence_sentinel"] == "NR" and "NOT_FOUND" in doc["absence_sentinels"]
    other = {**doc, "canonical_absence_sentinel": "NOT_FOUND"}
    assert compute_semantic_hash(doc) != compute_semantic_hash(other)

    # through the loader, on two files that differ in that key only
    a, b = tmp_path / "a" / "extraction_codebook.yaml", tmp_path / "b" / "extraction_codebook.yaml"
    for path, d in ((a, doc), (b, other)):
        path.parent.mkdir()
        path.write_text(yaml.safe_dump(d, sort_keys=False))
    assert load_codebook(a).semantic_hash != load_codebook(b).semantic_hash
    assert load_codebook(b).canonical_absence_sentinel == "NOT_FOUND"

    # companion: a key no prompt reads still does not move it
    assert compute_semantic_hash({**doc, "version": "9.9"}) == compute_semantic_hash(doc)


# ── INT-l12-1 — R612(d), C54 ──────────────────────────────────────────
class _Client:
    def __init__(self):
        self.calls, self.shows = [], []
        self._client = SimpleNamespace(base_url="http://pin.invalid")

    def show(self, model):
        self.shows.append(model)
        return SimpleNamespace(modelinfo={"general.context_length": 131_072})

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(message=SimpleNamespace(content="OK", thinking="R"),
                               prompt_eval_count=1, done_reason="stop", eval_count=1)


def test_an_elicited_run_declares_and_pins_two_stages_and_refuses_the_snippet_retry(
        review, elicited, spec, monkeypatch):
    db, cb = review
    assert E.extraction_stages(elicited) == ELICITED_STAGES
    assert E.extraction_stages(spec) == ("extract_pass1", "extract_pass2",
                                         "extract_retry_snippet")

    # the pipeline's own manifest opener declares the same set
    import scripts.run_pipeline as rp
    monkeypatch.setattr(rm, "git_state", lambda: CLEAN)
    monkeypatch.setattr(oc, "fetch_model_digest", lambda m: DIGEST)
    from engine.utils import ollama_preflight as pf
    monkeypatch.setattr(pf, "read_ollama_env", lambda: ("", {}))  # U19: the open read (R734(c)(i))
    run_id = rp._open_run_manifest(db, elicited, rp.STAGES.index("extract"))
    conn = db._conn
    declared = [r[0] for r in conn.execute(
        "SELECT stage FROM run_stage_configs WHERE run_id = ? AND arm_name = ? ORDER BY stage",
        (run_id, ARM))]
    assert declared == sorted(ELICITED_STAGES)
    assert "extract_retry_snippet" not in [r[0] for r in conn.execute(
        "SELECT stage FROM run_stage_configs WHERE run_id = ?", (run_id,))]
    assert E.verify_extraction_run(conn, elicited, run_id) == DIGEST   # extract pins (U05)
    pinned = json.loads(conn.execute(
        "SELECT configuration_json FROM arms WHERE arm_name = ?", (ARM,)).fetchone()[0])
    assert sorted(pinned["stages"]) == sorted(ELICITED_STAGES)
    assert E.verify_extraction_run(conn, elicited, run_id) == DIGEST

    # C54: a snippet-retry call under the elicited run is refused before the send
    import httpx
    monkeypatch.setattr(httpx.Client, "send",
                        lambda *a, **k: pytest.fail("a real HTTP request was attempted"))
    client = _Client()
    monkeypatch.setattr(oc, "_client", client)
    oc.clear_ceiling_cache()
    cfg = ec.stage_config("extract_retry_snippet", elicited)
    with rm.active_run(conn, run_id):
        with pytest.raises(rm.UndeclaredCall) as exc:
            oc.ollama_chat(messages=[{"role": "user", "content": "hello"}], **cfg.kwargs())
    assert exc.value.stage_key == "extract_retry_snippet" and exc.value.declared_model is None
    assert client.calls == [] and client.shows == []
    assert conn.execute("SELECT COUNT(*) FROM run_calls").fetchone()[0] == 0
    oc.clear_ceiling_cache()
