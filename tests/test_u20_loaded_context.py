"""U20 commit 2 (R744(c), R745, N25): under an active run, each completed call's ceiling is
compared with the context `/api/ps` reports for the model.

After the call and before the post-call input-fit checks, `ollama_chat` reads `/api/ps` through
the chat client and takes the `context_length` of the entry whose name and digest are the
call's. A difference, no such entry, or a failed read raises `LoadedContextMismatch`, a
`RunRefused`; the call is recorded with outcome 'error' (R745(d)), and both runners close the
run 'aborted' over the tuple of mid-run server-change refusals (R745(b)). Preflight re-raises
any `RunRefused` (R745(c)). A call naming no stage, a run `open_run` did not register, and a call
outside a run are not compared (R745(e)).

Every fake `ps` is built from a body committed by the 12aa-U20P probe with only name, digest or
context_length changed, and every fake `show` from a committed body, each through the library's
own response class (R743(b)'s rule).
"""
from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import ollama
import pytest

from engine.agents import ft_screener as ft
from engine.core import effective_config as ec
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.extraction_events import is_run_fault
from engine.core.review_paths import load_spec_for
from engine.utils import ollama_client as oc
from engine.utils import ollama_preflight as pf

REPO = Path(__file__).resolve().parent.parent
PROBE = REPO / "docs/session-reports/session-12/12aa_U20P"
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")

CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
A, B = "a" * 64, "b" * 64
MESSAGES = [{"role": "user", "content": "hello"}]
CEILING = 131_072  # gemma3:27b's trained context, the audit model's (reads_u20b.md)


def _ps(*entries) -> ollama.ProcessResponse:
    """`/api/ps` as received, one model per (name, digest, context_length)."""
    template = json.loads((PROBE / "load/P3_ps.json").read_text(encoding="utf-8"))["models"][0]
    return ollama.ProcessResponse(models=[
        {**template, "name": name, "model": name, "digest": digest, "context_length": ctx}
        for name, digest, ctx in entries])


class FakeClient:
    """Answers `show` from a committed body, `chat` with a fitting count, `ps` as set."""

    def __init__(self):
        self._client = SimpleNamespace(base_url="http://u20c.invalid")
        self.ps_reads = 0
        self.answer = None
        self.calls: list[dict] = []
        self.parameters = None

    def show(self, model):
        body = json.loads((PROBE / "show" / f"{model.replace(':', '_')}.response.json").read_text(encoding="utf-8"))
        if self.parameters is not None:
            body["parameters"] = self.parameters
        return ollama.ShowResponse(**body)

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        chars = oc.message_chars(kwargs.get("messages"))
        return SimpleNamespace(message=SimpleNamespace(content="OK", thinking=None),
                               prompt_eval_count=max(1, int(chars * 0.3)), done_reason="stop", eval_count=1)

    def ps(self):
        self.ps_reads += 1
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


class Digests:
    def __init__(self, value=A):
        self.value = value

    def __call__(self, model):
        return self.value


@pytest.fixture
def fake(monkeypatch):
    client = FakeClient()
    monkeypatch.setattr(oc, "_client", client)
    oc.clear_ceiling_cache()
    yield client
    oc.clear_ceiling_cache()


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


@pytest.fixture
def review(tmp_path):
    db = ReviewDatabase("u20c", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    yield db, load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    db.close()


def _model(spec) -> str:
    return ec.stage_config("audit", spec).model


def _open(db, spec, cb, digests, stages=("audit",), preflight_models=()):
    return rm.open_run(db._conn, spec, kind="screening", stages=stages, codebook=cb,
                       preflight_models=preflight_models, git=CLEAN, digest_fn=digests).run_id


def _audit_call(spec):
    return oc.ollama_chat(messages=MESSAGES, **ec.stage_config("audit", spec).kwargs())


def _mismatch(review, spec, fake):
    """One audit call under a registered run, expected to refuse; the exception and the run's rows."""
    db, cb = review
    run_id = _open(db, spec, cb, Digests())
    with rm.active_run(db._conn, run_id):
        with pytest.raises(rm.LoadedContextMismatch) as exc:
            _audit_call(spec)
    rows = [tuple(r) for r in db._conn.execute(
        "SELECT outcome, outcome_detail FROM run_calls WHERE run_id = ? ORDER BY call_id", (run_id,))]
    return exc.value, rows


# (a)
def test_a_a_loaded_context_equal_to_the_ceiling_completes_and_is_recorded(review, spec, fake, caplog):
    db, cb = review
    fake.answer = _ps((_model(spec), A, CEILING))
    run_id = _open(db, spec, cb, Digests())
    with caplog.at_level("INFO", logger="engine.utils.ollama_client"), rm.active_run(db._conn, run_id):
        _audit_call(spec)
    line = next(r.getMessage() for r in caplog.records if r.getMessage().startswith("input_fit "))
    record = json.loads(line.split(" ", 2)[2])
    assert record["ceiling"] == CEILING and record["loaded_context"] == CEILING
    assert fake.ps_reads == 1
    assert [r[0] for r in db._conn.execute("SELECT outcome FROM run_calls WHERE run_id = ?", (run_id,))] == ["completed"]


# (b)
def test_b_a_loaded_context_lower_than_the_ceiling_refuses(review, spec, fake):
    fake.answer = _ps((_model(spec), A, 8192))
    exc, rows = _mismatch(review, spec, fake)
    assert (exc.model, exc.ceiling, exc.read) == (_model(spec), CEILING, 8192)
    assert str(exc).startswith("LoadedContextMismatch: ")
    for part in (_model(spec), "131072", "8192"):
        assert part in str(exc)
    assert len(fake.calls) == 1
    assert len(rows) == 1 and rows[0][0] == "error"
    assert rows[0][1].startswith("LoadedContextMismatch: ") and "8192" in rows[0][1]


def test_b_the_comparison_precedes_the_post_call_checks(review, spec, fake, monkeypatch):
    fake.answer = _ps((_model(spec), A, 8192))
    monkeypatch.setattr(oc, "_check_input_was_read",
                        lambda *a: pytest.fail("the post-call checks ran before the comparison"))
    exc, _ = _mismatch(review, spec, fake)
    assert exc.read == 8192


# (c)
def test_c_a_loaded_context_higher_than_the_ceiling_refuses(review, spec, fake):
    fake.answer = _ps((_model(spec), A, 262_144))
    exc, _ = _mismatch(review, spec, fake)
    assert (exc.ceiling, exc.read) == (CEILING, 262_144)


# (d)
@pytest.mark.parametrize("entries", [
    pytest.param(lambda m: (), id="no_entries"),
    pytest.param(lambda m: (("llama3.2:3b", A, CEILING),), id="another_name"),
    pytest.param(lambda m: ((m, B, CEILING),), id="the_name_with_another_digest"),
])
def test_d_no_entry_with_the_calls_name_and_digest_refuses_as_absent(review, spec, fake, entries):
    fake.answer = _ps(*entries(_model(spec)))
    exc, _ = _mismatch(review, spec, fake)
    assert exc.read == "absent" and "absent" in str(exc)


# (e)
def test_e_a_failed_read_refuses_naming_the_error(review, spec, fake):
    fake.answer = RuntimeError("the ps read broke")
    exc, _ = _mismatch(review, spec, fake)
    assert "the ps read broke" in str(exc.read) and "the ps read broke" in str(exc)


# (f)
def test_f_the_class_is_a_run_refused_run_fault_in_the_shared_tuple():
    assert issubclass(rm.LoadedContextMismatch, rm.RunRefused)
    assert is_run_fault(rm.LoadedContextMismatch("refused"))
    assert set(rm.MID_RUN_SERVER_REFUSALS) == {rm.ModelDigestChanged, rm.ServerEnvChanged, rm.LoadedContextMismatch}


CLASSES = ["ModelDigestChanged", "ServerEnvChanged", "LoadedContextMismatch"]


def _end(db_path, run_id):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT end_status, end_reason FROM run_manifests WHERE run_id = ?",
                            (run_id,)).fetchone()
    finally:
        conn.close()


@pytest.mark.parametrize("name", CLASSES)
def test_f_run_pipeline_closes_aborted(review, spec, fake, monkeypatch, name):
    import scripts.run_pipeline as rp
    db, cb = review
    cls = getattr(rm, name)
    refusal = cls(f"{name}: refused")
    run_id = rm.open_run(db._conn, spec, kind="extraction", stages=("extract_pass1",),
                         codebook=cb, git=CLEAN, digest_fn=Digests()).run_id

    def extract(*a, **k):
        raise refusal
    monkeypatch.setattr(rp, "load_spec_for", lambda n, path=None: spec)
    monkeypatch.setattr(rp, "ReviewDatabase", lambda n: db)
    monkeypatch.setattr(rp, "_open_run_manifest", lambda d, s, i, **k: run_id)
    monkeypatch.setattr(rp, "is_adjudication_complete", lambda conn: True)
    monkeypatch.setattr(rp, "_stage_parse", lambda *a, **k: {})
    monkeypatch.setattr(rp, "_stage_extract", extract)
    with pytest.raises(cls):
        rp.run_pipeline("u20c", skip_to="extract")
    assert _end(db.db_path, run_id) == ("aborted", str(refusal))


@pytest.mark.parametrize("name", CLASSES)
def test_f_the_ft_runner_closes_aborted(review, spec, fake, monkeypatch, name):
    db, _ = review
    cls = getattr(rm, name)
    refusal = cls(f"{name}: refused")

    def verify(*a, **k):
        raise refusal
    monkeypatch.setattr(pf, "check_ollama_env", lambda: None)
    monkeypatch.setattr(pf, "read_ollama_env", lambda: ("", {}))
    monkeypatch.setattr(ft, "run_ft_verification", verify)
    with pytest.raises(cls):
        ft.run_ft_invocation(db, spec, verify_only=True, git=CLEAN, digest_fn=Digests())
    run_id = db._conn.execute("SELECT MAX(run_id) FROM run_manifests").fetchone()[0]
    assert _end(db.db_path, run_id) == ("aborted", str(refusal))


# (g) a pin: green on the parent by construction
def test_g_outside_a_run_nothing_is_read_and_the_record_is_unchanged(spec, fake, caplog):
    fake.answer = AssertionError("/api/ps read outside a run")
    with caplog.at_level("INFO", logger="engine.utils.ollama_client"):
        _audit_call(spec)
    assert fake.ps_reads == 0
    line = next(r.getMessage() for r in caplog.records if r.getMessage().startswith("input_fit "))
    assert set(json.loads(line.split(" ", 2)[2])) == {
        "model", "ceiling", "chars", "estimate_low", "count", "ratio", "done_reason"}


# (h) a pin: commit 1's behaviour, R744(b)
def test_h_a_malformed_num_ctx_line_refuses_even_when_the_request_sends_num_ctx(fake):
    fake.parameters = f"{'num_ctx':<31}abc"
    with pytest.raises(oc.CeilingUnavailable, match="num_ctx"):
        oc.effective_ceiling("gemma3:27b", {"num_ctx": 4096})


# (i)
def test_i_preflight_raises_a_loaded_context_mismatch(review, spec, fake):
    db, cb = review
    model = _model(spec)
    fake.answer = _ps((model, A, 8192))
    run_id = _open(db, spec, cb, Digests(), stages=("audit", "preflight"), preflight_models=[model])
    with rm.active_run(db._conn, run_id):
        with pytest.raises(rm.LoadedContextMismatch):
            pf.check_model(model, spec=spec)


# (j)
def test_j_preflight_raises_a_digest_change(review, spec, fake):
    db, cb = review
    model = _model(spec)
    digests = Digests()
    run_id = _open(db, spec, cb, digests, stages=("audit", "preflight"), preflight_models=[model])
    digests.value = B
    with rm.active_run(db._conn, run_id):
        with pytest.raises(rm.ModelDigestChanged):
            pf.check_model(model, spec=spec)


# (k) pins, R745(e)
def test_k_a_call_naming_no_stage_is_not_compared(review, spec, fake):
    db, cb = review
    fake.answer = AssertionError("/api/ps read for a call naming no stage")
    run_id = _open(db, spec, cb, Digests())
    kwargs = ec.stage_config("audit", spec).kwargs()
    kwargs.pop("stage")
    with rm.active_run(db._conn, run_id):
        oc.ollama_chat(messages=MESSAGES, **kwargs)
    assert fake.ps_reads == 0 and len(fake.calls) == 1


def test_k_a_run_open_run_did_not_register_is_not_compared(review, spec, fake, monkeypatch):
    db, cb = review
    fake.answer = AssertionError("/api/ps read under an unregistered run")
    run_id = _open(db, spec, cb, Digests())
    monkeypatch.setattr(rm, "_RUN_DIGEST_FNS", {})
    with rm.active_run(db._conn, run_id):
        _audit_call(spec)
    assert fake.ps_reads == 0 and len(fake.calls) == 1
