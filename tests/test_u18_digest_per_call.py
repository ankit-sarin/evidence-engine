"""P1 unit U18, commit 2 (R721(b), R726(d), R727): the model digest re-checked on every call.

The digest was read once, at `open_run`; every later call compared the model NAME only
(`check_declared_call`), so a model replaced under its tag mid-run was attributed to the
opening digest on every event (12y-U18A findings 6 and 7). Now, under an active run,
`ollama_chat` reads the model's current digest through the run's own `digest_fn` — the one
`open_run` was given, carried in a registry keyed by (database file, run_id); production's is
`fetch_model_digest`, through the engine client — after the declared-call check and before
the input-fit read and the send, and compares it with the manifest's `stage_digest`. A
difference raises `ModelDigestChanged`, a `RunRefused` and so a run fault: nothing is sent,
no `run_calls` row is written, and both runners close the run 'aborted' with its message.
The trained-context cache is keyed by that digest, not by the host.

Scratch databases with real manifests; Ollama is a fake client at `ollama_client._client`.
No model is called and no request leaves the process.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from engine.agents import ft_screener as ft
from engine.core import effective_config as ec
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.extraction_events import is_run_fault, run_fault_classes
from engine.core.review_paths import load_spec_for
from engine.search.models import Citation
from engine.utils import ollama_client as oc
from _loaded_context_fixture import loaded_ps  # U20 commit 2: the fake's /api/ps
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")

CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
A, B = "a" * 64, "b" * 64
MESSAGES = [{"role": "user", "content": "hello"}]


class FakeClient:
    """Stands in for `ollama.Client`: records every `chat` and every `show` (the input-fit
    read). A call refused before the send reaches neither."""

    def __init__(self, base_url="http://u18c.invalid"):
        self.calls: list[dict] = []
        self.shows: list[str] = []
        self.answers: list = []
        self._client = SimpleNamespace(base_url=base_url)

    def show(self, model):
        self.shows.append(model)
        return SimpleNamespace(modelinfo={"general.context_length": 131_072})

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        content = "OK"
        if "format" in kwargs and self.answers:
            content = json.dumps(self.answers.pop(0))
        chars = oc.message_chars(kwargs.get("messages"))
        return SimpleNamespace(
            message=SimpleNamespace(content=content, thinking=None),
            prompt_eval_count=max(1, int(chars * 0.3)), done_reason="stop", eval_count=1)

    def ps(self):
        return loaded_ps()

    @property
    def requests(self) -> int:
        return len(self.calls) + len(self.shows)


class Digests:
    """A `digest_fn` whose answer can change under a run, counting every read."""

    def __init__(self, value=A):
        self.value, self.reads = value, []

    def __call__(self, model):
        self.reads.append(model)
        return self.value


@pytest.fixture
def fake(monkeypatch):
    import httpx
    monkeypatch.setattr(httpx.Client, "send",
                        lambda *a, **k: pytest.fail("a U18 test made a real HTTP request"))
    monkeypatch.setattr(oc, "fetch_model_digest",
                        lambda m: pytest.fail(f"fetch_model_digest({m!r}): the run's digest_fn was not used"))
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
    db = ReviewDatabase("u18c", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    yield db, load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    db.close()


def _open(db, spec, cb, digests, stages=("audit",), preflight_models=()):
    return rm.open_run(db._conn, spec, kind="screening", stages=stages, codebook=cb,
                       preflight_models=preflight_models, git=CLEAN, digest_fn=digests).run_id


def _run_calls(db):
    return [tuple(r) for r in db._conn.execute(
        "SELECT stage, outcome FROM run_calls ORDER BY call_id")]


def _audit(spec):
    return ec.stage_config("audit", spec).kwargs()


# ── the refusal, before anything is sent ──────────────────────────────
def test_u18_a_tag_replaced_between_open_and_the_first_call_refuses(review, spec, fake):
    db, cb = review
    digests = Digests(A)
    run_id = _open(db, spec, cb, digests)
    digests.value = B                                   # the tag now names another model
    with rm.active_run(db._conn, run_id):
        with pytest.raises(rm.ModelDigestChanged) as exc:
            oc.ollama_chat(messages=MESSAGES, **_audit(spec))
    model = ec.stage_config("audit", spec).model
    assert (exc.value.stage_key, exc.value.model, exc.value.recorded, exc.value.current) == \
        ("audit", model, A, B)
    assert fake.requests == 0                           # no show, no chat
    assert _run_calls(db) == []                         # unrecorded, as C54's refusal


def test_u18_a_tag_replaced_between_two_calls_refuses(review, spec, fake):
    db, cb = review
    digests = Digests(A)
    run_id = _open(db, spec, cb, digests)
    with rm.active_run(db._conn, run_id):
        oc.ollama_chat(messages=MESSAGES, **_audit(spec))
        oc.ollama_chat(messages=MESSAGES, **_audit(spec))
        digests.value = B
        with pytest.raises(rm.ModelDigestChanged):
            oc.ollama_chat(messages=MESSAGES, **_audit(spec))
    assert len(fake.calls) == 2
    assert _run_calls(db) == [("audit", "completed"), ("audit", "completed")]
    assert rm.stage_digest(db._conn, run_id, "audit") == A     # what the sent calls' events carry


def test_u18_the_unchanged_case_sends_the_same_request_bytes(review, spec, fake):
    db, cb = review
    digests = Digests(A)
    run_id = _open(db, spec, cb, digests)
    opened = len(digests.reads)
    with rm.active_run(db._conn, run_id):
        oc.ollama_chat(messages=MESSAGES, **_audit(spec))
    expected = {k: v for k, v in _audit(spec).items() if k != "stage"}
    expected["messages"] = MESSAGES
    assert fake.calls == [expected]                    # the declared kwargs and nothing else
    hashes = [r[0] for r in db._conn.execute("SELECT request_hash FROM run_calls")]
    assert hashes == [rm.request_hash(expected)]
    assert len(digests.reads) == opened + 1            # and the digest was read for it


def test_u18_the_digest_check_reads_through_the_run_digest_fn(review, spec, fake):
    db, cb = review
    digests = Digests(A)
    run_id = _open(db, spec, cb, digests)
    opened = list(digests.reads)
    with rm.active_run(db._conn, run_id):
        for _ in range(3):
            oc.ollama_chat(messages=MESSAGES, **_audit(spec))
    model = ec.stage_config("audit", spec).model
    assert digests.reads[len(opened):] == [model] * 3     # the fixture's oc.fetch_model_digest fails if used


def test_u18_the_digest_check_precedes_the_input_fit_read(review, spec, fake, monkeypatch):
    db, cb = review
    digests = Digests(A)
    run_id = _open(db, spec, cb, digests)
    order: list[str] = []
    real_fit = oc._check_input_fits

    def fit(*a, **k):
        order.append("input_fit")
        return real_fit(*a, **k)

    def read(model):
        order.append("digest")
        return digests.value
    monkeypatch.setattr(oc, "_check_input_fits", fit)
    rm._RUN_DIGEST_FNS[(rm._database_file(db._conn), run_id)] = read
    with rm.active_run(db._conn, run_id):
        oc.ollama_chat(messages=MESSAGES, **_audit(spec))
        digests.value = B
        with pytest.raises(rm.ModelDigestChanged):
            oc.ollama_chat(messages=MESSAGES, **_audit(spec))
    assert order == ["digest", "input_fit", "digest"]   # a mismatch never reaches the fit read
    assert len(fake.calls) == 1


def test_u18_the_preflight_call_is_not_refused_on_a_fresh_run(review, spec, fake):
    db, cb = review
    model = ec.stage_config("audit", spec).model
    digests = Digests(A)
    run_id = _open(db, spec, cb, digests, stages=("audit", "preflight"), preflight_models=[model])
    opened = len(digests.reads)
    with rm.active_run(db._conn, run_id):
        oc.ollama_chat(messages=MESSAGES, **ec.stage_config("preflight", spec, model=model).kwargs())
    assert len(fake.calls) == 1
    assert _run_calls(db) == [(f"preflight:{model}", "completed")]
    assert rm.stage_digest(db._conn, run_id, f"preflight:{model}") == A
    assert len(digests.reads) == opened + 1            # checked against preflight:<model>'s digest


def test_u18_the_context_length_cache_is_keyed_by_digest(review, spec, fake):
    db, cb = review
    model = ec.stage_config("audit", spec).model
    for digest in (A, B):                               # one model name, two digests: two reads
        run_id = _open(db, spec, cb, Digests(digest))
        with rm.active_run(db._conn, run_id):
            oc.ollama_chat(messages=MESSAGES, **_audit(spec))
    assert fake.shows == [model, model]
    fake._client = SimpleNamespace(base_url="http://u18c-other.invalid")
    run_id = _open(db, spec, cb, Digests(B))            # another host, the same digest: cached
    with rm.active_run(db._conn, run_id):
        oc.ollama_chat(messages=MESSAGES, **_audit(spec))
    assert fake.shows == [model, model]


def test_u18_the_refusal_is_a_run_fault():
    exc = rm.ModelDigestChanged.at("audit", "m:1b", A, B)
    assert isinstance(exc, rm.RunRefused) and is_run_fault(exc)
    assert isinstance(exc, run_fault_classes())
    assert str(exc).startswith("ModelDigestChanged: ")
    for part in ("audit", "m:1b", A, B):
        assert part in str(exc)


# ── the close, through each runner (E5) ───────────────────────────────
def _end(db_path, run_id):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT end_status, end_reason FROM run_manifests WHERE run_id = ?",
                            (run_id,)).fetchone()
    finally:
        conn.close()


def test_u18_run_pipeline_closes_aborted_on_a_digest_change(review, spec, fake, monkeypatch):
    """test_t3's form (tests/test_cut_over.py): the runner's own close, here reached by a
    digest change at a call inside the extract stage rather than by a RunAborted."""
    import scripts.run_pipeline as rp
    db, cb = review
    digests = Digests(A)
    run_id = rm.open_run(db._conn, spec, kind="extraction", stages=("extract_pass1",),
                         codebook=cb, git=CLEAN, digest_fn=digests).run_id
    digests.value = B

    def extract(*a, **k):
        oc.ollama_chat(messages=MESSAGES, **ec.stage_config("extract_pass1", spec).kwargs())
    monkeypatch.setattr(rp, "load_spec_for", lambda name, path=None: spec)
    monkeypatch.setattr(rp, "ReviewDatabase", lambda name: db)
    monkeypatch.setattr(rp, "_open_run_manifest", lambda d, s, i, **k: run_id)
    monkeypatch.setattr(rp, "is_adjudication_complete", lambda conn: True)
    monkeypatch.setattr(rp, "_stage_parse", lambda *a, **k: {})
    monkeypatch.setattr(rp, "_stage_extract", extract)
    with pytest.raises(rm.ModelDigestChanged):
        rp.run_pipeline("u18c", skip_to="extract")
    status, reason = _end(db.db_path, run_id)
    assert status == "aborted"
    assert reason.startswith("ModelDigestChanged: ")
    assert fake.requests == 0


def _ft_eligible_paper(db):
    db.add_papers([Citation(title="Paper 1", abstract="Autonomous suturing abstract.", pmid="U18C1",
                            source="pubmed", authors=["A"], journal="J", year=2024)])
    pid = db._conn.execute("SELECT id FROM papers WHERE pmid = 'U18C1'").fetchone()[0]
    for s in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "FT_ELIGIBLE"):
        db.update_status(pid, s)
    write_parsed(db, pid, "# Paper\n\nAutonomous robotic suturing in surgery. " * 40)
    return pid


def test_u18_the_ft_runner_closes_aborted_on_a_digest_change(review, spec, fake, monkeypatch):
    from engine.utils import ollama_preflight as pf
    db, _ = review
    _ft_eligible_paper(db)
    monkeypatch.setattr(pf, "check_ollama_env", lambda: None)
    monkeypatch.setattr(pf, "read_ollama_env", lambda: ("", {}))  # U19: the open read (R734(c)(i))
    fake.answers = [{"decision": "FT_ELIGIBLE", "rationale": "r", "confidence": 0.9}]
    seen: dict[str, int] = {}

    def digests(model):                                 # A at open, B at the first call
        seen[model] = seen.get(model, 0) + 1
        return A if seen[model] == 1 else B
    with pytest.raises(rm.ModelDigestChanged):
        ft.run_ft_invocation(db, spec, verify_only=True, git=CLEAN, digest_fn=digests)
    run_id = db._conn.execute("SELECT MAX(run_id) FROM run_manifests").fetchone()[0]
    status, reason = _end(db.db_path, run_id)
    assert status == "aborted"
    assert reason.startswith("ModelDigestChanged: ")
    assert fake.calls == []
