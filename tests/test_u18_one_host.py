"""P1 unit U18, commit 1 (R721(b)–(c), R722, R724): one Ollama host for every engine read.

The host was four things on the run path (12y-U18A): the chat client, bound at import from
`OLLAMA_HOST`; the digest read, a literal `http://localhost:11434` on a fresh connection; the
preflight's `ollama.ps()`, the library's own module client; and the restart poll, a literal
`http://127.0.0.1:11434`. Now `ollama_client.ollama_host()` is read once, at import, and
`_build_client()` builds the engine client from it; the digest read and the preflight's ps go
through that client, the restart poll names the resolved host, and a host that is not loopback
is refused at `open_run` before anything is read or written (R721(c)).

The tests drive the resolver — set `_OLLAMA_HOST` and rebuild with `_build_client`, the same
path the import takes — not the environment (I4; the engine client is bound at first import,
R627(b)). Requests go to an `httpx.MockTransport`: nothing reaches a server.
"""

from __future__ import annotations

import ast
import json
import os
import shutil
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest

from engine.agents import extractor as E
from engine.core import effective_config as ec
from engine.core import run_manifest as rm
from engine.core.codebook import clear_cache, load_codebook_beside
from engine.core.database import ReviewDatabase
from engine.core.effective_config import sha256_canonical
from engine.core.extraction_events import run_fault_classes
from engine.core.review_spec import load_review_spec
from engine.utils import ollama_client as oc
from engine.utils import ollama_preflight as pf

REPO = Path(__file__).resolve().parent.parent
SPEC_PATH = REPO / "review_specs" / "surgical_autonomy.yaml"
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
DIGEST = "a" * 64
MODEL = "u18-model:1b"
SENTINEL = "http://127.0.0.1:59999"           # loopback, not the service's port
REMOTE = "http://10.1.2.3:4567"


def _bind(monkeypatch, host, seen, models=(MODEL,)):
    """Point the resolver at `host` and rebuild the engine client from it, over a
    transport that records every request and answers the four routes."""
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        path = request.url.path
        if path == "/api/tags":
            return httpx.Response(200, json={"models": [{"name": m, "digest": DIGEST} for m in models]})
        if path == "/api/ps":
            return httpx.Response(200, json={"models": [{"name": MODEL, "size": 2 * 1024**3}]})
        if path == "/api/chat":
            return httpx.Response(200, json={"model": MODEL, "created_at": "2026-10-28T00:00:00Z",
                                             "message": {"role": "assistant", "content": "OK"},
                                             "done": True})
        return httpx.Response(404, json={"error": path})
    monkeypatch.setattr(oc, "_OLLAMA_HOST", host, raising=False)
    monkeypatch.setattr(oc, "_client", oc._build_client(transport=httpx.MockTransport(handler)))


def _origin(url: str) -> str:
    return url.split("/api/", 1)[0]


# ── every read site, one host ─────────────────────────────────────────
def test_u18_every_engine_read_uses_the_resolved_host(monkeypatch):
    monkeypatch.delenv(oc.RESTART_OPT_OUT_ENV, raising=False)
    seen: list[str] = []
    _bind(monkeypatch, SENTINEL, seen)
    oc._client.chat(model=MODEL, messages=[{"role": "user", "content": "hi"}])   # the chat client
    assert oc.fetch_model_digest(MODEL) == DIGEST                                # the digest read
    assert pf._get_model_vram_gb(MODEL) == 2.0                                   # the preflight's ps
    with patch("engine.agents.extractor.subprocess.run"), \
         patch("engine.agents.extractor.httpx.get",
               return_value=SimpleNamespace(status_code=200)) as poll:
        E.restart_ollama(reason="u18", papers_done=0)                            # the restart poll
    assert [httpx.URL(u).path for u in seen] == ["/api/chat", "/api/tags", "/api/ps"]
    assert {_origin(u) for u in seen} | {_origin(poll.call_args.args[0])} == {SENTINEL}
    # the local-service predicate reads the same host: loopback, another port, not the service
    assert oc._client_host() == SENTINEL
    assert oc.server_context_length() is None


def test_u18_the_digest_read_uses_the_keep_alive_client(monkeypatch):
    seen: list[str] = []
    _bind(monkeypatch, SENTINEL, seen)
    with patch("engine.utils.ollama_client.httpx.get",
               side_effect=AssertionError("a fresh httpx connection")):
        assert oc.fetch_model_digest(MODEL) == DIGEST
    assert seen == [f"{SENTINEL}/api/tags"]


def test_u18_the_preflight_ps_uses_the_engine_client(monkeypatch):
    seen: list[str] = []
    _bind(monkeypatch, SENTINEL, seen)
    with patch("ollama.ps", side_effect=AssertionError("the library's module client")):
        assert pf._get_model_vram_gb(MODEL) == 2.0
    assert seen == [f"{SENTINEL}/api/ps"]


# ── no address literal on the Run 7 import closure ────────────────────
_ROOTS = ("scripts/run_pipeline.py", "engine/agents/extractor.py", "engine/agents/ft_screener.py",
          "engine/agents/auditor.py", "engine/utils/ollama_preflight.py", "engine/parsers/pdf_parser.py")


def _closure() -> set[str]:
    """The static import closure of the Run 7 entry points (12y-U18A §7's instrument)."""
    def mod_path(m):
        p = REPO / m.replace(".", "/")
        for c in (p.with_suffix(".py"), p / "__init__.py"):
            if c.exists():
                return str(c.relative_to(REPO))
    seen, stack = set(), list(_ROOTS)
    while stack:
        f = stack.pop()
        if f in seen:
            continue
        seen.add(f)
        for n in ast.walk(ast.parse((REPO / f).read_text())):
            names = []
            if isinstance(n, ast.Import):
                names = [a.name for a in n.names]
            elif isinstance(n, ast.ImportFrom) and n.module and n.level == 0:
                names = [n.module] + [f"{n.module}.{a.name}" for a in n.names]
            for m in names:
                if m.split(".")[0] in ("engine", "analysis", "scripts") and (p := mod_path(m)):
                    stack.append(p)
    return seen


def test_u18_no_literal_11434_on_the_run_path():
    modules = _closure()
    assert "engine/utils/ollama_client.py" in modules and len(modules) > 50
    hits = []
    for f in sorted(modules):
        for n in ast.walk(ast.parse((REPO / f).read_text())):
            if isinstance(n, ast.Constant) and (
                    (isinstance(n.value, str) and ("11434" in n.value or "11435" in n.value))
                    or (type(n.value) is int and n.value in (11434, 11435))):
                hits.append(f"{f}:{n.lineno}")
    assert hits == []


# ── the predicates (R722(d)) ──────────────────────────────────────────
def test_u18_both_loopback_spellings_resolve_to_a_local_host():
    for host in ("http://localhost:11434", "http://127.0.0.1:11434", "http://[::1]:11434"):
        assert oc.is_loopback_host(host) and oc.is_local_service_host(host), host
    assert not oc.is_loopback_host(REMOTE) and not oc.is_local_service_host(REMOTE)


def test_u18_a_loopback_host_on_another_port_is_not_the_local_service():
    assert oc.is_loopback_host("http://127.0.0.1:11435")
    assert not oc.is_local_service_host("http://127.0.0.1:11435")


# ── open_run ──────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def spec():
    if not LIVE_CODEBOOK.exists():
        pytest.skip("codebook not available")
    return load_review_spec(SPEC_PATH)


@pytest.fixture
def review(tmp_path):
    db = ReviewDatabase("rev", data_root=tmp_path)
    shutil.copy(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    clear_cache()
    yield db
    db.close()
    clear_cache()


def _stages(spec):
    return [*E.extraction_stages(spec), "audit"]


def _rows(db) -> tuple[int, int, int]:
    conn = sqlite3.connect(str(db.db_path))
    try:
        return tuple(conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                     for t in ("run_manifests", "arms", "run_stage_configs"))
    finally:
        conn.close()


def test_u18_the_manifest_records_the_ollama_host(review, spec):
    handle = rm.open_run(review._conn, spec, kind="extraction", stages=_stages(spec),
                         codebook=load_codebook_beside(review.db_path), git=CLEAN,
                         digest_fn=lambda m: DIGEST)
    raw, sha = review._conn.execute("SELECT manifest_json, manifest_sha256 FROM run_manifests "
                                    "WHERE run_id = ?", (handle.run_id,)).fetchone()
    body = json.loads(raw)
    assert body["ollama_host"] == oc.ollama_host()
    assert body["host"] != body["ollama_host"]            # the machine's name is kept beside it
    assert sha == sha256_canonical(body)                  # manifest_sha256 covers the key


def test_u18_a_non_loopback_host_is_refused_at_open_run(review, spec, monkeypatch):
    monkeypatch.setattr(oc, "_OLLAMA_HOST", REMOTE, raising=False)
    fetched: list[str] = []
    with pytest.raises(rm.OllamaHostNotLoopback) as exc:
        rm.open_run(review._conn, spec, kind="extraction", stages=_stages(spec),
                    codebook=load_codebook_beside(review.db_path), git=CLEAN,
                    digest_fn=lambda m: fetched.append(m) or DIGEST)
    assert isinstance(exc.value, rm.RunRefused) and isinstance(exc.value, run_fault_classes())
    assert REMOTE in str(exc.value)
    assert fetched == []                                  # before any digest read
    assert _rows(review) == (0, 0, 0)                     # before any write


def test_u18_a_rebound_client_at_a_fake_host_is_not_refused_at_open_run(review, spec, monkeypatch):
    """R722(c)'s limit, pinned on purpose: the refusal keys on the import-time host, so a
    client rebound in-process to a fake is not refused — and the digest is read through
    that same fake, never from a third object (A-4b's acceptance test)."""
    models = sorted({ec.stage_config(s, spec).model for s in _stages(spec)})
    asked: list[str] = []

    class FakeTransport:
        base_url = "http://u18.invalid"

        def get(self, path, **kw):
            asked.append(path)
            return httpx.Response(200, json={"models": [{"name": m, "digest": DIGEST} for m in models]})

    monkeypatch.setattr(oc, "_client", SimpleNamespace(_client=FakeTransport()))
    handle = rm.open_run(review._conn, spec, kind="extraction", stages=_stages(spec),
                         codebook=load_codebook_beside(review.db_path), git=CLEAN)
    assert asked and set(asked) == {"/api/tags"}
    digests = {r[0] for r in review._conn.execute(
        "SELECT model_digest FROM run_stage_configs WHERE run_id = ?", (handle.run_id,))}
    assert digests == {DIGEST}
