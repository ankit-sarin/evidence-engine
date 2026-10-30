"""U19 commit 2 (R734(d)): `open_run` refuses a loopback Ollama host that is not the
local service (another port), after `OllamaHostNotLoopback` and before any read or
write; and the `Environment=` line has one parser, `shlex`-based, owned by
`ollama_client` and used by both readers (N26).

Hosts are set through the resolver as `test_u18_one_host.py` sets them
(`_OLLAMA_HOST`); the subprocess boundary answers `systemctl show` from a hand-typed
line; the unit files are written under `tmp_path` (`test_ollama_input_fit.py`'s form).
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from engine.core import extraction_events as X
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.utils import ollama_client as oc
from engine.utils import ollama_preflight as pf
from _event_store_fixture import FIXTURE_DIGEST

REPO = Path(__file__).resolve().parents[1]
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
CLEAN = rm.GitState(commit="0" * 40, dirty=False, tag=None)
OTHER_PORT = "http://127.0.0.1:11435"
# The §3.4 line (12z_U19A.md), as `systemctl show ollama --property=Environment` printed it.
RAW = ("PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin "
       "OLLAMA_KV_CACHE_TYPE=f16 OLLAMA_MAX_LOADED_MODELS=1 OLLAMA_NUM_PARALLEL=1 "
       "OLLAMA_FLASH_ATTENTION=true OLLAMA_KEEP_ALIVE=-1")


@pytest.fixture
def spec():
    return load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")


@pytest.fixture
def review(tmp_path):
    db = ReviewDatabase("u19ls", data_root=tmp_path)
    yield db
    db.close()


def _rows(db):
    return tuple(db._conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                 for t in ("run_manifests", "run_stage_configs", "arms"))


def _systemctl(raw):
    def run(argv, *a, **k):
        assert list(argv[:3]) == ["systemctl", "show", "ollama"], argv
        return subprocess.CompletedProcess(argv, 0, stdout=f"Environment={raw}\n", stderr="")
    return run


# (h)
def test_u19_a_loopback_host_on_another_port_is_refused_at_open_run(review, spec, monkeypatch):
    monkeypatch.setattr(oc, "_OLLAMA_HOST", OTHER_PORT, raising=False)
    fetched: list[str] = []
    gits: list[int] = []
    monkeypatch.setattr(rm, "git_state", lambda *a, **k: gits.append(1) or CLEAN)
    with pytest.raises(rm.OllamaHostNotLocalService) as exc:
        rm.open_run(review._conn, spec, kind="extraction", stages=("extract_pass1",),
                    codebook=load_codebook(LIVE_CODEBOOK),
                    digest_fn=lambda m: fetched.append(m) or FIXTURE_DIGEST)
    msg = str(exc.value)
    assert msg.startswith("OllamaHostNotLocalService: ") and OTHER_PORT in msg
    assert fetched == [] and gits == []                   # before any read
    assert _rows(review) == (0, 0, 0)                     # before any write


# (i) — the guard does not over-refuse: a pin, green on the parent by construction
@pytest.mark.parametrize("host", ["http://127.0.0.1:11434", "http://localhost:11434"])
def test_u19_the_local_service_host_is_not_refused(review, spec, monkeypatch, host):
    monkeypatch.setattr(oc, "_OLLAMA_HOST", host, raising=False)
    handle = rm.open_run(review._conn, spec, kind="extraction", stages=("extract_pass1",),
                         codebook=load_codebook(LIVE_CODEBOOK), git=CLEAN,
                         digest_fn=lambda m: FIXTURE_DIGEST)
    assert handle.run_id and _rows(review)[0] == 1


# (j)
def test_u19_a_quoted_value_with_a_space_is_one_token_on_both_readers(monkeypatch, tmp_path):
    line = 'OLLAMA_KV_CACHE_TYPE=f16 "OLLAMA_ORIGINS=a b" OLLAMA_CONTEXT_LENGTH=8192'
    monkeypatch.setattr(pf.subprocess, "run", _systemctl(line))
    raw, env = pf.read_ollama_env()
    assert raw == line
    assert env["OLLAMA_ORIGINS"] == "a b" and env["OLLAMA_KV_CACHE_TYPE"] == "f16"
    assert "b" not in env and "b\"" not in env
    unit, dropins = tmp_path / "ollama.service", tmp_path / "ollama.service.d"
    dropins.mkdir()
    unit.write_text("[Service]\n")
    (dropins / "override.conf").write_text(f"[Service]\nEnvironment={line}\n")
    monkeypatch.setattr(oc, "SERVICE_UNIT_FILE", unit)
    monkeypatch.setattr(oc, "SERVICE_DROPIN_DIR", dropins)
    assert oc.server_context_length() == 8192
    assert oc.parse_environment_line(line) == env         # one parser


# (k)
def test_u19_the_two_readers_give_equal_maps_on_the_hosts_line(monkeypatch):
    monkeypatch.setattr(pf.subprocess, "run", _systemctl(RAW))
    raw, env = pf.read_ollama_env()
    assert raw == RAW
    assert env == oc.parse_environment_line(RAW) == {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin",
        "OLLAMA_KV_CACHE_TYPE": "f16", "OLLAMA_MAX_LOADED_MODELS": "1",
        "OLLAMA_NUM_PARALLEL": "1", "OLLAMA_FLASH_ATTENTION": "true", "OLLAMA_KEEP_ALIVE": "-1"}


# (l)
def test_u19_ollama_host_not_local_service_is_a_run_refused_run_fault():
    assert issubclass(rm.OllamaHostNotLocalService, rm.RunRefused)
    assert X.is_run_fault(rm.OllamaHostNotLocalService("refused"))
