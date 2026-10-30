"""U20 commit 1 (R743(c), INT-i12-11): the input-fit ceiling reads the `num_ctx` line of a
model's `/api/show` `parameters`.

Every fake `show` is built from a raw body committed by the 12aa-U20P probe, through the
library's own `ShowResponse`, with only the `parameters` string changed (R743(b)): names
padded to offset 31, repeated names, quoted values, no trailing newline. The service term is
pointed at temporary unit files (`test_ollama_input_fit.py`'s form).
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import ollama
import pytest

from engine.utils import ollama_client as oc

BODIES = Path(__file__).resolve().parents[1] / "docs/session-reports/session-12/12aa_U20P/show"
HOST = "http://127.0.0.1:11434"
TRAINED_131072 = "deepseek-r1:32b"


def _body(model: str) -> dict:
    return json.loads((BODIES / f"{model.replace(':', '_')}.response.json").read_text(encoding="utf-8"))


def _line(name: str, value: str) -> str:
    """One `parameters` line as the server writes it: the name padded to offset 31."""
    return f"{name:<31}{value}"


def _with_lines(model: str, *lines: str) -> str:
    """The model's `parameters` as received, with `lines` first."""
    return "\n".join([*lines, _body(model)["parameters"]])


class _Client:
    """Answers `show` from a committed body, `chat` with a set prompt count."""

    def __init__(self, parameters=None, count=100):
        self._client = SimpleNamespace(base_url=HOST)
        self.parameters = parameters
        self.count = count
        self.shows: list[str] = []

    def show(self, model):
        self.shows.append(model)
        body = _body(model)
        if self.parameters is not None:
            body["parameters"] = self.parameters
        return ollama.ShowResponse(**body)

    def chat(self, **kwargs):
        return SimpleNamespace(message=SimpleNamespace(content="ok"),
                               prompt_eval_count=self.count, done_reason="stop")


@pytest.fixture(autouse=True)
def _service(tmp_path, monkeypatch):
    unit = tmp_path / "ollama.service"
    dropins = tmp_path / "ollama.service.d"
    dropins.mkdir()
    unit.write_text('[Service]\nEnvironment="PATH=/usr/bin"\n')
    monkeypatch.setattr(oc, "SERVICE_UNIT_FILE", unit)
    monkeypatch.setattr(oc, "SERVICE_DROPIN_DIR", dropins)
    monkeypatch.setenv(oc.RESTART_OPT_OUT_ENV, "1")
    oc.clear_ceiling_cache()
    yield
    oc.clear_ceiling_cache()


def _use(monkeypatch, **kwargs) -> _Client:
    client = _Client(**kwargs)
    monkeypatch.setattr(oc, "_client", client)
    return client


# (a) the entry's id
def test_a_modelfile_num_ctx_lowers_the_ceiling(monkeypatch):
    _use(monkeypatch, parameters=_with_lines(TRAINED_131072, _line("num_ctx", "8192")), count=8192)
    assert oc.effective_ceiling(TRAINED_131072) == 8192
    with pytest.raises(oc.InputTruncated) as exc:
        oc.ollama_chat(model=TRAINED_131072, messages=[{"role": "user", "content": "x" * 3000}],
                       max_retries=0, retry_delay=0)
    assert exc.value.count == 8192 and exc.value.ceiling == 8192


# (b) a pin: green on the parent by construction (R736(c))
@pytest.mark.parametrize("requested", [4096, 16384])
def test_a_request_num_ctx_still_wins_over_the_modelfile_term(monkeypatch, requested):
    _use(monkeypatch, parameters=_with_lines(TRAINED_131072, _line("num_ctx", "8192")))
    assert oc.effective_ceiling(TRAINED_131072, {"num_ctx": requested}) == requested


# (c)
@pytest.mark.parametrize("value", ["abc", "0", "-1", "8192.5", '"8192"', "+8192", "8192 16384", ""])
def test_a_num_ctx_line_that_is_not_a_positive_integer_is_ceiling_unavailable(monkeypatch, value):
    _use(monkeypatch, parameters=_with_lines(TRAINED_131072, _line("num_ctx", value)))
    with pytest.raises(oc.CeilingUnavailable, match="num_ctx"):
        oc.effective_ceiling(TRAINED_131072)


# (d)
def test_two_num_ctx_lines_are_ceiling_unavailable(monkeypatch):
    _use(monkeypatch, parameters=_with_lines(TRAINED_131072, _line("num_ctx", "8192"), _line("num_ctx", "8192")))
    with pytest.raises(oc.CeilingUnavailable, match="num_ctx"):
        oc.effective_ceiling(TRAINED_131072)


# (e) a pin: the six committed bodies as received, the ceilings of reads_u20b.md's table
@pytest.mark.parametrize("model,ceiling", [
    ("qwen3:8b", 40_960),
    ("qwen3:32b", 40_960),
    ("gemma3:27b", 131_072),
    ("deepseek-r1:32b", 131_072),
    ("qwen2.5vl:7b", 128_000),
    ("llama3.2:3b", 131_072),
])
def test_a_committed_body_as_received_keeps_its_ceiling(monkeypatch, model, ceiling):
    client = _use(monkeypatch)
    assert oc.effective_ceiling(model) == ceiling
    assert client.shows == [model]
