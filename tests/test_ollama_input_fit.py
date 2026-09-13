"""INPUT-FIT-01 — the input-fit guard in the Ollama client wrapper.

T1 ceiling rule · T2 pre-call · T3 post-call upper · T4 post-call lower ·
T5 transparency · T9 silent middle-message drop · T10 the judge's full-text path.

A stub client throughout: no model call and no request to any server. The
service-environment read is pointed at temporary unit files.
"""

import copy
import json
from types import SimpleNamespace

import pytest

from engine.utils import ollama_client as oc

HOST = "http://127.0.0.1:11434"


class StubClient:
    """Stands in for `ollama.Client`: answers show() and chat(), records both."""

    def __init__(self, n_ctx_train=131_072, count=100, content="ok", host=HOST):
        self._client = SimpleNamespace(base_url=host)
        self.n_ctx_train = n_ctx_train
        self.count = count
        self.content = content
        self.shows: list[str] = []
        self.chats: list[dict] = []

    def show(self, model):
        self.shows.append(model)
        return SimpleNamespace(modelinfo={"arch.context_length": self.n_ctx_train,
                                          "arch.embedding_length": 4096})

    def chat(self, **kwargs):
        self.chats.append(kwargs)
        count = self.count(kwargs) if callable(self.count) else self.count
        return SimpleNamespace(message=SimpleNamespace(content=self.content),
                               prompt_eval_count=count, done_reason="stop")


@pytest.fixture(autouse=True)
def service(tmp_path, monkeypatch):
    """Temporary unit files for the service-environment read; a clean ceiling cache."""
    unit = tmp_path / "ollama.service"
    dropins = tmp_path / "ollama.service.d"
    dropins.mkdir()
    unit.write_text('[Service]\nEnvironment="PATH=/usr/bin"\n')
    monkeypatch.setattr(oc, "SERVICE_UNIT_FILE", unit)
    monkeypatch.setattr(oc, "SERVICE_DROPIN_DIR", dropins)
    monkeypatch.setenv(oc.RESTART_OPT_OUT_ENV, "1")
    oc.clear_ceiling_cache()
    yield SimpleNamespace(unit=unit, dropins=dropins)
    oc.clear_ceiling_cache()


def _use(monkeypatch, **kwargs) -> StubClient:
    client = StubClient(**kwargs)
    monkeypatch.setattr(oc, "_client", client)
    return client


def _chat(messages, **kwargs):
    return oc.ollama_chat(model="m", messages=messages, max_retries=0, retry_delay=0, **kwargs)


def _user(chars: int) -> list[dict]:
    return [{"role": "user", "content": "x" * chars}]


def _dropin(service, text: str, name: str = "override.conf") -> None:
    (service.dropins / name).write_text(text)


# ── T1: the ceiling rule ─────────────────────────────────────────────


def test_the_ceiling_is_the_trained_context_when_nothing_else_is_set(monkeypatch):
    _use(monkeypatch, n_ctx_train=131_072)
    assert oc.effective_ceiling("m") == 131_072


def test_the_server_default_caps_a_model_trained_beyond_it(monkeypatch):
    _use(monkeypatch, n_ctx_train=1_048_576)
    assert oc.effective_ceiling("m") == oc.SERVER_DEFAULT_CTX == 262_144


def test_the_service_environment_lowers_the_ceiling(monkeypatch, service):
    _use(monkeypatch, n_ctx_train=131_072)
    _dropin(service, '[Service]\nEnvironment="OLLAMA_CONTEXT_LENGTH=8192"\n')
    _dropin(service, '[Service]\nEnvironment="OLLAMA_CONTEXT_LENGTH=1024"\n', "override.conf.bak")
    assert oc.effective_ceiling("m") == 8192


def test_a_dropin_overrides_the_unit_file(monkeypatch, service):
    _use(monkeypatch)
    service.unit.write_text('[Service]\nEnvironment="OLLAMA_CONTEXT_LENGTH=4096"\n')
    _dropin(service, "[Service]\nEnvironment=OLLAMA_NUM_PARALLEL=1 OLLAMA_CONTEXT_LENGTH=16384\n")
    assert oc.server_context_length() == 16_384


def test_num_ctx_lowers_the_ceiling(monkeypatch):
    _use(monkeypatch, n_ctx_train=131_072)
    assert oc.effective_ceiling("m", {"num_ctx": 4096}) == 4096


def test_the_ceiling_is_the_minimum_of_all_terms(monkeypatch, service):
    _use(monkeypatch, n_ctx_train=40_960)
    _dropin(service, '[Service]\nEnvironment="OLLAMA_CONTEXT_LENGTH=32768"\n')
    assert oc.effective_ceiling("m", {"num_ctx": 16_384}) == 16_384
    assert oc.effective_ceiling("m", {"num_ctx": 36_000}) == 32_768
    assert oc.effective_ceiling("m") == 32_768


def test_the_service_environment_is_ignored_for_a_rebound_client(monkeypatch, service):
    _use(monkeypatch, n_ctx_train=131_072, host="http://127.0.0.1:11435")
    _dropin(service, '[Service]\nEnvironment="OLLAMA_CONTEXT_LENGTH=8192"\n')
    assert oc.effective_ceiling("m") == 131_072


def test_the_trained_context_is_read_once_per_model(monkeypatch):
    client = _use(monkeypatch)
    oc.effective_ceiling("a")
    oc.effective_ceiling("a")
    oc.effective_ceiling("b")
    assert client.shows == ["a", "b"]


def test_an_unreadable_ceiling_fails_closed_and_sends_nothing(monkeypatch):
    client = _use(monkeypatch)
    client.show = lambda model: SimpleNamespace(modelinfo={"arch.embedding_length": 4096})
    with pytest.raises(oc.CeilingUnavailable):
        _chat(_user(10))
    assert client.chats == []


# ── T2: pre-call ─────────────────────────────────────────────────────


def test_an_input_just_under_the_ceiling_is_sent(monkeypatch):
    client = _use(monkeypatch, count=15)
    assert 99 * oc.RATIO_MIN < 19
    _chat(_user(99), options={"num_ctx": 19})
    assert len(client.chats) == 1


def test_an_input_whose_low_estimate_reaches_the_ceiling_is_refused_unsent(monkeypatch):
    client = _use(monkeypatch)
    assert 100 * oc.RATIO_MIN >= 19
    with pytest.raises(oc.InputOverflow) as exc:
        _chat(_user(100), options={"num_ctx": 19})
    assert client.chats == []
    assert (exc.value.model, exc.value.chars, exc.value.ceiling) == ("m", 100, 19)
    assert exc.value.fields["estimate_low"] >= 19


# ── T3: post-call, upper ─────────────────────────────────────────────


def test_a_count_at_the_ceiling_raises_input_truncated(monkeypatch):
    _use(monkeypatch, count=1000)
    with pytest.raises(oc.InputTruncated) as exc:
        _chat(_user(3000), options={"num_ctx": 1000})
    assert (exc.value.count, exc.value.ceiling, exc.value.chars) == (1000, 1000, 3000)


def test_a_count_one_under_the_ceiling_passes(monkeypatch):
    _use(monkeypatch, count=999)
    assert _chat(_user(3000), options={"num_ctx": 1000}).prompt_eval_count == 999


# ── T4: post-call, lower ─────────────────────────────────────────────


def test_a_count_far_below_the_text_raises_input_dropped(monkeypatch):
    _use(monkeypatch, count=10)
    with pytest.raises(oc.InputDropped) as exc:
        _chat(_user(3000))
    assert (exc.value.count, exc.value.chars, exc.value.floor) == (10, 3000, 300.0)


def test_a_count_at_the_drop_floor_passes(monkeypatch):
    _use(monkeypatch, count=300)
    assert _chat(_user(3000)).prompt_eval_count == 300


def test_a_missing_count_is_logged_unverified_not_raised(monkeypatch, caplog):
    _use(monkeypatch, count=None)
    with caplog.at_level("WARNING", logger="engine.utils.ollama_client"):
        _chat(_user(3000))
    assert "input_fit UNVERIFIED" in caplog.text


def test_every_call_logs_one_structured_input_fit_line(monkeypatch, caplog):
    _use(monkeypatch, count=630)
    with caplog.at_level("INFO", logger="engine.utils.ollama_client"):
        _chat(_user(3000), paper_id=7)
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("input_fit paper_id=7 ")]
    assert len(lines) == 1
    record = json.loads(lines[0].split(" ", 2)[2])
    assert record == {"model": "m", "ceiling": 131_072, "chars": 3000, "estimate_low": 570,
                      "count": 630, "ratio": 0.21, "done_reason": "stop"}


def test_input_fit_errors_are_not_retried(monkeypatch):
    client = _use(monkeypatch, count=10)
    with pytest.raises(oc.InputDropped):
        oc.ollama_chat(model="m", messages=_user(3000), max_retries=2, retry_delay=0)
    assert len(client.chats) == 1


# ── T5: transparency ─────────────────────────────────────────────────


@pytest.mark.parametrize("messages", [
    [{"role": "user", "content": "one message " * 50}],
    [{"role": "system", "content": "You are an extractor."},
     {"role": "user", "content": "paper text " * 400},
     {"role": "user", "content": "Here is your prior analysis. " * 30}],
], ids=["one_message", "three_messages"])
def test_the_guard_changes_nothing_the_client_is_sent(monkeypatch, messages):
    client = _use(monkeypatch, count=lambda kw: int(oc.message_chars(kw["messages"]) * 0.21))
    before = copy.deepcopy(messages)
    extra = dict(format={"type": "object"}, options={"temperature": 0, "seed": 7}, think=False)

    oc.ollama_chat(model="m", messages=messages, max_retries=0, retry_delay=0, **copy.deepcopy(extra))
    client.chat(model="m", messages=copy.deepcopy(before), **copy.deepcopy(extra))  # no guard
    guarded, unguarded = client.chats

    assert json.dumps(guarded, sort_keys=True).encode() == json.dumps(unguarded, sort_keys=True).encode()
    assert guarded["messages"] is messages and messages == before


# ── T9: a middle message dropped by the server ───────────────────────


def test_a_silently_dropped_middle_message_raises_input_dropped(monkeypatch):
    messages = [{"role": "system", "content": "You are an extractor."},
                {"role": "user", "content": "paper text " * 20_000},
                {"role": "user", "content": "Here is your prior analysis. " * 20}]

    def server_drops_the_middle(kwargs):
        kept = [kwargs["messages"][0], kwargs["messages"][-1]]
        return int(oc.message_chars(kept) * 0.25) + 12

    client = _use(monkeypatch, count=server_drops_the_middle)
    chars = oc.message_chars(messages)
    assert chars * oc.RATIO_MIN < oc.effective_ceiling("m")  # passes the pre-call check
    with pytest.raises(oc.InputDropped) as exc:
        _chat(messages)
    assert exc.value.count < chars * oc.RATIO_DROP
    assert exc.value.count < oc.effective_ceiling("m")  # no truncation WARN would have logged
    assert len(client.chats) == 1


# ── T10: the judge's full-text path ──────────────────────────────────


def test_the_judges_full_text_path_at_its_budget_is_never_refused(monkeypatch):
    from analysis.paper1 import judge as judge_module
    from analysis.paper1.judge_prompts import PASS2_FULL_TEXT_BUDGET_TOKENS, _get_encoding, count_tokens
    from analysis.paper1.judge_schema import ArmOutput, JudgeInput
    from analysis.paper1.precheck import PreCheckFlags

    enc = _get_encoding()
    paragraph = ("The autonomous suturing system completed each anastomosis on ex vivo porcine "
                 "tissue while the surgeon supervised from the console; completion time, leak "
                 "pressure and stitch spacing were recorded for every trial. ")
    source = enc.decode(enc.encode(paragraph * 2000, disallowed_special=())[:PASS2_FULL_TEXT_BUDGET_TOKENS - 50])
    assert count_tokens(source) <= PASS2_FULL_TEXT_BUDGET_TOKENS  # sent in full, not windowed

    flags = PreCheckFlags(span_present=True, span_in_source=True, value_in_span=True, span_length=40,
                          span_match_method="exact_substring", value_match_method="categorical_exact")
    arms = [ArmOutput(arm_name=name, value="RCT", span="randomized controlled trial", precheck_flags=flags)
            for name in ("local", "openai_o4_mini_high", "anthropic_sonnet_4_6")]
    inp = JudgeInput(paper_id="p1", field_name="study_design", field_type="categorical",
                     field_definition="Design.", field_valid_values=["RCT", "Cohort"], arms=arms)
    payload = json.dumps({
        "paper_id": "p1", "field_name": "study_design",
        "arm_verdicts": [{"arm_slot": i, "verdict": "SUPPORTED", "verification_span": f"q{i}"}
                         for i in (1, 2, 3)],
        "overall_fabrication_detected": False,
    })
    client = _use(monkeypatch, content=payload,
                  count=lambda kw: int(oc.message_chars(kw["messages"]) * 0.21))
    monkeypatch.setattr(judge_module, "fetch_model_digest", lambda m: "sha256:abc")

    result = judge_module.run_pass2(inp, run_id="r1", source_text=source)

    sent = client.chats[0]
    assert sent["options"]["num_ctx"] == 24_576
    assert oc.message_chars(sent["messages"]) * oc.RATIO_MIN < 24_576
    assert result.paper_id == "p1"


# ── Migrated from the elicitation pipeline's private guard (C6) · T7 ──
#
# `engine/elicitation/sizing.py` and its tests in tests/test_units_and_sizing.py
# were removed in INPUT-FIT-01 Phase 2; the wrapper is the one authority now. Each
# test below carries the intent of the one it replaces.


def test_migrated_an_oversized_prompt_fails_hard_before_any_call(monkeypatch):
    """Was test_overflow_fails_hard_before_any_call: refused before the call, and the
    message names the ceiling."""
    client = _use(monkeypatch, n_ctx_train=131_072)
    over = int(131_072 / oc.RATIO_MIN) + 10
    with pytest.raises(oc.InputOverflow) as exc:
        oc.ollama_chat(model="deepseek-r1:32b", messages=_user(over), paper_id=498,
                       max_retries=0, retry_delay=0)
    assert client.chats == []
    assert exc.value.ceiling == 131_072
    assert "ceiling of 131,072" in str(exc.value)


def test_migrated_a_fitting_prompt_is_sent_with_its_estimate_logged(monkeypatch, caplog):
    """Was test_a_fitting_prompt_returns_its_estimate."""
    client = _use(monkeypatch, count=250)
    with caplog.at_level("INFO", logger="engine.utils.ollama_client"):
        _chat(_user(1000))
    assert len(client.chats) == 1
    line = next(r.getMessage() for r in caplog.records if r.getMessage().startswith("input_fit "))
    assert json.loads(line.split(" ", 2)[2])["estimate_low"] == round(1000 * oc.RATIO_MIN)


@pytest.mark.parametrize("count,raises", [(1000, True), (1001, True), (999, False)])
def test_migrated_truncation_is_reported_only_at_or_above_the_ceiling(monkeypatch, count, raises):
    """Was test_tripwire_fires_only_at_the_ceiling. The tripwire only logged; this raises.
    The no-count case is test_a_missing_count_is_logged_unverified_not_raised."""
    _use(monkeypatch, count=count)
    if raises:
        with pytest.raises(oc.InputTruncated):
            _chat(_user(3000), options={"num_ctx": 1000})
    else:
        assert _chat(_user(3000), options={"num_ctx": 1000}).prompt_eval_count == count


def test_the_elicitation_pipeline_keeps_no_private_guard():
    """T7: no private pre-call or post-call check remains in the elicitation pipeline."""
    import ast
    from pathlib import Path

    import engine.elicitation.pipeline as pipeline

    source = Path(pipeline.__file__)
    tree = ast.parse(source.read_text())
    names = ({n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
             | {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)})
    imported = ({a.name for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom)) for a in n.names}
                | {n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)})
    constants = {n.value for n in ast.walk(tree) if isinstance(n, ast.Constant)}

    private = {"enforce_fit", "truncation_tripwire", "estimate_tokens", "PromptTooLargeError",
               "CEILING_TOKENS", "WORST_RATIO", "INDEX_MARKER_INFLATION"}
    assert not names & private
    assert "sizing" not in imported and "engine.elicitation.sizing" not in imported
    assert 131_072 not in constants
    assert not (source.parent / "sizing.py").exists()
    assert "ollama_chat" in names  # it still calls the guarded wrapper
