"""The one resolver: the effective configuration of every model call (S3a).

MANIFEST-01 Phase 2a. Before this module, a model call's settings came from
wherever its site happened to take them — a spec field at some sites, a module
constant (`extractor.MODEL`) or a literal (`options={"temperature": 0}`) at most
(inventory rows C1, C8, C19). This module is now the only place a call site gets
a model name, options, `think`, `format` or `keep_alive` from, and it reads
**only the validated spec model**: every default it applies is a default declared
in `engine/core/review_spec.py`, and none lives here.

**R66 — session 7 changes what is recorded, not what is sent.** For every stage,
`sent_keys` is exactly the key set the site sent in MANIFEST-01 Phase 1 P1, with
`keep_alive` the one addition. Options a site does not send today (`seed`,
`num_ctx` at most sites) are *recorded* with source `modelfile_or_server` and are
not newly sent. The capture instrument (`tests/test_request_capture.py`) holds
every site to this.

Two entry points, deliberately separate:

* `stage_config(stage, spec=None, *, model=None)` — pure and cheap, no I/O.
  Call sites use it. With `spec=None` it resolves from the spec model's declared
  defaults, which is what a site called without a spec has always done.
* `resolve_run(spec, stages, ...)` — adds what a manifest needs and a call does
  not: the model digest (`fetch_model_digest`, `/api/tags`, which loads no model
  — R57), the prompt hash of each stage's rendered request (R60) and the
  per-option sources.

**One canonical hashing function.** `canonical_json` / `sha256_canonical` are the
only serialisation any manifest hash uses; `engine/core/run_manifest.py` and
migration 020's agreement test import them from here.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Callable, Iterable, Mapping

from engine.core.review_spec import (
    AuditModels, ExtractionModels, FTScreeningModels, OllamaRuntime, PDFParsing,
    PDFQualityCheck, PreflightConfig, ReviewSpec, ScreeningModels,
)

# ── The closed stage vocabulary ──────────────────────────────────────
#: One entry per Ollama generation site measured in MANIFEST-01 Phase 1 P1. The
#: abstract screener is ONE call site serving two roles with two models and two
#: prompts, so it is two stages; a stage carries exactly one model.
#: Migration 020 re-declares this tuple (R35); a test asserts the two agree.
OLLAMA_STAGES: tuple[str, ...] = (
    "abstract_screen_primary", "abstract_screen_verifier",
    "ft_screen_primary", "ft_screen_verifier",
    "audit",
    "extract_pass1", "extract_pass2", "extract_retry_snippet",
    "elicitation_pass1",
    "vision_parse", "pdf_quality",
    "preflight",
)

#: A cloud arm's stage is `cloud:<arm name>` — arms are data (R12), so the set
#: of cloud stages is the spec's, not this module's.
CLOUD_STAGE_PREFIX = "cloud:"

#: Source vocabulary for one option (R66). Re-declared in migration 020.
SOURCES = ("spec", "declared_default", "modelfile_or_server", "caller")

#: The two options R69 makes explicit in the manifest whether or not they are sent.
RECORDED_UNSENT = ("seed", "num_ctx")

#: The value recorded for an option that is not sent. A token, never None (R61).
UNSENT = "unset"


class UnknownStage(KeyError):
    """A stage name outside the closed vocabulary."""


# ── Canonical hashing (one function, owned here) ─────────────────────
def canonical_json(obj: Any) -> str:
    """The single serialisation every manifest hash is taken over."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_canonical(obj: Any) -> str:
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


# ── The effective configuration of one stage ─────────────────────────
@dataclass(frozen=True)
class EffectiveConfig:
    """Everything one call site sends, and where each value came from.

    `options` is exactly the options dict sent. `think` and `format` are sent
    only when their key is in `sent_keys`. `recorded` carries `seed` and
    `num_ctx` whether sent or not (R69). `sources` maps every option and
    top-level setting to one of `SOURCES`.
    """

    stage: str
    provider: str
    model: str
    options: Mapping[str, Any]
    think: bool | None
    format: Mapping[str, Any] | None
    keep_alive: int | str | None
    sent_keys: frozenset[str]
    recorded: Mapping[str, Any]
    sources: Mapping[str, str]

    def kwargs(self) -> dict[str, Any]:
        """The keyword arguments this stage hands to `ollama_chat` besides
        `messages`. The only way a site should build its request. `stage` is
        consumed by `ollama_chat` (run-call recording) and never sent."""
        if self.provider != "ollama":
            raise ValueError(f"{self.stage}: kwargs() is the Ollama request shape")
        out: dict[str, Any] = {"stage": self.stage, "model": self.model,
                               "options": dict(self.options)}
        if "format" in self.sent_keys:
            out["format"] = _thaw(self.format)
        if "think" in self.sent_keys:
            out["think"] = self.think
        if "keep_alive" in self.sent_keys:
            out["keep_alive"] = self.keep_alive
        return out

    def with_model(self, model: str, *, source: str = "caller") -> "EffectiveConfig":
        """A caller-named model (the preflight target, a `model=` override)."""
        return _replace(self, model=model, sources={**self.sources, "model": source})

    def with_options(self, extra: Mapping[str, Any] | None) -> "EffectiveConfig":
        """A caller's option override — today only `scripts/eval_auditor_models.py`.
        Recorded as source `caller`; a run manifest never uses one."""
        if not extra:
            return self
        opts = {**self.options, **extra}
        srcs = {**self.sources, **{f"options.{k}": "caller" for k in extra}}
        rec = {**self.recorded, **{k: v for k, v in extra.items() if k in RECORDED_UNSENT}}
        return _replace(self, options=opts, recorded=rec, sources=srcs)

    @property
    def options_hash(self) -> str:
        """Every request kwarg except model, messages and format (S3d's key)."""
        return sha256_canonical({
            "options": dict(self.options),
            "think": self.think if "think" in self.sent_keys else UNSENT,
            "keep_alive": self.keep_alive if "keep_alive" in self.sent_keys else UNSENT,
            "recorded": dict(self.recorded),
        })

    @property
    def format_schema_hash(self) -> str:
        """`'none'` when the stage sends no format — never NULL by omission."""
        return sha256_canonical(_thaw(self.format)) if "format" in self.sent_keys else "none"


def _replace(cfg: EffectiveConfig, **changes) -> EffectiveConfig:
    data = {f: getattr(cfg, f) for f in cfg.__dataclass_fields__}
    data.update(changes)
    data["options"] = MappingProxyType(dict(data["options"]))
    data["recorded"] = MappingProxyType(dict(data["recorded"]))
    data["sources"] = MappingProxyType(dict(data["sources"]))
    return EffectiveConfig(**data)


def _thaw(obj):
    if isinstance(obj, Mapping):
        return {k: _thaw(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_thaw(v) for v in obj]
    return obj


def _src(block, name: str) -> str:
    """`spec` if the YAML declared the field, `declared_default` otherwise."""
    return "spec" if block is not None and name in getattr(block, "model_fields_set", ()) else "declared_default"


def _block(spec, attr: str, cls):
    """The spec's block, or the spec model's declared defaults when no spec.

    `spec` may also be any object carrying just the block (a runner handed a
    `PDFQualityCheck` rather than a whole spec); a missing block is the default.
    """
    blk = getattr(spec, attr, None) if spec is not None else None
    if blk is None:
        return None, cls()
    if not isinstance(blk, cls):
        # A duck-typed block (test fakes carry only the fields they set): every
        # field it lacks is the spec model's declared default — the same answer
        # the sites' old `getattr(models, "pass1_think", True)` gave.
        return blk, _WithDefaults(blk, cls())
    return blk, blk


class _WithDefaults:
    def __init__(self, blk, defaults):
        self._blk, self._defaults = blk, defaults

    def __getattr__(self, name):
        return getattr(self._blk, name, getattr(self._defaults, name))


def _unsent(block, recorded: dict, sources: dict, options: dict) -> None:
    """R66/R69: seed and num_ctx are sent only if the spec declares them."""
    for name in RECORDED_UNSENT:
        value = getattr(block, name, None)
        if value is None:
            recorded[name] = UNSENT
            sources[f"options.{name}"] = "modelfile_or_server"
        else:
            options[name] = value
            recorded[name] = value
            sources[f"options.{name}"] = "spec"


def _freeze(stage, provider, model, options, think, fmt, keep_alive, sent, recorded, sources):
    return EffectiveConfig(
        stage=stage, provider=provider, model=model,
        options=MappingProxyType(dict(options)), think=think,
        format=MappingProxyType(dict(fmt)) if fmt is not None else None,
        keep_alive=keep_alive, sent_keys=frozenset(sent),
        recorded=MappingProxyType(dict(recorded)), sources=MappingProxyType(dict(sources)),
    )


def _keep_alive(spec) -> tuple[Any, str]:
    src_block, rt = _block(spec, "ollama", OllamaRuntime)
    return rt.keep_alive, _src(src_block, "keep_alive")


# ── Formats (lazy: the schema classes live beside their sites) ───────
def _format(stage: str, spec) -> dict | None:
    if stage in ("abstract_screen_primary", "abstract_screen_verifier"):
        from engine.agents.screener import ScreeningDecision
        return ScreeningDecision.model_json_schema()
    if stage == "ft_screen_primary":
        from engine.agents.ft_screener import FTScreeningDecision
        from engine.core import eligibility_render as render
        return render.with_reason_code_vocabulary(
            FTScreeningDecision.model_json_schema(), spec.eligibility)
    if stage == "ft_screen_verifier":
        from engine.agents.ft_screener import FTVerificationDecision
        return FTVerificationDecision.model_json_schema()
    if stage == "audit":
        from engine.agents.auditor import AuditVerdict
        return AuditVerdict.model_json_schema()
    if stage == "extract_pass2":
        from engine.agents.models import ExtractionOutput
        return ExtractionOutput.model_json_schema()
    return None


# ── stage_config: the pure resolver ──────────────────────────────────
def stage_config(stage: str, spec: ReviewSpec | None = None, *,
                 model: str | None = None) -> EffectiveConfig:
    """The effective configuration of one Ollama stage. No I/O.

    `model` is for the two sites whose model is a caller argument by nature:
    `preflight` (which model to probe) and a `model=` override a site's
    signature has always accepted. It is recorded as source `caller`.
    """
    if stage.startswith(CLOUD_STAGE_PREFIX):
        return cloud_stage_config(spec, stage[len(CLOUD_STAGE_PREFIX):])
    if stage not in OLLAMA_STAGES:
        raise UnknownStage(stage)

    options: dict[str, Any] = {}
    recorded: dict[str, Any] = {}
    sources: dict[str, str] = {}
    think: bool | None = None
    sent = {"model", "messages", "options", "keep_alive"}
    keep_alive, sources["keep_alive"] = _keep_alive(spec)

    if stage in ("abstract_screen_primary", "abstract_screen_verifier"):
        blk_src, blk = _block(spec, "screening_models", ScreeningModels)
        attr = "primary" if stage.endswith("primary") else "verification"
        chosen, sources["model"] = getattr(blk, attr), _src(blk_src, attr)
        options["temperature"], sources["options.temperature"] = blk.temperature, _src(blk_src, "temperature")
        think, sources["think"] = blk.think, _src(blk_src, "think")
        sent |= {"format", "think"}
        _unsent(blk, recorded, sources, options)
    elif stage in ("ft_screen_primary", "ft_screen_verifier"):
        blk_src, blk = _block(spec, "ft_screening_models", FTScreeningModels)
        attr = "primary" if stage.endswith("primary") else "verifier"
        chosen, sources["model"] = getattr(blk, attr), _src(blk_src, attr)
        options["temperature"], sources["options.temperature"] = blk.temperature, _src(blk_src, "temperature")
        think, sources["think"] = blk.think, _src(blk_src, "think")
        sent |= {"format", "think"}
        _unsent(blk, recorded, sources, options)
    elif stage == "audit":
        blk_src, blk = _block(spec, "audit", AuditModels)
        override = getattr(spec, "auditor_model", None) if spec is not None else None
        if override:
            chosen, sources["model"] = override, "spec"
        else:
            chosen, sources["model"] = blk.model, _src(blk_src, "model")
        options["temperature"], sources["options.temperature"] = blk.temperature, _src(blk_src, "temperature")
        think, sources["think"] = blk.think, _src(blk_src, "think")
        sent |= {"format", "think"}
        _unsent(blk, recorded, sources, options)
    elif stage in ("extract_pass1", "extract_pass2", "extract_retry_snippet", "elicitation_pass1"):
        blk_src, blk = _block(spec, "extraction_models", ExtractionModels)
        chosen, sources["model"] = blk.extractor, _src(blk_src, "extractor")
        options["temperature"], sources["options.temperature"] = blk.temperature, _src(blk_src, "temperature")
        think_attr = {"extract_pass1": "pass1_think", "elicitation_pass1": "pass1_think",
                      "extract_pass2": "pass2_think",
                      "extract_retry_snippet": "retry_think"}[stage]
        think, sources["think"] = getattr(blk, think_attr), _src(blk_src, think_attr)
        sent |= {"think"}
        if stage == "extract_pass2":
            sent |= {"format"}
        _unsent(blk, recorded, sources, options)
    elif stage == "vision_parse":
        blk_src, blk = _block(spec, "pdf_parsing", PDFParsing)
        chosen, sources["model"] = blk.vision_model, _src(blk_src, "vision_model")
        options["temperature"], sources["options.temperature"] = blk.vision_temperature, _src(blk_src, "vision_temperature")
        options["num_predict"], sources["options.num_predict"] = blk.vision_num_predict, _src(blk_src, "vision_num_predict")
        options["num_ctx"], sources["options.num_ctx"] = blk.vision_num_ctx, _src(blk_src, "vision_num_ctx")
        recorded["num_ctx"] = blk.vision_num_ctx
        recorded["seed"] = UNSENT
        sources["options.seed"] = "modelfile_or_server"
    elif stage == "pdf_quality":
        blk_src, blk = _block(spec, "pdf_quality_check", PDFQualityCheck)
        chosen, sources["model"] = blk.ai_model, _src(blk_src, "ai_model")
        options["temperature"], sources["options.temperature"] = blk.temperature, _src(blk_src, "temperature")
        _unsent(blk, recorded, sources, options)
    else:  # preflight
        blk_src, blk = _block(spec, "preflight", PreflightConfig)
        chosen, sources["model"] = "", "caller"
        options["temperature"], sources["options.temperature"] = blk.temperature, _src(blk_src, "temperature")
        options["num_predict"], sources["options.num_predict"] = blk.num_predict, _src(blk_src, "num_predict")
        recorded.update(seed=UNSENT, num_ctx=UNSENT)
        sources["options.seed"] = sources["options.num_ctx"] = "modelfile_or_server"

    if "format" in sent:
        sources["format"] = "declared_default"
    if model is not None:
        chosen, sources["model"] = model, "caller"
    return _freeze(stage, "ollama", chosen, options, think,
                   _format(stage, spec) if "format" in sent else None,
                   keep_alive, sent, recorded, sources)


def cloud_stage_config(spec: ReviewSpec, arm_name: str) -> EffectiveConfig:
    """The request a cloud arm sends: its model and provider parameters."""
    if spec is None:
        raise ValueError("a cloud stage resolves from a spec's arms block; there is no default arm")
    arm = spec.arm(arm_name)
    if arm.provider not in ("openai", "anthropic"):
        raise ValueError(f"arm {arm_name!r} is not a cloud arm (provider {arm.provider!r})")
    params = arm.effective_options()
    sources = {"model": "spec",
               **{f"options.{k}": ("spec" if arm.options is not None else "declared_default")
                  for k in params}}
    sent = {"model", "messages"} | set(params)
    return _freeze(f"{CLOUD_STAGE_PREFIX}{arm_name}", arm.provider, arm.model, params,
                   None, None, None, sent, {}, sources)


# ── Rendering, for prompt_hash (R60) ─────────────────────────────────
#: Fixed sentinel inputs. A stage's prompt_hash is the hash of the request its
#: own builder renders around these, so it identifies the TEMPLATE (system
#: message, user-turn wording, codebook, format) and never a spec sub-block —
#: which is how C17 (pico reaching ten surfaces unhashed) cannot recur here.
SENTINEL_PAPER = {"title": "T", "abstract": "A", "id": 1}
SENTINEL_TEXT = "Title: T\n\nAbstract: A"
SENTINEL_IMAGE = "<image>"


def render_messages(stage: str, spec: ReviewSpec | None, *,
                    codebook_path=None) -> list[dict]:
    """The messages `stage`'s own builder produces for the sentinel inputs."""
    if stage in ("abstract_screen_primary", "abstract_screen_verifier"):
        from engine.agents.screener import build_messages
        role = "primary" if stage.endswith("primary") else "verifier"
        return build_messages(SENTINEL_PAPER, spec, role=role)
    if stage in ("ft_screen_primary", "ft_screen_verifier"):
        from engine.agents.ft_screener import build_ft_messages
        which = "primary" if stage.endswith("primary") else "verifier"
        return build_ft_messages(SENTINEL_TEXT, spec, which)
    if stage == "audit":
        from engine.agents.auditor import build_audit_messages
        from engine.agents.models import EvidenceSpan
        span = EvidenceSpan(field_name="f", value="v", source_snippet="s", confidence=0.5, tier=1)
        # Both field-type variants: the audit template branches on it.
        return (build_audit_messages(span, field_type="text")
                + build_audit_messages(span, field_type="categorical"))
    if stage in ("extract_pass1", "extract_pass2", "extract_retry_snippet"):
        from engine.agents import extractor as ex
        prompt = ex.build_extraction_prompt(SENTINEL_TEXT, spec, codebook_path)
        if stage == "extract_pass1":
            return ex.pass1_messages(prompt)
        if stage == "extract_pass2":
            return ex.pass2_messages(prompt, "R")
        return ex.retry_snippet_messages("f", "v", SENTINEL_TEXT)
    if stage == "elicitation_pass1":
        from engine.elicitation import pipeline as el
        return el.pass1_messages(el.sentinel_pass1_prompt(spec, codebook_path))
    if stage == "vision_parse":
        from engine.parsers.pdf_parser import vision_messages
        return vision_messages(SENTINEL_IMAGE)
    if stage == "pdf_quality":
        from engine.acquisition.pdf_quality_check import classification_messages
        return classification_messages(SENTINEL_IMAGE)
    if stage == "preflight":
        from engine.utils.ollama_preflight import PREFLIGHT_MESSAGES
        return [dict(m) for m in PREFLIGHT_MESSAGES]
    if stage.startswith(CLOUD_STAGE_PREFIX):
        from engine.cloud.base import outbound_messages
        from engine.agents.extractor import build_extraction_prompt
        return outbound_messages(build_extraction_prompt(SENTINEL_TEXT, spec, codebook_path))
    raise UnknownStage(stage)


def prompt_hash(stage: str, spec: ReviewSpec | None, cfg: EffectiveConfig, *,
                codebook_path=None) -> str:
    """R60: hash of the rendered system + user messages + format, per stage."""
    messages = render_messages(stage, spec, codebook_path=codebook_path)
    body = {"messages": messages,
            "format": _thaw(cfg.format) if "format" in cfg.sent_keys else "none"}
    if cfg.provider != "ollama":
        # C16: a cloud template is its messages AND its provider parameters.
        body["parameters"] = dict(cfg.options)
    return sha256_canonical(body)


# ── resolve_run: what a manifest needs ───────────────────────────────
@dataclass(frozen=True)
class ResolvedStage:
    config: EffectiveConfig
    model_digest: str | None
    prompt_hash: str
    arm_name: str | None = None

    @property
    def options_hash(self) -> str:
        return self.config.options_hash

    @property
    def format_schema_hash(self) -> str:
        return self.config.format_schema_hash


def resolve_run(spec: ReviewSpec, stages: Iterable[str], *,
                digest_fn: Callable[[str], str] | None = None,
                preflight_models: Iterable[str] = (),
                codebook_path=None,
                arms_by_stage: Mapping[str, str] | None = None) -> dict[str, ResolvedStage]:
    """Every stage a run will call, resolved, digested and prompt-hashed.

    Keys are stage names, except `preflight`, which yields one entry per probed
    model keyed `preflight:<model>` — its model is the caller's by nature (R63).
    `digest_fn` defaults to `fetch_model_digest` (`/api/tags`; R57) and raises
    on failure — a manifest without a digest is refused, never written.
    """
    if digest_fn is None:
        from engine.utils.ollama_client import fetch_model_digest as digest_fn
    arms_by_stage = dict(arms_by_stage or {})
    digests: dict[str, str] = {}

    def _digest(model: str) -> str:
        if model not in digests:
            digests[model] = digest_fn(model)
        return digests[model]

    out: dict[str, ResolvedStage] = {}
    for stage in stages:
        if stage == "preflight":
            for m in preflight_models:
                cfg = stage_config("preflight", spec, model=m)
                out[f"preflight:{m}"] = ResolvedStage(
                    cfg, _digest(m), prompt_hash("preflight", spec, cfg))
            continue
        cfg = stage_config(stage, spec)
        digest = _digest(cfg.model) if cfg.provider == "ollama" else None
        arm = arms_by_stage.get(stage)
        if stage.startswith(CLOUD_STAGE_PREFIX):
            arm = stage[len(CLOUD_STAGE_PREFIX):]
        out[stage] = ResolvedStage(
            cfg, digest, prompt_hash(stage, spec, cfg, codebook_path=codebook_path), arm)
    return out
