"""The resolver (T1): explicit options, deterministic hashes, and C17.

MANIFEST-01 Phase 2a. The capture instrument (`test_request_capture.py`) proves
what each site SENDS; these tests prove what the resolver PRODUCES and that its
hashes move exactly when their inputs do.
"""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path

import pytest

from engine.core import effective_config as ec
from engine.core.review_paths import load_spec_for
from engine.core.run_manifest import spec_hash

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")

SCREENING_STAGES = ("abstract_screen_primary", "abstract_screen_verifier",
                    "ft_screen_primary", "ft_screen_verifier")


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


def _cfg(stage, spec):
    return ec.stage_config(stage, spec, model="m" if stage == "preflight" else None)


@pytest.mark.parametrize("stage", ec.OLLAMA_STAGES)
def test_every_stage_yields_explicit_options(stage, spec):
    """R61: nothing the resolver produces is None, so `exclude_none` below the
    boundary can never silently drop a declared value."""
    cfg = _cfg(stage, spec)
    assert cfg.model
    assert all(v is not None for v in cfg.options.values())
    kw = cfg.kwargs()
    assert all(v is not None for v in kw.values())
    assert set(kw) - {"stage"} | {"messages"} == set(cfg.sent_keys)
    # R69: seed and num_ctx are explicit in the record whether or not sent.
    assert set(ec.RECORDED_UNSENT) <= set(cfg.recorded)
    assert all(v is not None for v in cfg.recorded.values())
    # Every option and top-level setting names its source (R66).
    assert set(cfg.sources.values()) <= set(ec.SOURCES)
    assert "model" in cfg.sources and "keep_alive" in cfg.sources


@pytest.mark.parametrize("stage", ec.OLLAMA_STAGES)
def test_keep_alive_is_the_one_addition_and_equals_the_service_value(stage, spec):
    assert _cfg(stage, spec).kwargs()["keep_alive"] == -1


def test_seed_and_num_ctx_are_recorded_as_unsent_where_they_are_not_sent(spec):
    """R66: not newly sent. Vision is the one stage that has always sent num_ctx."""
    for stage in ec.OLLAMA_STAGES:
        cfg = _cfg(stage, spec)
        assert "seed" not in cfg.options
        assert cfg.recorded["seed"] == ec.UNSENT
        assert cfg.sources["options.seed"] == "modelfile_or_server"
        if stage == "vision_parse":
            assert cfg.options["num_ctx"] == 8192 and cfg.recorded["num_ctx"] == 8192
        else:
            assert "num_ctx" not in cfg.options and cfg.recorded["num_ctx"] == ec.UNSENT


def test_hashes_are_deterministic(spec):
    for stage in ec.OLLAMA_STAGES:
        a, b = _cfg(stage, spec), _cfg(stage, spec)
        assert a.options_hash == b.options_hash
        assert a.format_schema_hash == b.format_schema_hash
        assert ec.prompt_hash(stage, spec, a) == ec.prompt_hash(stage, spec, b)


def test_a_changed_spec_option_changes_options_hash_and_nothing_else(spec):
    before = _cfg("ft_screen_primary", spec)
    changed = spec.model_copy(update={"ft_screening_models":
                                      spec.ft_screening_models.model_copy(update={"temperature": 0.5})})
    after = _cfg("ft_screen_primary", changed)
    assert after.options_hash != before.options_hash
    assert after.format_schema_hash == before.format_schema_hash
    assert after.sources["options.temperature"] == before.sources["options.temperature"]


def test_a_declared_num_ctx_is_sent_and_its_source_is_the_spec(spec):
    changed = spec.model_copy(update={"extraction_models":
                                      spec.extraction_models.model_copy(update={"num_ctx": 32768})})
    cfg = _cfg("extract_pass1", changed)
    assert cfg.options["num_ctx"] == 32768
    assert cfg.sources["options.num_ctx"] == "spec"
    assert cfg.options_hash != _cfg("extract_pass1", spec).options_hash


def test_the_sent_temperature_keeps_its_literal_type(spec):
    """R66: the integer sites still send the integer; the FT sites send the float."""
    assert type(_cfg("extract_pass1", spec).options["temperature"]) is int
    assert type(_cfg("abstract_screen_primary", spec).options["temperature"]) is int
    assert type(_cfg("ft_screen_primary", spec).options["temperature"]) is float


# ── C17: prompt_hash is the rendered request, never a spec sub-block ─
def test_a_pico_edit_moves_the_screening_prompt_hashes_and_not_the_others(spec):
    pico = spec.pico.model_copy(update={"population": spec.pico.population + " X"})
    changed = spec.model_copy(update={"pico": pico})
    for stage in ec.OLLAMA_STAGES:
        a, b = _cfg(stage, spec), _cfg(stage, changed)
        moved = ec.prompt_hash(stage, spec, a) != ec.prompt_hash(stage, changed, b)
        assert moved == (stage in SCREENING_STAGES), stage
    # ...while the eligibility hash cannot see it at all (C17) and the whole-spec
    # hash the manifest records can.
    assert changed.screening_hash() == spec.screening_hash()
    assert spec_hash(changed) != spec_hash(spec)


def test_a_pico_edit_moves_ten_frozen_surfaces_and_not_the_four(spec):
    """C17 measured by the frozen surfaces' own renderers (MANIFEST-01 Phase 1 P3)."""
    from tests import test_eligibility as te
    pico = spec.pico.model_copy(update={"population": spec.pico.population + " X"})
    changed = spec.model_copy(update={"pico": pico})
    moved = {k for k in te.FROZEN
             if hashlib.sha256(te._surface(changed, k).encode()).hexdigest() != te.FROZEN[k]}
    assert moved == set(te.FROZEN) - {"H1", "H3", "H6", "H7"}


def test_a_title_edit_moves_only_the_abstract_primary_prompt_hash(spec):
    changed = spec.model_copy(update={"title": spec.title + " X"})
    moved = {s for s in ec.OLLAMA_STAGES
             if ec.prompt_hash(s, spec, _cfg(s, spec)) != ec.prompt_hash(s, changed, _cfg(s, changed))}
    assert moved == {"abstract_screen_primary"}


def test_a_cloud_prompt_hash_covers_its_parameters(spec):
    """C16/R58: the outbound template is messages AND parameters."""
    arm = next(a for a in spec.arms if a.provider == "openai")
    cfg = ec.cloud_stage_config(spec, arm.name)
    h1 = ec.prompt_hash(f"cloud:{arm.name}", spec, cfg)
    other = spec.model_copy(update={"arms": [
        a.model_copy(update={"options": {"reasoning_effort": "low",
                                          "response_format": {"type": "json_object"}}})
        if a.name == arm.name else a for a in spec.arms]})
    h2 = ec.prompt_hash(f"cloud:{arm.name}", other, ec.cloud_stage_config(other, arm.name))
    assert h1 != h2


def test_canonical_json_is_the_one_serialisation():
    assert ec.canonical_json({"b": 1, "a": [1, "é"]}) == '{"a":[1,"é"],"b":1}'


# ── T7: the retired constants and literals are gone ──────────────────
SITE_MODULES = [
    "engine/agents/screener.py", "engine/agents/ft_screener.py", "engine/agents/auditor.py",
    "engine/agents/extractor.py", "engine/elicitation/pipeline.py",
    "engine/parsers/pdf_parser.py", "engine/acquisition/pdf_quality_check.py",
    "engine/utils/ollama_preflight.py", "engine/cloud/openai_extractor.py",
    "engine/cloud/anthropic_extractor.py",
]


@pytest.mark.parametrize("rel", SITE_MODULES)
def test_no_site_builds_an_options_dict_or_names_a_model_literal(rel):
    """C1/C8/C19: a site's options and model come from the resolver. Checked on
    the AST: no `options=` keyword with a dict literal, and no `model=` keyword
    with a string literal, anywhere a call is made."""
    tree = ast.parse((REPO / rel).read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "options":
                    assert not isinstance(kw.value, ast.Dict), (rel, ast.unparse(node)[:80])
                if kw.arg == "model":
                    assert not (isinstance(kw.value, ast.Constant)
                                and isinstance(kw.value.value, str)), (rel, ast.unparse(node)[:80])


@pytest.mark.parametrize("module, name", [
    ("engine.agents.extractor", "MODEL"),
    ("engine.agents.auditor", "DEFAULT_AUDITOR_MODEL"),
    ("engine.agents.screener", "DEFAULT_PRIMARY_MODEL"),
    ("engine.agents.screener", "DEFAULT_VERIFICATION_MODEL"),
    ("engine.parsers.pdf_parser", "_VISION_MODEL"),
    ("engine.parsers.pdf_parser", "_VISION_NUM_PREDICT"),
    ("engine.parsers.pdf_parser", "_VISION_NUM_CTX"),
    ("engine.cloud.openai_extractor", "_DEFAULT_MODEL"),
    ("engine.cloud.anthropic_extractor", "_DEFAULT_COST_INPUT_PER_M"),
    ("engine.utils.ollama_client", "get_model_digest"),
])
def test_the_retired_constant_is_gone(module, name):
    import importlib
    assert not hasattr(importlib.import_module(module), name)


def test_no_cloud_extractor_carries_a_class_constant_arm():
    from engine.cloud.anthropic_extractor import AnthropicExtractor
    from engine.cloud.openai_extractor import OpenAIExtractor
    assert not hasattr(OpenAIExtractor, "ARM") and not hasattr(AnthropicExtractor, "ARM")
