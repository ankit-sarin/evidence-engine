"""Tests for codebook-driven extraction prompt.

Verifies that build_extraction_prompt() reads from extraction_codebook.yaml
and includes all field definitions, decision criteria, and examples.
"""

import yaml
import pytest

from engine.agents.extractor import build_extraction_prompt
from engine.core.review_spec import load_review_spec


@pytest.fixture
def spec():
    return load_review_spec("review_specs/surgical_autonomy_v1.yaml")


@pytest.fixture
def codebook():
    with open("data/surgical_autonomy/extraction_codebook.yaml") as f:
        return yaml.safe_load(f)


@pytest.fixture
def prompt(spec):
    return build_extraction_prompt("[PAPER TEXT]", spec)


class TestCodebookFieldCoverage:
    """Every codebook field appears in the generated prompt."""

    def test_all_codebook_fields_in_prompt(self, codebook, prompt):
        for field in codebook["fields"]:
            assert f"**{field['name']}**" in prompt, (
                f"Field {field['name']} missing from prompt"
            )

    def test_all_spec_fields_in_prompt(self, spec, prompt):
        for field in spec.extraction_schema.fields:
            assert f"**{field.name}**" in prompt


class TestCRACodebookContent:
    """clinical_readiness_assessment — the gap that caused distribution collapse."""

    def test_cra_has_proof_of_concept_definition(self, prompt):
        assert "working system demonstrated performing the task on a phantom" in prompt

    def test_cra_has_early_stage_definition(self, prompt):
        assert "Foundational algorithmic or computational work" in prompt

    def test_cra_has_approaching_definition(self, prompt):
        assert "demonstrated in vivo" in prompt

    def test_cra_has_decision_criteria(self, prompt):
        assert "Does the paper demonstrate a working system performing an autonomous task" in prompt

    def test_cra_has_examples(self, prompt):
        assert "Algorithm autonomously sutures on a tissue phantom using dVRK" in prompt
        assert "STAR robot performs in vivo bowel anastomosis in pigs" in prompt
        assert "Path-planning algorithm tested in simulation only" in prompt


class TestAutonomyLevelContent:
    """autonomy_level retains full decision tree and per-level definitions."""

    def test_decision_tree_present(self, prompt):
        assert "Does the robot execute any action without continuous real-time human control" in prompt

    def test_per_level_definitions(self, prompt):
        assert "Robot executes specific preprogrammed or learned tasks autonomously" in prompt  # Level 2
        assert "Robot generates candidate strategies" in prompt  # Level 3
        assert "Robot independently plans and executes" in prompt  # Level 4


class TestSystemMaturityContent:
    """system_maturity has per-value definitions."""

    def test_value_definitions(self, prompt):
        assert "FDA-cleared or CE-marked robot" in prompt
        assert "Purpose-built physical robot not commercially available" in prompt
        assert "No experimental demonstration" in prompt


class TestNoFieldGuidesRemnant:
    """_FIELD_GUIDES dict has been removed."""

    def test_no_field_guides_attribute(self):
        import engine.agents.extractor as mod
        assert not hasattr(mod, "_FIELD_GUIDES"), (
            "_FIELD_GUIDES still exists in extractor module"
        )

    def test_no_field_guides_in_source(self):
        import inspect
        source = inspect.getsource(__import__("engine.agents.extractor", fromlist=["_"]))
        assert "_FIELD_GUIDES" not in source


class TestPromptStructure:
    """Prompt has expected structural elements."""

    def test_has_all_tiers(self, prompt):
        assert "Tier 1" in prompt
        assert "Tier 2" in prompt
        assert "Tier 3" in prompt
        assert "Tier 4" in prompt

    def test_has_instructions(self, prompt):
        assert "## Instructions" in prompt

    def test_has_paper_text_section(self, prompt):
        assert "## Paper Text" in prompt
        assert "[PAPER TEXT]" in prompt

    def test_field_count_instruction(self, prompt):
        assert "20 fields total" in prompt
