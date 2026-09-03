"""Gate 2 — prompt construction carries no hand-curated field lists.

ELICIT-DESIGN-01 STEP 0 found five field names hard-coded into
`build_extraction_prompt`'s Instructions block, four of them redundant with the
field's own codebook `instruction` and one — `surgical_domain` — flatly
contradicting it: the clause instructed semicolon-joining where the codebook
defines a `Multiple` valid value, while the very next line of the same block
forbade combining allowed values. The prompt contradicted the codebook and
itself, and did so through Run 6.

The defect class, not the instance, is what this test pins: a per-field rule
written into prompt code cannot be reviewed against the codebook, because
nothing makes the two meet. Rules belong in the codebook entry, which
`_build_field_block` already renders.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest
import yaml

CODEBOOK = Path("data/surgical_autonomy/extraction_codebook.yaml")

# Prompt builders whose source must name no codebook field. Add new builders here.
BUILDERS = ("build_extraction_prompt",)


def _codebook_field_names() -> list[str]:
    data = yaml.safe_load(CODEBOOK.read_text())
    return [f["name"] for f in data["fields"]]


@pytest.mark.skipif(not CODEBOOK.exists(), reason="review codebook not present")
@pytest.mark.parametrize("builder", BUILDERS)
def test_prompt_builder_names_no_codebook_field(builder):
    from engine.agents import extractor

    src = inspect.getsource(getattr(extractor, builder))
    offenders = [n for n in _codebook_field_names() if n in src]
    assert not offenders, (
        f"{builder} hard-codes codebook field names {offenders}. Per-field rules "
        "belong in that field's codebook `instruction`, which _build_field_block "
        "already renders — see ELICIT-DESIGN-01 C3."
    )


@pytest.mark.skipif(not CODEBOOK.exists(), reason="review codebook not present")
def test_surgical_domain_multiple_rule_comes_from_the_codebook():
    """The specific defect: the codebook's rule is `Multiple`, not semicolons."""
    data = yaml.safe_load(CODEBOOK.read_text())
    entry = next(f for f in data["fields"] if f["name"] == "surgical_domain")
    assert "Multiple" in [v["value"] for v in entry["valid_values"]]
    assert "Multiple" in entry["instruction"]
    assert "semicolon" not in entry["instruction"].lower()
