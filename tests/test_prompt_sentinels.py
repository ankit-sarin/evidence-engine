"""R131 (WRITE-PATH-01 9b-2d T11): the two prompt-content sites read the
codebook's canonical absence sentinel and spell no absence literal of their own.

Each is rendered twice — against the live codebook, whose canonical sentinel is
"NR", and against a copy whose canonical sentinel is changed — so a site that
still hard-coded its sentinel would pass the first render and fail the second.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from engine.agents.extractor import build_extraction_prompt
from engine.core.codebook import load_codebook
from engine.core.review_spec import load_review_spec
from engine.elicitation.prompts import build_pass1_prompt
from engine.elicitation.units import build_unit_map

REPO = Path(__file__).resolve().parent.parent
LIVE = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")


@pytest.fixture(params=["NR", "NOT REPORTED"])
def codebook_path(request, tmp_path):
    doc = yaml.safe_load(LIVE.read_text())
    doc["canonical_absence_sentinel"] = request.param
    path = tmp_path / "extraction_codebook.yaml"
    path.write_text(yaml.safe_dump(doc, sort_keys=False))
    return path


def _instructions(prompt: str) -> str:
    return prompt.split("## Instructions", 1)[1]


def test_t11_the_extraction_prompt_renders_the_canonical_sentinel(spec, codebook_path):
    cb = load_codebook(codebook_path)
    text = _instructions(build_extraction_prompt("Paper.", spec, codebook_path))
    assert text.count(f'"{cb.canonical_absence_sentinel}"') == 2
    others = [s for s in cb.absence_sentinels if s != cb.canonical_absence_sentinel]
    for s in others:
        assert f'"{s}"' not in text, s


def test_t11_the_worked_example_uses_the_canonical_sentinel(codebook_path):
    cb = load_codebook(codebook_path)
    names = tuple(f["name"] for f in cb.fields)
    prompt = build_pass1_prompt(build_unit_map(1, "One sentence here."), cb.raw, names)
    example = prompt.split("## Worked example", 1)[1].split("## Output", 1)[0]
    values = re.findall(r'"value": "([^"]*)"', example)
    assert cb.canonical_absence_sentinel in values
    assert not set(values) & (set(cb.absence_sentinels) - {cb.canonical_absence_sentinel})


def test_t11_neither_site_spells_an_absence_literal():
    import inspect
    from engine.agents import extractor
    from engine.elicitation import prompts
    src = inspect.getsource(extractor.build_extraction_prompt) + inspect.getsource(prompts)
    assert '"NOT_FOUND"' not in src and '"NR"' not in src
