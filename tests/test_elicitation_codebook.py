"""ELICIT-DESIGN-01 acceptance gate 2 — prompt construction hand-lists no field.

A class's membership is derived from the codebook, so adding a field to the
codebook adds it to its class's contract with no code change. The three-way
class pin lives in `tests/test_codebook_field_class.py`.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest
import yaml

from engine.elicitation import classes as C
from engine.elicitation import prompts as P

CODEBOOK_PATH = Path("data/surgical_autonomy/extraction_codebook.yaml")

pytestmark = pytest.mark.skipif(
    not CODEBOOK_PATH.exists(), reason="review codebook not present"
)


@pytest.fixture(scope="module")
def cb():
    return yaml.safe_load(CODEBOOK_PATH.read_text())


# ══ gate 2 — no hand-curated field lists ══════════════════════════════


ELICITATION_BUILDERS = (
    P.build_pass1_prompt, P.group_by_class, P.prompt_field_order,
    P.build_pass2_priming_message,
)


@pytest.mark.parametrize("fn", ELICITATION_BUILDERS, ids=lambda f: f.__name__)
def test_prompt_builders_name_no_codebook_field(fn, cb):
    src = inspect.getsource(fn)
    offenders = [f["name"] for f in cb["fields"] if f["name"] in src]
    assert not offenders, f"{fn.__name__} hard-codes field names {offenders}"


def test_class_membership_is_derived_not_listed(cb):
    """Add a field to the codebook and its class picks it up with no code change."""
    grown = dict(cb)
    grown["fields"] = cb["fields"] + [{
        "name": "zzz_new_field", "type": "free_text", "field_class": "inferable",
        "definition": "A field that exists only in this test.",
    }]
    names = tuple(f["name"] for f in grown["fields"])
    assert "zzz_new_field" in [f["name"] for f in C.fields_by_class(C.INFERABLE, grown)]
    assert "zzz_new_field" in P.prompt_field_order(grown, names)
    prompt = P.build_pass1_prompt(_stub_unit_map(), grown, names)
    assert "zzz_new_field" in prompt


def test_an_unclassified_field_refuses_to_build_a_prompt(cb):
    broken = dict(cb)
    broken["fields"] = [dict(f) for f in cb["fields"]]
    broken["fields"][0].pop("field_class")
    with pytest.raises(C.CodebookContractError):
        C.classes_by_field(broken)


def test_a_codebook_with_no_escape_token_refuses_to_build_a_prompt():
    with pytest.raises(C.CodebookContractError):
        C.escape_token({"fields": []})


def test_pass1_prompt_states_both_tokens_and_all_three_contracts(cb):
    names = tuple(f["name"] for f in cb["fields"])
    prompt = P.build_pass1_prompt(_stub_unit_map(), cb, names)
    assert C.escape_token(cb) in prompt
    for s in C.absence_sentinels(cb):
        assert s in prompt.upper()
    for cls in C.CLASSES:
        assert P._CLASS_TITLE[cls].split(" —")[0] in prompt
    assert "evidence keys BEFORE the value key" in prompt


def _stub_unit_map():
    from engine.elicitation.units import build_unit_map
    return build_unit_map(1, "Alpha beta gamma delta. Epsilon zeta eta theta.")
