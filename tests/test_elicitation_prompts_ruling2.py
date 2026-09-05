"""ELICIT-DESIGN-02 Ruling 2 — the escape token is taught, not merely offered.

ELICIT-DESIGN-01's F1: `NO_EVIDENCE_LOCATABLE` was used **0 times in 180 field
entries**, while `NR` was used 23 times and 19 of the 54 VALUE_WITHOUT_CITATION
firings were uncited `NR`. The escape hatch built for exactly that case was never
taken. The token existed only in a preamble two hundred lines above the field the
model was answering, and the sentinel habit won.

These are regression tests on load-bearing prompt lines. Each one is text a
measurement says must be present; a rewrite that drops one would repeat F1 and,
without these, would do it silently.

The three additions are per Ruling 2 and D4:
  (a) the escape alternative on EVERY per-field response-format line (20, not 3);
  (b) one compact worked example, generic content, no corpus paper;
  (c) the explicit sentinel rule.

**No parse-time aliasing.** Ruling 2 is a prompt change and only a prompt change:
an uncited sentinel remains a coded violation and is NOT quietly rewritten into
the escape token. That is asserted here too, because it is the shortcut this
ruling could most easily be "helped" with later.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from engine.elicitation import classes as C
from engine.elicitation.contracts import (
    DIRECTLY_STATED, VALUE_WITHOUT_CITATION, check_response,
)
from engine.elicitation.prompts import build_pass1_prompt
from engine.elicitation.units import build_unit_map

CODEBOOK_PATH = Path("data/surgical_autonomy/extraction_codebook.yaml")

pytestmark = pytest.mark.skipif(
    not CODEBOOK_PATH.exists(), reason="review codebook not present"
)

PAPER = " ".join(f"Sentence number {i} carries some words for the unit map."
                 for i in range(1, 40))


@pytest.fixture(scope="module")
def cb():
    return yaml.safe_load(CODEBOOK_PATH.read_text())


@pytest.fixture(scope="module")
def prompt(cb):
    names = tuple(f["name"] for f in cb["fields"])
    return build_pass1_prompt(build_unit_map(1, PAPER), cb, names)


# ══ 2(a) — every per-field response-format line ═══════════════════════


def test_the_escape_alternative_appears_once_per_field(prompt, cb):
    """D4: per-field, not per-class. 20 lines, one for each field asked for."""
    assert prompt.count("Nothing citable for this field?") == len(cb["fields"])


def test_each_escape_line_carries_the_token_and_an_empty_citation_list(prompt, cb):
    escape = C.escape_token(cb)
    for line in prompt.splitlines():
        if "Nothing citable for this field?" in line:
            assert escape in line
            assert '"unit_indices": []' in line


def test_the_escape_line_is_read_from_the_codebook(cb):
    """Rename the token in the codebook and the prompt follows."""
    custom = {**cb, "escape_token": "ZZ_NOTHING_FOUND"}
    names = tuple(f["name"] for f in cb["fields"])
    p = build_pass1_prompt(build_unit_map(1, PAPER), custom, names)
    assert p.count("ZZ_NOTHING_FOUND") >= len(cb["fields"])
    assert "NO_EVIDENCE_LOCATABLE" not in p


# ══ 2(b) — the worked example ═════════════════════════════════════════


def test_the_worked_example_is_present(prompt):
    assert "Worked example" in prompt


def test_it_contrasts_all_three_cases(prompt, cb):
    """Escape, ordinary value, and a CITED sentinel — the distinction F1 says
    is not landing is precisely between the first and the third."""
    start = prompt.index("Worked example")
    block = prompt[start:start + 2000]
    assert C.escape_token(cb) in block
    assert '"unit_indices": []' in block          # the escape case
    assert '"unit_indices": [88]' in block        # the cited cases
    assert '"value": "NR"' in block               # the cited-sentinel case


def test_the_example_uses_no_field_from_any_codebook(prompt, cb):
    """A demonstration answer for a field the model is about to be asked would
    be a leading example, not a teaching one."""
    start = prompt.index("Worked example")
    block = prompt[start:start + 2000]
    assert "funding_source" in block
    for f in cb["fields"]:
        assert f["name"] not in block


# ══ 2(c) — the sentinel rule ══════════════════════════════════════════


def test_the_sentinel_rule_is_present_at_its_stated_intent(prompt, cb):
    assert "The sentinel rule." in prompt
    start = prompt.index("The sentinel rule.")
    rule = prompt[start:start + 400]
    assert "NR" in rule
    assert "claim about the" in rule and "paper" in rule
    assert "cite the sentence stating that absence" in rule
    assert C.escape_token(cb) in rule


# ══ No parse-time aliasing ════════════════════════════════════════════


def test_an_uncited_sentinel_is_still_a_coded_violation(cb):
    """Ruling 2 changes the prompt. It does NOT teach the parser to forgive."""
    import json

    um = build_unit_map(1, PAPER)
    raw = json.dumps({"fields": [
        {"field_name": "study_type", "unit_indices": [], "value": "NR"},
    ]})
    res = check_response(raw, um, cb, ("study_type",))
    rec = res.records["study_type"]
    assert not rec.ok
    assert VALUE_WITHOUT_CITATION in rec.violations
    assert rec.value == "NR", "the sentinel is recorded, not rewritten"
    assert not rec.is_escape


def test_a_cited_sentinel_passes(cb):
    import json

    um = build_unit_map(1, PAPER)
    raw = json.dumps({"fields": [
        {"field_name": "study_type", "unit_indices": [3], "value": "NR"},
    ]})
    assert check_response(raw, um, cb, ("study_type",)).records["study_type"].ok


# ══ Lines a prior rewrite already cost a smoke run ════════════════════


def test_the_integer_only_instruction_survives(prompt):
    """ELICIT-DESIGN-01 lost this line in the class rewrite and the whole
    response came back with [S12] markers, unparseable."""
    assert 'Use the\ninteger only, not the "[S12]" marker' in prompt
    assert "never `[S12, S13]`" in prompt


def test_directly_stated_is_taught_in_the_inferable_contract(prompt):
    assert DIRECTLY_STATED in prompt
