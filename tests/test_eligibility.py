"""SCREEN-AUTH-01 Phase 2b — the eligibility model, its renderer, and identity.

The identity test is the point of this file. Phase 2b relocated screening's topic
content out of seven literal sites into `spec.eligibility` and one renderer, and
the claim being tested is that **no rendered byte changed**. The ten hashes below
were measured against the literals at HEAD 8d43b99/fefd112, before the relocation
existed.

They are expected to go RED at the fold step (2c/2f), when the transitional
fields are deleted and the paraphrases collapse to canonical text. That is a
content change and it should be approved deliberately — updating these hashes is
how it gets approved, not a chore on the way past.
"""

import hashlib
import json
from unittest import mock

import pytest
import yaml

from engine.adjudication import ft_screening_adjudicator as fsa
from engine.adjudication import screening_adjudicator as sa
from engine.agents import ft_screener, screener
from engine.core import eligibility_render as render
from engine.core.review_paths import load_spec_for
from engine.core.review_spec import (
    Criterion,
    Eligibility,
    ReviewSpecError,
    SpecialtyScope,
    StagePolicy,
    VerifierTest,
    load_review_spec,
)

SPEC_PATH = "review_specs/surgical_autonomy.yaml"


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


@pytest.fixture(scope="module")
def elig(spec):
    return spec.eligibility


def _scope(**kw):
    base = dict(id="specialty-scope", reason_code="wrong_specialty",
                included=["abdominal surgery"], excluded=["neurosurgery"])
    base.update(kw)
    return SpecialtyScope(**base)


# ── T1 — model validators ────────────────────────────────────────────


def test_reason_code_on_an_inclusion_is_rejected():
    with pytest.raises(ValueError, match="reason_code is for exclusions only"):
        Criterion(id="inc-x", kind="inclusion", text="t",
                  stages=["abstract_primary"], reason_code="wrong_specialty")


def test_a_criterion_must_name_at_least_one_stage():
    with pytest.raises(ValueError):
        Criterion(id="inc-x", kind="inclusion", text="t", stages=[])


def test_transitional_text_for_an_unrendered_stage_is_rejected():
    with pytest.raises(ValueError, match="the criterion is not rendered at"):
        Criterion(id="exc-x", kind="exclusion", text="t",
                  stages=["abstract_primary"],
                  transitional_text={"ft_verifier": "other"})


def test_a_reason_code_description_without_a_reason_code_is_rejected():
    with pytest.raises(ValueError, match="no reason_code"):
        Criterion(id="exc-x", kind="exclusion", text="t",
                  stages=["abstract_primary"], reason_code_prompt_text="d")


def test_a_verifier_test_must_derive_from_something():
    with pytest.raises(ValueError):
        VerifierTest(id="vt-x", text="t", stages=["ft_verifier"], derives_from=[])


def test_a_verifier_test_may_not_cite_an_unknown_id():
    with pytest.raises(ValueError, match="unknown id"):
        Eligibility(
            criteria=[Criterion(id="inc-a", kind="inclusion", text="t",
                                stages=["ft_verifier"])],
            specialty_scope=_scope(),
            verifier_tests=[VerifierTest(id="vt-x", text="t",
                                         stages=["ft_verifier"],
                                         derives_from=["inc-nope"])],
        )


def test_a_verifier_test_may_cite_the_specialty_scope():
    e = Eligibility(
        criteria=[Criterion(id="inc-a", kind="inclusion", text="t",
                            stages=["ft_verifier"])],
        specialty_scope=_scope(),
        verifier_tests=[VerifierTest(id="vt-x", text="t", stages=["ft_verifier"],
                                     derives_from=["specialty-scope"])],
    )
    assert e.verifier_tests[0].derives_from == ["specialty-scope"]


def test_duplicate_criterion_ids_are_rejected():
    with pytest.raises(ValueError, match="duplicate criterion id"):
        Eligibility(
            criteria=[Criterion(id="inc-a", kind="inclusion", text="one",
                                stages=["ft_primary"]),
                      Criterion(id="inc-a", kind="inclusion", text="two",
                                stages=["ft_primary"])],
            specialty_scope=_scope(),
        )


# ── T4 — the retired sections are refused, not ignored ───────────────


def test_a_spec_still_carrying_screening_criteria_fails_loudly(tmp_path):
    raw = yaml.safe_load(open(SPEC_PATH).read())
    raw["screening_criteria"] = {"inclusion": ["x"], "exclusion": ["y"]}
    p = tmp_path / "legacy.yaml"
    p.write_text(yaml.dump(raw))
    with pytest.raises(ReviewSpecError) as exc:
        load_review_spec(p)
    assert "screening_criteria" in str(exc.value)


def test_a_spec_with_a_top_level_specialty_scope_fails_loudly(tmp_path):
    raw = yaml.safe_load(open(SPEC_PATH).read())
    raw["specialty_scope"] = {"included": ["x"], "excluded": ["y"]}
    p = tmp_path / "legacy.yaml"
    p.write_text(yaml.dump(raw))
    with pytest.raises(ReviewSpecError) as exc:
        load_review_spec(p)
    assert "specialty_scope" in str(exc.value)


# ── T2 — the renderer, per stage ─────────────────────────────────────


def test_the_primary_pass_sees_fewer_exclusions_than_the_verifier(elig):
    primary = elig.criteria_for("abstract_primary", "exclusion")
    verifier = elig.criteria_for("abstract_verifier", "exclusion")
    assert len(primary) == 4
    assert len(verifier) == 8
    assert {c.id for c in primary} < {c.id for c in verifier}


def test_a_criterion_absent_from_a_stage_is_not_rendered_there(elig):
    block = render.exclusion_block(elig, "abstract_primary")
    assert "Conference abstracts without full paper" not in block
    assert "Conference abstracts without full paper" in render.exclusion_block(
        elig, "ft_primary")


def test_transitional_text_is_honoured_at_the_stage_that_declares_it(elig):
    primary = render.exclusion_block(elig, "abstract_primary")
    full = render.exclusion_block(elig, "ft_primary")
    assert "Non-surgical robotics (industrial, rehabilitation, exoskeletons, prosthetics)" in primary
    assert "warehouse" not in primary
    assert "warehouse" in full


def test_a_verifier_test_renders_its_stage_wording(elig):
    t = next(t for t in elig.verifier_tests if t.id == "vt-executes-action")
    assert elig.text_at(t, "abstract_verifier").startswith("Does the abstract")
    assert elig.text_at(t, "ft_verifier").startswith("Does the full text")


def test_a_test_without_transitional_text_is_shared_verbatim(elig):
    t = next(t for t in elig.verifier_tests if t.id == "vt-autonomous-component")
    assert t.transitional_text is None
    assert elig.text_at(t, "abstract_verifier") == elig.text_at(t, "ft_verifier")


def test_verifier_tests_are_numbered_per_stage(elig):
    assert render.verifier_tests_block(elig, "abstract_verifier").count("\n1. ") == 0
    assert render.verifier_tests_block(elig, "abstract_verifier").startswith("1. ")
    ft = render.verifier_tests_block(elig, "ft_verifier")
    assert "\n5. " in ft
    assert "\n6. " not in ft


def test_reason_code_descriptions_differ_between_prompt_and_sheet(elig):
    prompt = render.reason_code_descriptions(elig, "prompt")
    sheet = render.reason_code_descriptions(elig, "sheet")
    assert set(prompt) == set(sheet)
    # Every one of the seven disagrees today; that is the finding 2c rules on.
    assert all(prompt[c] != sheet[c] for c in prompt)


def test_topic_reason_codes_are_described_by_the_spec(elig):
    prompt = render.reason_code_descriptions(elig, "prompt")
    assert prompt["wrong_specialty"] == elig.specialty_scope.reason_code_prompt_text
    owner = next(c for c in elig.criteria if c.reason_code == "no_autonomy_content")
    assert prompt["no_autonomy_content"] == owner.reason_code_prompt_text


# ── T3 — the identity gate ───────────────────────────────────────────

#: Measured against the literals before the relocation: P1–P4 and H1/H3 in
#: SCREEN-AUTH-01 Phase 2a, H4–H7 in Phase 2b Part A step A0.
BASELINE = {
    "P1": "f67dbc6ec09716557ebde4d22395bb9111b7e29e7aaad5dbc20627e50db8e7e3",
    "P2": "d917d540898452f8b7f9d77e82ff4af4862bf9f317cffb788f88e2ac03a922d5",
    "P3": "ac09b938f6c693b66ab3498a47c793b938498cd2cac8d2b08bbc550919669be4",
    "P4": "5a096462058b66cec969900d1b6ea576bb73b0b7ef8a6e92b5ab484c52b873b6",
    "H1": "5b6d6432c8f6ae3ae7f96e7d9559d4f785cd9ccae5f19918a1deaeae0f3c1d5c",
    "H3": "ff3845544773ae6debf23f9f06f6f1b752e1e2a62011f94bda87b8282335ac3b",
    "H4": "4312210e8c8f948cc88b2edd078bf36714b2dfe563ca7dc94cc8e3d71078d83f",
    "H5": "0fec97f5d0790ebf50eed76a455b81734b54d0769a0857e251314133195669bb",
    "H6": "5eaf242030b561839adf756f3c2e398cdd2aef89fa3bf70c4d32078d6544e3f3",
    "H7": "404cbc8f6176af6950c435a353f311a3937a5917e5e2724a101a1181a227a413",
}

_PAPER = {"title": "T", "abstract": "A"}
_FT_TEXT = "Title: T\n\nAbstract: A"


def _rendered(spec):
    return {
        "P1": screener._build_prompt(_PAPER, spec, role="primary"),
        "P2": screener._build_prompt(_PAPER, spec, role="verifier"),
        "P3": ft_screener.build_ft_screening_prompt(_FT_TEXT, spec),
        "P4": ft_screener.build_ft_verification_prompt(_FT_TEXT, spec),
        "H1": "\n".join(sa._build_decision_criteria(spec)),
        "H3": "\n".join(fsa._build_ft_decision_criteria(spec)),
        "H4": sa._build_reference_content(spec),
        "H5": fsa._build_ft_reference_content(spec),
        "H6": sa._build_edge_case_guidance(spec),
        "H7": fsa._build_ft_edge_case_guidance(spec),
    }


@pytest.mark.parametrize("key", sorted(BASELINE))
def test_rendering_is_byte_identical_to_the_pre_relocation_literals(spec, key):
    got = _rendered(spec)[key]
    assert hashlib.sha256(got.encode("utf-8")).hexdigest() == BASELINE[key], (
        f"{key} changed. The relocation must not alter a rendered byte; if this "
        f"is the fold step, update the baseline deliberately.\n---\n{got}\n---"
    )


def test_the_export_rubric_has_no_spec_less_fallback(tmp_path):
    """A spec-less export used to emit a fifth, divergent copy of the criteria."""
    from engine.core.database import ReviewDatabase
    from engine.search.models import Citation

    db = ReviewDatabase("t", data_root=tmp_path)
    db.add_papers([Citation(title="T", abstract="A", pmid="1", doi="10.1/x",
                            source="pubmed", authors=["A"], journal="J", year=2024)])
    db.update_status(1, "ABSTRACT_SCREEN_FLAGGED")
    with pytest.raises(ValueError, match="requires a Review Spec"):
        sa.export_adjudication_queue(db, tmp_path / "o.xlsx", review_spec=None)


# ── T1 — the FULL model request, not just the user message ───────────
#
# Phase 2b's ten surface hashes covered the user prompt only. The abstract
# screener's SYSTEM message carried a sentence of review topic content that sat
# outside all of them — it could have been rewritten with every gate green.
# These four hash the whole message list as sent.

REQUEST_BASELINE = {
    "R1": "be5a2c417dc3a6ab829a88db6a8b8e083e419ca96192a9a2313b3683d4335b83",
    "R2": "5b9b7201507a81cba477a109180147f5b880e84619c5d22307b660587085498e",
    "R3": "ac4a0b0a36eef747ab8aec136a1379d8a5179df0f3895f4a2a3ccd7509e6489e",
    "R4": "41161ecb4ee4ac8f742e066c2c0c0bb0dc683c472505112a71e08c5c51fb989b",
}


class _Captured(Exception):
    """Raised to unwind once the request is in hand, so nothing is ever sent."""


def _capture(module, fn, *args, **kwargs):
    """The message list `fn` would send. No model call: the client is replaced."""
    box = {}

    def recorder(*_a, **_kw):
        box["messages"] = _kw.get("messages")
        raise _Captured()

    with mock.patch.object(module, "ollama_chat", recorder):
        with pytest.raises(_Captured):
            fn(*args, **kwargs)
    return box["messages"]


def _requests(spec):
    paper = {"title": "T", "abstract": "A", "id": 1}
    ft = "Title: T\n\nAbstract: A"
    return {
        "R1": _capture(screener, screener.screen_paper, paper, spec, 1, role="primary"),
        "R2": _capture(screener, screener.screen_paper, paper, spec, 2, role="verifier"),
        "R3": _capture(ft_screener, ft_screener.ft_screen_paper, ft, spec),
        "R4": _capture(ft_screener, ft_screener.ft_verify_paper, ft, spec),
    }


@pytest.mark.parametrize("key", sorted(REQUEST_BASELINE))
def test_the_full_request_is_byte_identical_to_the_pre_relocation_literals(spec, key):
    messages = _requests(spec)[key]
    blob = json.dumps(messages, sort_keys=True, ensure_ascii=False).encode("utf-8")
    assert hashlib.sha256(blob).hexdigest() == REQUEST_BASELINE[key], (
        f"{key} changed. Both messages carry review content; if this is the fold "
        f"step, update the baseline deliberately.\n---\n{blob.decode()}\n---"
    )


def test_every_screening_request_sends_exactly_a_system_and_a_user_message(spec):
    for key, messages in _requests(spec).items():
        assert [m["role"] for m in messages] == ["system", "user"], key


# ── T2 — the system-message contract ─────────────────────────────────


def test_the_abstract_stages_carry_their_topic_sentence_in_the_spec(elig):
    for stage in ("abstract_primary", "abstract_verifier"):
        topic = elig.policy_for(stage).system_text
        assert topic and "autonomous" in topic
        assert topic in render.system_message(elig, stage)


def test_the_ft_stages_declare_no_topic_sentence(elig):
    for stage in ("ft_primary", "ft_verifier"):
        assert elig.policy_for(stage).system_text is None
        assert "{topic}" not in render.system_message(elig, stage)


def test_a_topic_slot_with_no_system_text_is_refused(elig):
    stripped = elig.model_copy(deep=True)
    stripped.stage_policies["abstract_primary"].system_text = None
    with pytest.raises(ValueError, match="no system_text to fill it"):
        render.system_message(stripped, "abstract_primary")


def test_system_text_for_a_stage_with_no_topic_slot_is_refused(elig):
    extra = elig.model_copy(deep=True)
    extra.stage_policies["ft_primary"] = StagePolicy(system_text="topic content")
    with pytest.raises(ValueError, match="no topic slot"):
        render.system_message(extra, "ft_primary")


def test_stage_policies_reject_a_stage_name_that_is_not_a_stage():
    with pytest.raises(ValueError):
        Eligibility(
            criteria=[Criterion(id="inc-a", kind="inclusion", text="t",
                                stages=["ft_primary"])],
            specialty_scope=_scope(),
            stage_policies={"not_a_stage": StagePolicy()},
        )
