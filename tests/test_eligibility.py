"""SCREEN-AUTH-01 — the eligibility model, its renderer, and the frozen renderings.

Every screening surface — four model requests and six adjudication-sheet surfaces
— is rendered from `spec.eligibility` by `engine.core.eligibility_render`. Phase
2b moved the content there and proved the move changed no byte; Phase 2c folded
away the per-stage overrides, derived policy prose, gave every exclusion a reason
code, and was approved by the PI from before/after renders
(docs/session-reports/screen-auth-2c-render/).

The fourteen hashes at the bottom of this file are those approved renderings.
They are FROZEN: a change to any of them is a change to what the model or the
human adjudicator is told, and it requires a measured smoke (2f) and an
architect ruling — not a baseline refresh.
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
from engine.core.database import ReviewDatabase
from engine.core.review_paths import load_spec_for
from engine.core.review_spec import (
    STRUCTURAL_REASON_CODES,
    Criterion,
    Eligibility,
    ReviewSpecError,
    SpecialtyScope,
    StagePolicy,
    VerifierTest,
    load_review_spec,
)
from engine.search.models import Citation

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


def _inc(id="inc-a", **kw):
    return Criterion(id=id, kind="inclusion", text="t", stages=["ft_primary"], **kw)


def _exc(id="exc-a", code="code_a", **kw):
    return Criterion(id=id, kind="exclusion", text="t", stages=["ft_primary"],
                     reason_code=code, **kw)


# ── T1 — reason codes ────────────────────────────────────────────────


def test_reason_code_on_an_inclusion_is_rejected():
    with pytest.raises(ValueError, match="reason_code is for exclusions only"):
        _inc(reason_code="wrong_specialty")


def test_an_exclusion_must_declare_a_reason_code():
    with pytest.raises(ValueError, match="must declare a reason_code"):
        Criterion(id="exc-x", kind="exclusion", text="t", stages=["ft_primary"])


def test_eligible_cannot_be_the_reason_a_rule_excludes():
    with pytest.raises(ValueError, match="code a paper that passes carries"):
        _exc(code="eligible")


def test_two_criteria_may_not_share_a_reason_code():
    with pytest.raises(ValueError, match="is declared by both"):
        Eligibility(criteria=[_exc("exc-a", "same"), _exc("exc-b", "same")],
                    specialty_scope=_scope())


def test_a_criterion_may_not_share_the_specialty_scope_code():
    with pytest.raises(ValueError, match="is declared by both"):
        Eligibility(criteria=[_exc("exc-a", "wrong_specialty")], specialty_scope=_scope())


def test_the_effective_vocabulary_is_exactly_twelve_codes_in_prompt_order(elig):
    assert elig.reason_codes() == (
        "eligible", "review_article", "editorial_commentary", "abstract_only",
        "wrong_intervention", "teleoperation_only", "analysis_only",
        "no_autonomy_content", "insufficient_data", "wrong_specialty",
        "protocol_only", "duplicate_cohort",
    )
    assert set(STRUCTURAL_REASON_CODES) <= set(elig.reason_codes())


def test_every_exclusion_code_is_unique_and_every_criterion_has_one(elig):
    codes = [c.reason_code for c in elig.criteria if c.kind == "exclusion"]
    assert None not in codes
    assert len(codes) == len(set(codes)) == 8


def test_a_criterion_may_reference_a_structural_code_whose_description_the_engine_owns(elig):
    owner = elig.evidence_criterion()
    assert owner.reason_code == "insufficient_data"
    block = render.reason_code_prompt_block(elig)
    assert f"insufficient_data: {STRUCTURAL_REASON_CODES['insufficient_data']}" in block
    assert owner.text not in block  # the criterion renders as a criterion, not as the description


def test_the_reason_code_menu_lists_only_the_structural_codes(elig):
    listed = [l.split(":")[0].strip("- ").strip()
              for l in render.reason_code_block_lines(elig)[1:]]
    assert listed == list(STRUCTURAL_REASON_CODES)


# ── Structure: stages, ids, derives_from (unchanged by the fold) ─────


def test_a_criterion_must_name_at_least_one_stage():
    with pytest.raises(ValueError):
        Criterion(id="inc-x", kind="inclusion", text="t", stages=[])


def test_a_verifier_test_must_derive_from_something():
    with pytest.raises(ValueError):
        VerifierTest(id="vt-x", text="t", stages=["ft_verifier"], derives_from=[])


def test_a_verifier_test_may_not_cite_an_unknown_id():
    with pytest.raises(ValueError, match="unknown id"):
        Eligibility(
            criteria=[Criterion(id="inc-a", kind="inclusion", text="t", stages=["ft_verifier"])],
            specialty_scope=_scope(),
            verifier_tests=[VerifierTest(id="vt-x", text="t", stages=["ft_verifier"],
                                         derives_from=["inc-nope"])],
        )


def test_a_verifier_test_may_cite_the_specialty_scope():
    e = Eligibility(
        criteria=[Criterion(id="inc-a", kind="inclusion", text="t", stages=["ft_verifier"])],
        specialty_scope=_scope(),
        verifier_tests=[VerifierTest(id="vt-x", text="t", stages=["ft_verifier"],
                                     derives_from=["specialty-scope"])],
    )
    assert e.verifier_tests[0].derives_from == ["specialty-scope"]


def test_duplicate_criterion_ids_are_rejected():
    with pytest.raises(ValueError, match="duplicate criterion id"):
        Eligibility(criteria=[_inc("inc-a"), _inc("inc-a")], specialty_scope=_scope())


def test_the_primary_pass_sees_fewer_exclusions_than_the_verifier(elig):
    primary = elig.criteria_for("abstract_primary", "exclusion")
    verifier = elig.criteria_for("abstract_verifier", "exclusion")
    assert len(primary) == 4
    assert len(verifier) == 8
    assert {c.id for c in primary} < {c.id for c in verifier}


def test_a_criterion_absent_from_a_stage_is_not_rendered_there(elig):
    block = render.exclusion_block(elig, "abstract_primary")
    assert "Conference abstracts without full paper" not in block
    assert "Conference abstracts without full paper" in render.exclusion_block(elig, "ft_primary")


# ── T2 — examples ────────────────────────────────────────────────────


def test_empty_examples_are_rejected():
    with pytest.raises(ValueError, match="examples, when present"):
        _exc(examples=["  "])
    with pytest.raises(ValueError, match="examples, when present"):
        _exc(examples=[])


def test_examples_render_after_the_text_at_every_stage_the_criterion_renders(elig):
    c = next(c for c in elig.criteria if c.examples)
    for stage in c.stages:
        block = render.exclusion_block(elig, stage)
        assert f"{c.text} (e.g., {'; '.join(c.examples)})" in block, stage


def test_codes_are_shown_at_full_text_stages_and_nowhere_else(elig):
    c = next(c for c in elig.criteria if c.id == "exc-teleoperation-only")
    assert render.criterion_line(c, "ft_primary").startswith("[teleoperation_only] ")
    assert render.criterion_line(c, "ft_adjudication").startswith("[teleoperation_only] ")
    assert not render.criterion_line(c, "abstract_verifier").startswith("[")
    assert render.specialty_block_lines(elig, "ft_verifier")[0] == "SPECIALTY SCOPE [wrong_specialty]:"
    assert render.specialty_block_lines(elig, "abstract_verifier")[0] == "SPECIALTY SCOPE:"


# ── T3 — policy prose, per stage family ──────────────────────────────


def test_the_abstract_primary_instruction_is_derived_from_its_policy(elig):
    text = render.decision_instruction(elig, "abstract_primary")
    assert "partial evidence that the paper might meet the criteria, include it" in text
    assert "do not default to inclusion when evidence is absent" in text
    assert "surgical robotics at all" not in text


def test_the_abstract_primary_instruction_follows_a_changed_policy(elig):
    flipped = elig.model_copy(deep=True)
    flipped.stage_policies["abstract_primary"] = StagePolicy(when_uncertain="exclude")
    text = render.decision_instruction(flipped, "abstract_primary")
    assert "leaves eligibility uncertain, exclude it" in text
    assert "do not default to inclusion" not in text


def test_verifier_instructions_carry_framing_tests_and_their_stage_verdict(elig):
    abstract = render.decision_instruction(elig, "abstract_verifier")
    assert abstract.startswith("You are the VERIFICATION pass.")
    assert "Apply these tests strictly:\n1. " in abstract
    assert abstract.endswith("If ANY test fails, EXCLUDE. Only include papers that clearly pass all tests.")
    ft = render.decision_instruction(elig, "ft_verifier")
    assert ft.startswith("Apply these tests strictly:\n1. ")
    assert ft.endswith("mark as FT_FLAGGED. Only mark FT_ELIGIBLE if the paper clearly passes all tests.")


def test_a_stage_with_no_instruction_refuses_to_render_one(elig):
    with pytest.raises(ValueError, match="renders no decision instruction"):
        render.decision_instruction(elig, "ft_primary")


def test_an_adjudication_stage_policy_cannot_be_declared():
    with pytest.raises(ValueError, match="derived from the model stage"):
        Eligibility(criteria=[_inc()], specialty_scope=_scope(),
                    stage_policies={"abstract_adjudication": StagePolicy(when_uncertain="include")})


def test_a_verifier_that_includes_on_uncertainty_is_refused():
    with pytest.raises(ValueError, match="includes on uncertainty"):
        Eligibility(criteria=[_inc()], specialty_scope=_scope(),
                    stage_policies={"ft_verifier": StagePolicy(when_uncertain="include")})


def test_stage_policies_reject_a_stage_name_that_is_not_a_stage():
    with pytest.raises(ValueError):
        Eligibility(criteria=[_inc()], specialty_scope=_scope(),
                    stage_policies={"not_a_stage": StagePolicy()})


def test_adjudication_guidance_is_derived_from_the_stage_it_adjudicates(spec, elig):
    abstract = sa._build_edge_case_guidance(spec)
    assert "lean toward INCLUDE" in abstract and "choose EXCLUDE" in abstract
    ft = fsa._build_ft_edge_case_guidance(spec)
    assert "lean toward" not in ft and "choose" not in ft  # ft_primary declares no policy
    flipped = elig.model_copy(deep=True)
    flipped.stage_policies["abstract_primary"] = StagePolicy(when_uncertain="exclude")
    assert "lean toward EXCLUDE" in render.edge_case_guidance(flipped, "abstract_adjudication")


def test_scope_notes_appear_once_on_the_abstract_instructions_sheet(spec, elig):
    notes = elig.specialty_scope.notes.strip()
    instructions = "\n".join(sa._build_decision_criteria(spec)) + "\n" + sa._build_edge_case_guidance(spec)
    assert instructions.count(notes) == 1


def test_the_absent_abstract_fallback_states_the_evidence_criterion(spec, elig):
    owner = elig.evidence_criterion()
    expected = f"Abstract: [Not available. Exclusion criterion: {owner.text}]"
    assert render.absent_abstract_fallback(elig, "abstract_primary") == expected
    assert expected in screener._build_prompt({"title": "T", "abstract": ""}, spec, role="primary")


def test_the_fallback_refuses_when_no_evidence_criterion_applies(elig):
    stripped = elig.model_copy(deep=True)
    stripped.evidence_criterion().reason_code = "something_else"
    with pytest.raises(ValueError, match="no rule to state"):
        render.absent_abstract_fallback(stripped, "abstract_primary")


# ── T4 — the evidence object ─────────────────────────────────────────


def test_one_test_wording_serves_both_verifiers(elig):
    t = next(t for t in elig.verifier_tests if t.id == "vt-executes-action")
    assert "{evidence}" in t.text
    assert "1. Does the abstract describe" in render.verifier_tests_block(elig, "abstract_verifier")
    assert "1. Does the full text describe" in render.verifier_tests_block(elig, "ft_verifier")
    for stage in ("abstract_verifier", "ft_verifier"):
        assert "{evidence}" not in render.verifier_tests_block(elig, stage)


def test_an_unknown_placeholder_in_a_verifier_test_is_refused():
    with pytest.raises(ValueError, match="unknown placeholder"):
        VerifierTest(id="vt-x", text="Does the {paper} say so?", stages=["ft_verifier"],
                     derives_from=["inc-a"])


def test_verifier_tests_are_numbered_per_stage(elig):
    assert render.verifier_tests_block(elig, "abstract_verifier").startswith("1. ")
    ft = render.verifier_tests_block(elig, "ft_verifier")
    assert "\n5. " in ft and "\n6. " not in ft


# ── T5 — write-time validation ───────────────────────────────────────


def _db_with_one_paper(tmp_path):
    db = ReviewDatabase("t", data_root=tmp_path)
    db.add_papers([Citation(title="T", abstract="A", pmid="1", doi="10.1/x",
                            source="pubmed", authors=["A"], journal="J", year=2024)])
    return db


@pytest.mark.parametrize("code", ["no_parsed_text", "bogus", "wrong_specialty_typo"])
def test_a_reason_code_outside_the_vocabulary_is_refused_before_writing(tmp_path, elig, code):
    db = _db_with_one_paper(tmp_path)
    with pytest.raises(ValueError, match="not in this review's reason-code vocabulary"):
        db.add_ft_screening_decision(1, "m", "FT_EXCLUDE", code, "r", 0.5,
                                     reason_codes=elig.reason_codes())
    n = db._conn.execute("SELECT COUNT(*) FROM ft_screening_decisions").fetchone()[0]
    assert n == 0


def test_a_reason_code_in_the_vocabulary_is_written(tmp_path, elig):
    db = _db_with_one_paper(tmp_path)
    db.add_ft_screening_decision(1, "m", "FT_EXCLUDE", "teleoperation_only", "r", 0.5,
                                 reason_codes=elig.reason_codes())
    row = db._conn.execute("SELECT reason_code FROM ft_screening_decisions").fetchone()
    assert row[0] == "teleoperation_only"


def test_the_vocabulary_must_be_supplied_to_write(tmp_path):
    db = _db_with_one_paper(tmp_path)
    with pytest.raises(TypeError):
        db.add_ft_screening_decision(1, "m", "FT_ELIGIBLE", "eligible", "r", 0.5)


# ── T6 — the retired sections and the transitional carriers are refused ──


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


_CARRIERS = [
    ("criterion.transitional_text", lambda e: e["criteria"][0].__setitem__("transitional_text", {"abstract_primary": "x"})),
    ("criterion.reason_code_prompt_text", lambda e: e["criteria"][0].__setitem__("reason_code_prompt_text", "x")),
    ("criterion.reason_code_sheet_text", lambda e: e["criteria"][0].__setitem__("reason_code_sheet_text", "x")),
    ("verifier_test.transitional_text", lambda e: e["verifier_tests"][0].__setitem__("transitional_text", {"ft_verifier": "x"})),
    ("specialty_scope.reason_code_prompt_text", lambda e: e["specialty_scope"].__setitem__("reason_code_prompt_text", "x")),
    ("specialty_scope.reason_code_sheet_text", lambda e: e["specialty_scope"].__setitem__("reason_code_sheet_text", "x")),
    ("stage_policy.instruction_text", lambda e: e["stage_policies"]["abstract_primary"].__setitem__("instruction_text", "x")),
    ("stage_policy.absent_abstract_text", lambda e: e["stage_policies"]["abstract_primary"].__setitem__("absent_abstract_text", "x")),
    ("stage_policy.system_text", lambda e: e["stage_policies"]["abstract_primary"].__setitem__("system_text", "x")),
    ("stage_policy.rubric_text", lambda e: e["stage_policies"]["abstract_primary"].__setitem__("rubric_text", ["x"])),
]


@pytest.mark.parametrize("name,inject", _CARRIERS, ids=[c[0] for c in _CARRIERS])
def test_a_spec_carrying_a_retired_transitional_field_fails_and_names_it(tmp_path, name, inject):
    raw = yaml.safe_load(open(SPEC_PATH).read())
    inject(raw["eligibility"])
    p = tmp_path / "legacy.yaml"
    p.write_text(yaml.dump(raw, allow_unicode=True))
    with pytest.raises(ReviewSpecError) as exc:
        load_review_spec(p)
    assert name.split(".")[1] in str(exc.value)


# ── The request as sent ──────────────────────────────────────────────


class _Captured(Exception):
    """Raised to unwind once the request is in hand, so nothing is ever sent."""


def _capture(module, fn, *args, **kwargs):
    """The messages and format `fn` would send. No model call: the client is replaced."""
    box = {}

    def recorder(*_a, **_kw):
        box["messages"] = _kw.get("messages")
        box["format"] = _kw.get("format")
        raise _Captured()

    with mock.patch.object(module, "ollama_chat", recorder):
        with pytest.raises(_Captured):
            fn(*args, **kwargs)
    return box


def _requests(spec):
    paper = {"title": "T", "abstract": "A", "id": 1}
    ft = "Title: T\n\nAbstract: A"
    return {
        "R1": _capture(screener, screener.screen_paper, paper, spec, 1, role="primary"),
        "R2": _capture(screener, screener.screen_paper, paper, spec, 2, role="verifier"),
        "R3": _capture(ft_screener, ft_screener.ft_screen_paper, ft, spec),
        "R4": _capture(ft_screener, ft_screener.ft_verify_paper, ft, spec),
    }


def test_every_screening_request_sends_exactly_a_system_and_a_user_message(spec):
    for key, req in _requests(spec).items():
        assert [m["role"] for m in req["messages"]] == ["system", "user"], key


def test_only_the_abstract_primary_system_message_names_the_review(spec):
    assert f'titled "{spec.title}"' in render.system_message("abstract_primary", spec.title)
    for stage in ("abstract_verifier", "ft_primary", "ft_verifier"):
        assert spec.title not in render.system_message(stage, spec.title)
    assert "verification agent" in render.system_message("abstract_verifier", spec.title)


def test_a_missing_review_title_is_refused_where_the_message_names_the_review():
    with pytest.raises(ValueError, match="no review title was given"):
        render.system_message("abstract_primary", "  ")


def test_the_ft_format_schema_lists_the_whole_vocabulary(spec, elig):
    description = _requests(spec)["R3"]["format"]["properties"]["reason_code"]["description"]
    assert description == "One of: " + ", ".join(elig.reason_codes())


def test_the_export_rubric_has_no_spec_less_fallback(tmp_path):
    """A spec-less export used to emit a fifth, divergent copy of the criteria."""
    db = _db_with_one_paper(tmp_path)
    db.update_status(1, "ABSTRACT_SCREEN_FLAGGED")
    with pytest.raises(ValueError, match="requires a Review Spec"):
        sa.export_adjudication_queue(db, tmp_path / "o.xlsx", review_spec=None)


# ── T7 — the fourteen frozen renderings ──────────────────────────────
#
# FROZEN after SCREEN-AUTH-01 2c. Approved by the PI from the before/after renders
# in docs/session-reports/screen-auth-2c-render/.
#
# Per surface: the 2b pin (the relocation's identity value); for R1–R4 the value
# at 83defc5 under the 2c request definition, which hashes {format, messages}
# rather than messages alone (ruling R31); and the frozen value.
#
#   P1  2b f67dbc6ec09716557ebde4d22395bb9111b7e29e7aaad5dbc20627e50db8e7e3
#   P2  2b d917d540898452f8b7f9d77e82ff4af4862bf9f317cffb788f88e2ac03a922d5
#   P3  2b ac09b938f6c693b66ab3498a47c793b938498cd2cac8d2b08bbc550919669be4
#   P4  2b 5a096462058b66cec969900d1b6ea576bb73b0b7ef8a6e92b5ab484c52b873b6
#   H1  2b 5b6d6432c8f6ae3ae7f96e7d9559d4f785cd9ccae5f19918a1deaeae0f3c1d5c
#   H3  2b ff3845544773ae6debf23f9f06f6f1b752e1e2a62011f94bda87b8282335ac3b
#   H4  2b 4312210e8c8f948cc88b2edd078bf36714b2dfe563ca7dc94cc8e3d71078d83f
#   H5  2b 0fec97f5d0790ebf50eed76a455b81734b54d0769a0857e251314133195669bb
#   H6  2b 5eaf242030b561839adf756f3c2e398cdd2aef89fa3bf70c4d32078d6544e3f3
#   H7  2b 404cbc8f6176af6950c435a353f311a3937a5917e5e2724a101a1181a227a413  (unchanged by 2c)
#   R1  2b be5a2c417dc3a6ab829a88db6a8b8e083e419ca96192a9a2313b3683d4335b83  (messages only)
#       83defc5 under R31  33bdeea91007fb81f2401566f01bc05b6a3c0315db003f41c6dcd4c8808abee3
#   R2  2b 5b9b7201507a81cba477a109180147f5b880e84619c5d22307b660587085498e  (messages only)
#       83defc5 under R31  2c89ef61c690d0c544408f34ee6ea80702520f8cd5a9b1b61da93b6223c4d5f8
#   R3  2b ac4a0b0a36eef747ab8aec136a1379d8a5179df0f3895f4a2a3ccd7509e6489e  (messages only)
#       83defc5 under R31  86f5bf5a24f37f8fe7aba889a26a1344a124447a6ee966d543761ef119b96a77
#   R4  2b 41161ecb4ee4ac8f742e066c2c0c0bb0dc683c472505112a71e08c5c51fb989b  (messages only)
#       83defc5 under R31  e6e7bb27bbff9e4b9d9a4fa22401fbabfe38010d50799079efcfe4a658cc1c1f

FROZEN = {
    "P1": "c2c60b16f7f554d3b25093e4bfed6bd91434950f876a86e5208589b061f342dd",
    "P2": "a5f8b253e7b4a2daa4e4f7ee278bac94de9be88a0b3cf84617b708ce9afac388",
    "P3": "ed7dd6742b01da7b38ec5b78d584c4f7a7880e1b12606172c823dac75a8c4c46",
    "P4": "e05cb95fd481e8bcc0310f8feea4a5e8858a2533d642e9865514e95a79535dc2",
    "H1": "59b7bd5b98360f8fa1d916a0645e6be21be4f4a80f3962c6f5dc995d181a2df0",
    "H3": "fc06eb3acd607bdbfe0669343bfc1fb679b289ff9556c7f209f70a526fd99677",
    "H4": "ec9f0a44ec039857f3eaeb11aee2a15e3263c4ba24ef196352279692729b0e9f",
    "H5": "b45a69ad098f0e062bb58149467d7be23525c9e56ab9bf51d0ada2a4a5d16505",
    "H6": "4e9a62d128c64a8c55ad136e54e1925b1eaf60d2ffe2baf12d9496ded4bba380",
    "H7": "404cbc8f6176af6950c435a353f311a3937a5917e5e2724a101a1181a227a413",
    "R1": "e02ce2c9a77ab578415bb6ca32477a952bd2727de19558d80fd19fd5ddf84649",
    "R2": "bc36e29191d4a61ce1049634d39adabcd58e52fdd177d1665453ac370f071ff3",
    "R3": "c5cfdac02ce72b107d5a6738c199e27c4a25e42a738621d3a8edfc183511cf93",
    "R4": "bd7adb8f11795d0fbc47ff7665975a78aa95a807181541f57b04b122683892b4",
}

_FROZEN_MESSAGE = (
    "frozen after SCREEN-AUTH-01 2c — any change requires a measured smoke (2f) "
    "and an architect ruling."
)


def _surface(spec, key):
    paper, ft = {"title": "T", "abstract": "A"}, "Title: T\n\nAbstract: A"
    if key.startswith("R"):
        req = _requests(spec)[key]
        return json.dumps({"format": req["format"], "messages": req["messages"]},
                          sort_keys=True, ensure_ascii=False)
    return {
        "P1": lambda: screener._build_prompt(paper, spec, role="primary"),
        "P2": lambda: screener._build_prompt(paper, spec, role="verifier"),
        "P3": lambda: ft_screener.build_ft_screening_prompt(ft, spec),
        "P4": lambda: ft_screener.build_ft_verification_prompt(ft, spec),
        "H1": lambda: "\n".join(sa._build_decision_criteria(spec)),
        "H3": lambda: "\n".join(fsa._build_ft_decision_criteria(spec)),
        "H4": lambda: sa._build_reference_content(spec),
        "H5": lambda: fsa._build_ft_reference_content(spec),
        "H6": lambda: sa._build_edge_case_guidance(spec),
        "H7": lambda: fsa._build_ft_edge_case_guidance(spec),
    }[key]()


@pytest.mark.parametrize("key", sorted(FROZEN))
def test_the_screening_surfaces_are_frozen(spec, key):
    got = _surface(spec, key)
    assert hashlib.sha256(got.encode("utf-8")).hexdigest() == FROZEN[key], (
        f"{key} changed: {_FROZEN_MESSAGE}\n---\n{got}\n---"
    )
