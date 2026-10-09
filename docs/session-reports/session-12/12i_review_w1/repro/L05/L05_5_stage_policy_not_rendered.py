"""A required stage policy that no model request renders; the human sheet renders it."""
from _common import LIVE_SPEC
from engine.core.review_spec import load_review_spec, StagePolicy
from engine.core import eligibility_render as render
from engine.agents.ft_screener import build_ft_screening_prompt, build_ft_verification_prompt
from engine.agents.screener import _build_prompt as build_screening_prompt
spec = load_review_spec(LIVE_SPEC)
alt = spec.model_copy(deep=True)
alt.eligibility.stage_policies["ft_primary"] = StagePolicy(when_uncertain="include", exclusion_basis="evidenced_exclusion_only")
alt.eligibility.stage_policies["ft_verifier"] = StagePolicy(when_uncertain="exclude", when_evidence_absent="include", exclusion_basis="absence_is_evidence")
alt.eligibility.stage_policies["abstract_verifier"] = StagePolicy(when_evidence_absent="include", exclusion_basis="absence_is_evidence")
# re-validate: the changed policies are accepted by the Eligibility validator
type(alt.eligibility).model_validate(alt.eligibility.model_dump())
print("changed policies validate: True")
print("screening_hash moved:", spec.screening_hash() != alt.screening_hash())
print("ft_primary prompt identical:        ", build_ft_screening_prompt("T", spec) == build_ft_screening_prompt("T", alt))
print("ft_verifier prompt identical:       ", build_ft_verification_prompt("T", spec) == build_ft_verification_prompt("T", alt))
paper = {"title": "t", "abstract": "a"}
print("abstract_verifier prompt identical: ", build_screening_prompt(paper, spec, role="verifier") == build_screening_prompt(paper, alt, role="verifier"))
print("ft_adjudication guidance (live):", repr(render.edge_case_guidance(spec.eligibility, "ft_adjudication")[-80:]))
g = render.edge_case_guidance(alt.eligibility, "ft_adjudication")
print("ft_adjudication guidance (alt) :", g[g.index("When in doubt"):])
# a verifier stage with no tests loads and renders an empty test list
nt = spec.model_copy(deep=True)
nt.eligibility.verifier_tests = [t for t in nt.eligibility.verifier_tests if "ft_verifier" not in t.stages]
type(nt.eligibility).model_validate(nt.eligibility.model_dump())
print("no-ft-verifier-tests spec validates; instruction =", repr(render.decision_instruction(nt.eligibility, "ft_verifier")))
# a spec with no evidence-sufficiency criterion at abstract_primary loads; the refusal arrives per paper
ne = spec.model_copy(deep=True)
ne.eligibility.criteria = [c for c in ne.eligibility.criteria if c.reason_code != "insufficient_data"]
type(ne.eligibility).model_validate(ne.eligibility.model_dump())
try:
    build_screening_prompt({"title": "t", "abstract": None}, ne, role="primary")
except ValueError as e:
    print("no-evidence-criterion spec validates; first abstract-less paper raises ValueError:", str(e)[:70])
