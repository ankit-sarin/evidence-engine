"""Rendering of the eligibility authority into prompts and adjudication sheets.

One authority, one renderer. Before this module the same eligibility rules were
written out at seven literal sites — four prompt templates and three rubric
builders — which had drifted apart: paraphrases that narrowed a criterion,
seven reason codes described one way to the model and another way to the human,
and a fifth copy no live path could reach.

Every function here is pure: it takes an `Eligibility` (and a stage) and returns
a string. None of them reads a file, a database, or a clock, so a prompt can be
rendered and hashed in a test without a review on disk.

**The transitional fields are load-bearing until the fold.** A stage renders
`transitional_text[stage]` where it has one and `text` otherwise; the reason-code
block renders the prompt wording and the reference sheet renders the sheet
wording. That is not an endorsement of the divergence — it is what makes the
relocation provable, because every rendered byte can be compared against what the
literals produced. The fold step (2c/2f) deletes those fields, and the identity
tests pinned against today's hashes are expected to go red then, deliberately.
"""

from engine.core.constants import FT_REASON_CODES
from engine.core.review_spec import Criterion, Eligibility, SpecialtyScope

#: Reason codes whose description says nothing about *this* review's topic — they
#: apply to any systematic review, so they belong to the engine rather than to a
#: spec. Each carries both of today's wordings for the same reason the spec's
#: topic codes do; 2c collapses each pair to one.
STRUCTURAL_REASON_CODES: dict[str, dict[str, str]] = {
    "eligible": {
        "prompt": "Paper passes all criteria",
        "sheet": "Paper meets all inclusion criteria based on full text",
    },
    "protocol_only": {
        "prompt": "Study protocol without results",
        "sheet": "Paper describes a study protocol without results",
    },
    "duplicate_cohort": {
        "prompt": "Overlapping dataset with another included paper",
        "sheet": "Same cohort/data as another included paper",
    },
    "insufficient_data": {
        "prompt": "Commentary, letter, or editorial with no extractable data",
        "sheet": "Insufficient methodological detail to assess eligibility",
    },
}

#: Engine text, not review text: it describes why a paper reached the FT
#: adjudication queue, which is a property of the pipeline and not of any review.
FT_FLAGGED_WORKFLOW_NOTE = (
    "These papers were flagged because the primary screener and verifier "
    "disagreed. Review the full-text reason code and both rationales to make "
    "your decision."
)

_TESTS_PLACEHOLDER = "{tests}"


# ── Prompt blocks ────────────────────────────────────────────────────


def inclusion_block(elig: Eligibility, stage: str) -> str:
    """Inclusion criteria as the prompts list them, one `  - ` line each."""
    return "\n".join(
        f"  - {elig.text_at(c, stage)}"
        for c in elig.criteria_for(stage, "inclusion")
    )


def exclusion_block(elig: Eligibility, stage: str) -> str:
    """Exclusion criteria for `stage`, honouring that stage's transitional text.

    The abstract primary pass sees a deliberately shorter list than the verifier:
    high recall first, precision second. That subset is declared per criterion in
    `stages`, not hardcoded here, so which pass sees what is a spec decision.
    """
    return "\n".join(
        f"  - {elig.text_at(c, stage)}"
        for c in elig.criteria_for(stage, "exclusion")
    )


def specialty_prompt_block(elig: Eligibility) -> str:
    """The specialty scope as the prompts embed it, with its surrounding blanks."""
    return "\n" + elig.specialty_scope.format_for_prompt() + "\n"


def verifier_tests_block(elig: Eligibility, stage: str) -> str:
    """The stage's verifier tests, numbered from one in declaration order."""
    return "\n".join(
        f"{n}. {elig.text_at(t, stage)}"
        for n, t in enumerate(elig.tests_for(stage), start=1)
    )


def decision_instruction(elig: Eligibility, stage: str) -> str:
    """The stage's decision instruction, with its test block substituted in.

    A verifier stage stores its instruction as a template carrying `{tests}` so
    the numbered block and the prose around it stay one unit; a stage with no
    tests stores plain prose and the substitution is a no-op.
    """
    text = elig.policy_for(stage).instruction_text or ""
    if _TESTS_PLACEHOLDER in text:
        text = text.replace(_TESTS_PLACEHOLDER, verifier_tests_block(elig, stage))
    return text


def absent_abstract_text(elig: Eligibility, stage: str) -> str:
    """What the prompt says in place of an abstract that does not exist."""
    return elig.policy_for(stage).absent_abstract_text or ""


# ── Reason codes ─────────────────────────────────────────────────────


def _declarers(elig: Eligibility) -> dict[str, Criterion | SpecialtyScope]:
    """Reason code → the criterion or scope that declares it."""
    found: dict[str, Criterion | SpecialtyScope] = {}
    for c in elig.criteria:
        if c.reason_code and (c.reason_code_prompt_text or c.reason_code_sheet_text):
            found[c.reason_code] = c
    ss = elig.specialty_scope
    if ss.reason_code_prompt_text or ss.reason_code_sheet_text:
        found[ss.reason_code] = ss
    return found


def reason_code_descriptions(elig: Eligibility, surface: str) -> dict[str, str]:
    """Every reason code's description for `surface` ("prompt" or "sheet").

    A code described by the spec (its meaning depends on this review's topic)
    wins; otherwise the structural description in this module is used. Order is
    `FT_REASON_CODES`, which is what both surfaces render today and what the
    stored `reason_code` column is checked against.
    """
    declared = _declarers(elig)
    out: dict[str, str] = {}
    for code in FT_REASON_CODES:
        owner = declared.get(code)
        if owner is not None:
            text = (
                owner.reason_code_prompt_text if surface == "prompt"
                else owner.reason_code_sheet_text
            )
            if text:
                out[code] = text
                continue
        out[code] = STRUCTURAL_REASON_CODES[code][surface]
    return out


def reason_code_prompt_block(elig: Eligibility) -> str:
    """The FT prompt's reason-code menu, one `  - code: description` line each."""
    return "\n".join(
        f"  - {code}: {desc}"
        for code, desc in reason_code_descriptions(elig, "prompt").items()
    )


# ── Adjudication rubrics ─────────────────────────────────────────────


def _specialty_summary_lines(elig: Eligibility) -> list[str]:
    ss = elig.specialty_scope
    return [
        f"SPECIALTY SCOPE — Included: {', '.join(ss.included)}",
        f"SPECIALTY SCOPE — Excluded: {', '.join(ss.excluded)}",
    ]


#: TRANSITIONAL. The abstract sheet repeats the scope notes as a trailing
#: "EDGE CASE:" rubric line; the FT sheet does not, though both render the notes
#: again in their edge-case guidance. Which sheet says it twice is an accident of
#: two builders written months apart, recorded here so the relocation is
#: byte-identical and the divergence is visible rather than inherited.
_EDGE_CASE_RUBRIC_STAGES = frozenset({"abstract_adjudication"})


def decision_criteria(elig: Eligibility, stage: str) -> list[str]:
    """The adjudication sheet's decision rubric for `stage`."""
    lines = list(elig.policy_for(stage).rubric_text or []) + _specialty_summary_lines(elig)
    notes = elig.specialty_scope.notes
    if stage in _EDGE_CASE_RUBRIC_STAGES and notes:
        lines.append(f"EDGE CASE: {notes.strip()}")
    return lines


def criteria_reference_block(elig: Eligibility, stage: str) -> list[str]:
    """Inclusion and exclusion criteria as the reference sheet lists them.

    The sheet marks inclusions `+` and exclusions `-`; the prompts mark both
    `-`. Two markers for one set of rules is a formatting divergence, carried
    here so the relocation changes no byte, and a candidate for the fold.
    """
    lines = ["INCLUSION CRITERIA:"]
    lines += [f"  + {elig.text_at(c, stage)}" for c in elig.criteria_for(stage, "inclusion")]
    lines += ["", "EXCLUSION CRITERIA:"]
    lines += [f"  - {elig.text_at(c, stage)}" for c in elig.criteria_for(stage, "exclusion")]
    lines.append("")
    return lines


def specialty_reference_block(elig: Eligibility) -> list[str]:
    """The reference sheet's specialty block — `+`/`-` per specialty.

    A third rendering of the same scope, distinct from `format_for_prompt`. It
    exists as its own function so the identity gate can pin it; it folds into
    `format_for_prompt` when the markers are unified.
    """
    ss = elig.specialty_scope
    lines = ["SPECIALTY SCOPE:", "  Included specialties:"]
    lines += [f"    + {s}" for s in ss.included]
    lines.append("  Excluded specialties:")
    lines += [f"    - {s}" for s in ss.excluded]
    if ss.notes:
        lines.append(f"  Notes: {ss.notes}")
    return lines


def edge_case_guidance(elig: Eligibility, stage: str, trailing: str | None = None) -> str:
    """Scope notes, the stage's guidance, and any engine-owned trailing note."""
    parts: list[str] = []
    ss = elig.specialty_scope
    if ss.notes:
        parts.append(ss.notes.strip())
    instruction = elig.policy_for(stage).instruction_text
    if instruction:
        parts.append(instruction)
    if trailing:
        parts.append(trailing)
    return " ".join(parts)
