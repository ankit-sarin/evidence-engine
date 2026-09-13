"""Rendering of the eligibility authority into prompts and adjudication sheets.

One authority, one renderer. Every screening surface — four model requests and
two adjudication sheets — is rendered here from `spec.eligibility`, and nothing
in this module holds review content. What it holds is engine text: the role an
agent plays, the verdict vocabulary a stage speaks, and the sentence shapes that
turn a stage policy into an instruction.

Every function is pure: it takes an `Eligibility` (and a stage) and returns a
string or a list of lines. None reads a file, a database, or a clock, so any
surface can be rendered and hashed in a test without a review on disk.

The fold (SCREEN-AUTH-01 Phase 2c) removed every per-stage wording override. A
criterion renders its canonical text wherever it renders. The differences that
remain between stages are stated here, each with its reason: what the stage is
reading (abstract or full text), which verdict names it speaks, and whether it
records a reason code.
"""

import copy

from engine.core.review_spec import (
    STRUCTURAL_REASON_CODES,
    VERIFIER_STAGES,
    EVIDENCE_PLACEHOLDER,
    Criterion,
    Eligibility,
)

#: What each stage is reading. Fills `{evidence}` in verifier tests and names the
#: object in policy prose.
EVIDENCE_OBJECT: dict[str, str] = {
    "abstract_primary": "abstract",
    "abstract_verifier": "abstract",
    "abstract_adjudication": "abstract",
    "ft_primary": "full text",
    "ft_verifier": "full text",
    "ft_adjudication": "full text",
}

#: Full-text decisions record a reason code, so full-text surfaces show each
#: exclusion's code beside it. Abstract decisions record none, so abstract
#: surfaces show none — a code a stage cannot record is noise to it.
CODED_STAGES = frozenset({"ft_primary", "ft_verifier", "ft_adjudication"})

#: The model stage each human adjudication stage resolves. The adjudication
#: stage's policy IS that stage's policy; a spec cannot declare a separate one.
ADJUDICATES: dict[str, str] = {
    "abstract_adjudication": "abstract_primary",
    "ft_adjudication": "ft_primary",
}

#: The verdict names each adjudication sheet accepts (positive, negative). These
#: are the values the importers and the database already validate against.
_ADJUDICATION_LABELS: dict[str, tuple[str, str]] = {
    "abstract_adjudication": ("INCLUDE", "EXCLUDE"),
    "ft_adjudication": ("FT_ELIGIBLE", "FT_SCREENED_OUT"),
}

#: What catches a false positive after an adjudicator leans toward inclusion.
_DOWNSTREAM_CHECK: dict[str, str] = {
    "abstract_adjudication": "downstream full-text screening will catch false positives",
}

#: Verification framing that sits inside a stage's decision instruction. The FT
#: verifier's equivalent opens its prompt above the PICO block and stays there.
_VERIFIER_FRAMING: dict[str, str] = {
    "abstract_verifier": (
        "You are the VERIFICATION pass. This paper was already included by a "
        "primary screener. Your job is to catch false positives."
    ),
}

#: The closing rule of each verification pass, in that stage's verdict names.
_VERIFIER_CLOSER: dict[str, str] = {
    "abstract_verifier": (
        "If ANY test fails, EXCLUDE. Only include papers that clearly pass all tests."
    ),
    "ft_verifier": (
        "If ANY test fails, mark as FT_FLAGGED. Only mark FT_ELIGIBLE if the paper "
        "clearly passes all tests."
    ),
}

#: Engine text, not review text: why a paper reached the FT adjudication queue.
FT_FLAGGED_WORKFLOW_NOTE = (
    "These papers were flagged because the primary screener and verifier "
    "disagreed. Review the full-text reason code and both rationales to make "
    "your decision."
)

_TOPIC_PLACEHOLDER = "{topic}"

#: The system message each screening stage sends. Engine text: the agent's role
#: and its output contract. Only the abstract primary pass names the review, via
#: `{topic}`; the verifiers are told what they are for, not what the review is.
SYSTEM_TEMPLATES: dict[str, str] = {
    "abstract_primary": (
        "You are a systematic review screening agent. Screen the paper for the "
        'review titled "{topic}". Follow the criteria and instructions in the '
        "user message. Respond ONLY with the requested JSON."
    ),
    "abstract_verifier": (
        "You are a systematic review abstract verification agent. Your job is "
        "to catch false positives. Be strict. Respond ONLY with the requested "
        "JSON."
    ),
    "ft_primary": (
        "You are a systematic review full-text screening agent. Evaluate "
        "eligibility based on the full paper text. Respond ONLY with the "
        "requested JSON."
    ),
    "ft_verifier": (
        "You are a systematic review full-text verification agent. Your job is "
        "to catch false positives. Be strict. Respond ONLY with the requested "
        "JSON."
    ),
}


# ── Criteria ─────────────────────────────────────────────────────────


def criterion_line(c: Criterion, stage: str) -> str:
    """One criterion exactly as every surface at `stage` renders it."""
    line = c.text
    if c.examples:
        line += f" (e.g., {'; '.join(c.examples)})"
    if stage in CODED_STAGES and c.reason_code:
        line = f"[{c.reason_code}] {line}"
    return line


def _criteria_lines(elig: Eligibility, stage: str, kind: str) -> list[str]:
    return [f"  - {criterion_line(c, stage)}" for c in elig.criteria_for(stage, kind)]


def inclusion_block(elig: Eligibility, stage: str) -> str:
    """Inclusion criteria for `stage`, one `  - ` line each."""
    return "\n".join(_criteria_lines(elig, stage, "inclusion"))


def exclusion_block(elig: Eligibility, stage: str) -> str:
    """Exclusion criteria for `stage`, one `  - ` line each.

    Which exclusions a stage sees is declared per criterion in `stages`: the
    abstract primary pass is deliberately shown fewer, for recall.
    """
    return "\n".join(_criteria_lines(elig, stage, "exclusion"))


def specialty_block_lines(elig: Eligibility, stage: str) -> list[str]:
    """The specialty scope as every surface renders it; coded at FT stages."""
    lines = elig.specialty_scope.format_for_prompt().split("\n")
    if lines[0] != "SPECIALTY SCOPE:":
        raise ValueError("format_for_prompt changed its heading; the code tag has nowhere to go")
    if stage in CODED_STAGES:
        lines[0] = f"SPECIALTY SCOPE [{elig.specialty_scope.reason_code}]:"
    return lines


def specialty_prompt_block(elig: Eligibility, stage: str) -> str:
    """The specialty scope as a prompt embeds it, with its surrounding blanks."""
    return "\n" + "\n".join(specialty_block_lines(elig, stage)) + "\n"


# ── Verifier tests and policy prose ──────────────────────────────────


def verifier_tests_block(elig: Eligibility, stage: str) -> str:
    """The stage's verifier tests, numbered from one, evidence object filled in."""
    token = "{" + EVIDENCE_PLACEHOLDER + "}"
    return "\n".join(
        f"{n}. {t.text.replace(token, EVIDENCE_OBJECT[stage])}"
        for n, t in enumerate(elig.tests_for(stage), start=1)
    )


def _uncertain_sentence(value: str | None, stage: str) -> str | None:
    evidence = EVIDENCE_OBJECT[stage]
    if value == "include":
        return (
            f"If the {evidence} gives partial evidence that the paper might meet "
            "the criteria, include it — a later verification pass will catch "
            "false positives."
        )
    if value == "exclude":
        return f"If the {evidence} leaves eligibility uncertain, exclude it."
    return None


def _absent_sentence(value: str | None, stage: str) -> str | None:
    evidence = EVIDENCE_OBJECT[stage]
    if value == "exclude":
        return (
            f"If there is no {evidence}, or it gives too little information to "
            "determine eligibility, exclude it — do not default to inclusion when "
            "evidence is absent."
        )
    if value == "include":
        return (
            f"If there is no {evidence}, or it gives too little information to "
            "determine eligibility, include it for a later pass to resolve."
        )
    return None


def decision_instruction(elig: Eligibility, stage: str) -> str:
    """The instruction that closes a stage's prompt, derived from its policy.

    A verification pass gets its framing, its numbered tests and its closing
    rule; its policy is fixed by being a verifier. The abstract primary pass
    gets one sentence per declared policy value, and nothing else — in
    particular no free-standing bar of its own on top of the criteria.
    """
    if stage in VERIFIER_STAGES:
        parts = []
        if stage in _VERIFIER_FRAMING:
            parts.append(_VERIFIER_FRAMING[stage])
        parts.append("Apply these tests strictly:\n" + verifier_tests_block(elig, stage))
        parts.append(_VERIFIER_CLOSER[stage])
        return "\n\n".join(parts)
    if stage != "abstract_primary":
        raise ValueError(f"stage {stage!r} renders no decision instruction")
    policy = elig.policy_for(stage)
    lines = ["Decide 'include' or 'exclude'."]
    for sentence in (
        _uncertain_sentence(policy.when_uncertain, stage),
        _absent_sentence(policy.when_evidence_absent, stage),
    ):
        if sentence:
            lines.append(sentence)
    lines.append("Exclude when the paper clearly meets an exclusion criterion listed above.")
    return "\n".join(lines)


def absent_abstract_fallback(elig: Eligibility, stage: str) -> str:
    """What a prompt says in place of an abstract that does not exist.

    It states the evidence-sufficiency criterion itself, so the fallback can
    never say something the criterion does not.
    """
    c = elig.evidence_criterion()
    if c is None or stage not in c.stages:
        raise ValueError(
            f"stage {stage!r} can render a paper with no abstract, but no "
            "evidence-sufficiency criterion (reason_code 'insufficient_data') "
            "applies there, so there is no rule to state in its place."
        )
    return f"Abstract: [Not available. Exclusion criterion: {criterion_line(c, stage)}]"


# ── Reason codes ─────────────────────────────────────────────────────


def reason_code_block_lines(elig: Eligibility) -> list[str]:
    """The reason-code menu: how to pick a code, then the structural codes.

    Topic codes are not repeated here — each is shown in brackets beside the
    criterion or scope that declares it, which is the one place its meaning
    lives. The structural codes name no criterion, so they are listed with the
    engine's description.
    """
    lines = [
        "REASON CODES (use exactly one): the bracketed code of the exclusion "
        "criterion or specialty scope that applies, or one of:"
    ]
    lines += [f"  - {code}: {desc}" for code, desc in STRUCTURAL_REASON_CODES.items()]
    return lines


def reason_code_prompt_block(elig: Eligibility) -> str:
    """The reason-code menu as the FT prompt embeds it."""
    return "\n".join(reason_code_block_lines(elig))


def with_reason_code_vocabulary(schema: dict, elig: Eligibility) -> dict:
    """A copy of a decision schema whose reason_code field names the vocabulary.

    The structured-output schema is part of the request too. A hardcoded list in
    the model class would be a fourth vocabulary copy the prompt could disagree
    with.
    """
    out = copy.deepcopy(schema)
    out["properties"]["reason_code"]["description"] = (
        "One of: " + ", ".join(elig.reason_codes())
    )
    return out


# ── Adjudication sheets ──────────────────────────────────────────────


def decision_criteria(elig: Eligibility, stage: str) -> list[str]:
    """The adjudication rubric for `stage`, derived from the criteria themselves."""
    positive, negative = _ADJUDICATION_LABELS[stage]
    lines = [f"{positive} only if every inclusion criterion holds:"]
    lines += _criteria_lines(elig, stage, "inclusion")
    reason = " (the bracketed code names the reason)" if stage in CODED_STAGES else ""
    lines.append(f"{negative} if any exclusion criterion applies{reason}:")
    lines += _criteria_lines(elig, stage, "exclusion")
    ss = elig.specialty_scope
    tag = f" [{ss.reason_code}]" if stage in CODED_STAGES else ""
    lines.append(f"SPECIALTY SCOPE{tag} — Included: {', '.join(ss.included)}")
    lines.append(f"SPECIALTY SCOPE{tag} — Excluded: {', '.join(ss.excluded)}")
    return lines


def criteria_reference_block(elig: Eligibility, stage: str) -> list[str]:
    """Inclusion and exclusion criteria as the reference sheet lists them.

    Identical to the prompt at the stage adjudicated: same markers, same
    examples, same codes. The sheet a human decides from shows the rules the
    model decided from.
    """
    lines = ["INCLUSION CRITERIA:"] + _criteria_lines(elig, stage, "inclusion")
    lines += ["", "EXCLUSION CRITERIA:"] + _criteria_lines(elig, stage, "exclusion")
    lines.append("")
    return lines


def _adjudication_uncertain(value: str | None, stage: str) -> str | None:
    if value is None:
        return None
    positive, negative = _ADJUDICATION_LABELS[stage]
    if value == "include":
        tail = f" — {_DOWNSTREAM_CHECK[stage]}" if stage in _DOWNSTREAM_CHECK else ""
        return f"When in doubt between {positive} and {negative}, lean toward {positive}{tail}."
    return f"When in doubt between {positive} and {negative}, lean toward {negative}."


def _adjudication_absent(value: str | None, stage: str) -> str | None:
    if value is None:
        return None
    positive, negative = _ADJUDICATION_LABELS[stage]
    evidence = EVIDENCE_OBJECT[stage]
    if value == "exclude":
        return (
            f"If the {evidence} gives too little information to decide, choose "
            f"{negative} — do not default to inclusion when evidence is absent."
        )
    return f"If the {evidence} gives too little information to decide, choose {positive}."


def edge_case_guidance(elig: Eligibility, stage: str, trailing: str | None = None) -> str:
    """Scope notes, then the adjudicated stage's policy, then any engine note.

    The notes appear here and not in the rubric: one place per sheet.
    """
    parts: list[str] = []
    notes = elig.specialty_scope.notes
    if notes:
        parts.append(notes.strip())
    policy = elig.policy_for(ADJUDICATES[stage])
    for sentence in (
        _adjudication_uncertain(policy.when_uncertain, stage),
        _adjudication_absent(policy.when_evidence_absent, stage),
    ):
        if sentence:
            parts.append(sentence)
    if trailing:
        parts.append(trailing)
    return " ".join(parts)


# ── The model request ────────────────────────────────────────────────


def system_message(stage: str, review_title: str) -> str:
    """The stage's system message, the review title substituted where it has a slot."""
    template = SYSTEM_TEMPLATES[stage]
    if _TOPIC_PLACEHOLDER not in template:
        return template
    if not review_title or not review_title.strip():
        raise ValueError(
            f"stage {stage!r} names the review in its system message and no "
            "review title was given. Rendering an empty slot would silently drop "
            "the review's subject from the request."
        )
    return template.replace(_TOPIC_PLACEHOLDER, review_title.strip())


def messages(stage: str, user_prompt: str, *, review_title: str) -> list[dict[str, str]]:
    """The full message list as sent for `stage`.

    Both messages in one place, because review content reaches the model through
    both, and a gate that hashes only the user prompt cannot see half of it.
    """
    return [
        {"role": "system", "content": system_message(stage, review_title)},
        {"role": "user", "content": user_prompt},
    ]
