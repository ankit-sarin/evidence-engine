"""Cross-model audit agent to verify extractions.

Model and options come from the one resolver (`engine/core/effective_config.py`,
stage `audit`): `auditor_model` in the Review Spec if set, else the spec model's
declared `audit.model`, or the `model` parameter on individual functions.
"""

import logging
from typing import Literal

from pydantic import BaseModel

from engine.agents.models import EvidenceSpan
from engine.core.constants import INVALID_SNIPPET_RE
from engine.core.effective_config import EffectiveConfig, stage_config
from engine.utils.ollama_client import ollama_chat
from engine.core.locator import locate
# Re-exported under its old name: human_review.py and the frozen provenance
# ladder (analysis/provenance/legacy.py) import it from here (9b-2d R6).
from engine.core.locator import normalize as _normalize  # noqa: F401

logger = logging.getLogger(__name__)


# ── Audit Output Model ──────────────────────────────────────────────


class AuditVerdict(BaseModel):
    """Structured output from the audit agent."""

    status: Literal["verified", "flagged"]
    grep_found: bool
    reasoning: str


# ── Grep Verification ────────────────────────────────────────────────


def grep_verify(source_snippet: str, paper_text: str) -> bool:
    """Check if source_snippet exists in paper_text (exact or fuzzy).

    The shared locator's verdict (R17, 9b-2d): normalized exact substring, else
    the best word window's SequenceMatcher ratio strictly above 0.85.
    """
    return locate(paper_text, source_snippet).located


# ── Semantic Verification ────────────────────────────────────────────


def semantic_verify(
    span: EvidenceSpan, paper_text: str, field_type: str = "text",
    model: str | None = None,
    *, cfg: EffectiveConfig | None = None,
) -> AuditVerdict:
    """Use an LLM to verify if extracted value matches the source snippet.

    For categorical fields, the prompt asks whether the source text supports
    the classification rather than whether it contains the exact phrase.

    `cfg` is the resolved `audit` stage; without one the spec model's declared
    defaults apply. The `ollama_options` override retired with its only caller,
    `scripts/eval_auditor_models.py` (R125).
    """
    cfg = (cfg or stage_config("audit", None, model=model))
    if model is not None and model != cfg.model:
        cfg = cfg.with_model(model)
    response = ollama_chat(
        messages=build_audit_messages(span, field_type=field_type), **cfg.kwargs())

    raw = response.message.content or ""
    return AuditVerdict.model_validate_json(raw)


def build_audit_messages(span: EvidenceSpan, field_type: str = "text") -> list[dict]:
    """The message list `semantic_verify` sends. One builder for the call and
    for the resolver's prompt hash (R60)."""
    if field_type == "categorical":
        verification_question = (
            f"Does the source snippet provide sufficient evidence to classify "
            f"this paper as '{span.value}' for the field '{span.field_name}'?\n"
            f"Note: This is a categorical/classification field. The exact phrase "
            f"'{span.value}' does NOT need to appear verbatim in the text. "
            f"The question is whether the described content reasonably supports "
            f"this classification."
        )
    else:
        verification_question = (
            f"Does the extracted value accurately represent what the source snippet states?\n"
            f"Consider:\n"
            f"- Is the value factually supported by the snippet?\n"
            f"- Is there any misinterpretation or hallucination?\n"
            f"- Is the value a reasonable extraction for this field?"
        )

    prompt = f"""/no_think
You are an audit agent verifying data extraction from a scientific paper.

Field: {span.field_name}
Field type: {field_type}
Extracted value: {span.value}
Source snippet from paper: {span.source_snippet}

{verification_question}

Respond with JSON: {{"status": "verified" or "flagged", "grep_found": true, "reasoning": "..."}}"""

    return [
        {
            "role": "system",
            "content": (
                "You are an audit agent verifying data extractions from "
                "scientific papers. For free-text fields, be strict: flag anything "
                "not clearly supported by the source snippet. For categorical fields, "
                "verify that the source text reasonably supports the chosen category — "
                "the category label does not need to appear verbatim. "
                "Respond ONLY with JSON."
            ),
        },
        {"role": "user", "content": prompt},
    ]


# ── Single Span Audit ────────────────────────────────────────────────


def audit_span(
    span_data: dict, paper_text: str, field_type: str = "text",
    field_tier: int = 1, model: str | None = None,
    non_value_tokens: frozenset[str] = frozenset(),
    *, cfg: EffectiveConfig | None = None,
) -> tuple[str, str]:
    """Audit a single evidence span. Returns (audit_status, reasoning).

    4-state outcome:
    - 'invalid_snippet' — snippet contains ellipsis bridging
    - 'verified' — grep pass AND semantic pass
    - 'contested' — grep fail AND semantic pass
    - 'flagged' — semantic fail

    `non_value_tokens` (ELICIT-DESIGN-02 D1/D2) comes from the codebook via
    `classes.non_value_tokens_for`, never from a list here.
    """
    source_snippet = span_data.get("source_snippet", "")
    value = span_data.get("value", "")

    # A terminal state is not an auditable claim (ELICIT-DESIGN-02 D1, site 1).
    # NO_EVIDENCE_LOCATABLE says the extractor found nothing to cite;
    # CONTRACT_UNMET says the engine refused to store a value. Neither asserts
    # anything about the paper, so there is nothing to grep for and nothing for a
    # semantic check to agree or disagree with. Before this branch both fell
    # through to the empty-snippet rule below and came back 'flagged' — a 27B
    # call per field, spent to mislabel a correct refusal as an audit failure.
    if str(value).strip().upper() in non_value_tokens:
        return "verified", (
            f"'{value}' is a terminal state, not a value claim — no evidence to "
            f"audit (ELICIT-DESIGN-02 Ruling 1)."
        )

    # R124 (9b-2d): there is no auto-verified absence list. A sentinel is a
    # value like any other to the locator (R17); with no snippet it is not
    # located, exactly as a value with no snippet is not.

    # Fix A: Invalid snippet detection (before any other logic)
    if source_snippet and source_snippet.strip():
        if INVALID_SNIPPET_RE.search(source_snippet):
            return "invalid_snippet", "Source snippet contains ellipsis bridging — marked invalid."

    # Empty snippet on a non-absence value
    if not source_snippet or not source_snippet.strip():
        return "flagged", "Extracted value present but no source snippet provided."

    # R124 (9b-2d): every tier is located — the tier-4 pass that set
    # grep_pass = True unchecked is retired.
    grep_pass = locate(paper_text, source_snippet).located

    # Compute semantic result
    span = EvidenceSpan(
        field_name=span_data["field_name"],
        value=value,
        source_snippet=source_snippet,
        confidence=span_data.get("confidence", 0.5),
        tier=field_tier,
    )
    verdict = semantic_verify(span, paper_text, field_type=field_type, model=model,
                              cfg=cfg)
    semantic_pass = verdict.status == "verified"

    # Fix D: 4-state outcome
    if grep_pass and semantic_pass:
        status = "verified"
        reasoning = verdict.reasoning
    elif not grep_pass and semantic_pass:
        status = "contested"
        reasoning = f"Grep failed but semantic verified. {verdict.reasoning}"
    else:
        # grep_pass or not, semantic failed → flagged
        status = "flagged"
        reasoning = verdict.reasoning

    return status, reasoning


# ── Low-Yield Detection ──────────────────────────────────────────────
#
# `check_low_yield` and `run_audit` retired at the cut-over (9b-FLIP, R111):
# recoverable at 49e4cd6:engine/agents/auditor.py. The event-side auditor is
# `engine.agents.audit_events`; LOW_YIELD is computed on read there.

def is_populated(value, non_value_tokens: frozenset[str] = frozenset(), *,
                 absence_sentinels: frozenset[str]) -> bool:
    """R136, the one predicate: a value that is not None, not blank, not one of
    the codebook's absence sentinels (upper-cased) and not a non-value token.
    Every other value counts, "Not assessable" included. `count_populated_fields`
    and the event-side `audit_events.low_yield` both decide through it."""
    if value is None:
        return False
    if not isinstance(value, str):
        return True
    v = value.strip()
    return bool(v) and v.upper() not in absence_sentinels and v.upper() not in non_value_tokens


def count_populated_fields(extraction_data: dict | list,
                           non_value_tokens: frozenset[str] = frozenset(), *,
                           absence_sentinels: frozenset[str]) -> int:
    """Count non-null, non-absence extracted fields in an extraction.

    Handles both v1 format (dict of field_name→value) and v2 format
    (list of span dicts with 'field_name' and 'value' keys).

    `non_value_tokens` (ELICIT-DESIGN-02 D1, site 2) excludes terminal states
    from the LOW_YIELD numerator. A CONTRACT_UNMET field is precisely a field
    that yielded nothing, so counting it as populated would make the guard read
    a refusal as a result — and the more fields an extraction refused, the
    healthier it would look.

    `absence_sentinels` (R124/R136) is the codebook's set, upper-cased
    (`Codebook.absence_sentinel_set`) and required: an empty set is not inert —
    it would count every absence as populated. Every other value counts, a
    declared categorical such as "Not assessable" included, and so does an
    undeclared legacy form such as "Not discussed".
    """
    count = 0
    _populated = lambda value: is_populated(  # noqa: E731
        value, non_value_tokens, absence_sentinels=absence_sentinels)

    if isinstance(extraction_data, list):
        # v2 format: list of span objects [{field_name, value, ...}, ...]
        for span in extraction_data:
            if _populated(span.get("value") if isinstance(span, dict) else None):
                count += 1
    elif isinstance(extraction_data, dict):
        # v1 format: {field_name: value, ...}
        for _key, value in extraction_data.items():
            if _populated(value):
                count += 1

    return count
