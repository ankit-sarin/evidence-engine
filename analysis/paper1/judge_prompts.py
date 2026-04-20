"""Prompt builders for the Paper 1 LLM-as-judge pipeline (Pass 1)."""

from __future__ import annotations

import hashlib
import random
from typing import List, Tuple

from analysis.paper1.judge_schema import ArmOutput, JudgeInput

SPAN_TRUNCATE_CHARS = 400
SPAN_TRUNCATE_NOTE = " [span truncated for prompt length]"
SEED_MOD = 2**32


def compute_seed(paper_id: str, field_name: str, run_id: str) -> int:
    """Derive a reproducible 32-bit seed from (paper_id, field_name, run_id).

    Python's built-in hash() is not stable across processes, so we use
    SHA-256 over a canonical string form and fold to 32 bits.
    """
    key = f"{paper_id}\x1f{field_name}\x1f{run_id}".encode("utf-8")
    digest = hashlib.sha256(key).digest()
    return int.from_bytes(digest[:4], "big") % SEED_MOD


def randomize_arm_assignment(
    arms: List[ArmOutput],
    seed: int,
) -> Tuple[List[ArmOutput], List[str]]:
    """Shuffle arms deterministically.

    Returns (shuffled_arms, permutation_arm_names) where
    permutation_arm_names[i] is the arm_name now in slot i+1 of the prompt.
    """
    indices = list(range(len(arms)))
    rng = random.Random(seed)
    rng.shuffle(indices)
    shuffled = [arms[i] for i in indices]
    return shuffled, [a.arm_name for a in shuffled]


def _truncate_span(span: str) -> str:
    if len(span) <= SPAN_TRUNCATE_CHARS:
        return span
    return span[:SPAN_TRUNCATE_CHARS] + "..." + SPAN_TRUNCATE_NOTE


def _format_value(value) -> str:
    if value is None or value == "":
        return "(absent / NR)"
    return value


def _format_span(span) -> str:
    if span is None or span == "":
        return "(no span provided)"
    return _truncate_span(span)


def _render_slot(slot_index: int, arm: ArmOutput) -> str:
    flags = arm.precheck_flags
    return (
        f"--- Slot {slot_index} ---\n"
        f"VALUE: {_format_value(arm.value)}\n"
        f'SUPPORTING SPAN: "{_format_span(arm.span)}"\n'
        "PRE-CHECK FLAGS (deterministic, not judge output):\n"
        f"  - span_present: {flags.span_present}\n"
        f"  - span_in_source: {flags.span_in_source}\n"
        f"  - value_in_span: {flags.value_in_span}"
    )


_SYSTEM_ROLE = (
    "=== SYSTEM ROLE ===\n"
    "You are an impartial evaluator comparing extraction outputs "
    "from different information extraction systems. You do NOT "
    "know which system produced which output. Ordering of slots "
    "is randomized per call and does not identify the source."
)

_TASK_BLOCK = """=== YOUR TASK ===
1. For each pair of slots (slot_a < slot_b), rate semantic
   equivalence at Level 1:
     EQUIVALENT — Same semantic content. Differences are
       surface phrasing, lexical variation, or entity-form
       normalization only.
     PARTIAL — Overlapping content but one is more complete
       or more specific.
     DIVERGENT — Non-overlapping or contradictory content.

2. For PARTIAL or DIVERGENT pairs only, assign a Level 2
   disagreement type:
     GRANULARITY — Same underlying fact at different precision.
     SELECTION — Different valid selection from ambiguous or
       multi-valued source.
     OMISSION — One slot returned absent/NR; the other has
       a value.
     CONTRADICTION — Mutually exclusive claims; at least one
       is wrong.
     FABRICATION — Content not traceable to the supporting
       span or the source.

   Do NOT assign a Level 2 type to EQUIVALENT pairs — surface
   phrasing differences do not constitute a disagreement type.

3. Assess overall fabrication_risk across all slots:
     low — All values appear grounded in their spans and the
       pre-check flags are consistent.
     medium — At least one value appears questionably grounded
       OR one or more span_in_source flags are False with
       uncertain cause.
     high — At least one value appears clearly ungrounded OR
       pre-check flags strongly suggest a slot invented content.

4. If ANY pair is EQUIVALENT, propose a consensus value
   (using the most complete phrasing among the equivalent
   slots). Otherwise, set proposed_consensus to null.

5. Keep rationales concise (1–2 sentences each). Do not
   pad. Do not include any text outside the JSON object."""

_OUTPUT_FORMAT_BLOCK = """=== OUTPUT FORMAT ===
Respond with a single JSON object. The grammar will reject any
other structure. Example shape (values below are illustrative
only — do not copy them):

{
  "pairwise_ratings": [
    {
      "slot_a": 1,
      "slot_b": 2,
      "rating": "EQUIVALENT",
      "rationale": "<one or two sentences>"
    },
    {
      "slot_a": 1,
      "slot_b": 3,
      "rating": "PARTIAL",
      "disagreement_type": "GRANULARITY",
      "rationale": "<one or two sentences>"
    }
  ],
  "fabrication_risk": "low",
  "proposed_consensus": "<consensus value or null>",
  "overall_rationale": "<one or two sentences>"
}"""


_BIAS_BLOCK = """=== BIAS-MITIGATION INSTRUCTIONS ===
- Slot ordering is randomized. Do not assume slot 1 is more
  authoritative than slot 2.
- Rate on content, not verbosity. A longer value is not
  automatically more correct; a shorter value is not
  automatically incomplete.
- The pre-check flags are mechanical string matches, not
  your conclusion. You may override them — for example, a
  numerically equivalent value with different formatting
  may fail value_in_span but still be EQUIVALENT."""


def build_pass1_prompt(
    input: JudgeInput,
    shuffled_arms: List[ArmOutput],
) -> str:
    """Render the full Pass 1 prompt. Deterministic given inputs."""

    field_lines = [
        "=== FIELD UNDER REVIEW ===",
        f"Field name: {input.field_name}",
        f"Field type: {input.field_type}",
        f"Definition: {input.field_definition}",
    ]
    if input.field_type == "categorical" and input.field_valid_values:
        field_lines.append(
            f"Valid values: {', '.join(input.field_valid_values)}"
        )
    field_block = "\n".join(field_lines)

    slots_rendered = "\n\n".join(
        _render_slot(i + 1, arm) for i, arm in enumerate(shuffled_arms)
    )
    extracted_block = "=== EXTRACTED OUTPUTS ===\n" + slots_rendered

    output_block = _OUTPUT_FORMAT_BLOCK

    return "\n\n".join(
        [
            _SYSTEM_ROLE,
            field_block,
            extracted_block,
            _TASK_BLOCK,
            output_block,
            _BIAS_BLOCK,
        ]
    )


__all__ = [
    "SPAN_TRUNCATE_CHARS",
    "build_pass1_prompt",
    "compute_seed",
    "randomize_arm_assignment",
]
