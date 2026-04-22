"""Prompt builders for the Paper 1 LLM-as-judge pipeline (Pass 1 + Pass 2)."""

from __future__ import annotations

import hashlib
import random
from typing import List, Optional, Tuple

import tiktoken

from analysis.paper1.judge_schema import ArmOutput, JudgeInput

SPAN_TRUNCATE_CHARS = 400
SPAN_TRUNCATE_NOTE = " [span truncated for prompt length]"
SEED_MOD = 2**32

# Canonical absence sentinels. An arm value that, after whitespace-trim
# and case-fold, matches one of these is interpreted as "the arm claims
# this field is NOT reported in the paper". Empty string and None are
# also treated as absence claims. Mirrored in extraction_codebook.yaml
# under `absence_sentinels` for human readers.
ABSENCE_SENTINELS: frozenset[str] = frozenset({
    "NR",
    "N/A",
    "NA",
    "NOT_FOUND",
    "NOT FOUND",
    "NOT REPORTED",
})

# Pass 2 windowing constants.
PASS2_FULL_TEXT_BUDGET_TOKENS = 20_000
PASS2_WINDOW_RADIUS_TOKENS = 500
PASS2_CHARS_PER_TOKEN_APPROX = 4  # English prose; coarse-grained for seeking
PASS2_GAP_MARKER = "\n\n[...gap in source...]\n\n"

_ENCODING_NAME = "cl100k_base"
_enc_cache: tiktoken.Encoding | None = None


def _get_encoding() -> tiktoken.Encoding:
    global _enc_cache
    if _enc_cache is None:
        _enc_cache = tiktoken.get_encoding(_ENCODING_NAME)
    return _enc_cache


def count_tokens(text: str) -> int:
    """Approximate token count via cl100k_base. Deterministic."""
    if not text:
        return 0
    return len(_get_encoding().encode(text, disallowed_special=()))


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


# ═════════════════════════════════════════════════════════════════════
# Pass 2 — per-arm fabrication verification
# ═════════════════════════════════════════════════════════════════════


def compute_seed_pass2(paper_id: str, field_name: str, run_id: str) -> int:
    """Pass 2 seed, distinct from Pass 1 (same inputs → different seed)."""
    key = f"{paper_id}\x1f{field_name}\x1f{run_id}\x1fp2".encode("utf-8")
    digest = hashlib.sha256(key).digest()
    return int.from_bytes(digest[:4], "big") % SEED_MOD


def _snap_paragraph_before(text: str, char_pos: int) -> int:
    """Return the start of the paragraph containing char_pos (nearest \\n\\n before)."""
    if char_pos <= 0:
        return 0
    idx = text.rfind("\n\n", 0, char_pos)
    if idx == -1:
        idx = text.rfind("\n", 0, char_pos)
    return 0 if idx == -1 else idx + 1


def _snap_paragraph_after(text: str, char_pos: int) -> int:
    """Return the end of the paragraph containing char_pos (nearest \\n\\n after)."""
    if char_pos >= len(text):
        return len(text)
    idx = text.find("\n\n", char_pos)
    if idx == -1:
        idx = text.find("\n", char_pos)
    return len(text) if idx == -1 else idx


def _merge_overlapping(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not intervals:
        return []
    sorted_iv = sorted(intervals)
    merged: list[tuple[int, int]] = [sorted_iv[0]]
    for s, e in sorted_iv[1:]:
        last_s, last_e = merged[-1]
        if s <= last_e:
            merged[-1] = (last_s, max(last_e, e))
        else:
            merged.append((s, e))
    return merged


def window_source_text(
    source_text: str,
    arm_spans: list[str | None],
    budget_tokens: int = PASS2_FULL_TEXT_BUDGET_TOKENS,
    radius_tokens: int = PASS2_WINDOW_RADIUS_TOKENS,
) -> Tuple[str, bool, int]:
    """Return (text, was_windowed, token_count).

    If source fits in budget_tokens, return as-is. Otherwise build a
    paragraph-snapped ±radius_tokens window around the union of arm
    spans, merging overlaps. If no spans can be located, fall back to
    the first budget_tokens of the source.
    """
    if not source_text:
        return "", False, 0

    full_count = count_tokens(source_text)
    if full_count <= budget_tokens:
        return source_text, False, full_count

    enc = _get_encoding()
    radius_chars = radius_tokens * PASS2_CHARS_PER_TOKEN_APPROX

    # Locate each non-empty arm span in source via exact substring
    # (first 200 chars if span is long). Spans that can't be located
    # are dropped from the anchor set.
    offsets: list[tuple[int, int]] = []
    for span in arm_spans:
        if not span:
            continue
        needle = span[:200] if len(span) > 200 else span
        idx = source_text.find(needle)
        if idx == -1:
            # Try a smaller prefix before giving up
            idx = source_text.find(span[:80]) if len(span) >= 80 else -1
            if idx == -1:
                continue
        offsets.append((idx, idx + len(needle)))

    if not offsets:
        # Degenerate fallback — use the first budget_tokens of source.
        tokens = enc.encode(source_text, disallowed_special=())[:budget_tokens]
        windowed = enc.decode(tokens)
        return windowed, True, len(tokens)

    # Expand, snap to paragraph boundaries, merge overlaps.
    expanded: list[tuple[int, int]] = []
    for start, end in offsets:
        w_start = _snap_paragraph_before(
            source_text, max(0, start - radius_chars)
        )
        w_end = _snap_paragraph_after(
            source_text, min(len(source_text), end + radius_chars)
        )
        expanded.append((w_start, w_end))
    merged = _merge_overlapping(expanded)

    chunks = [source_text[s:e] for s, e in merged]
    windowed = PASS2_GAP_MARKER.join(chunks)

    # Re-measure and trim if still over budget. Trim from the tail.
    tok_count = count_tokens(windowed)
    if tok_count > budget_tokens:
        toks = enc.encode(windowed, disallowed_special=())[:budget_tokens]
        windowed = enc.decode(toks)
        tok_count = len(toks)

    return windowed, True, tok_count


def arm_short_circuit_eligible(arm: ArmOutput) -> bool:
    """A clean pre-check (span in source AND value in span) makes the arm
    short-circuit eligible — the Pass 2 prompt will nudge the judge toward
    SUPPORTED unless the source directly contradicts."""
    flags = arm.precheck_flags
    return bool(flags.span_in_source and flags.value_in_span)


def is_absence_claim(value: Optional[str]) -> bool:
    """Return True if `value` is an absence claim (sentinel, empty, null).

    Whitespace-trimmed and case-folded. A True result means the arm is
    asserting the field is NOT reported in the paper — not a positive
    extraction. Pass 2 verifies absence claims against a different
    rubric (see build_pass2_prompt).
    """
    if value is None:
        return True
    stripped = value.strip()
    if not stripped:
        return True
    return stripped.upper() in ABSENCE_SENTINELS


def _render_pass2_slot(slot_index: int, arm: ArmOutput) -> str:
    flags = arm.precheck_flags
    clean = arm_short_circuit_eligible(arm)
    if is_absence_claim(arm.value):
        status = "ABSENCE CLAIM"
    elif clean:
        status = "CLEAN PRE-CHECK"
    else:
        status = "NEEDS FULL VERIFICATION"
    return (
        f"--- Slot {slot_index} ({status}) ---\n"
        f"VALUE: {_format_value(arm.value)}\n"
        f'CLAIMED SUPPORTING SPAN: "{_format_span(arm.span)}"\n'
        "PRE-CHECK FLAGS:\n"
        f"  - span_present: {flags.span_present}\n"
        f"  - span_in_source: {flags.span_in_source}\n"
        f"  - value_in_span: {flags.value_in_span}"
    )


_PASS2_SYSTEM_ROLE = (
    "=== SYSTEM ROLE ===\n"
    "You are an impartial fact-checker. Your task is to verify whether "
    "each extraction arm's claimed value is grounded in the source paper. "
    "Slot ordering is randomized per call and does not identify the "
    "source system. Assume nothing about which slot produced which value."
)

_PASS2_TASK_BLOCK = """=== YOUR TASK ===
For each slot, read the source excerpt (below) and return one verdict:

  SUPPORTED            — The source directly states or clearly implies
                         the claimed value. The claimed supporting span
                         (if provided) appears in the source, or an
                         equivalent statement does.

  PARTIALLY_SUPPORTED  — The source partially supports the claim. One of:
                         a) value is in the right general area but is less
                            specific than, or inconsistent in detail with,
                            the source;
                         b) the claimed span is close paraphrase but not
                            verbatim and the value is defensible;
                         c) the source discusses the concept but does not
                            commit to the specific value the arm reports.

  UNSUPPORTED          — The source does not state, imply, or commit to
                         the claimed value. The arm appears to have
                         invented or materially distorted the content.

Short-circuit rule (soft bias, not a bypass):
  For slots marked "CLEAN PRE-CHECK", the claimed span was already
  mechanically located in the source and the value appears in that span.
  Default these to SUPPORTED UNLESS the source, read in context,
  directly contradicts the claim or reveals that the span is out of
  context and misleads. You may still override — do not rubber-stamp.

Required output per verdict:
  - arm_slot: the slot index.
  - verdict: one of the three values above.
  - verification_span: a short quote (<=300 chars) from the source that
    grounds your verdict. Optional for SUPPORTED clean short-circuits;
    otherwise strongly recommended.
  - reasoning: required for PARTIALLY_SUPPORTED and UNSUPPORTED
    (one or two sentences; what was in the source and how it differed
    from the arm's claim).
  - fabrication_hypothesis: required for UNSUPPORTED only. One short
    sentence describing the most plausible mechanism of fabrication
    (e.g., "plausible-sounding default", "over-generalization from
    another table", "hallucinated phrasing", "conflation with adjacent
    field").

Finally, set overall_fabrication_detected to true iff ANY slot is
UNSUPPORTED.

Keep rationales concise. No text outside the JSON object."""


_PASS2_ABSENCE_RUBRIC_TEMPLATE = """=== ABSENCE-CLAIM VERIFICATION ===
One or more slots are marked "ABSENCE CLAIM". For those slots the arm
is asserting that the paper does NOT report {field_name} — the arm
value is a codebook absence sentinel (NR / N/A / NOT_FOUND / empty),
not a positive extraction. Verify each absence claim by searching the
source text for any statement of {field_name}:

  SUPPORTED            — The source does not state or commit to a
                         value for {field_name}. The arm's absence
                         claim is defensible. A verification_span is
                         optional; if you include one, quote the
                         closest-related text you considered.

  PARTIALLY_SUPPORTED  — The source mentions {field_name} but
                         ambiguously, indirectly, or without a clear
                         value (e.g., describes the concept in prose
                         but never commits to a number/category). The
                         arm was defensibly cautious but could have
                         captured the partial information.

  UNSUPPORTED          — The source clearly states a value for
                         {field_name}. The arm missed a reported
                         value. For these cases, fabrication_hypothesis
                         should describe the miss (e.g., "missed
                         value reported in Table 2", "defaulted to NR
                         despite explicit sample size in methods").

Apply this rubric ONLY to slots tagged "ABSENCE CLAIM". For slots
tagged "CLEAN PRE-CHECK" or "NEEDS FULL VERIFICATION", use the
standard rubric above."""


_PASS2_OUTPUT_FORMAT_BLOCK = """=== OUTPUT FORMAT ===
Respond with a single JSON object. The grammar will reject any other
structure. Example shape (values are illustrative only):

{
  "paper_id": "<paper id>",
  "field_name": "<field name>",
  "arm_verdicts": [
    {
      "arm_slot": 1,
      "verdict": "SUPPORTED",
      "verification_span": "<short source quote or null>"
    },
    {
      "arm_slot": 2,
      "verdict": "PARTIALLY_SUPPORTED",
      "verification_span": "<short source quote>",
      "reasoning": "<one or two sentences>"
    },
    {
      "arm_slot": 3,
      "verdict": "UNSUPPORTED",
      "verification_span": "<short source quote or null>",
      "reasoning": "<one or two sentences>",
      "fabrication_hypothesis": "<one short sentence>"
    }
  ],
  "overall_fabrication_detected": true
}"""


def build_pass2_prompt(
    input: JudgeInput,
    shuffled_arms: List[ArmOutput],
    source_text: str,
    source_text_windowed: bool,
) -> str:
    """Render the Pass 2 (fabrication verification) prompt. Deterministic."""

    field_lines = [
        "=== FIELD UNDER REVIEW ===",
        f"Paper id: {input.paper_id}",
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
        _render_pass2_slot(i + 1, arm) for i, arm in enumerate(shuffled_arms)
    )
    extracted_block = "=== EXTRACTED OUTPUTS ===\n" + slots_rendered

    source_header = (
        "=== SOURCE EXCERPT (windowed) ==="
        if source_text_windowed
        else "=== SOURCE TEXT (full) ==="
    )
    source_block = f"{source_header}\n{source_text}"

    sections = [
        _PASS2_SYSTEM_ROLE,
        field_block,
        extracted_block,
        source_block,
        _PASS2_TASK_BLOCK,
    ]
    if any(is_absence_claim(arm.value) for arm in shuffled_arms):
        sections.append(
            _PASS2_ABSENCE_RUBRIC_TEMPLATE.format(field_name=input.field_name)
        )
    sections.append(_PASS2_OUTPUT_FORMAT_BLOCK)
    return "\n\n".join(sections)


__all__ = [
    "ABSENCE_SENTINELS",
    "PASS2_CHARS_PER_TOKEN_APPROX",
    "PASS2_FULL_TEXT_BUDGET_TOKENS",
    "PASS2_GAP_MARKER",
    "PASS2_WINDOW_RADIUS_TOKENS",
    "SPAN_TRUNCATE_CHARS",
    "arm_short_circuit_eligible",
    "build_pass1_prompt",
    "build_pass2_prompt",
    "compute_seed",
    "compute_seed_pass2",
    "count_tokens",
    "is_absence_claim",
    "randomize_arm_assignment",
    "window_source_text",
]
