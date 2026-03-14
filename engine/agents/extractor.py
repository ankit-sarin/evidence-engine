"""Two-pass extraction agent using DeepSeek-R1:32b via Ollama."""

import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import ollama

from engine.agents.models import EvidenceSpan, ExtractionOutput, ExtractionResult
from engine.core.constants import INVALID_SNIPPET_RE
from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec

logger = logging.getLogger(__name__)

MODEL = "deepseek-r1:32b"
OLLAMA_TIMEOUT = 900.0  # 15 minutes per API call
MAX_RETRIES = 2
RETRY_DELAY = 30  # seconds between retries
SNIPPET_MAX_RETRIES = 2

_client = ollama.Client(timeout=OLLAMA_TIMEOUT)


# ── Prompt Builder ───────────────────────────────────────────────────


def build_extraction_prompt(paper_text: str, spec: ReviewSpec) -> str:
    """Build the extraction prompt from paper text and review spec schema."""
    field_blocks: list[str] = []

    tier_label = {
        1: "Tier 1 — Explicit (expected κ > 0.90)",
        2: "Tier 2 — Interpretive (expected κ 0.70–0.85)",
        3: "Tier 3 — Numeric/Tables (variable κ)",
        4: "Tier 4 — Judgment (expected κ 0.50–0.70)",
    }
    for tier in (1, 2, 3, 4):
        fields = spec.extraction_schema.fields_by_tier(tier)
        if not fields:
            continue
        lines = [f"\n### {tier_label[tier]}"]
        for f in fields:
            enum_note = f" (allowed values: {', '.join(f.enum_values)})" if f.enum_values else ""
            lines.append(
                f"- **{f.name}** ({f.type}{enum_note}): {f.description}"
            )
            # Add supplementary guidance for complex fields
            guide = _FIELD_GUIDES.get(f.name)
            if guide:
                lines.append(guide)
        field_blocks.append("\n".join(lines))

    schema_text = "\n".join(field_blocks)
    total_fields = sum(
        len(spec.extraction_schema.fields_by_tier(t)) for t in (1, 2, 3, 4)
    )

    return f"""Extract structured data from the following paper for a systematic review.

## Extraction Schema
{schema_text}

## Instructions
For each field above, extract the value from the paper and provide:
- **field_name**: Exactly as listed above.
- **value**: The extracted data. If the field is not found in the paper, set to "NOT_FOUND".
- **source_snippet**: A verbatim quote (1-3 sentences) copied character-for-character from the paper that supports your extraction. Do NOT paraphrase, summarize, or rephrase in any way. Do NOT bridge distant passages with "..." or ellipses — quote one continuous passage only. If value is "NOT_FOUND", set source_snippet to "". Never fabricate a snippet — every non-empty snippet must be a real quote from the paper. For Tier 4 judgment fields, quote the passage that most informed your judgment.
- **confidence**: How clearly the paper states this information (0.0 to 1.0). For Tier 4 judgment fields, this reflects your confidence in your assessment.
- **tier**: The tier number of the field (1, 2, 3, or 4).

You MUST emit exactly one entry per field listed above ({total_fields} fields total), including Tier 4 judgment fields.

## Paper Text
{paper_text}"""


# ── Supplementary field guides (injected after field description) ────

_FIELD_GUIDES: dict[str, str] = {
    "autonomy_level": """\
  **Decision tree (when the paper does not explicitly reference Yang levels):**
  1. Does the robot execute any action without continuous real-time human control? → If no → Level 1
  2. If yes — does the surgeon define the exact plan and initiate execution? → If yes → Level 2
  3. Does the robot generate candidate strategies for the surgeon to select from? → If yes → Level 3
  4. Does the robot independently plan and execute based on patient-specific data, with surgeon monitoring? → If yes → Level 4
  5. Does the robot operate without any human in the loop? → If yes → Level 5
  **On algorithms/simulations:** Classify based on what the system demonstrates, not the hardware. A simulated algorithm that autonomously plans and executes is still Level 2+.
  **On "Mixed/Multiple":** Use ONLY when the paper explicitly tests multiple distinct autonomy levels. Not an escape hatch for uncertainty — pick the best fit for ambiguous cases.""",

    "task_monitor": """\
  Examples: H = surgeon watches a screen while teleoperating. R = vision system tracks tissue deformation in real time. Shared = robot uses CV to track a needle while surgeon monitors on display.""",

    "task_generate": """\
  Covers trajectory, action sequence, parameters (speed, force, path), or surgical strategy.
  Examples: H = surgeon places suture entry/exit points. R = path-planning algorithm generates optimal trajectory from tissue geometry. Shared = surgeon defines target anatomy, robot generates trajectory.""",

    "task_select": """\
  Examples: H = robot generates three paths, surgeon selects one. R = RL controller evaluates strategies and commits to highest-scored (also use R when system generates a single plan and executes — no selection step). Shared = robot narrows to shortlist, surgeon approves.""",

    "task_execute": """\
  Examples: H = standard teleoperation. R = robot drives needle autonomously. Shared = cooperative control (surgeon holds instrument, robot applies active constraints).""",

    "system_maturity": """\
  Value definitions:
  - Commercial clinical system — FDA-cleared/CE-marked robot in approved capacity (da Vinci teleop, Mako)
  - Commercial system + research autonomy — Commercial robot modified for autonomous tasks not in cleared indication (dVRK autonomous suturing)
  - Research prototype (hardware) — Purpose-built physical robot not commercially available (STAR, custom needle-steering robot)
  - Algorithm on existing platform — New software/algorithm on existing robot, focus is the algorithm
  - Simulation / computational only — No physical robot, purely in-silico
  - Conceptual / framework — No experimental demonstration, proposes design or taxonomy""",

    "study_design": """\
  Select best fit. If a paper demonstrates a new algorithm on a phantom, classify as "Initial technical demonstration" or "Algorithm development and evaluation" depending on emphasis.""",

    "country": """\
  Use first author's institution country if not explicitly stated. Metadata-inferred is acceptable.""",

    "secondary_outcomes": """\
  Format: semicolon-separated "metric: value" entries. Example: "Accuracy: 94.2% ± 3.1%; Force: 2.3 ± 0.8 N; Success rate: 18/20". Enter NR if only one outcome reported.""",
}


# ── Ollama Retry Wrapper ─────────────────────────────────────────────


def _ollama_chat_with_retry(**kwargs):
    """Call ollama.chat with timeout and retry on transient failures."""
    for attempt in range(1 + MAX_RETRIES):
        try:
            return _client.chat(**kwargs)
        except Exception as exc:
            if attempt < MAX_RETRIES:
                logger.warning(
                    "Ollama call failed (attempt %d/%d): %s — retrying in %ds",
                    attempt + 1, 1 + MAX_RETRIES, exc, RETRY_DELAY,
                )
                time.sleep(RETRY_DELAY)
            else:
                logger.error(
                    "Ollama call failed after %d attempts: %s",
                    1 + MAX_RETRIES, exc,
                )
                raise


# ── Pass 1: Reasoning ────────────────────────────────────────────────


def extract_pass1_reasoning(prompt: str) -> str:
    """Run Pass 1: let DeepSeek-R1 reason freely, return the thinking trace."""
    response = _ollama_chat_with_retry(
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a systematic review data extractor. Read the paper "
                    "carefully and reason through each extraction field step by step. "
                    "Think about what the paper says for each field before extracting."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        options={"temperature": 0},
    )

    content = response.message.content or ""
    return parse_thinking_trace(content)


def parse_thinking_trace(content: str) -> str:
    """Extract content between <think> and </think> tags.

    If no tags found, return the full content as the reasoning trace.
    """
    match = re.search(r"<think>(.*?)</think>", content, re.DOTALL)
    if match:
        return match.group(1).strip()
    return content.strip()


# ── Pass 2: Structured Output ────────────────────────────────────────


def extract_pass2_structured(
    prompt: str,
    reasoning_trace: str,
    spec: ReviewSpec,
    paper_id: int,
) -> ExtractionResult:
    """Run Pass 2: use reasoning trace as context, force structured JSON output."""
    schema_hash = spec.extraction_hash()

    response = _ollama_chat_with_retry(
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a systematic review data extractor. "
                    "Use your prior reasoning to produce accurate structured output. "
                    "Respond ONLY with the requested JSON."
                ),
            },
            {"role": "user", "content": prompt},
            {
                "role": "user",
                "content": (
                    f"Here is your prior analysis of this paper:\n\n"
                    f"{reasoning_trace}\n\n"
                    f"Now output the structured extraction as JSON matching the schema. "
                    f"Include all fields from the extraction schema."
                ),
            },
        ],
        format=ExtractionOutput.model_json_schema(),
        options={"temperature": 0},
        think=False,
    )

    raw = response.message.content or ""
    output = ExtractionOutput.model_validate_json(raw)

    return ExtractionResult(
        paper_id=paper_id,
        fields=output.fields,
        reasoning_trace=reasoning_trace,
        model=MODEL,
        extraction_schema_hash=schema_hash,
        extracted_at=datetime.now(timezone.utc),
    )


# ── Snippet Validation ──────────────────────────────────────────────


def _has_invalid_snippet(snippet: str | None) -> bool:
    """Return True if snippet is non-null and contains ellipsis bridging."""
    return bool(snippet and INVALID_SNIPPET_RE.search(snippet))


def _retry_snippet(
    field_name: str,
    value: str,
    paper_text: str,
    paper_id: int,
) -> str | None:
    """Request a clean verbatim snippet for a single field.

    Returns the new snippet string, or None if the model still produces
    an invalid snippet or fails.
    """
    prompt = (
        f"You previously extracted the value below from a scientific paper.\n\n"
        f"Field: {field_name}\n"
        f"Value: {value}\n\n"
        f"Provide a single contiguous verbatim sentence copied exactly from "
        f"the text that supports this value. No ellipsis. No bridging between "
        f"passages. If no single sentence supports this value, return null "
        f"for the snippet.\n\n"
        f"Respond ONLY with JSON: {{\"source_snippet\": \"...\" or null}}\n\n"
        f"## Paper Text\n{paper_text}"
    )
    try:
        response = _ollama_chat_with_retry(
            model=MODEL,
            messages=[
                {"role": "system", "content": "Respond ONLY with JSON."},
                {"role": "user", "content": prompt},
            ],
            options={"temperature": 0},
            think=False,
        )
        raw = response.message.content or ""
        data = json.loads(raw)
        new_snippet = data.get("source_snippet")
        if new_snippet and _has_invalid_snippet(new_snippet):
            return None
        return new_snippet
    except Exception:
        return None


def _validate_and_retry_snippets(
    fields: list[EvidenceSpan],
    paper_text: str,
    paper_id: int,
) -> list[EvidenceSpan]:
    """Validate snippets post-extraction; retry invalid ones up to SNIPPET_MAX_RETRIES times."""
    validated = []
    for span in fields:
        if not _has_invalid_snippet(span.source_snippet):
            validated.append(span)
            continue

        new_snippet = None
        for attempt in range(1, SNIPPET_MAX_RETRIES + 1):
            logger.debug(
                "Paper %d, field %s: invalid snippet retry %d/%d",
                paper_id, span.field_name, attempt, SNIPPET_MAX_RETRIES,
            )
            new_snippet = _retry_snippet(
                span.field_name, span.value, paper_text, paper_id,
            )
            if new_snippet is not None:
                break

        validated.append(EvidenceSpan(
            field_name=span.field_name,
            value=span.value,
            source_snippet=new_snippet or "",
            confidence=span.confidence,
            tier=span.tier,
        ))
    return validated


# ── Single-Paper Extraction ──────────────────────────────────────────


def extract_paper(
    paper_id: int,
    paper_text: str,
    spec: ReviewSpec,
    db: ReviewDatabase,
) -> ExtractionResult:
    """Run the full two-pass extraction on a single paper and store results."""
    prompt = build_extraction_prompt(paper_text, spec)

    # Pass 1: reasoning
    reasoning_trace = extract_pass1_reasoning(prompt)

    # Pass 2: structured output
    result = extract_pass2_structured(prompt, reasoning_trace, spec, paper_id)

    # Validate snippets and retry invalid ones before storing
    validated_fields = _validate_and_retry_snippets(
        result.fields, paper_text, paper_id,
    )
    result = ExtractionResult(
        paper_id=result.paper_id,
        fields=validated_fields,
        reasoning_trace=result.reasoning_trace,
        model=result.model,
        extraction_schema_hash=result.extraction_schema_hash,
        extracted_at=result.extracted_at,
    )

    # Store extraction + all spans atomically (single transaction)
    extracted_data = [span.model_dump() for span in result.fields]
    span_dicts = [
        {
            "field_name": s.field_name,
            "value": s.value,
            "source_snippet": s.source_snippet,
            "confidence": s.confidence,
        }
        for s in result.fields
    ]
    ext_id = db.add_extraction_atomic(
        paper_id=paper_id,
        schema_hash=result.extraction_schema_hash,
        extracted_data=extracted_data,
        reasoning_trace=reasoning_trace,
        model=MODEL,
        spans=span_dicts,
    )

    return result


# ── Batch Extraction Pipeline ─────────────────────────────────────────


def run_extraction(db: ReviewDatabase, spec: ReviewSpec, review_name: str) -> dict:
    """Run extraction on all PARSED papers. Skip if already extracted with current hash."""
    papers = db.get_papers_by_status("PARSED")
    total = len(papers)
    schema_hash = spec.extraction_hash()
    logger.info("Starting extraction on %d papers (schema hash: %s)", total, schema_hash[:12])

    stats = {"extracted": 0, "skipped": 0, "failed": 0, "total_spans": 0}
    review_dir = Path(db.db_path).parent

    for i, paper in enumerate(papers, 1):
        pid = paper["id"]
        title = paper["title"]

        # Check staleness: skip if already extracted with current schema hash
        existing = db._conn.execute(
            "SELECT id FROM extractions WHERE paper_id = ? AND extraction_schema_hash = ?",
            (pid, schema_hash),
        ).fetchone()
        if existing:
            logger.info("Paper %d: already extracted with current schema — skipping", pid)
            stats["skipped"] += 1
            continue

        # Load parsed Markdown
        parsed_dir = review_dir / "parsed_text"
        md_files = sorted(parsed_dir.glob(f"{pid}_v*.md"), reverse=True)
        if not md_files:
            logger.warning("Paper %d: no parsed text found — skipping", pid)
            stats["failed"] += 1
            continue

        paper_text = md_files[0].read_text()

        try:
            result = extract_paper(pid, paper_text, spec, db)
            db.update_status(pid, "EXTRACTED")
            stats["extracted"] += 1
            stats["total_spans"] += len(result.fields)
            logger.info(
                "Extracted %d/%d — %d fields from '%s'",
                i, total, len(result.fields), title[:60],
            )
        except Exception as exc:
            logger.error("Paper %d extraction failed: %s", pid, exc)
            db.update_status(pid, "EXTRACT_FAILED")
            stats["failed"] += 1

    logger.info(
        "Extraction complete: %d extracted, %d skipped, %d failed, %d total spans",
        stats["extracted"],
        stats["skipped"],
        stats["failed"],
        stats["total_spans"],
    )
    return stats
