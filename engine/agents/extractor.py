"""Two-pass extraction agent using DeepSeek-R1:32b via Ollama."""

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import ollama

from engine.agents.models import EvidenceSpan, ExtractionOutput, ExtractionResult
from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec

logger = logging.getLogger(__name__)

MODEL = "deepseek-r1:32b"


# ── Prompt Builder ───────────────────────────────────────────────────


def build_extraction_prompt(paper_text: str, spec: ReviewSpec) -> str:
    """Build the extraction prompt from paper text and review spec schema."""
    field_blocks: list[str] = []

    for tier in (1, 2, 3):
        tier_label = {1: "Tier 1 — Required", 2: "Tier 2 — Important", 3: "Tier 3 — Optional"}
        fields = spec.extraction_schema.fields_by_tier(tier)
        if not fields:
            continue
        lines = [f"\n### {tier_label[tier]}"]
        for f in fields:
            enum_note = f" (allowed values: {', '.join(f.enum_values)})" if f.enum_values else ""
            lines.append(
                f"- **{f.name}** ({f.type}{enum_note}): {f.description}"
            )
        field_blocks.append("\n".join(lines))

    schema_text = "\n".join(field_blocks)

    return f"""Extract structured data from the following paper for a systematic review.

## Extraction Schema
{schema_text}

## Instructions
For each field above, extract the value from the paper and provide:
- **value**: The extracted data. If the field is not found in the paper, set to "NOT_FOUND".
- **source_snippet**: A verbatim quote (1-3 sentences) from the paper that supports your extraction. If NOT_FOUND, set to empty string.
- **confidence**: How clearly the paper states this information (0.0 to 1.0).
- **tier**: The tier number of the field (1, 2, or 3).

## Paper Text
{paper_text}"""


# ── Pass 1: Reasoning ────────────────────────────────────────────────


def extract_pass1_reasoning(prompt: str) -> str:
    """Run Pass 1: let DeepSeek-R1 reason freely, return the thinking trace."""
    response = ollama.chat(
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

    response = ollama.chat(
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

    # Store extraction in database
    extracted_data = [span.model_dump() for span in result.fields]
    ext_id = db.add_extraction(
        paper_id=paper_id,
        schema_hash=result.extraction_schema_hash,
        extracted_data=extracted_data,
        reasoning_trace=reasoning_trace,
        model=MODEL,
    )

    # Store each evidence span
    for span in result.fields:
        db.add_evidence_span(
            extraction_id=ext_id,
            field_name=span.field_name,
            value=span.value,
            source_snippet=span.source_snippet,
            confidence=span.confidence,
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
            stats["failed"] += 1

    logger.info(
        "Extraction complete: %d extracted, %d skipped, %d failed, %d total spans",
        stats["extracted"],
        stats["skipped"],
        stats["failed"],
        stats["total_spans"],
    )
    return stats
