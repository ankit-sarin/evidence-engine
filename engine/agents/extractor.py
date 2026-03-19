"""Two-pass extraction agent using DeepSeek-R1:32b via Ollama."""

import json
import logging
import re
import subprocess
import time
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

import httpx
import yaml

from engine.agents.models import EvidenceSpan, ExtractionOutput, ExtractionResult
from engine.core.constants import INVALID_SNIPPET_RE
from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec
from engine.utils.ollama_client import ollama_chat

logger = logging.getLogger(__name__)

MODEL = "deepseek-r1:32b"
MAX_RETRIES = 2
RETRY_DELAY = 30  # seconds between retries
SNIPPET_MAX_RETRIES = 2
RESTART_EVERY_N = 25  # proactive Ollama restart interval (0 = disabled)


# ── Codebook Loader ──────────────────────────────────────────────────


@lru_cache(maxsize=4)
def _load_codebook(codebook_path: str) -> dict:
    """Load extraction codebook YAML, cached per path."""
    with open(codebook_path) as f:
        return yaml.safe_load(f)


def _find_codebook_path(review_dir: str | Path | None = None) -> Path:
    """Locate the extraction codebook YAML for a review."""
    if review_dir:
        p = Path(review_dir) / "extraction_codebook.yaml"
        if p.exists():
            return p
    # Fallback: search data/ subdirectories
    for p in Path("data").glob("*/extraction_codebook.yaml"):
        return p
    raise FileNotFoundError("No extraction_codebook.yaml found")


# ── Prompt Builder ───────────────────────────────────────────────────


def _build_field_block(cb_field: dict) -> str:
    """Build the prompt section for a single field from its codebook entry."""
    name = cb_field["name"]
    ftype = cb_field["type"]
    definition = cb_field.get("definition", "")
    instruction = cb_field.get("instruction", "")

    lines: list[str] = []

    # Header: field name, type, and allowed values for categorical
    valid_values = cb_field.get("valid_values", [])
    if valid_values:
        value_names = [v["value"] for v in valid_values]
        enum_note = f" (allowed values: {', '.join(value_names)})"
    else:
        enum_note = ""

    lines.append(f"- **{name}** ({ftype}{enum_note}): {definition}")

    # Instruction (extraction guidance)
    if instruction:
        lines.append(f"  *Instruction:* {instruction}")

    # Per-value definitions for categorical fields
    if valid_values:
        lines.append("  **Value definitions:**")
        for vv in valid_values:
            lines.append(f"  - **{vv['value']}** — {vv['definition']}")

    # Decision criteria
    decision_criteria = cb_field.get("decision_criteria")
    if decision_criteria:
        lines.append(f"  **Decision criteria:**")
        for dc_line in decision_criteria.strip().splitlines():
            lines.append(f"  {dc_line}")

    # Examples
    examples = cb_field.get("examples", [])
    if examples:
        lines.append("  **Examples:**")
        for ex in examples:
            lines.append(f"  - {ex['scenario']} → **{ex['value']}**")

    # Source quote requirement
    if cb_field.get("source_quote_required"):
        lines.append("  *Source quote required for this field.*")

    return "\n".join(lines)


def build_extraction_prompt(
    paper_text: str,
    spec: ReviewSpec,
    codebook_path: str | Path | None = None,
) -> str:
    """Build the extraction prompt from paper text and codebook YAML.

    The codebook provides structured field definitions, per-value descriptions,
    decision criteria, and examples. All prompt content comes from the codebook —
    no hand-maintained field guides.

    Args:
        paper_text: Parsed markdown of the paper.
        spec: ReviewSpec (used for field ordering and schema hash).
        codebook_path: Path to extraction_codebook.yaml. If None, auto-discovered.
    """
    # Load codebook
    if codebook_path is None:
        cb_path = _find_codebook_path()
    else:
        cb_path = Path(codebook_path)
    codebook = _load_codebook(str(cb_path))

    # Index codebook fields by name
    cb_fields = {f["name"]: f for f in codebook["fields"]}

    tier_label = {
        1: "Tier 1 — Explicit (expected κ > 0.90)",
        2: "Tier 2 — Interpretive (expected κ 0.70–0.85)",
        3: "Tier 3 — Numeric/Tables (variable κ)",
        4: "Tier 4 — Judgment (expected κ 0.50–0.70)",
    }

    field_blocks: list[str] = []
    for tier in (1, 2, 3, 4):
        fields = spec.extraction_schema.fields_by_tier(tier)
        if not fields:
            continue
        lines = [f"\n### {tier_label[tier]}"]
        for f in fields:
            cb_entry = cb_fields.get(f.name)
            if cb_entry:
                lines.append(_build_field_block(cb_entry))
            else:
                # Fallback: bare spec definition (should not happen if codebook is complete)
                enum_note = f" (allowed values: {', '.join(f.enum_values)})" if f.enum_values else ""
                lines.append(f"- **{f.name}** ({f.type}{enum_note}): {f.description}")
                logger.warning("Field %s not found in codebook — using bare spec definition", f.name)
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
  - For **sample_size**: report as a single integer representing the total sample. If multiple groups, sum them. Example: if 4 pigs + 5 phantoms = "9".
  - For **validation_setting** and **surgical_domain**: if multiple categories apply, list all separated by semicolons (e.g., "In vivo (animal); Phantom/Simulation"). Each value must exactly match one allowed value.
  - For **system_maturity** and **study_design**: select the single best-fit category. For system_maturity, pick the most advanced stage demonstrated. For study_design, pick the primary design.
  - For all categorical fields: use ONLY the exact allowed values listed. Do not paraphrase, abbreviate, or combine them.
- **source_snippet**: A verbatim quote (1-3 sentences) copied character-for-character from the paper that supports your extraction. Do NOT paraphrase, summarize, or rephrase in any way. Do NOT bridge distant passages with "..." or ellipses — quote one continuous passage only. If value is "NOT_FOUND", set source_snippet to "". Never fabricate a snippet — every non-empty snippet must be a real quote from the paper. For Tier 4 judgment fields, quote the passage that most informed your judgment.
- **confidence**: How clearly the paper states this information (0.0 to 1.0). For Tier 4 judgment fields, this reflects your confidence in your assessment.
- **tier**: The tier number of the field (1, 2, 3, or 4).

You MUST emit exactly one entry per field listed above ({total_fields} fields total), including Tier 4 judgment fields.

## Paper Text
{paper_text}"""


# ── Ollama Retry Wrapper ─────────────────────────────────────────────
# Retry and timeout logic now lives in engine.utils.ollama_client.ollama_chat.
# _ollama_chat_with_retry is kept as a thin pass-through for internal callers.


# ── Pass 1: Reasoning ────────────────────────────────────────────────


def extract_pass1_reasoning(prompt: str) -> str:
    """Run Pass 1: let DeepSeek-R1 reason freely, return the thinking trace."""
    response = ollama_chat(
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

    response = ollama_chat(
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
        response = ollama_chat(
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
    model_digest: str | None = None,
    auditor_model_digest: str | None = None,
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
    if not span_dicts:
        raise ValueError(
            f"Paper {paper_id}: extraction produced 0 evidence spans — "
            "refusing to store empty extraction"
        )
    ext_id = db.add_extraction_atomic(
        paper_id=paper_id,
        schema_hash=result.extraction_schema_hash,
        extracted_data=extracted_data,
        reasoning_trace=reasoning_trace,
        model=MODEL,
        spans=span_dicts,
        model_digest=model_digest,
        auditor_model_digest=auditor_model_digest,
    )

    return result


# ── Proactive Ollama Restart ──────────────────────────────────────────


def restart_ollama(reason: str = "proactive", papers_done: int = 0) -> None:
    """Restart the Ollama service and wait for it to become responsive.

    Uses ``sudo systemctl restart ollama`` to fully clear CUDA context
    fragmentation (a simple model unload/reload is not sufficient).

    Raises RuntimeError if Ollama doesn't come back within 60 seconds.
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info(
        "[%s] Ollama restart (%s, %d papers done) — running sudo systemctl restart ollama",
        ts, reason, papers_done,
    )
    try:
        subprocess.run(
            ["sudo", "systemctl", "restart", "ollama"],
            timeout=30, check=True, capture_output=True,
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to restart Ollama: {exc}") from exc

    # Poll /api/tags until responsive (max 60s)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            resp = httpx.get("http://127.0.0.1:11434/api/tags", timeout=5)
            if resp.status_code == 200:
                elapsed = 60 - (deadline - time.monotonic())
                logger.info(
                    "Ollama restart complete — server responsive after %.1fs", elapsed,
                )
                return
        except (httpx.ConnectError, httpx.TimeoutException):
            pass
        time.sleep(2)

    raise RuntimeError("Ollama did not become responsive within 60s after restart")


# ── Batch Extraction Pipeline ─────────────────────────────────────────


def run_extraction(
    db: ReviewDatabase,
    spec: ReviewSpec,
    review_name: str,
    restart_every: int = RESTART_EVERY_N,
) -> dict:
    """Run extraction on all eligible papers. Skip if already extracted with current hash.

    Picks up papers at FT_ELIGIBLE (reviews with FT screening) and PARSED
    (reviews without FT screening). A paper cannot be at both statuses
    simultaneously, so querying both is safe.
    """
    ft_papers = db.get_papers_by_status("FT_ELIGIBLE")
    parsed_papers = db.get_papers_by_status("PARSED")
    papers = ft_papers + parsed_papers
    total = len(papers)
    schema_hash = spec.extraction_hash()
    logger.info("Starting extraction on %d papers (schema hash: %s)", total, schema_hash[:12])

    # Pre-flight: verify extraction model is loaded and responsive
    from engine.utils.ollama_preflight import require_preflight
    require_preflight([MODEL], runner_name="Extraction")

    # Capture model digests before extraction loop
    from engine.utils.ollama_client import get_model_digest
    from engine.agents.auditor import DEFAULT_AUDITOR_MODEL
    extractor_digest = get_model_digest(MODEL)
    auditor_digest = get_model_digest(DEFAULT_AUDITOR_MODEL)
    logger.info(
        "Model digests — extractor (%s): %s, auditor (%s): %s",
        MODEL, extractor_digest or "unavailable",
        DEFAULT_AUDITOR_MODEL, auditor_digest or "unavailable",
    )

    # Pre-flight: warn about stale extractions from a different schema version
    from engine.utils.extraction_cleanup import check_stale_extractions
    stale_count = check_stale_extractions(db, schema_hash)
    if stale_count > 0:
        logger.warning(
            "Found %d papers with stale schema extractions. Run "
            "python -m engine.utils.extraction_cleanup --review %s to clean up "
            "before re-extracting.",
            stale_count, review_name,
        )

    from engine.utils.progress import ProgressReporter

    stats = {"extracted": 0, "skipped": 0, "failed": 0, "total_spans": 0}
    review_dir = Path(db.db_path).parent
    progress = ProgressReporter(total, "Local extraction")
    papers_since_restart = 0  # counter for proactive restart

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
            progress.report(pid, "SKIPPED", 0)
            continue

        # Load parsed Markdown
        parsed_dir = review_dir / "parsed_text"
        md_files = sorted(parsed_dir.glob(f"{pid}_v*.md"), reverse=True)
        if not md_files:
            logger.warning("Paper %d: no parsed text found — skipping", pid)
            stats["failed"] += 1
            progress.report(pid, "FAILED", 0)
            continue

        paper_text = md_files[0].read_text()
        t_paper = time.time()

        try:
            result = extract_paper(
                pid, paper_text, spec, db,
                model_digest=extractor_digest,
                auditor_model_digest=auditor_digest,
            )
            db.update_status(pid, "EXTRACTED")
            stats["extracted"] += 1
            stats["total_spans"] += len(result.fields)
            elapsed = time.time() - t_paper
            progress.report(pid, "EXTRACTED", elapsed)
            logger.info(
                "Extracted %d/%d — %d fields from '%s'",
                i, total, len(result.fields), title[:60],
            )
        except Exception as exc:
            logger.error("Paper %d extraction failed: %s", pid, exc)
            db.update_status(pid, "EXTRACT_FAILED")
            stats["failed"] += 1
            elapsed = time.time() - t_paper
            progress.report(pid, "FAILED", elapsed)

        # Proactive Ollama restart to clear CUDA context fragmentation
        papers_since_restart += 1
        if restart_every > 0 and papers_since_restart >= restart_every:
            restart_ollama(
                reason=f"proactive after {papers_since_restart} papers",
                papers_done=stats["extracted"] + stats["skipped"] + stats["failed"],
            )
            papers_since_restart = 0

    progress.summary()
    logger.info(
        "Extraction complete: %d extracted, %d skipped, %d failed, %d total spans",
        stats["extracted"],
        stats["skipped"],
        stats["failed"],
        stats["total_spans"],
    )
    return stats
