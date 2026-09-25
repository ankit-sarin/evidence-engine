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
from pydantic import ValidationError

from engine.agents.models import EvidenceSpan, ExtractionOutput, ExtractionResult
from engine.core.constants import INVALID_SNIPPET_RE
from engine.core.database import ReviewDatabase
from engine.core.codebook import (
    CODEBOOK_FILENAME, load_codebook, load_codebook_beside, load_codebook_for,
)
from engine.core.review_spec import ReviewSpec
from engine.core.completeness import (
    MAX_COMPLETENESS_ATTEMPTS,
    IncompleteExtractionError,
    check_completeness,
    drop_unexpected,
    enforce_completeness,
    expected_field_names,
)
from engine.core.citation_guard import (
    LEGACY, UncitedValueError, check_citations, enforce_citations,
)
from engine.core.events import mint_extraction_uid
from engine.core.extraction_events import legacy_record
from engine.core.extraction_telemetry import record_call
from engine.core.effective_config import EffectiveConfig, stage_config
from engine.core.parsed_text import ParsedTextError, ParsedTextRef, read_parsed_text
from engine.core.selection import SelectionResult, select_for_extraction
from engine.core import run_manifest as rm
from engine.utils.ollama_client import InputFitError, ollama_chat
from engine.utils.ollama_lock import foreign_lock_held, hold_experiment_lock

logger = logging.getLogger(__name__)

MAX_RETRIES = 2
RETRY_DELAY = 30  # seconds between retries
SNIPPET_MAX_RETRIES = 2
RESTART_EVERY_N = 25  # proactive Ollama restart interval (0 = disabled)


# ── Codebook Loader ──────────────────────────────────────────────────


# The codebook is located, validated, identity-checked and hashed by
# engine.core.codebook. What stood here was a `data/*/extraction_codebook.yaml`
# glob returning the first hit with no identity check — on a two-review box it
# could hand one review's codebook to another review's extraction, and nothing
# raised (CODEBOOK-AUTH-01).


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
        codebook_path: Optional override. Defaults to the review's own
            codebook; an override must declare the same review.
    """
    # Load codebook. The review is the spec's, so an override naming a
    # different review is refused rather than silently prompting for it.
    cb = (load_codebook(codebook_path) if codebook_path
          else load_codebook_for(spec.review_id))

    tier_label = {
        1: "Tier 1 — Explicit (expected κ > 0.90)",
        2: "Tier 2 — Interpretive (expected κ 0.70–0.85)",
        3: "Tier 3 — Numeric/Tables (variable κ)",
        4: "Tier 4 — Judgment (expected κ 0.50–0.70)",
    }

    field_blocks: list[str] = []
    for tier in (1, 2, 3, 4):
        fields = cb.fields_by_tier(tier)
        if not fields:
            continue
        lines = [f"\n### {tier_label[tier]}"]
        for cb_entry in fields:
            lines.append(_build_field_block(cb_entry))
        field_blocks.append("\n".join(lines))

    schema_text = "\n".join(field_blocks)
    total_fields = len(cb.fields)

    return f"""Extract structured data from the following paper for a systematic review.

## Extraction Schema
{schema_text}

## Instructions
For each field above, extract the value from the paper and provide:
- **field_name**: Exactly as listed above.
- **value**: The extracted data. If the field is not found in the paper, set to "NOT_FOUND".
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


# Populated by extract_pass1_reasoning(); read by the retry driver's telemetry
# row. Module-level rather than threaded through the signature, matching
# _LAST_PASS2_TELEMETRY.
_LAST_PASS1_TELEMETRY: dict = {}


def _with_think(cfg: EffectiveConfig, think: bool | None) -> EffectiveConfig:
    """A `think=` argument these signatures have always accepted, as an override."""
    if think is None or think == cfg.think:
        return cfg
    from engine.core.effective_config import _replace
    return _replace(cfg, think=think, sources={**cfg.sources, "think": "caller"})


def pass1_messages(prompt: str) -> list[dict]:
    """Pass 1's message list — one builder for the call and the prompt hash (R60)."""
    return [
        {
            "role": "system",
            "content": (
                "You are a systematic review data extractor. Read the paper "
                "carefully and reason through each extraction field step by step. "
                "Think about what the paper says for each field before extracting."
            ),
        },
        {"role": "user", "content": prompt},
    ]


def extract_pass1_reasoning(prompt: str, think: bool | None = None, *,
                            cfg: EffectiveConfig | None = None,
                            paper_id: int | None = None) -> str:
    """Run Pass 1: let DeepSeek-R1 reason freely, return the thinking trace.

    `think` is passed explicitly and never left to the Ollama default —
    REGRESSION-01: 0.21.0 auto-enables thinking for deepseek-r1, and relying on
    a version-dependent default is what let the interface change go unnoticed.
    It comes from the resolver (stage `extract_pass1`); a `think=` argument is a
    caller override.
    """
    cfg = _with_think(cfg or stage_config("extract_pass1"), think)
    _LAST_PASS1_TELEMETRY.clear()
    response = ollama_chat(paper_id=paper_id, messages=pass1_messages(prompt),
                           **cfg.kwargs())

    content = response.message.content or ""
    thinking = getattr(response.message, "thinking", None)
    trace, branch = parse_thinking_trace(content, thinking)
    _LAST_PASS1_TELEMETRY.update(
        thinking_present=True,
        thinking_chars=len(trace),
        parse_branch=branch,
        finish_reason=getattr(response, "done_reason", None),
        prompt_eval_count=getattr(response, "prompt_eval_count", None),
    )
    return trace


class MissingThinkingChannelError(RuntimeError):
    """A think-enabled call returned no reasoning channel.

    REGRESSION-01: this used to be a silent fallback that returned the whole
    response content as the "reasoning trace". On Ollama 0.21.0 that fallback
    fired on *every* Pass 1 call — deepseek-r1 stopped emitting inline `<think>`
    tags and moved thinking to `message.thinking`, so the regex never matched.
    Pass 2 was then primed with the model's first-draft *answer* instead of its
    reasoning, and paraphrased it rather than quoting the paper: local anchored
    rate fell from 54.3% to 10.5% on identical papers.

    Substituting an answer for a reasoning trace is never safe, so absence is now
    an error rather than a fallback.
    """


def parse_thinking_trace(content: str, thinking: str | None = None) -> tuple[str, str]:
    """Return (reasoning_trace, parse_branch).

    Sources, in order:
      1. `native`      — Ollama >= 0.12 puts thinking in `message.thinking`.
      2. `legacy-tags` — older builds emitted inline `<think>…</think>` in content.

    There is deliberately no third branch. If a think-enabled call yields neither,
    that is a runtime-contract change and must surface as an error.
    """
    if thinking and thinking.strip():
        return thinking.strip(), "native"

    match = re.search(r"<think>(.*?)</think>", content or "", re.DOTALL)
    if match:
        logger.warning(
            "Thinking arrived as inline <think> tags (legacy shape) — Ollama "
            "runtime is older than 0.12 or the model template changed."
        )
        return match.group(1).strip(), "legacy-tags"

    raise MissingThinkingChannelError(
        "Think-enabled call returned no reasoning channel: message.thinking was "
        "empty and no <think> tags were present. Refusing to substitute the "
        "response content as a reasoning trace (see REGRESSION-01). "
        f"content[:200]={(content or '')[:200]!r}"
    )


# ── Pass 2: Structured Output ────────────────────────────────────────

# Populated by extract_pass2_structured() on every call; read by the completeness
# retry driver when it writes a telemetry row. Not thread-safe by design — the
# local arm runs one paper at a time.
_LAST_PASS2_TELEMETRY: dict = {}


def pass2_messages(prompt: str, reasoning_trace: str) -> list[dict]:
    """Pass 2's message list — one builder for the call and the prompt hash (R60)."""
    return [
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
    ]


def extract_pass2_structured(
    prompt: str,
    reasoning_trace: str,
    spec: ReviewSpec,
    paper_id: int,
    think: bool | None = None,
    codebook_hash: str | None = None,
    *, cfg: EffectiveConfig | None = None,
) -> ExtractionResult:
    """Run Pass 2: use reasoning trace as context, force structured JSON output.

    `codebook_hash` is the provenance stamp for the result. A caller that
    already holds the codebook passes its hash — the elicitation pipeline does,
    and it must, because its review lives under a temp data root that
    `spec.review_id` cannot find. Callers that do not hold one fall back to the
    review's own codebook.
    """
    schema_hash = (
        codebook_hash if codebook_hash is not None
        else load_codebook_for(spec.review_id).semantic_hash
    )
    cfg = _with_think(cfg or stage_config("extract_pass2", spec), think)

    response = ollama_chat(paper_id=paper_id,
                           messages=pass2_messages(prompt, reasoning_trace), **cfg.kwargs())

    raw = response.message.content or ""
    # INSTRUMENT-01: stash the pre-parse response and Ollama's own done_reason
    # (its finish_reason equivalent) for the telemetry writer. Module-level
    # rather than threaded through the signature so no caller changes.
    _LAST_PASS2_TELEMETRY.update(
        raw_content=raw,
        finish_reason=getattr(response, "done_reason", None),
        prompt_eval_count=getattr(response, "prompt_eval_count", None),
        model=cfg.model,
    )
    output = ExtractionOutput.model_validate_json(raw)

    return ExtractionResult(
        paper_id=paper_id,
        fields=output.fields,
        reasoning_trace=reasoning_trace,
        model=cfg.model,
        codebook_hash=schema_hash,
        extracted_at=datetime.now(timezone.utc),
    )


# ── Snippet Validation ──────────────────────────────────────────────


def _has_invalid_snippet(snippet: str | None) -> bool:
    """Return True if snippet is non-null and contains ellipsis bridging."""
    return bool(snippet and INVALID_SNIPPET_RE.search(snippet))


def retry_snippet_messages(field_name: str, value: str, paper_text: str) -> list[dict]:
    """The snippet retry's message list — one builder for the call and the
    prompt hash (R60)."""
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
    return [
        {"role": "system", "content": "Respond ONLY with JSON."},
        {"role": "user", "content": prompt},
    ]


def _retry_snippet(
    field_name: str,
    value: str,
    paper_text: str,
    paper_id: int,
    *, cfg: EffectiveConfig | None = None,
) -> str | None:
    """Request a clean verbatim snippet for a single field.

    Returns the new snippet string, or None if the model still produces
    an invalid snippet or fails.
    """
    cfg = cfg or stage_config("extract_retry_snippet")
    try:
        response = ollama_chat(
            paper_id=paper_id,
            messages=retry_snippet_messages(field_name, value, paper_text),
            **cfg.kwargs(),
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
    *, cfg: EffectiveConfig | None = None,
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
                span.field_name, span.value, paper_text, paper_id, cfg=cfg,
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


def _absence_tokens(codebook_path: Path) -> tuple[str, frozenset[str]]:
    """(escape token, absence sentinels) from the codebook beside the database.

    Located from the DATABASE's own directory, not from `data/<review_id>`: the
    review root is wherever this review's database is, and a run under a
    `data_root` override would otherwise read a different review's codebook —
    or none at all — while writing to the right database.

    Both values are required at load now, so neither can be absent. The old
    version returned `None` and an empty set from behind a bare `except`, which
    made a missing file, a parse error and a legitimately tokenless codebook
    indistinguishable — and an empty sentinel set is not inert downstream.
    """
    cb = load_codebook(codebook_path)
    return cb.escape_token, frozenset(s.strip().upper() for s in cb.absence_sentinels)


# ── Single-Paper Extraction ──────────────────────────────────────────


@lru_cache(maxsize=1)
def _default_run_id() -> str:
    """One run id per process, for the per-run unit-map directory."""
    return datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")


def extract_paper(
    paper_id: int,
    paper_text: str,
    spec: ReviewSpec,
    db: ReviewDatabase,
    model_digest: str | None = None,
    auditor_model_digest: str | None = None,
    unit_map_dir_name: str | None = None,
    attempt: int | None = None,
    parsed_text_ref: ParsedTextRef | None = None,
    *,
    run_id: int,
) -> ExtractionResult:
    """Run the full two-pass extraction on a single paper and store results.

    `run_id` is the run manifest's id (R116), required and never read from the
    active-run contextvar. `unit_map_dir_name` is the elicited path's per-run
    unit-map directory, a different thing that used to share the name.

    `parsed_text_ref` is the text identity selection resolved (9b-2a). It is
    accepted now so the call shape does not change when the event writer that
    stamps it on every claim lands (slice 2(c)); nothing reads it yet.

    Dispatches to the elicited pipeline when the ReviewSpec asks for it
    (`extraction_models.elicitation`). The flag defaults OFF so that upgrading
    the engine changes no existing review's behaviour; Run 7 turns it on
    deliberately, after the ELICIT-DESIGN-01 smoke.
    """
    if getattr(getattr(spec, "extraction_models", None), "elicitation", False):
        from engine.elicitation.pipeline import extract_paper_elicited

        return extract_paper_elicited(
            paper_id, paper_text, spec, db,
            unit_map_dir_name=unit_map_dir_name or _default_run_id(),
            model_digest=model_digest,
            auditor_model_digest=auditor_model_digest,
            attempt=attempt,
            run_id=run_id,
            parsed_text_ref=parsed_text_ref,
        )

    # The review root is where this review's database is, so a run under a
    # data_root override reads its own codebook (MIGRATE R1). Loaded once here
    # and used for the prompt's provenance stamp, the absence tokens and the
    # stored hashes — one read, one answer.
    cb_path = Path(db.db_path).parent / CODEBOOK_FILENAME
    cb = load_codebook(cb_path)

    prompt = build_extraction_prompt(paper_text, spec, cb_path)

    # Model, options and the per-pass think policy (`extraction_models.pass1_think`
    # / `.pass2_think`, REGRESSION-01) come from the one resolver and are passed
    # explicitly on every call (S3a).
    cfg1 = stage_config("extract_pass1", spec)
    cfg2 = stage_config("extract_pass2", spec)
    cfg_retry = stage_config("extract_retry_snippet", spec)

    # Pass 1: reasoning
    reasoning_trace = extract_pass1_reasoning(prompt, cfg=cfg1, paper_id=paper_id)

    # Pass 2: structured output
    result = extract_pass2_structured(prompt, reasoning_trace, spec, paper_id,
                                      codebook_hash=cb.semantic_hash, cfg=cfg2)

    # Validate snippets and retry invalid ones before storing
    validated_fields = _validate_and_retry_snippets(
        result.fields, paper_text, paper_id, cfg=cfg_retry,
    )
    result = ExtractionResult(
        paper_id=result.paper_id,
        fields=validated_fields,
        reasoning_trace=result.reasoning_trace,
        model=result.model,
        codebook_hash=result.codebook_hash,
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

    # INSTRUMENT-01: completeness, not merely non-emptiness. The local arm's two
    # collapsed Run 6 extractions (papers 415, 719) each stored a single span
    # with a non-codebook field name and passed the check above.
    # The guard sits here rather than in ReviewDatabase.add_extraction_atomic
    # because the database layer is generic — it serves migrations and tests and
    # has no ReviewSpec to derive an expected field set from.
    expected = expected_field_names(spec, cb_path)
    escape, sentinels = _absence_tokens(cb_path)
    try:
        # R118: a missing or duplicated field refuses (retryable); an unexpected
        # one is logged here and dropped below, before anything else sees it.
        enforce_completeness(span_dicts, expected, paper_id=paper_id, arm=cfg1.model)
        span_dicts = drop_unexpected(span_dicts, expected)

        # ELICIT-DESIGN-01 (section 4.6(c)): no value is stored with nothing behind
        # it. LEGACY mode, because this prompt explicitly tells the model to emit
        # an empty source_snippet for an absence value — failing those would
        # punish the model for obeying the prompt it was given. Every other value
        # still needs a quote. The elicitation path runs the same predicate in
        # STRICT mode, where a sentinel is a value like any other and owes a
        # citation.
        enforce_citations(
            span_dicts, paper_id=paper_id, arm=cfg1.model, mode=LEGACY,
            escape_token=escape, absence_sentinels=sentinels,
        )
    except (IncompleteExtractionError, UncitedValueError) as exc:
        # 9b-2c R3: the refusal carries this attempt's record, so an exhausted
        # budget stores what the arm produced (R139/R140), never nothing.
        kept = drop_unexpected(span_dicts, expected)
        exc.record = legacy_record(
            paper_id=paper_id, arm=spec.extraction_models.arm, run_id=run_id,
            extraction_uid=mint_extraction_uid(), parsed_text=parsed_text_ref,
            model=cfg1.model, model_digest=model_digest, expected=expected, spans=kept,
            offenders=check_citations(kept, escape_token=escape, absence_sentinels=sentinels,
                                      mode=LEGACY).offenders,
            duplicated=check_completeness(span_dicts, expected).duplicated,
            attempts=attempt or 1)
        raise
    extracted_data = drop_unexpected(extracted_data, expected)
    result = result.model_copy(update={"fields": drop_unexpected(result.fields, expected)})

    # The codebook the prompt was built from travels with the extraction.
    # Recording only the spec's hash meant a codebook edit moved nothing in
    # provenance and staleness detection could not see it (CODEBOOK-AUTH-01).
    ext_id = db.add_extraction_atomic(
        paper_id=paper_id,
        # extraction_schema_hash is no longer written: it hashed a spec section
        # that no longer exists. The column stays as the historical record of
        # the runs made while it was the authority (SCHEMA-DERIVE-01).
        schema_hash=None,
        extracted_data=extracted_data,
        reasoning_trace=reasoning_trace,
        model=cfg1.model,
        spans=span_dicts,
        model_digest=model_digest,
        auditor_model_digest=auditor_model_digest,
        codebook_hash=cb.semantic_hash,
        codebook_sha256=cb.sha256,
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

    # OPS-GUARD-01 (C): never restart Ollama out from under someone else's
    # experiment. `foreign_lock_held()` is deliberately narrower than
    # `check_experiment_lock()`: this process holding the lock (run_extraction
    # wraps its loop in it) must still be able to restart, because the periodic
    # restart is how a long extraction clears CUDA context fragmentation.
    # Skipping is always safe — the restart is a mitigation, not a correctness
    # requirement, and extraction continues either way.
    if foreign_lock_held():
        logger.warning(
            "[%s] RESTART SKIPPED — experiment lock held (%s, %d papers done). "
            "Another process is running an Ollama experiment; continuing without "
            "restarting.",
            ts, reason, papers_done,
        )
        return

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


def extract_paper_with_completeness(
    paper_id: int,
    paper_text: str,
    spec: ReviewSpec,
    db: ReviewDatabase,
    model_digest: str | None = None,
    auditor_model_digest: str | None = None,
    max_attempts: int = MAX_COMPLETENESS_ATTEMPTS,
    parsed_text_ref: ParsedTextRef | None = None,
    *,
    run_id: int,
) -> ExtractionResult:
    """Extract one paper, re-running the identical two-pass request until complete.

    Mirrors CloudExtractorBase.extract_with_completeness so the retry policy is
    the same on every arm. The prompt is rebuilt identically on each attempt —
    same paper text, same spec, same codebook — because the failure being
    guarded against is response shape, not prompt content.

    extract_paper() raises IncompleteExtractionError *before* its write, so an
    exhausted paper leaves nothing behind.
    """
    review_dir = Path(db.db_path).parent
    expected = expected_field_names(spec, review_dir / "extraction_codebook.yaml")
    unit_map_dir_name = _default_run_id()
    # The telemetry `arm` label has always been the extraction model's name.
    model_name = stage_config("extract_pass1", spec).model
    last_error: Exception | None = None

    # One bounded retry budget covers every pre-write refusal: an incomplete
    # Pass 2, a missing or illegal terminal state, and an uncited value at the
    # write boundary. They are the same kind of event — the request was answered
    # in a way that cannot be stored — and giving them separate budgets would let
    # a paper alternate between them indefinitely.
    #
    # `Pass1ContractError` was the fourth member and is gone: ELICIT-DESIGN-02
    # Ruling 1 retired the paper-level contract refusal it expressed. A field
    # that fails its contract now takes the CONTRACT_UNMET terminal state instead
    # of dropping the paper, and `TerminalStateError` — a subclass of
    # `IncompleteExtractionError`, so already covered here — is what fires if a
    # field reaches the boundary with no state at all. The elicited path's own
    # bounded Pass-1 retry (Ruling 4, two attempts with typed feedback) sits
    # inside `extract_paper_elicited` and is a different loop from this one.
    # 9b-2c R4: a Pass-2 response that does not parse joins the same budget;
    # exhausted, it is `response_unparseable` (F9), not a stored paper.
    RETRYABLE = (IncompleteExtractionError, UncitedValueError, ValidationError)

    for attempt in range(1, max_attempts + 1):
        _LAST_PASS2_TELEMETRY.clear()
        _LAST_PASS1_TELEMETRY.clear()
        try:
            result = extract_paper(
                paper_id, paper_text, spec, db,
                model_digest=model_digest,
                auditor_model_digest=auditor_model_digest,
                unit_map_dir_name=unit_map_dir_name, attempt=attempt,
                parsed_text_ref=parsed_text_ref, run_id=run_id,
            )
        except RETRYABLE as exc:
            last_error = exc
            incomplete = isinstance(exc, IncompleteExtractionError)
            kind = ("incomplete" if incomplete else
                    "unparseable" if isinstance(exc, ValidationError) else "contract")
            record_call(
                review_dir, arm=model_name, paper_id=paper_id, attempt=attempt,
                outcome=f"{kind}_retry" if attempt < max_attempts else f"{kind}_exhausted",
                model=_LAST_PASS2_TELEMETRY.get("model", model_name),
                finish_reason=_LAST_PASS2_TELEMETRY.get("finish_reason"),
                raw_content=_LAST_PASS2_TELEMETRY.get("raw_content"),
                spans_parsed=exc.n_stored if incomplete else None,
                fields_expected=exc.n_expected if incomplete else len(expected),
                missing_fields=exc.missing if incomplete else None,
                thinking_present=_LAST_PASS1_TELEMETRY.get("thinking_present"),
                thinking_chars=_LAST_PASS1_TELEMETRY.get("thinking_chars"),
                parse_branch=_LAST_PASS1_TELEMETRY.get("parse_branch"),
                pass1_done_reason=_LAST_PASS1_TELEMETRY.get("finish_reason"),
                pass1_prompt_eval_count=_LAST_PASS1_TELEMETRY.get("prompt_eval_count"),
                pass2_prompt_eval_count=_LAST_PASS2_TELEMETRY.get("prompt_eval_count"),
                error=None if incomplete else str(exc),
                extra=_LAST_PASS1_TELEMETRY.get("elicitation"),
            )
            logger.warning(
                "Paper %d (%s): %s attempt %d/%d — %s. Re-issuing identical request.",
                paper_id, model_name, kind.upper(), attempt, max_attempts, exc,
            )
            continue

        check = check_completeness(result.fields, expected)
        record_call(
            review_dir, arm=model_name, paper_id=paper_id, attempt=attempt, outcome="stored",
            model=_LAST_PASS2_TELEMETRY.get("model", model_name),
            finish_reason=_LAST_PASS2_TELEMETRY.get("finish_reason"),
            raw_content=_LAST_PASS2_TELEMETRY.get("raw_content"),
            spans_parsed=check.n_produced,
            fields_expected=check.n_expected,
            missing_fields=check.missing,
            thinking_present=_LAST_PASS1_TELEMETRY.get("thinking_present"),
            thinking_chars=_LAST_PASS1_TELEMETRY.get("thinking_chars"),
            parse_branch=_LAST_PASS1_TELEMETRY.get("parse_branch"),
            pass1_done_reason=_LAST_PASS1_TELEMETRY.get("finish_reason"),
            pass1_prompt_eval_count=_LAST_PASS1_TELEMETRY.get("prompt_eval_count"),
            pass2_prompt_eval_count=_LAST_PASS2_TELEMETRY.get("prompt_eval_count"),
            extra=_LAST_PASS1_TELEMETRY.get("elicitation"),
        )
        if attempt > 1:
            logger.info(
                "Paper %d (%s): complete on attempt %d/%d",
                paper_id, model_name, attempt, max_attempts,
            )
        return result

    logger.error(
        "Paper %d (%s): UNSTORABLE after %d attempts — failing the paper, "
        "NOT storing a partial extraction. %s",
        paper_id, model_name, max_attempts, last_error,
    )
    raise last_error


# ── Batch Extraction Pipeline ─────────────────────────────────────────


def run_extraction(
    db: ReviewDatabase,
    spec: ReviewSpec,
    review_name: str,
    restart_every: int = RESTART_EVERY_N,
    experiment_lock: bool = True,
    selection: SelectionResult | None = None,
    *,
    run_id: int,
) -> dict:
    """Run extraction on all eligible papers, holding the experiment lock.

    OPS-GUARD-01 (part 5): the whole run is wrapped in
    `hold_experiment_lock()` so the health cron (and any other lock-aware
    tooling) stands down for its duration, and so `restart_ollama()` can tell
    "the lock is mine" from "the lock is someone else's". The acquire is
    blocking: waiting for another experiment to finish is safe; running
    unguarded is what OPS-OLLAMA-02 was about.

    Pass experiment_lock=False to opt out (tests, or a deliberate concurrent
    run). Everything else is unchanged.

    `selection` is a `select_for_extraction` result the caller already holds
    (`run_pipeline`'s extract stage), so the corpus is selected once per run;
    without it the run selects for itself.

    `run_id` is the open run manifest's id (R116): required, never taken from
    the active-run contextvar. Every call the run makes is recorded against it.
    """
    if not experiment_lock:
        return _run_extraction_unlocked(db, spec, review_name, restart_every,
                                        run_id=run_id, selection=selection)
    with hold_experiment_lock():
        return _run_extraction_unlocked(db, spec, review_name, restart_every,
                                        run_id=run_id, selection=selection)


def extraction_stages(spec: ReviewSpec) -> tuple[str, ...]:
    """The local extraction stages a run declares for this spec, pass 1 first —
    the same renaming `run_pipeline._open_run_manifest` applies."""
    first = ("elicitation_pass1" if spec.extraction_models.elicitation
             else "extract_pass1")
    return (first, "extract_pass2", "extract_retry_snippet")


def verify_extraction_run(conn, spec: ReviewSpec, run_id: int) -> str:
    """R117: the run's one extraction digest, checked against the arm's pin.
    Raises `StageNotInRun` / `ArmPinMismatch` before anything is selected."""
    return rm.extraction_digest(conn, run_id, arm=spec.extraction_models.arm,
                                stages=extraction_stages(spec))


def _run_extraction_unlocked(
    db: ReviewDatabase,
    spec: ReviewSpec,
    review_name: str,
    restart_every: int = RESTART_EVERY_N,
    *,
    run_id: int,
    selection: SelectionResult | None = None,
) -> dict:
    """Extraction loop proper. Callers should prefer run_extraction().

    First, before anything is selected, the run's digest is checked against the
    arm's pin (R117); a refusal writes nothing and makes no call. That digest is
    the one this run records — there is no second fetch (F12). The auditor
    digest is the run's `audit` stage digest if it declared one, else NULL.

    Takes the papers `select_for_extraction` chose for the spec's arm: the
    corpus by eligibility axis, less papers this arm already holds a live claim
    on under the current parsed text (reuse key), less papers whose text is
    refused (R122). `papers.status` is not read (D9, R119).
    """
    extractor_digest = verify_extraction_run(db._conn, spec, run_id)
    auditor_digest = rm.stage_digest(db._conn, run_id, "audit")
    if selection is None:
        selection = select_for_extraction(db._conn, arm=spec.extraction_models.arm)
    with rm.active_run(db._conn, run_id):
        return _extract_selected(db, spec, selection, restart_every, run_id=run_id,
                                 extractor_digest=extractor_digest,
                                 auditor_digest=auditor_digest)


def _extract_selected(db: ReviewDatabase, spec: ReviewSpec, selection: SelectionResult,
                      restart_every: int, *, run_id: int, extractor_digest: str,
                      auditor_digest: str | None) -> dict:
    """The per-paper loop over a selection, inside the run's active_run."""
    papers = selection.to_extract
    total = len(papers)
    # The codebook beside THIS database — a run under a data_root
    # override must compare against its own review (MIGRATE R1).
    schema_hash = load_codebook_beside(db.db_path).semantic_hash
    logger.info("Starting extraction on %d papers (schema hash: %s)", total, schema_hash[:12])

    # Pre-flight: verify extraction model is loaded and responsive
    from engine.utils.ollama_preflight import require_preflight
    extractor_model = stage_config("extract_pass1", spec).model
    require_preflight([extractor_model], runner_name="Extraction", spec=spec)

    # The digests are the run manifest's (R117): resolved once at run open,
    # checked against the arm's pin above. No second fetch, so the value stored
    # can never disagree with the one the manifest recorded (F12).
    logger.info("Model digests (run %d) — extractor (%s): %s, auditor: %s",
                run_id, extractor_model, extractor_digest, auditor_digest)

    # Pre-flight: an informational count of extractions that do not carry the
    # current codebook hash. It used to tell the operator to run the cleanup
    # utility, whose delete branch is retired (R94, row D10); nothing is ever
    # deleted to make room for a re-extraction.
    from engine.utils.extraction_cleanup import check_stale_extractions
    stale_count = check_stale_extractions(db, schema_hash)
    if stale_count > 0:
        logger.info(
            "Informational: %d papers hold extractions without the current "
            "codebook hash. Whether a paper is re-extracted is decided at "
            "selection by the reuse key from session 9; earlier extractions "
            "are superseded by event, never removed.",
            stale_count,
        )

    from engine.utils.progress import ProgressReporter

    stats = {"extracted": 0, "failed": 0, "total_spans": 0,
             "skipped_asserted": len(selection.skipped_asserted),
             "skipped_refused": len(selection.skipped_refused)}
    review_dir = Path(db.db_path).parent
    progress = ProgressReporter(total, "Local extraction")
    papers_since_restart = 0  # counter for proactive restart

    for i, (pid, ref) in enumerate(papers, 1):
        row = db._conn.execute("SELECT title FROM papers WHERE id = ?", (pid,)).fetchone()
        title = (row[0] if row else None) or ""

        # The text selection resolved, re-verified on this read (R95): a file
        # changed since selection is refused here rather than consumed.
        try:
            paper_text = read_parsed_text(ref)
        except ParsedTextError as exc:
            logger.error("Paper %d: parsed text refused — %s (%s)",
                         pid, exc.reason_code, exc)
            stats["skipped_refused"] += 1
            progress.report(pid, "FAILED", 0)
            continue
        t_paper = time.time()

        try:
            result = extract_paper_with_completeness(
                pid, paper_text, spec, db,
                model_digest=extractor_digest,
                auditor_model_digest=auditor_digest,
                parsed_text_ref=ref,
                run_id=run_id,
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
            # INPUT-FIT-01: an input-fit refusal carries the numbers that explain
            # it (model, ceiling, characters, count); they go in this same entry.
            fit_fields = exc.fields if isinstance(exc, InputFitError) else None
            logger.exception(
                "Paper %d extraction failed: %s%s", pid, exc,
                f" | input_fit={fit_fields}" if fit_fields else "",
            )
            db.update_status(pid, "EXTRACT_FAILED")
            stats["failed"] += 1
            elapsed = time.time() - t_paper
            progress.report(pid, "FAILED", elapsed)

        # Proactive Ollama restart to clear CUDA context fragmentation
        papers_since_restart += 1
        if restart_every > 0 and papers_since_restart >= restart_every:
            try:
                restart_ollama(
                    reason=f"proactive after {papers_since_restart} papers",
                    papers_done=stats["extracted"] + stats["failed"],
                )
                papers_since_restart = 0
            except RuntimeError:
                logger.exception(
                    "Proactive Ollama restart failed — continuing without restart. "
                    "Monitor for instability."
                )

    progress.summary()
    logger.info(
        "Extraction complete: %d extracted, %d skipped (asserted), %d skipped "
        "(refused), %d failed, %d total spans",
        stats["extracted"],
        stats["skipped_asserted"],
        stats["skipped_refused"],
        stats["failed"],
        stats["total_spans"],
    )
    return stats
