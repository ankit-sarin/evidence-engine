"""OpenAI o4-mini cloud extraction arm for concordance study."""

import json
import logging
import os
import time

import openai

from engine.cloud.base import CloudExtractorBase

logger = logging.getLogger(__name__)

# o4-mini pricing defaults (March 2026) — used when spec has no cloud_models
_DEFAULT_MODEL = "o4-mini-2025-04-16"
_DEFAULT_COST_INPUT_PER_M = 1.10   # $/1M input tokens
_DEFAULT_COST_OUTPUT_PER_M = 4.40  # $/1M output tokens

# Module-level aliases for backward compatibility with tests that import these
COST_INPUT_PER_M = _DEFAULT_COST_INPUT_PER_M
COST_OUTPUT_PER_M = _DEFAULT_COST_OUTPUT_PER_M
MODEL_STRING = _DEFAULT_MODEL


class OpenAIExtractor(CloudExtractorBase):
    """Cloud extraction using OpenAI o4-mini with reasoning_effort=high."""

    ARM = "openai_o4_mini_high"

    def __init__(
        self,
        db_path: str,
        review_spec_path: str,
        api_key: str | None = None,
    ):
        super().__init__(db_path, review_spec_path)
        key = api_key or os.environ.get("OPENAI_API_KEY")
        if not key:
            raise ValueError(
                "OpenAI API key required — pass api_key or set OPENAI_API_KEY"
            )
        self.client = openai.OpenAI(api_key=key)

        # Read model/cost config from spec; fall back to defaults
        cloud_cfg = getattr(self.spec, "cloud_models", None)
        openai_cfg = getattr(cloud_cfg, "openai", None) if cloud_cfg else None
        if openai_cfg:
            self.model_string = openai_cfg.model
            self.cost_input_per_m = openai_cfg.cost_input_per_m
            self.cost_output_per_m = openai_cfg.cost_output_per_m
        else:
            self.model_string = _DEFAULT_MODEL
            self.cost_input_per_m = _DEFAULT_COST_INPUT_PER_M
            self.cost_output_per_m = _DEFAULT_COST_OUTPUT_PER_M
            logger.warning(
                "No cloud_models.openai in review spec — using defaults: "
                "model=%s, cost_in=$%.2f/M, cost_out=$%.2f/M",
                self.model_string, self.cost_input_per_m, self.cost_output_per_m,
            )

    def extract_paper(self, paper_id: int, parsed_text: str) -> dict:
        """Extract a single paper via OpenAI o4-mini."""
        prompt = self.build_prompt(parsed_text)

        response = self.client.chat.completions.create(
            model=self.model_string,
            reasoning_effort="high",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a systematic review data extractor. "
                        "Output valid JSON matching the requested schema. "
                        "Be thorough and cite source text for every extracted value."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )

        # Extract content
        content = response.choices[0].message.content or ""

        # Extract reasoning trace
        reasoning_trace = ""
        reasoning_content = getattr(
            response.choices[0].message, "reasoning_content", None
        )
        if reasoning_content:
            reasoning_trace = reasoning_content
        elif hasattr(response, "usage") and hasattr(response.usage, "completion_tokens_details"):
            details = response.usage.completion_tokens_details
            reasoning_toks = getattr(details, "reasoning_tokens", 0) if details else 0
            if reasoning_toks:
                reasoning_trace = f"Reasoning tokens used: {reasoning_toks}"

        # Parse JSON
        try:
            extracted_data = json.loads(content)
        except json.JSONDecodeError:
            logger.error("Paper %d: failed to parse OpenAI JSON response", paper_id)
            extracted_data = {"fields": [], "raw": content}

        # Token usage
        usage = response.usage
        input_tokens = usage.prompt_tokens if usage else 0
        output_tokens = usage.completion_tokens if usage else 0
        reasoning_tokens = 0
        if usage and hasattr(usage, "completion_tokens_details"):
            details = usage.completion_tokens_details
            reasoning_tokens = getattr(details, "reasoning_tokens", 0) if details else 0

        cost_usd = (
            input_tokens * self.cost_input_per_m / 1_000_000
            + output_tokens * self.cost_output_per_m / 1_000_000
        )

        # Parse into spans
        spans = self.parse_response_to_spans(extracted_data)

        return {
            "paper_id": paper_id,
            "extracted_data": extracted_data,
            "reasoning_trace": reasoning_trace,
            "prompt_text": prompt,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "reasoning_tokens": reasoning_tokens,
            "cost_usd": cost_usd,
            "spans": spans,
        }

    def run(
        self,
        max_papers: int | None = None,
        max_cost_usd: float | None = None,
    ) -> dict:
        """Run extraction on pending papers."""
        pending = self.get_pending_papers(self.ARM)
        total = len(pending)
        if max_papers:
            pending = pending[:max_papers]

        logger.info(
            "OpenAI extraction: %d pending, processing %d",
            total, len(pending),
        )

        from engine.utils.progress import ProgressReporter

        stats = {"extracted": 0, "failed": 0, "total_cost": 0.0}
        progress = ProgressReporter(len(pending), "Cloud o4-mini")

        for i, paper in enumerate(pending, 1):
            pid = paper["paper_id"]
            t_paper = time.time()

            try:
                parsed_text = self.load_parsed_text(pid)
            except FileNotFoundError as exc:
                logger.warning("Paper %d: %s — skipping", pid, exc)
                stats["failed"] += 1
                progress.report(pid, "FAILED", time.time() - t_paper)
                continue

            # Retry logic — auth errors abort immediately, rate limits use long backoff
            result = None
            for attempt in range(3):
                try:
                    result = self.extract_paper(pid, parsed_text)
                    break
                except openai.AuthenticationError as exc:
                    logger.critical(
                        "OpenAI API key is invalid or expired — aborting run: %s", exc,
                    )
                    raise
                except openai.RateLimitError as exc:
                    retry_after = None
                    if hasattr(exc, "response") and exc.response is not None:
                        retry_after = exc.response.headers.get("retry-after")
                    wait = int(retry_after) if retry_after else 30 * (2 ** attempt)
                    if attempt < 2:
                        logger.info(
                            "Paper %d: rate limited (attempt %d/3) — waiting %ds",
                            pid, attempt + 1, wait,
                        )
                        time.sleep(wait)
                    else:
                        logger.error(
                            "Paper %d failed after 3 rate-limited attempts: %s",
                            pid, exc,
                        )
                        stats["failed"] += 1
                except Exception as exc:
                    if attempt < 2:
                        wait = 2 ** (attempt + 1)
                        logger.warning(
                            "Paper %d attempt %d failed: %s — retrying in %ds",
                            pid, attempt + 1, exc, wait,
                        )
                        time.sleep(wait)
                    else:
                        logger.error("Paper %d failed after 3 attempts: %s", pid, exc)
                        stats["failed"] += 1

            if result is None:
                progress.report(pid, "FAILED", time.time() - t_paper)
                continue

            try:
                self.store_result(
                    paper_id=pid,
                    arm=self.ARM,
                    model_string=self.model_string,
                    extracted_data=result["extracted_data"],
                    reasoning_trace=result["reasoning_trace"],
                    prompt_text=result["prompt_text"],
                    input_tokens=result["input_tokens"],
                    output_tokens=result["output_tokens"],
                    reasoning_tokens=result["reasoning_tokens"],
                    cost_usd=result["cost_usd"],
                    spans=result["spans"],
                )
            except Exception as exc:
                logger.error(
                    "Paper %d: store_result failed: %s", pid, exc,
                )
                stats["failed"] += 1
                progress.report(pid, "FAILED", time.time() - t_paper)
                continue

            stats["extracted"] += 1
            stats["total_cost"] += result["cost_usd"]
            progress.report(pid, "EXTRACTED", time.time() - t_paper)

            if max_cost_usd and stats["total_cost"] > max_cost_usd:
                print(f"Cost ceiling ${max_cost_usd:.2f} exceeded — stopping")
                break

        progress.summary()

        # Post-extraction distribution check
        monitor = self.run_distribution_check(stats)
        if not monitor["skipped"]:
            stats["distribution_monitor"] = monitor

        return stats
