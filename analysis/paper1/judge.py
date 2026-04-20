"""LLM-as-judge orchestrator (Pass 1) for Paper 1 concordance pairs.

No DB writes. No CLI. Pure orchestration: prompt build → Ollama
chat → Pydantic validation → JudgeResult wrapper.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from itertools import combinations
from typing import Optional

from pydantic import ValidationError

from analysis.paper1.judge_prompts import (
    build_pass1_prompt,
    compute_seed,
    randomize_arm_assignment,
)
from analysis.paper1.judge_schema import (
    JudgeInput,
    JudgeResult,
    PairwiseRating,
    Pass1Output,
)
from engine.utils.ollama_client import get_model_digest, ollama_chat

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "gemma3:27b"
DEFAULT_TEMPERATURE = 0.0
DEFAULT_NUM_CTX = 8192
DEFAULT_FORMAT = "json"


class JudgeError(Exception):
    """Base class for Pass 1 judge failures."""


class JudgeCallError(JudgeError):
    """The Ollama call itself failed (timeout, transport, etc.)."""


class JudgeParseError(JudgeError):
    """The model response did not validate against the Pass1 schema."""

    def __init__(self, msg: str, raw_response: Optional[str] = None):
        super().__init__(msg)
        self.raw_response = raw_response


class JudgeInvariantError(JudgeError):
    """The JudgeInput violates a structural precondition."""


def _validate_invariants(input: JudgeInput) -> None:
    if len(input.arms) < 2:
        raise JudgeInvariantError(
            f"run_pass1 requires >= 2 arms; got {len(input.arms)}"
        )
    names = [a.arm_name for a in input.arms]
    if len(set(names)) != len(names):
        raise JudgeInvariantError(
            f"Duplicate arm_name in arms: {names}"
        )


def _hash_prompt(prompt: str) -> str:
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()


def _extract_response_text(response) -> str:
    """Pull the raw text from an ollama_chat response across shapes."""
    # ollama-python returns a ChatResponse-like object with .message.content
    message = getattr(response, "message", None)
    if message is not None:
        content = getattr(message, "content", None)
        if content is not None:
            return content
        if isinstance(message, dict):
            return message.get("content", "") or ""
    if isinstance(response, dict):
        msg = response.get("message") or {}
        if isinstance(msg, dict) and msg.get("content"):
            return msg["content"]
        if response.get("response"):
            return response["response"]
    return ""


def run_pass1(
    input: JudgeInput,
    run_id: str,
    model: str = DEFAULT_MODEL,
) -> JudgeResult:
    """Run Pass 1 of the LLM-as-judge pipeline on one triple."""

    _validate_invariants(input)

    seed = compute_seed(input.paper_id, input.field_name, run_id)
    shuffled_arms, arm_permutation = randomize_arm_assignment(input.arms, seed)
    prompt = build_pass1_prompt(input, shuffled_arms)
    prompt_hash = _hash_prompt(prompt)

    try:
        response = ollama_chat(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            format=Pass1Output.model_json_schema(),
            options={
                "temperature": DEFAULT_TEMPERATURE,
                "seed": seed,
                "num_ctx": DEFAULT_NUM_CTX,
            },
            think=False,
        )
    except Exception as exc:
        raise JudgeCallError(f"Ollama call failed: {exc}") from exc

    raw_response = _extract_response_text(response)

    try:
        pass1 = Pass1Output.model_validate_json(raw_response)
    except ValidationError as exc:
        raise JudgeParseError(
            f"Pass1Output validation failed: {exc}",
            raw_response=raw_response,
        ) from exc
    except ValueError as exc:
        raise JudgeParseError(
            f"Pass1Output parse failed: {exc}",
            raw_response=raw_response,
        ) from exc

    digest = get_model_digest(model) or model
    timestamp_iso = datetime.now(timezone.utc).isoformat()

    return JudgeResult(
        paper_id=input.paper_id,
        field_name=input.field_name,
        arm_permutation=arm_permutation,
        pass1=pass1,
        prompt_hash=prompt_hash,
        judge_model_digest=digest,
        judge_model_name=model,
        raw_response=raw_response,
        seed=seed,
        timestamp_iso=timestamp_iso,
    )


def de_randomize_pairs(
    output: Pass1Output,
    arm_permutation: list[str],
) -> dict[tuple[str, str], PairwiseRating]:
    """Map slot-index-keyed ratings to stable (arm_a, arm_b) pairs where
    arm_a < arm_b lexicographically.
    """
    n = len(arm_permutation)
    # Defensive: ensure every expected pair shows up once, no strays.
    expected_slot_pairs = set(combinations(range(1, n + 1), 2))
    result: dict[tuple[str, str], PairwiseRating] = {}

    for rating in output.pairwise_ratings:
        slot_pair = (rating.slot_a, rating.slot_b)
        if slot_pair not in expected_slot_pairs:
            raise ValueError(
                f"Rating references invalid slot pair {slot_pair}; "
                f"expected one of {sorted(expected_slot_pairs)}"
            )
        name_a = arm_permutation[rating.slot_a - 1]
        name_b = arm_permutation[rating.slot_b - 1]
        key = tuple(sorted((name_a, name_b)))
        if key in result:
            raise ValueError(f"Duplicate arm pair after de-randomization: {key}")
        result[key] = rating

    return result


__all__ = [
    "DEFAULT_MODEL",
    "JudgeCallError",
    "JudgeError",
    "JudgeInvariantError",
    "JudgeParseError",
    "de_randomize_pairs",
    "run_pass1",
]
