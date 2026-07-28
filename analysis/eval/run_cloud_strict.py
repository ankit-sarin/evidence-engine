"""SCHEMA-EVAL-01 cloud arm runner: strict-schema condition (B) only.

Condition A for the cloud arms is the Run 6 production extraction already stored
in `cloud_extractions` — the brief spends the call budget on the novel condition
and reuses existing data for the control. This runner therefore makes exactly
`n_papers × 2` calls.

Prompt is byte-identical to production (`build_extraction_prompt`). The only
change is the response contract:

  * OpenAI — `response_format={"type": "json_schema", "strict": true, …}`
    instead of `{"type": "json_object"}`. Strict mode requires every property in
    `required` and `additionalProperties: false` throughout, which is exactly the
    20-field object this eval wants to test.
  * Anthropic — a single forced tool whose `input_schema` is the same object,
    with `tool_choice` pinned to it. That is Anthropic's structured-output
    mechanism; there is no `response_format` equivalent.

`reasoning_effort=high` is kept for OpenAI so the response contract is the only
variable on that arm. On Anthropic it could not be: the API rejects extended
thinking together with a forced tool (400, "Thinking may not be enabled when
tool_choice forces tool use"), so the Anthropic condition necessarily runs with
thinking OFF. That is a second changed variable on that arm and every Anthropic
number below must be read with it in mind.

Writes to the eval store only. Never touches review.db.

Usage:
    PYTHONPATH=. python -m analysis.eval.run_cloud_strict --review surgical_autonomy
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

from analysis.eval.schema_eval import (
    CONDITION_B,
    CallResult,
    select_sample,
    strict_extraction_schema,
    strict_response_to_spans,
    write_results,
)
from engine.agents.extractor import build_extraction_prompt
from engine.core.completeness import check_completeness, expected_field_names
from engine.core.review_spec import load_review_spec

logger = logging.getLogger(__name__)

OPENAI_MODEL = "o4-mini-2025-04-16"
ANTHROPIC_MODEL = "claude-sonnet-4-6"
TOOL_NAME = "record_extraction"

SYSTEM = (
    "You are a systematic review data extractor. "
    "Output valid JSON matching the requested schema. "
    "Be thorough and cite source text for every extracted value."
)


def run_openai(paper_id, stratum, prompt, schema, expected) -> CallResult:
    import openai

    client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    t0 = time.time()
    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            reasoning_effort="high",
            messages=[{"role": "system", "content": SYSTEM},
                      {"role": "user", "content": prompt}],
            response_format={
                "type": "json_schema",
                "json_schema": {"name": "extraction", "strict": True, "schema": schema},
            },
        )
        choice = resp.choices[0]
        raw = choice.message.content or ""
        payload = json.loads(raw) if raw else {}
        spans = strict_response_to_spans(payload, expected)
        chk = check_completeness(spans, expected)
        usage = resp.usage
        details = getattr(usage, "completion_tokens_details", None)
        return CallResult(
            arm="openai_o4_mini_high", condition=CONDITION_B, paper_id=paper_id,
            stratum=stratum, ok=True, n_spans=len(spans), fields_expected=len(expected),
            missing=list(chk.missing), complete=chk.complete,
            latency_s=round(time.time() - t0, 2),
            input_tokens=usage.prompt_tokens if usage else None,
            output_tokens=usage.completion_tokens if usage else None,
            reasoning_tokens=getattr(details, "reasoning_tokens", None) if details else None,
            finish_reason=getattr(choice, "finish_reason", None),
            parse_path="strict_json_schema", spans=spans, raw_content=raw,
        )
    except Exception as exc:
        logger.exception("openai paper %d failed", paper_id)
        return CallResult(
            arm="openai_o4_mini_high", condition=CONDITION_B, paper_id=paper_id,
            stratum=stratum, ok=False, n_spans=0, fields_expected=len(expected),
            latency_s=round(time.time() - t0, 2), error=str(exc)[:500],
        )


def run_anthropic(paper_id, stratum, prompt, schema, expected) -> CallResult:
    import anthropic

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    t0 = time.time()
    try:
        # The Anthropic API rejects extended thinking together with a forced
        # tool: "Thinking may not be enabled when tool_choice forces tool use."
        # (400, observed on all 5 first-pass calls.) Forced structured output and
        # extended thinking are mutually exclusive on this provider, so this
        # condition necessarily runs with thinking OFF — a second changed
        # variable that is declared rather than hidden.
        resp = client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=16000,
            system=SYSTEM,
            tools=[{
                "name": TOOL_NAME,
                "description": "Record the structured extraction for this paper.",
                "input_schema": schema,
            }],
            tool_choice={"type": "tool", "name": TOOL_NAME},
            messages=[{"role": "user", "content": prompt}],
        )
        payload, raw, think_chars = {}, "", 0
        for block in resp.content:
            if block.type == "tool_use" and block.name == TOOL_NAME:
                payload = block.input
                raw = json.dumps(payload)
            elif block.type == "thinking":
                think_chars += len(block.thinking or "")
            elif block.type == "text":
                raw = raw or block.text
        spans = strict_response_to_spans(payload, expected)
        chk = check_completeness(spans, expected)
        return CallResult(
            arm="anthropic_sonnet_4_6", condition=CONDITION_B, paper_id=paper_id,
            stratum=stratum, ok=True, n_spans=len(spans), fields_expected=len(expected),
            missing=list(chk.missing), complete=chk.complete,
            latency_s=round(time.time() - t0, 2),
            input_tokens=resp.usage.input_tokens, output_tokens=resp.usage.output_tokens,
            finish_reason=getattr(resp, "stop_reason", None),
            think_chars=think_chars, parse_path="forced_tool_schema",
            spans=spans, raw_content=raw,
        )
    except Exception as exc:
        logger.exception("anthropic paper %d failed", paper_id)
        return CallResult(
            arm="anthropic_sonnet_4_6", condition=CONDITION_B, paper_id=paper_id,
            stratum=stratum, ok=False, n_spans=0, fields_expected=len(expected),
            latency_s=round(time.time() - t0, 2), error=str(exc)[:500],
        )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Cloud strict-schema condition")
    p.add_argument("--review", required=True)
    p.add_argument("--data-root", default="data")
    p.add_argument("--spec", default="review_specs/surgical_autonomy_v1.yaml")
    p.add_argument("--n-papers", type=int, default=5)
    p.add_argument("--arms", default="openai,anthropic")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    review_dir = Path(args.data_root) / args.review
    spec = load_review_spec(args.spec)
    expected = expected_field_names(spec, review_dir / "extraction_codebook.yaml")
    schema = strict_extraction_schema(expected)

    _, cloud_sample = select_sample(review_dir, n_cloud=args.n_papers)
    arms = [a.strip() for a in args.arms.split(",") if a.strip()]
    logger.info("Cloud sample: %s | arms=%s | %d required properties",
                [s.paper_id for s in cloud_sample], arms, len(schema["required"]))

    results: list[CallResult] = []
    for sp in cloud_sample:
        files = sorted((review_dir / "parsed_text").glob(f"{sp.paper_id}_v*.md"),
                       key=lambda f: int(f.stem.rsplit("_v", 1)[1]))
        prompt = build_extraction_prompt(files[-1].read_text(), spec)
        for arm in arms:
            fn = run_openai if arm == "openai" else run_anthropic
            logger.info("paper %d (%s) — %s strict", sp.paper_id, sp.stratum, arm)
            r = fn(sp.paper_id, sp.stratum, prompt, schema, expected)
            logger.info("    ok=%s spans=%d complete=%s %.1fs %s",
                        r.ok, r.n_spans, r.complete, r.latency_s, r.error or "")
            results.append(r)

    write_results(review_dir, results, "cloud_strict")
    return 0


if __name__ == "__main__":
    sys.exit(main())
