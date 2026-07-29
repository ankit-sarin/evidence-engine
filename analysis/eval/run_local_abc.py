"""SCHEMA-EVAL-02 runner: 40 papers x 3 response contracts, local arm.

Byte-identical prompts across conditions; `think` taken from the Review Spec
policy (pass1 True, pass2 False) so Pass 1 consumes the native thinking channel.

Budget discipline: the hard cap is 40 x 3 = 120 extractions, so this runner does
**not** retry on a completeness failure — a retry would be a 121st call. Instead it
records whether the guard passed and therefore whether production *would* have
retried (`would_retry`). Measure 2 is reported on that basis and the distinction
is stated in the report.

Results are appended one line at a time: a ~15 hour batch must not lose finished
work to a late crash.

Writes to the eval store only; never to review.db's extraction tables, and the
production `extract_paper()` is deliberately not used because it stores.

Usage:
    PYTHONPATH=. python -m analysis.eval.run_local_abc --review surgical_autonomy
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path

from analysis.eval.schema_eval2 import (
    COND_A,
    COND_B,
    COND_C,
    CONDITIONS,
    Result,
    append_result,
    required_slot_schema,
    select_sample,
    slots_to_spans,
)
from engine.agents.extractor import (
    MODEL,
    _LAST_PASS1_TELEMETRY,
    build_extraction_prompt,
    extract_pass1_reasoning,
)
from engine.agents.models import ExtractionOutput
from engine.core.completeness import check_completeness, expected_field_names
from engine.core.review_spec import load_review_spec
from engine.utils.ollama_client import ollama_chat
from engine.utils.ollama_lock import hold_experiment_lock

logger = logging.getLogger(__name__)


def pass2_messages(prompt: str, trace: str) -> list[dict]:
    """Byte-identical to extract_pass2_structured's message list."""
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
                f"{trace}\n\n"
                f"Now output the structured extraction as JSON matching the schema. "
                f"Include all fields from the extraction schema."
            ),
        },
    ]


def parse_unconstrained(raw: str) -> tuple[list[dict], str]:
    """Condition A parse, using the same salvage ladder the cloud path uses.

    Condition A must not be penalised for shape alone, so every rescue branch the
    engine would apply is applied here; `parse_path` names which one fired, which
    is the shape-tax measurement.
    """
    text = (raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```\s*$", "", text)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return [], "unparseable"

    if isinstance(payload, list):
        return [s for s in payload if isinstance(s, dict) and "field_name" in s], "bare_list"
    if not isinstance(payload, dict):
        return [], "unrecognized"
    for key in ("fields", "extractions", "extracted_fields", "data", "results"):
        if isinstance(payload.get(key), list):
            return payload[key], f"wrapped:{key}"
    if "field_name" in payload:
        return [payload], "single_span_dict"
    if payload and all(isinstance(v, dict) for v in payload.values()):
        return [{"field_name": k, **v} for k, v in payload.items()], "flat_field_dict"
    if payload and all(isinstance(v, str) for v in payload.values()):
        return ([{"field_name": k, "value": v, "source_snippet": "",
                  "confidence": 0.0, "tier": 1} for k, v in payload.items()],
                "flat_value_dict")
    return [], "unrecognized"


def run_one(paper, condition: str, paper_text: str, spec, expected, slot_schema) -> Result:
    prompt = build_extraction_prompt(paper_text, spec)
    models = spec.extraction_models
    t0 = time.time()
    base = dict(
        condition=condition, paper_id=paper.paper_id,
        length_stratum=paper.length_stratum, study_type=paper.study_type,
        fields_expected=len(expected),
    )
    try:
        trace = extract_pass1_reasoning(prompt, think=models.pass1_think)
        t1 = time.time()
        branch = _LAST_PASS1_TELEMETRY.get("parse_branch")
        thinking_chars = _LAST_PASS1_TELEMETRY.get("thinking_chars")

        kwargs = {"options": {"temperature": 0}, "think": models.pass2_think}
        if condition == COND_B:
            kwargs["format"] = ExtractionOutput.model_json_schema()
        elif condition == COND_C:
            kwargs["format"] = slot_schema

        resp = ollama_chat(model=MODEL, messages=pass2_messages(prompt, trace), **kwargs)
        t2 = time.time()
        raw = resp.message.content or ""

        if condition == COND_A:
            spans, parse_path = parse_unconstrained(raw)
        elif condition == COND_B:
            try:
                spans = [s.model_dump() for s in
                         ExtractionOutput.model_validate_json(raw).fields]
                parse_path = "schema_valid"
            except Exception:
                spans, sub = parse_unconstrained(raw)
                parse_path = f"schema_invalid:{sub}"
        else:
            try:
                spans = slots_to_spans(json.loads(raw), expected)
                parse_path = "slots_valid"
            except Exception:
                spans, sub = parse_unconstrained(raw)
                parse_path = f"slots_invalid:{sub}"

        chk = check_completeness(spans, expected)
        return Result(
            ok=True, n_spans=len(spans), complete=chk.complete,
            missing=list(chk.missing), parse_path=parse_path, parse_branch=branch,
            thinking_chars=thinking_chars,
            pass1_latency_s=round(t1 - t0, 1), pass2_latency_s=round(t2 - t1, 1),
            total_latency_s=round(t2 - t0, 1),
            prompt_eval_count=getattr(resp, "prompt_eval_count", None),
            eval_count=getattr(resp, "eval_count", None),
            done_reason=getattr(resp, "done_reason", None),
            retries=0 if chk.complete else 1,  # would-retry count, not a real retry
            spans=spans, raw_content=raw, **base,
        )
    except Exception as exc:
        logger.exception("paper %d (%s) failed", paper.paper_id, condition)
        return Result(
            ok=False, n_spans=0, complete=False, missing=[], parse_path=None,
            parse_branch=_LAST_PASS1_TELEMETRY.get("parse_branch"),
            thinking_chars=_LAST_PASS1_TELEMETRY.get("thinking_chars"),
            pass1_latency_s=0.0, pass2_latency_s=0.0,
            total_latency_s=round(time.time() - t0, 1),
            prompt_eval_count=None, eval_count=None, done_reason=None, retries=0,
            spans=[], raw_content=None, error=str(exc)[:400], **base,
        )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="SCHEMA-EVAL-02: A/B/C local contract eval")
    p.add_argument("--review", required=True)
    p.add_argument("--data-root", default="data")
    p.add_argument("--spec", default="review_specs/surgical_autonomy_v1.yaml")
    p.add_argument("--label", default="local_abc")
    p.add_argument("--resume", action="store_true",
                   help="skip (condition, paper) pairs already present in the output")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    review_dir = Path(args.data_root) / args.review
    spec = load_review_spec(args.spec)
    expected = expected_field_names(spec, review_dir / "extraction_codebook.yaml")
    slot_schema = required_slot_schema(expected)
    sample = select_sample(review_dir)

    done: set[tuple[str, int]] = set()
    if args.resume:
        from analysis.eval.schema_eval2 import read_results
        done = {(r["condition"], r["paper_id"]) for r in read_results(review_dir)}
        logger.info("resume: %d results already present", len(done))

    total = len(sample) * len(CONDITIONS)
    logger.info(
        "SCHEMA-EVAL-02: %d papers x %d conditions = %d extractions | fields=%d | "
        "think pass1=%s pass2=%s",
        len(sample), len(CONDITIONS), total, len(expected),
        spec.extraction_models.pass1_think, spec.extraction_models.pass2_think,
    )
    logger.info("papers: %s", [pp.paper_id for pp in sample])

    n = 0
    t_start = time.time()
    with hold_experiment_lock():
        logger.info("experiment lock acquired — cron and foreign restarts stand down")
        for paper in sample:
            files = sorted((review_dir / "parsed_text").glob(f"{paper.paper_id}_v*.md"),
                           key=lambda f: int(f.stem.rsplit("_v", 1)[1]))
            if not files:
                logger.warning("paper %d: no parsed text — skipping", paper.paper_id)
                continue
            text = files[-1].read_text()
            for condition in CONDITIONS:
                n += 1
                if (condition, paper.paper_id) in done:
                    logger.info("[%d/%d] p%d %s — already done, skipping",
                                n, total, paper.paper_id, condition)
                    continue
                logger.info("[%d/%d] p%d (%s, %s) — %s", n, total, paper.paper_id,
                            paper.length_stratum, paper.study_type, condition)
                r = run_one(paper, condition, text, spec, expected, slot_schema)
                append_result(review_dir, args.label, r)
                elapsed = time.time() - t_start
                logger.info(
                    "    ok=%s spans=%d complete=%s parse=%s branch=%s %.0fs "
                    "| elapsed %.1fh eta %.1fh",
                    r.ok, r.n_spans, r.complete, r.parse_path, r.parse_branch,
                    r.total_latency_s, elapsed / 3600,
                    (elapsed / n) * (total - n) / 3600,
                )

    logger.info("done: %d extractions in %.1fh", n, (time.time() - t_start) / 3600)
    return 0


if __name__ == "__main__":
    sys.exit(main())
