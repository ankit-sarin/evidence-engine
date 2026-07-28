"""SCHEMA-EVAL-01 local arm runner: unconstrained (A) vs constrained (B).

Both conditions use the byte-identical production prompt from
`build_extraction_prompt` and the identical two-pass flow. The only difference
is whether Pass 2 passes `format=<schema>` to Ollama.

Writes to the eval store only — never to review.db's extraction tables. The
production `extract_paper()` is deliberately NOT used, because it stores.

Holds the OPS-GUARD-01 experiment lock for the whole run, so the health cron
and any foreign restart stand down.

Usage:
    PYTHONPATH=. python -m analysis.eval.run_local_ab --review surgical_autonomy
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from analysis.eval.schema_eval import (
    CONDITION_A,
    CONDITION_B,
    CallResult,
    select_sample,
    write_results,
)
from engine.agents.extractor import (
    MODEL,
    build_extraction_prompt,
    extract_pass1_reasoning,
)
from engine.agents.models import ExtractionOutput
from engine.core.completeness import check_completeness, expected_field_names
from engine.core.review_spec import load_review_spec
from engine.utils.ollama_client import ollama_chat
from engine.utils.ollama_lock import hold_experiment_lock

logger = logging.getLogger(__name__)


def _pass2_messages(prompt: str, reasoning_trace: str) -> list[dict]:
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
                f"{reasoning_trace}\n\n"
                f"Now output the structured extraction as JSON matching the schema. "
                f"Include all fields from the extraction schema."
            ),
        },
    ]


def _parse_unconstrained(raw: str) -> tuple[list[dict], str]:
    """Parse an unconstrained response with the same salvage ladder the cloud
    path uses, so condition A is not penalised for shape alone.

    Returns (spans, parse_path) where parse_path names which branch succeeded —
    the shape-tax measurement.
    """
    import re

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
        # {"study_type": "Original Research", ...} — values only, no evidence.
        return ([{"field_name": k, "value": v, "source_snippet": "",
                  "confidence": 0.0, "tier": 1} for k, v in payload.items()],
                "flat_value_dict")
    return [], "unrecognized"


def run_paper(paper_id: int, stratum: str, paper_text: str, spec, expected,
              condition: str) -> CallResult:
    prompt = build_extraction_prompt(paper_text, spec)
    t0 = time.time()
    try:
        trace = extract_pass1_reasoning(prompt)
        kwargs = {"options": {"temperature": 0}, "think": False}
        if condition == CONDITION_B:
            kwargs["format"] = ExtractionOutput.model_json_schema()

        resp = ollama_chat(model=MODEL, messages=_pass2_messages(prompt, trace), **kwargs)
        raw = resp.message.content or ""

        if condition == CONDITION_B:
            try:
                spans = [s.model_dump() for s in
                         ExtractionOutput.model_validate_json(raw).fields]
                parse_path = "schema_valid"
            except Exception:
                spans, parse_path = _parse_unconstrained(raw)
                parse_path = f"schema_invalid:{parse_path}"
        else:
            spans, parse_path = _parse_unconstrained(raw)

        chk = check_completeness(spans, expected)
        return CallResult(
            arm=MODEL, condition=condition, paper_id=paper_id, stratum=stratum,
            ok=True, n_spans=len(spans), fields_expected=len(expected),
            missing=list(chk.missing), complete=chk.complete,
            latency_s=round(time.time() - t0, 2),
            finish_reason=getattr(resp, "done_reason", None),
            think_chars=len(trace or ""), parse_path=parse_path,
            spans=spans, raw_content=raw,
        )
    except Exception as exc:
        logger.exception("Paper %d (%s) failed", paper_id, condition)
        return CallResult(
            arm=MODEL, condition=condition, paper_id=paper_id, stratum=stratum,
            ok=False, n_spans=0, fields_expected=len(expected),
            latency_s=round(time.time() - t0, 2), error=str(exc)[:400],
        )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Local A/B: unconstrained vs constrained")
    p.add_argument("--review", required=True)
    p.add_argument("--data-root", default="data")
    p.add_argument("--spec", default="review_specs/surgical_autonomy_v1.yaml")
    p.add_argument("--n-papers", type=int, default=10)
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    review_dir = Path(args.data_root) / args.review
    spec = load_review_spec(args.spec)
    expected = expected_field_names(spec, review_dir / "extraction_codebook.yaml")

    local_sample, _ = select_sample(review_dir, n_local=args.n_papers)
    logger.info("Sample: %s", [(s.paper_id, s.stratum, s.chars) for s in local_sample])
    logger.info("Expected fields: %d", len(expected))

    results: list[CallResult] = []
    with hold_experiment_lock():
        logger.info("Experiment lock acquired — health cron will stand down")
        for i, sp in enumerate(local_sample, 1):
            files = sorted((review_dir / "parsed_text").glob(f"{sp.paper_id}_v*.md"),
                           key=lambda f: int(f.stem.rsplit("_v", 1)[1]))
            text = files[-1].read_text()
            for condition in (CONDITION_A, CONDITION_B):
                logger.info("[%d/%d] paper %d (%s) — %s",
                            i, len(local_sample), sp.paper_id, sp.stratum, condition)
                r = run_paper(sp.paper_id, sp.stratum, text, spec, expected, condition)
                logger.info("    spans=%d complete=%s parse=%s %.1fs",
                            r.n_spans, r.complete, r.parse_path, r.latency_s)
                results.append(r)

    write_results(review_dir, results, "local_ab")
    return 0


if __name__ == "__main__":
    sys.exit(main())
