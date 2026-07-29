"""REGRESSION-01 smoke: 3 papers through the fixed production path.

Compares post-fix snippets against Run 6 (pre-regression, Ollama 0.17.7) and
against SCHEMA-EVAL-01's condition B (post-regression, same code path, Ollama
0.21.0, before the fix) on the same papers.

The check is direction plus character: snippets should be verbatim quotes from
the paper again rather than authored prose. Exact recovery of Run 6's rate is not
expected at n=3.

Writes to the eval store only. Holds the experiment lock. Never touches
review.db's extraction tables.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from analysis.eval.schema_eval import CallResult, store_path, write_results
from analysis.provenance import classifier as C
from engine.agents.extractor import (
    MODEL,
    _LAST_PASS1_TELEMETRY,
    build_extraction_prompt,
    extract_pass1_reasoning,
    extract_pass2_structured,
)
from engine.core.completeness import check_completeness, expected_field_names
from engine.core.review_spec import load_review_spec
from engine.utils.ollama_lock import hold_experiment_lock

logger = logging.getLogger(__name__)

SMOKE_PAPERS = (39, 466, 629)  # from the SCHEMA-EVAL-01 sample: long, collapse, ordinary


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="REGRESSION-01 post-fix smoke")
    p.add_argument("--review", required=True)
    p.add_argument("--data-root", default="data")
    p.add_argument("--spec", default="review_specs/surgical_autonomy_v1.yaml")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    review_dir = Path(args.data_root) / args.review
    spec = load_review_spec(args.spec)
    expected = expected_field_names(spec, review_dir / "extraction_codebook.yaml")
    models = spec.extraction_models
    logger.info("think policy: pass1=%s pass2=%s", models.pass1_think, models.pass2_think)

    results: list[CallResult] = []
    with hold_experiment_lock():
        for pid in SMOKE_PAPERS:
            files = sorted((review_dir / "parsed_text").glob(f"{pid}_v*.md"),
                           key=lambda f: int(f.stem.rsplit("_v", 1)[1]))
            text = files[-1].read_text()
            prompt = build_extraction_prompt(text, spec)
            t0 = time.time()
            try:
                trace = extract_pass1_reasoning(prompt, think=models.pass1_think)
                res = extract_pass2_structured(prompt, trace, spec, pid,
                                               think=models.pass2_think)
                spans = [s.model_dump() for s in res.fields]
                chk = check_completeness(spans, expected)
                paper = C.PaperIndex.build(pid, text)
                classes = [C.classify_span(s.get("source_snippet"), s.get("value"), paper)
                           .taxonomy_class for s in spans]
                anchored = sum(1 for c in classes if c == C.ANCHORED)
                logger.info(
                    "p%d: spans=%d complete=%s ANCHORED=%d/%d branch=%s think_chars=%s %.1fs",
                    pid, len(spans), chk.complete, anchored, len(spans),
                    _LAST_PASS1_TELEMETRY.get("parse_branch"),
                    _LAST_PASS1_TELEMETRY.get("thinking_chars"), time.time() - t0,
                )
                results.append(CallResult(
                    arm=MODEL, condition="POSTFIX", paper_id=pid, stratum="smoke",
                    ok=True, n_spans=len(spans), fields_expected=len(expected),
                    missing=list(chk.missing), complete=chk.complete,
                    latency_s=round(time.time() - t0, 2),
                    think_chars=_LAST_PASS1_TELEMETRY.get("thinking_chars"),
                    parse_path=_LAST_PASS1_TELEMETRY.get("parse_branch"),
                    spans=spans,
                ))
            except Exception as exc:
                logger.exception("p%d failed", pid)
                results.append(CallResult(
                    arm=MODEL, condition="POSTFIX", paper_id=pid, stratum="smoke",
                    ok=False, n_spans=0, fields_expected=len(expected),
                    latency_s=round(time.time() - t0, 2), error=str(exc)[:400]))

    write_results(review_dir, results, "regression01_smoke")
    return 0


if __name__ == "__main__":
    sys.exit(main())
