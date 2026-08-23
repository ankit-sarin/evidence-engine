"""QUALGAP-01 runner: 40 papers x 2 runtime cells against a user-space Ollama 0.17.7.

Budget: 80 local extractions, hard cap. No completeness retries (a retry would be
an 81st call); the guard result is recorded as `would_retry`, exactly as
SCHEMA-EVAL-02 did, so measure 2 is comparable across the two studies.

Everything about the request is held identical to SCHEMA-EVAL-02 condition B —
same sample, same `build_extraction_prompt`, same pass-2 message list, same
array schema, same temperature — so the only moving parts are the server version
and, between cells, the pass-1 `think` argument.

Two safety rails, both deliberate:

  * **The client is rebound to the 0.17.7 port explicitly**, not left to
    `OLLAMA_HOST`. `engine.utils.ollama_client` builds its client at import time,
    so an env var set in the wrong order would silently send all 80 calls to the
    0.21.0 production server and answer the wrong question. The server version is
    then asserted against /api/version before any spend.
  * **The restart-on-timeout branch is disarmed**, via the supported opt-out
    `EVIDENCE_ENGINE_NO_OLLAMA_RESTART` (OPSFIX-01). `ollama_chat`'s last-resort
    recovery runs `sudo systemctl restart ollama`, which would restart the
    *production* service — irrelevant to this run (we are not talking to it) and
    out of bounds for this task. This originally monkeypatched the private
    `_restart_ollama_and_retry`; the env switch replaced that, and the branch is
    additionally flock-gated now, so a foreign experiment is safe regardless.

Writes to the eval store only; never to review.db.

Usage:
    PYTHONPATH=. python -m analysis.eval.run_qualgap01 --review surgical_autonomy --probe
    PYTHONPATH=. python -m analysis.eval.run_qualgap01 --review surgical_autonomy
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path

import httpx
import ollama

from analysis.eval.qualgap01 import (
    CELL_V1,
    CELL_V2,
    CELLS,
    DEFAULT_HOST,
    EXPECTED_SERVER_VERSION,
    PASS1_THINK,
    Probe,
    RuntimeResult,
    append_result,
    store_dir,
)
from analysis.eval.run_local_abc import pass2_messages, parse_unconstrained
from analysis.eval.schema_eval2 import select_sample
from engine.agents.extractor import MODEL, build_extraction_prompt, parse_thinking_trace
from engine.agents.models import ExtractionOutput
from engine.core.completeness import check_completeness, expected_field_names
from engine.core.review_spec import load_review_spec
from engine.utils import ollama_client as oc
from engine.utils.ollama_lock import hold_experiment_lock

logger = logging.getLogger(__name__)


# ── rails ────────────────────────────────────────────────────────────────


def bind_runtime(host: str) -> str:
    """Point the shared client at `host`, disarm the restart branch, return version.

    Raises RuntimeError if the server is not the expected version — a run that
    silently lands on 0.21.0 would look like a finding and be a fabrication.

    Disarming used to mean monkeypatching the private
    `oc._restart_ollama_and_retry`; OPSFIX-01 replaced that with the supported
    `EVIDENCE_ENGINE_NO_OLLAMA_RESTART` switch, and additionally flock-gated the
    branch so a foreign experiment is protected whether or not this is set.
    """
    oc._client = ollama.Client(host=host, timeout=oc._httpx_timeout)
    os.environ[oc.RESTART_OPT_OUT_ENV] = "1"

    resp = httpx.get(f"{host.rstrip('/')}/api/version", timeout=10.0)
    resp.raise_for_status()
    version = resp.json().get("version")
    if version != EXPECTED_SERVER_VERSION:
        raise RuntimeError(
            f"QUALGAP-01 refuses to run: {host} reports Ollama {version!r}, "
            f"expected {EXPECTED_SERVER_VERSION!r}."
        )
    logger.info("bound to Ollama %s at %s (restart recovery disarmed)", version, host)
    return version


# ── pass 1, with `think` optionally absent ───────────────────────────────


def pass1(prompt: str, think: bool | None):
    """Byte-identical to `extract_pass1_reasoning`, except `think` may be omitted.

    `think=None` omits the kwarg entirely, reproducing the Run 6-era call shape
    (commit 9190e41 added the argument). Omitting is not the same as False: it
    hands the decision to the runtime, and what the runtime decided is precisely
    what changed between 0.17.7 and 0.21.0.
    """
    kwargs = {} if think is None else {"think": think}
    response = oc.ollama_chat(
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
        **kwargs,
    )
    content = response.message.content or ""
    thinking = getattr(response.message, "thinking", None)
    trace, branch = parse_thinking_trace(content, thinking)
    return trace, branch, content, response


# ── one extraction ───────────────────────────────────────────────────────


def run_one(paper, cell: str, paper_text: str, spec, expected) -> RuntimeResult:
    prompt = build_extraction_prompt(paper_text, spec)
    t0 = time.time()
    base = dict(
        condition=cell, paper_id=paper.paper_id,
        length_stratum=paper.length_stratum, study_type=paper.study_type,
        fields_expected=len(expected),
    )
    branch = thinking_chars = None
    content = trace = None
    try:
        trace, branch, content, _r1 = pass1(prompt, PASS1_THINK[cell])
        thinking_chars = len(trace)
        t1 = time.time()

        resp = oc.ollama_chat(
            model=MODEL,
            messages=pass2_messages(prompt, trace),
            options={"temperature": 0},
            think=spec.extraction_models.pass2_think,
            format=ExtractionOutput.model_json_schema(),
        )
        t2 = time.time()
        raw = resp.message.content or ""

        try:
            spans = [s.model_dump() for s in ExtractionOutput.model_validate_json(raw).fields]
            parse_path = "schema_valid"
        except Exception:
            spans, sub = parse_unconstrained(raw)
            parse_path = f"schema_invalid:{sub}"

        chk = check_completeness(spans, expected)
        return RuntimeResult(
            ok=True, n_spans=len(spans), complete=chk.complete,
            missing=list(chk.missing), parse_path=parse_path, parse_branch=branch,
            thinking_chars=thinking_chars,
            pass1_latency_s=round(t1 - t0, 1), pass2_latency_s=round(t2 - t1, 1),
            total_latency_s=round(t2 - t0, 1),
            prompt_eval_count=getattr(resp, "prompt_eval_count", None),
            eval_count=getattr(resp, "eval_count", None),
            done_reason=getattr(resp, "done_reason", None),
            retries=0 if chk.complete else 1,  # would-retry count, not a real retry
            spans=spans, raw_content=raw,
            pass1_content=content, pass1_trace=trace, **base,
        )
    except Exception as exc:
        logger.exception("paper %d (%s) failed", paper.paper_id, cell)
        return RuntimeResult(
            ok=False, n_spans=0, complete=False, missing=[], parse_path=None,
            parse_branch=branch, thinking_chars=thinking_chars,
            pass1_latency_s=0.0, pass2_latency_s=0.0,
            total_latency_s=round(time.time() - t0, 1),
            prompt_eval_count=None, eval_count=None, done_reason=None, retries=0,
            spans=[], raw_content=None, error=str(exc)[:400],
            pass1_content=content, pass1_trace=trace, **base,
        )


# ── pre-flight probe ─────────────────────────────────────────────────────

_PROBE_PROMPT = (
    "Paper excerpt: In this single-centre feasibility study, 12 patients underwent "
    "robot-assisted anastomosis with a supervised-autonomy controller. Mean operative "
    "time was 41 minutes.\n\n"
    "Extract two fields and reason about each first: sample_size, autonomy_level."
)

_PROBE_SCHEMA = {
    "type": "object",
    "properties": {"sample_size": {"type": "string"}, "autonomy_level": {"type": "string"}},
    "required": ["sample_size", "autonomy_level"],
    "additionalProperties": False,
}


def probe(cell: str, version: str) -> Probe:
    """One small call per cell: response shape and parse_branch, not quality.

    Deliberately not a full paper — the question is which channel the reasoning
    arrives on and whether the runtime compiles a JSON schema, both of which a
    short prompt answers for a fraction of the cost.
    """
    t0 = time.time()
    try:
        trace, branch, content, _ = pass1(_PROBE_PROMPT, PASS1_THINK[cell])
        fmt_ok, n_keys = False, 0
        try:
            r2 = oc.ollama_chat(
                model=MODEL,
                messages=[{"role": "user", "content": _PROBE_PROMPT}],
                options={"temperature": 0}, think=False, format=_PROBE_SCHEMA,
            )
            payload = json.loads(r2.message.content or "")
            fmt_ok, n_keys = isinstance(payload, dict), len(payload)
        except Exception as exc:
            logger.warning("probe pass2 (%s): %s", cell, exc)
        return Probe(
            cell=cell, server_version=version, parse_branch=branch,
            thinking_chars=len(trace), content_chars=len(content),
            has_think_tags="<think>" in content, pass2_format_ok=fmt_ok,
            pass2_keys=n_keys, latency_s=round(time.time() - t0, 1),
        )
    except Exception as exc:
        logger.exception("probe failed for %s", cell)
        return Probe(
            cell=cell, server_version=version, parse_branch=None, thinking_chars=None,
            content_chars=0, has_think_tags=False, pass2_format_ok=False, pass2_keys=0,
            latency_s=round(time.time() - t0, 1), error=str(exc)[:400],
        )


# ── entry point ──────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="QUALGAP-01: Ollama 0.17.7 runtime A/B")
    p.add_argument("--review", required=True)
    p.add_argument("--data-root", default="data")
    p.add_argument("--spec", default="review_specs/surgical_autonomy_v1.yaml")
    p.add_argument("--host", default=DEFAULT_HOST)
    p.add_argument("--label", default="runtime_v12")
    p.add_argument("--probe", action="store_true", help="pre-flight probes only, no batch")
    p.add_argument("--cells", nargs="*", default=list(CELLS),
                   help="subset of cells to run (V2 is dropped if 0.17.7 rejects think=True)")
    p.add_argument("--resume", action="store_true",
                   help="skip (cell, paper) pairs already present in the output")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    review_dir = Path(args.data_root) / args.review
    version = bind_runtime(args.host)

    if args.probe:
        with hold_experiment_lock():
            logger.info("experiment lock acquired for probes")
            probes = [probe(c, version) for c in args.cells]
        out = store_dir(review_dir) / "preflight_probes.json"
        out.write_text(json.dumps([asdict(x) for x in probes], indent=2))
        print(json.dumps([asdict(x) for x in probes], indent=2))
        logger.info("wrote %s", out)
        return 0

    spec = load_review_spec(args.spec)
    expected = expected_field_names(spec, review_dir / "extraction_codebook.yaml")
    sample = select_sample(review_dir)

    done: set[tuple[str, int]] = set()
    if args.resume:
        from analysis.eval.qualgap01 import read_results
        done = {(r["condition"], r["paper_id"]) for r in read_results(review_dir)}
        logger.info("resume: %d results already present", len(done))

    cells = [c for c in args.cells if c in CELLS]
    total = len(sample) * len(cells)
    logger.info(
        "QUALGAP-01: %d papers x %d cells = %d extractions on Ollama %s | fields=%d | "
        "pass1 think: %s | pass2 think=%s | contract=B (array schema)",
        len(sample), len(cells), total, version, len(expected),
        {c: PASS1_THINK[c] for c in cells}, spec.extraction_models.pass2_think,
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
            for cell in cells:
                n += 1
                if (cell, paper.paper_id) in done:
                    logger.info("[%d/%d] p%d %s — already done, skipping",
                                n, total, paper.paper_id, cell)
                    continue
                logger.info("[%d/%d] p%d (%s, %s) — %s", n, total, paper.paper_id,
                            paper.length_stratum, paper.study_type, cell)
                r = run_one(paper, cell, text, spec, expected)
                append_result(review_dir, args.label, r)
                elapsed = time.time() - t_start
                logger.info(
                    "    ok=%s spans=%d complete=%s parse=%s branch=%s think_chars=%s "
                    "%.0fs | elapsed %.1fh eta %.1fh",
                    r.ok, r.n_spans, r.complete, r.parse_path, r.parse_branch,
                    r.thinking_chars, r.total_latency_s, elapsed / 3600,
                    (elapsed / n) * (total - n) / 3600,
                )

    logger.info("done: %d extractions in %.1fh", n, (time.time() - t_start) / 3600)
    return 0


if __name__ == "__main__":
    sys.exit(main())
