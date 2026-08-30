"""CAPTURE-01 runner — ~40 Pass-1-only calls on Ollama 0.21.0, both channels kept.

Closes PRIME-01's blocking unknown: what the 0.21.0 Pass-1 *draft* channel looks
like. See `analysis/eval/capture01.py` for the storage contract.

What this runner deliberately does **not** do:

  * **No Pass-2 call.** Not one, anywhere. This is why the run is ~3 h and not ~6.
  * **No parsing.** `parse_thinking_trace` is never imported, so no salvage path
    and no whole-content fallback can touch the captured text. Raw is the record.
  * **No write to `review.db`.** Paper text is read from `parsed_text/*.md`, the
    same source PRIME-01 normalizes against, so the richness metric stays
    comparable. The DB is opened read-only (`immutable=1`) only if a caller asks
    for study types, and not at all on the default path.

Ops:

  * The whole run holds `hold_experiment_lock()`, so the 07:00 health cron and any
    foreign `restart_ollama()` stand down (OPS-GUARD-01).
  * Proactive `restart_ollama()` every `RESTART_EVERY_N` calls to clear CUDA
    fragmentation. Because *we* hold the lock, `foreign_lock_held()` is False and
    the restart is permitted — that is the intended self-vs-foreign distinction.
  * Bounded retry per call (3 attempts total) via `ollama_chat`. On exhaustion the
    failure is recorded as a row and the run continues; acceptance is never
    silently widened.

The one surprise rule: `think=True` on 0.21.0 must yield a non-empty
`message.thinking` distinct from `message.content`. A row that violates that is
recorded and flagged, not worked around.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from analysis.eval.capture01 import (
    EXPECTED_SERVER_VERSION,
    LABEL,
    OPTIONS,
    PASS1_THINK,
    SYSTEM_PASS1,
    CaptureResult,
    append_result,
    read_results,
    store_dir,
)
from analysis.eval.schema_eval2 import select_sample
from engine.agents.extractor import MODEL, RESTART_EVERY_N, build_extraction_prompt, restart_ollama
from engine.core.review_spec import load_review_spec
from engine.utils import ollama_client as oc
from engine.utils.ollama_lock import hold_experiment_lock

logger = logging.getLogger("capture01")


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def preflight_version() -> str:
    """Abort unless the server is exactly the pinned runtime. Never adapt to it."""
    version = oc._client._client.get("/api/version").json()["version"]
    if version != EXPECTED_SERVER_VERSION:
        raise SystemExit(
            f"ABORT: Ollama {version!r}, expected {EXPECTED_SERVER_VERSION!r}. "
            "CAPTURE-01 is a statement about the pinned runtime; do not change the "
            "server to satisfy this check."
        )
    return version


def model_digest() -> str | None:
    try:
        for m in oc._client.list().get("models", []):
            if m.get("model") == MODEL:
                return m.get("digest")
    except Exception:  # provenance is nice-to-have; never fail the run for it
        logger.warning("could not read model digest", exc_info=True)
    return None


def capture_one(paper, paper_text: str, spec, version: str, digest: str | None) -> CaptureResult:
    """One Pass-1 call. Everything after the call is bookkeeping, not parsing."""
    prompt = build_extraction_prompt(paper_text, spec)
    started = _now()
    t0 = time.time()
    base = dict(
        paper_id=paper.paper_id,
        started_utc=started,
        server_version=version,
        model=MODEL,
        model_digest=digest,
        options=dict(OPTIONS),
        options_sha256=_sha(json.dumps(OPTIONS, sort_keys=True)),
        prompt_sha256=_sha(prompt),
        prompt_chars=len(prompt),
        system_pass1_sha256=_sha(SYSTEM_PASS1),
        think=PASS1_THINK,
        length_stratum=getattr(paper, "length_stratum", None),
        study_type=getattr(paper, "study_type", None),
    )
    try:
        response = oc.ollama_chat(
            model=MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PASS1},
                {"role": "user", "content": prompt},
            ],
            paper_id=paper.paper_id,
            max_retries=2,  # 3 attempts total, per the brief
            options=dict(OPTIONS),
            think=PASS1_THINK,
        )
        elapsed = time.time() - t0
        content = response.message.content
        thinking = getattr(response.message, "thinking", None)
        return CaptureResult(
            ok=True,
            finished_utc=_now(),
            pass1_content=content,
            pass1_thinking=thinking,
            pass1_content_chars=len(content or ""),
            pass1_thinking_chars=len(thinking or ""),
            done_reason=getattr(response, "done_reason", None),
            prompt_eval_count=getattr(response, "prompt_eval_count", None),
            eval_count=getattr(response, "eval_count", None),
            pass1_latency_s=round(elapsed, 1),
            **base,
        )
    except Exception as exc:
        logger.exception("paper %d failed after retries", paper.paper_id)
        return CaptureResult(
            ok=False,
            finished_utc=_now(),
            pass1_latency_s=round(time.time() - t0, 1),
            error=f"{type(exc).__name__}: {exc}"[:400],
            **base,
        )


def channel_check(r: CaptureResult) -> tuple[bool, str]:
    """The smoke gate, also logged per row in the full run.

    Empty thinking under `think=True`, or thinking identical to content, is the
    interface surprise the brief says to STOP on — not something to route around.
    """
    if not r.ok:
        return False, "call failed"
    if not (r.pass1_content or "").strip():
        return False, "EMPTY CONTENT"
    if not (r.pass1_thinking or "").strip():
        return False, "EMPTY THINKING under think=True — interface surprise"
    if (r.pass1_content or "").strip() == (r.pass1_thinking or "").strip():
        return False, "CHANNELS IDENTICAL — interface surprise"
    return True, "ok"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="CAPTURE-01: 0.21.0 Pass-1 draft capture")
    p.add_argument("--review", required=True)
    p.add_argument("--data-root", default="data")
    p.add_argument("--spec", default="review_specs/surgical_autonomy_v1.yaml")
    p.add_argument("--label", default=LABEL)
    p.add_argument("--smoke", type=int, default=0,
                   help="capture only the first N papers (the mandatory 3-paper gate)")
    p.add_argument("--resume", action="store_true",
                   help="skip papers already present in the output")
    p.add_argument("--restart-every", type=int, default=RESTART_EVERY_N)
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    review_dir = Path(args.data_root) / args.review

    version = preflight_version()
    digest = model_digest()
    spec = load_review_spec(args.spec)
    sample = select_sample(review_dir)

    done = {r["paper_id"] for r in read_results(review_dir, args.label)} if args.resume else set()
    if done:
        logger.info("resume: %d papers already captured", len(done))

    todo = [pp for pp in sample if pp.paper_id not in done]
    if args.smoke:
        todo = todo[: args.smoke]

    logger.info(
        "CAPTURE-01 | Ollama %s | model %s (%s) | think=%s | options=%s | "
        "%d papers (sample=%d, done=%d)%s",
        version, MODEL, (digest or "?")[:12], PASS1_THINK, OPTIONS,
        len(todo), len(sample), len(done), "  [SMOKE]" if args.smoke else "",
    )
    logger.info("papers: %s", [pp.paper_id for pp in todo])

    n = ok = failed = 0
    surprises: list[tuple[int, str]] = []
    t_start = time.time()

    with hold_experiment_lock():
        logger.info("experiment lock acquired — cron and foreign restarts stand down")
        for paper in todo:
            files = sorted((review_dir / "parsed_text").glob(f"{paper.paper_id}_v*.md"),
                           key=lambda f: int(f.stem.rsplit("_v", 1)[1]))
            if not files:
                logger.warning("paper %d: no parsed text — skipping", paper.paper_id)
                continue
            text = files[-1].read_text()

            if args.restart_every and n and n % args.restart_every == 0:
                logger.info("proactive Ollama restart after %d calls", n)
                restart_ollama(reason="capture01 cadence", papers_done=n)

            n += 1
            logger.info("[%d/%d] p%d (%s, %s)", n, len(todo), paper.paper_id,
                        paper.length_stratum, paper.study_type)
            r = capture_one(paper, text, spec, version, digest)
            append_result(review_dir, args.label, r)

            good, why = channel_check(r)
            if r.ok:
                ok += 1
            else:
                failed += 1
            if not good and r.ok:
                surprises.append((paper.paper_id, why))
            elapsed = time.time() - t_start
            logger.info(
                "    ok=%s content=%s thinking=%s done=%s %.0fs | check=%s | "
                "elapsed %.2fh eta %.2fh",
                r.ok, r.pass1_content_chars, r.pass1_thinking_chars, r.done_reason,
                r.pass1_latency_s or 0, why,
                elapsed / 3600, (elapsed / n) * (len(todo) - n) / 3600,
            )

    logger.info("CHECKPOINT: captured %d/%d (ok=%d failed=%d) in %.2fh -> %s",
                n, len(todo), ok, failed, (time.time() - t_start) / 3600,
                store_dir(review_dir) / f"{args.label}.jsonl")
    if surprises:
        logger.error("INTERFACE SURPRISES (STOP-and-surface): %s", surprises)
        return 2
    if args.smoke:
        logger.info("SMOKE GATE: %d/%d passed channel-distinctness and completeness",
                    ok - len(surprises), len(todo))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
