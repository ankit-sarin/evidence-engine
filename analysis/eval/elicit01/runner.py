"""ELICIT-01 runner — 38 papers x {COPY, INDEX}, Pass-1 only, on Ollama 0.21.0.

No Pass-2 call anywhere. No write to review.db. Raw capture: both channels are
persisted verbatim and nothing is parsed, repaired or clamped at capture time.

Ops: whole run under `hold_experiment_lock()`; proactive restart every
RESTART_EVERY_N calls; bounded retry (3 attempts) via ollama_chat; a failed call
is recorded as a failure row and the run continues.

Tripwire: any row whose prompt_eval_count equals the enforced ceiling (131,072)
is marked TRUNCATED. PARSE-01 established that `done_reason` cannot detect input
truncation -- it reports `stop` either way -- so this is the only post-hoc signal.
Expected count is 0; the manifest fit check should have made it impossible.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from analysis.eval.elicit01.manifest import CEILING_TOKENS, EXCLUDED, newest_parsed
from analysis.eval.elicit01.prompts import (
    CONDITION_COPY, CONDITION_INDEX, CONDITIONS, SYSTEM_PASS1,
    build_copy_prompt, build_index_prompt,
)
from analysis.eval.elicit01.units import build_unit_map
from analysis.eval.schema_eval2 import select_sample
from engine.agents.extractor import MODEL, RESTART_EVERY_N, restart_ollama
from engine.utils import ollama_client as oc
from engine.utils.ollama_lock import hold_experiment_lock

logger = logging.getLogger("elicit01")

EXPECTED_SERVER_VERSION = "0.21.0"
OPTIONS: dict = {"temperature": 0}
PASS1_THINK = True
LABEL = "elicit01"


@dataclass
class Row:
    paper_id: int
    condition: str
    ok: bool
    started_utc: str
    finished_utc: str
    server_version: str | None = None
    model: str | None = None
    model_digest: str | None = None
    options: dict = field(default_factory=dict)
    options_sha256: str | None = None
    prompt_sha256: str | None = None
    prompt_chars: int | None = None
    system_sha256: str | None = None
    think: bool = PASS1_THINK
    n_units: int | None = None
    raw_content: str | None = None
    raw_thinking: str | None = None
    content_chars: int | None = None
    thinking_chars: int | None = None
    done_reason: str | None = None
    prompt_eval_count: int | None = None
    eval_count: int | None = None
    latency_s: float | None = None
    truncated: bool = False
    attempts: int = 1
    error: str | None = None


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def preflight_version() -> str:
    v = oc._client._client.get("/api/version").json()["version"]
    if v != EXPECTED_SERVER_VERSION:
        raise SystemExit(f"ABORT: Ollama {v!r}, expected {EXPECTED_SERVER_VERSION!r}")
    return v


def digest() -> str | None:
    try:
        for m in oc._client.list().get("models", []):
            if m.get("model") == MODEL:
                return m.get("digest")
    except Exception:
        return None
    return None


def call(paper_id: int, condition: str, prompt: str, n_units: int | None,
         version: str, dg: str | None) -> Row:
    base = dict(
        paper_id=paper_id, condition=condition, started_utc=_now(),
        server_version=version, model=MODEL, model_digest=dg,
        options=dict(OPTIONS), options_sha256=_sha(json.dumps(OPTIONS, sort_keys=True)),
        prompt_sha256=_sha(prompt), prompt_chars=len(prompt),
        system_sha256=_sha(SYSTEM_PASS1), think=PASS1_THINK, n_units=n_units,
    )
    t0 = time.time()
    try:
        r = oc.ollama_chat(
            model=MODEL,
            messages=[{"role": "system", "content": SYSTEM_PASS1},
                      {"role": "user", "content": prompt}],
            paper_id=paper_id, max_retries=2,
            options=dict(OPTIONS), think=PASS1_THINK,
        )
        content = r.message.content
        thinking = getattr(r.message, "thinking", None)
        pe = getattr(r, "prompt_eval_count", None)
        return Row(ok=True, finished_utc=_now(),
                   raw_content=content, raw_thinking=thinking,
                   content_chars=len(content or ""), thinking_chars=len(thinking or ""),
                   done_reason=getattr(r, "done_reason", None),
                   prompt_eval_count=pe, eval_count=getattr(r, "eval_count", None),
                   latency_s=round(time.time() - t0, 1),
                   truncated=bool(pe is not None and pe >= CEILING_TOKENS),
                   **base)
    except Exception as exc:
        logger.exception("p%d %s failed", paper_id, condition)
        return Row(ok=False, finished_utc=_now(), latency_s=round(time.time() - t0, 1),
                   error=f"{type(exc).__name__}: {exc}"[:400], **base)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="ELICIT-01 COPY vs INDEX")
    ap.add_argument("--review", default="surgical_autonomy")
    ap.add_argument("--data-root", default="data")
    ap.add_argument("--smoke", type=int, default=0)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--restart-every", type=int, default=RESTART_EVERY_N)
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    review_dir = Path(args.data_root) / args.review
    cb = review_dir / "extraction_codebook.yaml"
    store = review_dir / "eval" / LABEL
    store.mkdir(parents=True, exist_ok=True)
    out = store / f"{LABEL}.jsonl"

    version = preflight_version()
    dg = digest()
    sample = [p for p in select_sample(review_dir) if p.paper_id not in EXCLUDED]
    if args.smoke:
        wanted = [39, 67, 121][: args.smoke]
        sample = [p for p in sample if p.paper_id in wanted]

    done = set()
    if args.resume and out.exists():
        done = {(r["paper_id"], r["condition"])
                for r in (json.loads(l) for l in out.read_text().splitlines() if l.strip())
                if r.get("ok")}
        logger.info("resume: %d (paper, condition) pairs already done", len(done))

    total = len(sample) * len(CONDITIONS)
    logger.info("ELICIT-01 | Ollama %s | %s (%s) | think=%s | %d papers x %d conditions = %d calls%s",
                version, MODEL, (dg or "?")[:12], PASS1_THINK, len(sample),
                len(CONDITIONS), total, "  [SMOKE]" if args.smoke else "")

    n = ok = failed = trunc = 0
    t_start = time.time()
    with hold_experiment_lock():
        logger.info("experiment lock acquired")
        for p in sample:
            raw = newest_parsed(review_dir / "parsed_text", p.paper_id).read_text()
            um = build_unit_map(p.paper_id, raw)
            prompts = {
                CONDITION_COPY: (build_copy_prompt(raw, cb), None),
                CONDITION_INDEX: (build_index_prompt(um.render(), um.n, cb), um.n),
            }
            for cond in CONDITIONS:           # fixed order: COPY then INDEX
                if (p.paper_id, cond) in done:
                    continue
                if args.restart_every and n and n % args.restart_every == 0:
                    logger.info("proactive Ollama restart after %d calls", n)
                    restart_ollama(reason="elicit01 cadence", papers_done=n)
                n += 1
                prompt, nu = prompts[cond]
                logger.info("[%d/%d] p%d %s (%s)", n, total, p.paper_id, cond, p.length_stratum)
                row = call(p.paper_id, cond, prompt, nu, version, dg)
                with out.open("a") as fh:
                    fh.write(json.dumps(asdict(row)) + "\n")
                ok += row.ok
                failed += (not row.ok)
                trunc += row.truncated
                el = time.time() - t_start
                logger.info("    ok=%s content=%s think=%s pe=%s done=%s %.0fs%s | "
                            "elapsed %.2fh eta %.2fh",
                            row.ok, row.content_chars, row.thinking_chars,
                            row.prompt_eval_count, row.done_reason, row.latency_s or 0,
                            "  *** TRUNCATED ***" if row.truncated else "",
                            el / 3600, (el / n) * (total - n) / 3600)

    logger.info("CHECKPOINT: %d/%d calls (ok=%d failed=%d truncated=%d) in %.2fh -> %s",
                n, total, ok, failed, trunc, (time.time() - t_start) / 3600, out)
    if trunc:
        logger.error("TRIPWIRE: %d rows at the context ceiling", trunc)
        return 2
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
