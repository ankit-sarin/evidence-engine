"""CAPTURE-01 — store and record shape for the 0.21.0 Pass-1 draft capture.

PRIME-01 measured Pass-1 channel quote-richness on 0.17.7 and on Run 6, and found
the two channels categorically different: drafts (``message.content``) repeat the
paper in 37.9-42.9% of their 8-word windows, thinking traces in 0.4%. It could not
answer the same question for **0.21.0**, the pinned production runtime, because no
0.21.0 Pass-1 draft text exists on disk: the eval runners store ``raw_content`` =
the *Pass-2* response and ``think_chars`` = an integer *length*, and they bypass
``extract_paper()``, so ``record_call`` telemetry never fires.

This module is the storage half of closing that gap. Its whole contract:

  * **Pass 1 only.** No Pass-2 call is made anywhere in CAPTURE-01. Nothing is
    written to ``review.db``.

  * **Raw capture, no interpretation.** ``parse_thinking_trace`` is deliberately
    *not* imported. Both channels are persisted exactly as the server returned
    them; every derived quantity is computed later, by the analysis step, from
    this JSONL. If capture and expectation disagree, the JSONL is the record.

  * **Self-describing rows.** Each row carries the runtime version string, model
    tag, the full options dict and its SHA-256, and the prompt SHA-256, so a row
    can be interpreted without reference to the code that produced it.

The sample is not defined here. It is ``schema_eval2.select_sample`` (SEED=20260729,
N_TOTAL=40) — the same 40 papers SCHEMA-EVAL-02 drew and QUALGAP-01 reused, verified
identical across both stores.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

# The runtime this study is *about*. QUALGAP-01 acquitted the runtime and the
# project stays pinned here; a mismatch is an abort, never an adaptation.
EXPECTED_SERVER_VERSION = "0.21.0"

LABEL = "capture01"

# Pass-1 `think` is declared, never inherited from a version-dependent default.
# This is the REGRESSION-01 rule; QUALGAP-01's V2 cell used the same value.
PASS1_THINK = True

# Byte-identical to the Pass-1 system message in `extract_pass1_reasoning` and in
# QUALGAP-01's `pass1()`. Duplicated as a literal on purpose: if production ever
# edits its prompt, this study must not silently follow it.
SYSTEM_PASS1 = (
    "You are a systematic review data extractor. Read the paper "
    "carefully and reason through each extraction field step by step. "
    "Think about what the paper says for each field before extracting."
)

OPTIONS: dict = {"temperature": 0}


@dataclass
class CaptureResult:
    """One Pass-1 call, raw. Field order is the persist list from the brief."""

    paper_id: int
    ok: bool
    started_utc: str
    finished_utc: str

    # provenance — enough to interpret the row standalone
    server_version: str | None = None
    model: str | None = None
    model_digest: str | None = None
    options: dict = field(default_factory=dict)
    options_sha256: str | None = None
    prompt_sha256: str | None = None
    prompt_chars: int | None = None
    system_pass1_sha256: str | None = None
    think: bool = PASS1_THINK
    length_stratum: str | None = None
    study_type: str | None = None

    # the deliverable — both channels, unparsed
    pass1_content: str | None = None
    pass1_thinking: str | None = None
    pass1_content_chars: int | None = None
    pass1_thinking_chars: int | None = None

    # runtime telemetry
    done_reason: str | None = None
    prompt_eval_count: int | None = None
    eval_count: int | None = None
    pass1_latency_s: float | None = None
    attempts: int = 1
    error: str | None = None

    def to_json(self) -> dict:
        return asdict(self)


def store_dir(review_dir: Path) -> Path:
    d = review_dir / "eval" / LABEL
    d.mkdir(parents=True, exist_ok=True)
    return d


def append_result(review_dir: Path, label: str, result: CaptureResult) -> Path:
    """Append one row. Flush + fsync so a killed run keeps every completed call."""
    path = store_dir(review_dir) / f"{label}.jsonl"
    with path.open("a") as fh:
        fh.write(json.dumps(result.to_json()) + "\n")
        fh.flush()
    return path


def read_results(review_dir: Path, label: str = LABEL) -> list[dict]:
    path = store_dir(review_dir) / f"{label}.jsonl"
    if not path.exists():
        return []
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
