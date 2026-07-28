"""Per-call extraction telemetry (INSTRUMENT-01).

SPANLOSS-01 could not close its own diagnosis: seven of the 17 collapsed openai
calls report 600–2,000 visible tokens while the stored response held ~45, and
the question "was the response truncated?" was unanswerable because
`finish_reason` was read from no provider and the pre-parse `content` string was
never persisted — only `json.loads`'s output was.

This module records what would have answered it. One JSON line per API call,
appended to a gitignored file under the review directory, written *before* the
result is accepted or rejected so a failed attempt leaves a trace too.

Deliberately file-based: the task that motivated it forbids schema changes, and
a JSONL sidecar is also the right shape for this data — append-only, one row per
attempt, cheap to grep, and disposable once a run is trusted.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

TELEMETRY_DIRNAME = "telemetry"
TELEMETRY_FILENAME = "extraction_calls.jsonl"
SCHEMA_VERSION = "extraction-telemetry-1"

# Raw responses are the point of this file, but a runaway response should not be
# able to blow up the log. Truncation is recorded explicitly when it happens.
RAW_CONTENT_LIMIT = 200_000

_WRITE_LOCK = threading.Lock()


def telemetry_path(review_dir: str | Path) -> Path:
    return Path(review_dir) / TELEMETRY_DIRNAME / TELEMETRY_FILENAME


def record_call(
    review_dir: str | Path,
    *,
    arm: str,
    paper_id: int,
    attempt: int,
    outcome: str,
    model: str | None = None,
    finish_reason: str | None = None,
    raw_content: str | None = None,
    spans_parsed: int | None = None,
    fields_expected: int | None = None,
    missing_fields: tuple[str, ...] | list[str] | None = None,
    salvage: str | None = None,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    reasoning_tokens: int | None = None,
    error: str | None = None,
    extra: dict | None = None,
) -> Path | None:
    """Append one telemetry record. Never raises — telemetry must not break a run.

    `outcome` is one of: 'stored', 'incomplete_retry', 'incomplete_exhausted',
    'error'. `finish_reason` carries the provider's own field verbatim
    (`finish_reason` for OpenAI, `stop_reason` for Anthropic, `done_reason` for
    Ollama) so the values stay traceable to their source rather than being
    normalized into a lossy common vocabulary.
    """
    raw = raw_content or ""
    truncated = len(raw) > RAW_CONTENT_LIMIT
    row = {
        "schema": SCHEMA_VERSION,
        "ts": datetime.now(timezone.utc).isoformat(),
        "arm": arm,
        "paper_id": paper_id,
        "attempt": attempt,
        "outcome": outcome,
        "model": model,
        "finish_reason": finish_reason,
        "raw_content": raw[:RAW_CONTENT_LIMIT] if raw else None,
        "raw_content_chars": len(raw) if raw_content is not None else None,
        "raw_content_truncated": truncated,
        "spans_parsed": spans_parsed,
        "fields_expected": fields_expected,
        "missing_fields": list(missing_fields) if missing_fields else None,
        "salvage": salvage,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "reasoning_tokens": reasoning_tokens,
        "error": error,
    }
    if extra:
        row["extra"] = extra

    try:
        path = telemetry_path(review_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(row, default=str)
        with _WRITE_LOCK:
            with path.open("a") as fh:
                fh.write(line + "\n")
        return path
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Telemetry write failed (continuing): %s", exc)
        return None


def read_calls(review_dir: str | Path) -> list[dict]:
    """Read back all telemetry records. Skips malformed lines rather than failing."""
    path = telemetry_path(review_dir)
    if not path.exists():
        return []
    out = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            logger.warning("Skipping malformed telemetry line in %s", path)
    return out
