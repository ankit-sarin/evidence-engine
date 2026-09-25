"""Per-verdict audit telemetry (WRITE-PATH-01 9b-2d).

The semantic verdict has no event home yet: it is ruled run-linked telemetry
here and promoted to a table at the session-10 migration. One JSON line per
`semantic_verify` verdict — and per claim that owed one but had no snippet to
show the model (9b-2d R4) — appended to a gitignored file beside the extraction
telemetry, carrying the run it belongs to so it can be joined back to the
manifest.

The shape mirrors `extraction_telemetry`: a versioned schema, append-only
JSONL under `<review_dir>/telemetry/`, never raising — telemetry must not break
a run.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path

from engine.core.extraction_telemetry import TELEMETRY_DIRNAME

logger = logging.getLogger(__name__)

TELEMETRY_FILENAME = "audit_calls.jsonl"
SCHEMA_VERSION = "audit-telemetry-1"

#: The row's keys, in order — pinned by test (T12).
FIELDS = ("schema", "run_id", "paper_id", "claim_id", "field_name", "arm",
          "auditor_model", "auditor_digest", "verdict", "rationale", "occurred_at")

_WRITE_LOCK = threading.Lock()


def telemetry_path(review_dir: str | Path) -> Path:
    return Path(review_dir) / TELEMETRY_DIRNAME / TELEMETRY_FILENAME


def record_verdict(review_dir: str | Path, *, run_id: int, paper_id: int, claim_id: str,
                   field_name: str, arm: str, auditor_model: str | None,
                   auditor_digest: str | None, verdict: str,
                   rationale: str | None) -> Path | None:
    """Append one verdict row. Never raises."""
    row = dict(zip(FIELDS, (SCHEMA_VERSION, run_id, paper_id, claim_id, field_name, arm,
                            auditor_model, auditor_digest, verdict, rationale,
                            datetime.now(timezone.utc).isoformat())))
    try:
        path = telemetry_path(review_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        with _WRITE_LOCK:
            with path.open("a") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        return path
    except Exception:  # pragma: no cover - telemetry must not break a run
        logger.exception("audit telemetry write failed (run %s, paper %s)", run_id, paper_id)
        return None


def read_verdicts(review_dir: str | Path) -> list[dict]:
    path = telemetry_path(review_dir)
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
