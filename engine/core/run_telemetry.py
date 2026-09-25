"""Per-run telemetry (R167, 9c-C7).

A run-level result — one per run, not per paper or attempt — has no home in
`extraction_telemetry` (one row per extraction attempt, keyed by paper) or
`audit_telemetry` (one row per audited claim). The first user is the local
extract stage's distribution-collapse check, whose COLLAPSED result is recorded
here and never becomes a paper outcome or aborts the run.

Same pattern as the two sibling sinks: one JSON line per event, appended to a
gitignored file under the review directory, and the writer never raises —
telemetry must not break a run. Session 10 may promote it to a table.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

TELEMETRY_DIRNAME = "telemetry"
TELEMETRY_FILENAME = "run_events.jsonl"
SCHEMA_VERSION = "run-telemetry-1"

_WRITE_LOCK = threading.Lock()


def telemetry_path(review_dir: str | Path) -> Path:
    return Path(review_dir) / TELEMETRY_DIRNAME / TELEMETRY_FILENAME


def record_run_event(review_dir: str | Path, *, run_id: int, kind: str,
                     payload: dict) -> Path | None:
    """Append one run-level event. Never raises — telemetry must not break a run."""
    row = {
        "schema": SCHEMA_VERSION,
        "ts": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "kind": kind,
        "payload": payload,
    }
    try:
        path = telemetry_path(review_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(row, default=str)
        with _WRITE_LOCK:
            with path.open("a") as fh:
                fh.write(line + "\n")
        return path
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Run telemetry write failed (continuing): %s", exc)
        return None


def read_run_events(review_dir: str | Path) -> list[dict]:
    """Read back all run events. Skips malformed lines rather than failing."""
    path = telemetry_path(review_dir)
    if not path.exists():
        return []
    out = []
    for line in path.read_text().splitlines():
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out
