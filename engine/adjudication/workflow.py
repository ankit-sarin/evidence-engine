"""Review adjudication workflow — 9-stage sequential enforcement.

Screening stages (1–5):
  1. SCREENING_COMPLETE        — auto: set when screening finishes
  2. DIAGNOSTIC_SAMPLE_COMPLETE — manual: human confirms 50-paper FP analysis
  3. CATEGORIES_CONFIGURED     — auto: adjudication_categories.yaml exists & validates
  4. QUEUE_EXPORTED            — auto: export_adjudication_queue succeeds
  5. ADJUDICATION_COMPLETE     — auto: import with zero unresolved papers

Extraction stages (6–9):
  6. EXTRACTION_COMPLETE       — auto: all included papers reach EXTRACTED status
  7. AI_AUDIT_COMPLETE_STAGE   — auto: audit run finishes (all papers audited)
  8. AUDIT_QUEUE_EXPORTED      — auto: export_audit_review_queue succeeds
  9. AUDIT_REVIEW_COMPLETE     — auto: import with zero unresolved spans
"""

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# Ordered workflow stages — screening (1-5) then extraction (6-9)
WORKFLOW_STAGES = (
    "SCREENING_COMPLETE",
    "DIAGNOSTIC_SAMPLE_COMPLETE",
    "CATEGORIES_CONFIGURED",
    "QUEUE_EXPORTED",
    "ADJUDICATION_COMPLETE",
    "EXTRACTION_COMPLETE",
    "AI_AUDIT_COMPLETE_STAGE",
    "AUDIT_QUEUE_EXPORTED",
    "AUDIT_REVIEW_COMPLETE",
)

# Subsets for display grouping
SCREENING_STAGES = WORKFLOW_STAGES[:5]
EXTRACTION_STAGES = WORKFLOW_STAGES[5:]

# Human-readable next-step guidance per stage
_NEXT_STEP_GUIDANCE = {
    "SCREENING_COMPLETE": (
        "Run screening (run_screening or screen_expanded.py) to complete "
        "primary + verification screening."
    ),
    "DIAGNOSTIC_SAMPLE_COMPLETE": (
        "Review a 50-paper diagnostic sample of flagged papers to identify "
        "FP patterns, then run:\n"
        "  python -m engine.adjudication.advance_stage "
        "--review <name> --stage DIAGNOSTIC_SAMPLE_COMPLETE "
        '--note "50-paper sample reviewed, N FP categories identified"'
    ),
    "CATEGORIES_CONFIGURED": (
        "Create or update adjudication_categories.yaml for this review.\n"
        "  Location: data/<review>/adjudication_categories.yaml\n"
        "  Generate a starter template with: generate_starter_config()"
    ),
    "QUEUE_EXPORTED": (
        "Export the adjudication queue:\n"
        "  export_adjudication_queue(review_db, output_path, review_name=<name>)"
    ),
    "ADJUDICATION_COMPLETE": (
        "Complete human review of the exported screening queue, then run:\n"
        "  import_adjudication_decisions(review_db, <path_to_completed_xlsx>)"
    ),
    "EXTRACTION_COMPLETE": (
        "Run full-text extraction on all included papers:\n"
        "  python scripts/run_pipeline.py --spec <spec> --name <name> --skip-to extract"
    ),
    "AI_AUDIT_COMPLETE_STAGE": (
        "Run AI audit on all extracted papers:\n"
        "  python scripts/run_pipeline.py --spec <spec> --name <name> --skip-to audit"
    ),
    "AUDIT_QUEUE_EXPORTED": (
        "Export the audit review queue for human review:\n"
        "  from engine.adjudication.audit_adjudicator import export_audit_review_queue\n"
        "  export_audit_review_queue(review_db, 'path/to/audit_queue.xlsx')"
    ),
    "AUDIT_REVIEW_COMPLETE": (
        "Complete human review of the audit queue, then run:\n"
        "  from engine.adjudication.audit_adjudicator import import_audit_review_decisions\n"
        "  import_audit_review_decisions(review_db, 'path/to/completed_audit_queue.xlsx')"
    ),
}

# ── Schema ──────────────────────────────────────────────────────────

_WORKFLOW_STATE_TABLE = """
CREATE TABLE IF NOT EXISTS workflow_state (
    id              INTEGER PRIMARY KEY,
    stage_name      TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'complete', 'bypassed')),
    completed_at    TEXT,
    metadata        TEXT,
    UNIQUE(stage_name)
);
"""


def ensure_workflow_table(conn: sqlite3.Connection) -> None:
    """Create the workflow_state table if it doesn't exist."""
    conn.executescript(_WORKFLOW_STATE_TABLE)
    conn.commit()

    # Seed all stages as pending if not already present
    for stage in WORKFLOW_STAGES:
        conn.execute(
            "INSERT OR IGNORE INTO workflow_state (stage_name, status) VALUES (?, 'pending')",
            (stage,),
        )
    conn.commit()


# ── Read/Write ──────────────────────────────────────────────────────


def get_workflow_status(conn: sqlite3.Connection) -> list[dict]:
    """Return all workflow stages with their status, ordered."""
    ensure_workflow_table(conn)
    result = []
    for stage in WORKFLOW_STAGES:
        row = conn.execute(
            "SELECT status, completed_at, metadata FROM workflow_state WHERE stage_name = ?",
            (stage,),
        ).fetchone()
        if row:
            result.append({
                "stage_name": stage,
                "status": row["status"],
                "completed_at": row["completed_at"],
                "metadata": row["metadata"],
            })
        else:
            result.append({
                "stage_name": stage,
                "status": "pending",
                "completed_at": None,
                "metadata": None,
            })
    return result


def complete_stage(conn: sqlite3.Connection, stage_name: str,
                   metadata: str | None = None) -> None:
    """Mark a workflow stage as complete."""
    if stage_name not in WORKFLOW_STAGES:
        raise ValueError(f"Unknown stage: {stage_name}")

    ensure_workflow_table(conn)
    now = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """UPDATE workflow_state
           SET status = 'complete', completed_at = ?, metadata = ?
           WHERE stage_name = ?""",
        (now, metadata, stage_name),
    )
    conn.commit()
    logger.info("Workflow stage completed: %s", stage_name)


def bypass_stage(conn: sqlite3.Connection, stage_name: str,
                 metadata: str | None = None) -> None:
    """Mark a stage as bypassed (force override). Logs a warning."""
    if stage_name not in WORKFLOW_STAGES:
        raise ValueError(f"Unknown stage: {stage_name}")

    ensure_workflow_table(conn)
    now = datetime.now(timezone.utc).isoformat()
    bypass_note = f"Stage {stage_name} bypassed by operator at {now}."
    if metadata:
        bypass_note += f" Note: {metadata}"

    conn.execute(
        """UPDATE workflow_state
           SET status = 'bypassed', completed_at = ?, metadata = ?
           WHERE stage_name = ?""",
        (now, bypass_note, stage_name),
    )
    conn.commit()
    logger.warning("AUDIT: %s", bypass_note)


def is_stage_done(conn: sqlite3.Connection, stage_name: str) -> bool:
    """Check if a stage is complete or bypassed."""
    ensure_workflow_table(conn)
    row = conn.execute(
        "SELECT status FROM workflow_state WHERE stage_name = ?",
        (stage_name,),
    ).fetchone()
    return row is not None and row["status"] in ("complete", "bypassed")


def reset_stage(conn: sqlite3.Connection, stage_name: str) -> None:
    """Reset a stage back to pending (for re-running)."""
    if stage_name not in WORKFLOW_STAGES:
        raise ValueError(f"Unknown stage: {stage_name}")

    ensure_workflow_table(conn)
    conn.execute(
        """UPDATE workflow_state
           SET status = 'pending', completed_at = NULL, metadata = NULL
           WHERE stage_name = ?""",
        (stage_name,),
    )
    conn.commit()


# ── Workflow Checks ─────────────────────────────────────────────────


def get_current_blocker(conn: sqlite3.Connection) -> dict | None:
    """Return the first incomplete stage, or None if all complete.

    Returns dict with stage_name, index, and next_step guidance.
    """
    statuses = get_workflow_status(conn)
    for i, s in enumerate(statuses):
        if s["status"] == "pending":
            return {
                "stage_name": s["stage_name"],
                "index": i,
                "next_step": _NEXT_STEP_GUIDANCE.get(s["stage_name"], ""),
            }
    return None


def is_adjudication_complete(conn: sqlite3.Connection) -> bool:
    """Check if the screening adjudication workflow is complete."""
    return is_stage_done(conn, "ADJUDICATION_COMPLETE")


def is_audit_review_complete(conn: sqlite3.Connection) -> bool:
    """Check if the extraction audit review workflow is complete."""
    return is_stage_done(conn, "AUDIT_REVIEW_COMPLETE")


def can_advance_to(conn: sqlite3.Connection, stage_name: str) -> bool:
    """Check if all prerequisite stages are done for the given stage."""
    if stage_name not in WORKFLOW_STAGES:
        raise ValueError(f"Unknown stage: {stage_name}")

    target_idx = WORKFLOW_STAGES.index(stage_name)
    for i in range(target_idx):
        if not is_stage_done(conn, WORKFLOW_STAGES[i]):
            return False
    return True


def advance_stage(conn: sqlite3.Connection, stage_name: str,
                  note: str, *, force: bool = False) -> dict:
    """Advance the workflow to the given stage.

    Validates that all prerequisite stages are complete.
    If force=True, bypasses prerequisite checks and logs a warning.

    Returns dict with result info.
    """
    if stage_name not in WORKFLOW_STAGES:
        raise ValueError(f"Unknown stage: {stage_name}")

    if is_stage_done(conn, stage_name):
        return {
            "status": "already_complete",
            "message": f"Stage {stage_name} is already complete.",
        }

    if not force and not can_advance_to(conn, stage_name):
        blocker = get_current_blocker(conn)
        return {
            "status": "blocked",
            "message": (
                f"Cannot advance to {stage_name}: "
                f"prerequisite {blocker['stage_name']} is not complete."
            ),
            "blocker": blocker,
        }

    if force and not can_advance_to(conn, stage_name):
        bypass_stage(conn, stage_name, metadata=note)
        return {
            "status": "bypassed",
            "message": f"Stage {stage_name} bypassed with --force. Logged to audit trail.",
        }

    complete_stage(conn, stage_name, metadata=note)
    return {
        "status": "complete",
        "message": f"Stage {stage_name} marked complete.",
    }


# ── Display ─────────────────────────────────────────────────────────


def format_workflow_status(conn: sqlite3.Connection,
                           review_name: str = "") -> str:
    """Format the workflow status as a human-readable string."""
    statuses = get_workflow_status(conn)
    header = f"Review Workflow"
    if review_name:
        header += f" — {review_name}"

    lines = [header]
    blocker_found = False

    for i, s in enumerate(statuses):
        # Section headers
        if i == 0:
            lines.append("  ── Screening Adjudication ──")
        elif s["stage_name"] == "EXTRACTION_COMPLETE":
            lines.append("  ── Extraction Audit ──")
        if s["status"] == "complete":
            ts = s["completed_at"][:16].replace("T", " ") if s["completed_at"] else ""
            lines.append(f"  [✓] {s['stage_name']} ({ts})")
        elif s["status"] == "bypassed":
            ts = s["completed_at"][:16].replace("T", " ") if s["completed_at"] else ""
            lines.append(f"  [!] {s['stage_name']} ({ts}) — BYPASSED")
        else:
            if not blocker_found:
                guidance = _NEXT_STEP_GUIDANCE.get(s["stage_name"], "")
                # Show first line of guidance only
                short = guidance.split("\n")[0] if guidance else "awaiting completion"
                lines.append(f"  [ ] {s['stage_name']} — {short}")
                blocker_found = True
            else:
                lines.append(f"  [ ] {s['stage_name']}")

    return "\n".join(lines)
