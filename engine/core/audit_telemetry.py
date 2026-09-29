"""The semantic verdict — B8/R218: `audit_verdicts` (022), not a file.

WRITE-PATH-01 9b-2d ruled it run-linked telemetry, written to a JSONL sink
because no table existed yet. 022 built `audit_verdicts` for exactly this;
10a-C5 moves the writer onto it. `record_verdict` inserts one row on the
caller's connection and does not commit — the caller's unit of work does
(D8), the same savepoint as the paper's `citation_located` events and its
`audited_ai` paper event, so a verdict and the audit it belongs to commit or
roll back together. No reader is added here: R147 stands — telemetry, never
a filter on which claims get an event or a state.
"""

from __future__ import annotations

SCHEMA_VERSION = "audit-telemetry-1"

#: The row's keys, in order — pinned by test, and the column order the INSERT
#: below writes in (the file's original purpose for this constant; it never
#: named a JSONL field order alone).
FIELDS = ("schema", "run_id", "paper_id", "claim_id", "field_name", "arm",
          "auditor_model", "auditor_digest", "verdict", "rationale", "occurred_at")


class VerdictWithoutRun(Exception):
    """A verdict exists only under a run (R68's spirit) — `audit_verdicts.run_id`
    is NOT NULL, and a caller with no run_id has nothing to link the verdict to."""


def record_verdict(conn, *, run_id: int | None, paper_id: int, claim_id: str,
                   field_name: str, arm: str, auditor_model: str,
                   auditor_digest: str | None, verdict: str,
                   rationale: str | None, occurred_at: str) -> int:
    """Insert one `audit_verdicts` row. Does not commit.

    `verdict` is passed through unvalidated beyond this: the DDL's CHECK
    (`'verified'` or `'flagged'`) is the one place that vocabulary is
    enforced (R218), so this function does not duplicate it.
    """
    if run_id is None:
        raise VerdictWithoutRun(
            f"verdict refused: paper {paper_id} claim {claim_id!r} carries no "
            "run_id — a verdict exists only under a run.")
    cur = conn.execute(
        "INSERT INTO audit_verdicts (schema, run_id, paper_id, claim_id, field_name, "
        "arm, auditor_model, auditor_digest, verdict, rationale, occurred_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (SCHEMA_VERSION, run_id, paper_id, claim_id, field_name, arm, auditor_model,
         auditor_digest, verdict, rationale, occurred_at))
    return cur.lastrowid
