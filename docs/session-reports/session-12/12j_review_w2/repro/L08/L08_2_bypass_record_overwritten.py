"""L08-C3: the bypass record ("Logged to audit trail") is one mutable row; a later
complete_stage (every automatic completer calls it unconditionally) or reset_stage erases it.
In-memory SQLite; the workflow module only."""
import sqlite3
from engine.adjudication.workflow import (advance_stage, complete_stage, reset_stage,
                                          ensure_workflow_table)

conn = sqlite3.connect(":memory:")
conn.row_factory = sqlite3.Row
ensure_workflow_table(conn)
S = "ABSTRACT_ADJUDICATION_COMPLETE"          # the human gate run_pipeline checks


def row():
    return tuple(conn.execute("SELECT status, metadata FROM workflow_state "
                              "WHERE stage_name=?", (S,)).fetchone())


r = advance_stage(conn, S, note="skip the human review", force=True)
print("advance_stage(force=True):", r["message"])
print("  row:", row())
# what screening_adjudicator's import does when a later import resolves everything:
complete_stage(conn, S, metadata="import complete")
print("after a later complete_stage:", row())
print("  rows anywhere still saying 'bypassed':",
      conn.execute("SELECT COUNT(*) FROM workflow_state WHERE status='bypassed' "
                   "OR metadata LIKE '%bypassed%'").fetchone()[0])

advance_stage(conn, "PDF_ACQUISITION", note="x")       # prerequisites met -> complete
r = advance_stage(conn, "FULL_TEXT_ADJUDICATION_COMPLETE", note="forced", force=True)
print(r["status"], tuple(conn.execute("SELECT status, metadata FROM workflow_state WHERE "
      "stage_name='FULL_TEXT_ADJUDICATION_COMPLETE'").fetchone()))
reset_stage(conn, "FULL_TEXT_ADJUDICATION_COMPLETE")
print("after reset_stage:", tuple(conn.execute("SELECT status, completed_at, metadata FROM "
      "workflow_state WHERE stage_name='FULL_TEXT_ADJUDICATION_COMPLETE'").fetchone()))
