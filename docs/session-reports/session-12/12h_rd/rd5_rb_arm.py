"""12h RD-5 — what the screen-start manifest left in the retained rb_12g review, and what
live's arm registry holds. Read-only (mode=ro). usage: rd5_rb_arm.py <rb_review.db> <live_review.db>"""
import json, sqlite3, sys

def ro(p):
    return sqlite3.connect(f"file:{p}?mode=ro", uri=True)

rb, live = ro(sys.argv[1]), ro(sys.argv[2])
arm_cols = ("arm_name", "arm_kind", "configuration_marker", "pinned_run_id", "pinned_sha256", "retired_at")
arms = [dict(zip(arm_cols, r)) for r in rb.execute(f"SELECT {', '.join(arm_cols)} FROM arms")]
cfg = json.loads(rb.execute("SELECT configuration_json FROM arms").fetchone()[0])
stages = rb.execute(
    "SELECT s.run_id, s.stage, s.arm_name, s.model_name, "
    "(SELECT count(*) FROM run_calls c WHERE c.run_id = s.run_id AND c.stage = s.stage) "
    "FROM run_stage_configs s ORDER BY s.run_id, s.stage").fetchall()
print(json.dumps({
    "rb_12g": {
        "run_manifests": [dict(zip(("run_id", "run_kind", "end_status", "end_reason"), r)) for r in
                          rb.execute("SELECT run_id, run_kind, end_status, end_reason FROM run_manifests")],
        "arms": arms,
        "pinned_configuration_stage_keys": sorted(cfg.get("stages", {})),
        "pinned_configuration_top_keys": sorted(cfg),
        "stage_rows": [dict(zip(("run_id", "stage", "arm_name", "model", "calls"), r)) for r in stages],
        "stage_rows_declared": len(stages),
        "stage_rows_with_calls": sum(1 for r in stages if r[4]),
        "stage_rows_naming_an_arm": sum(1 for r in stages if r[2]),
        "field_events": rb.execute("SELECT count(*) FROM field_events").fetchone()[0],
        "field_events_on_the_pinned_arm": rb.execute(
            "SELECT count(*) FROM field_events WHERE arm IN (SELECT arm_name FROM arms)").fetchone()[0],
    },
    "live": {
        "arms": [dict(zip(arm_cols, r)) for r in live.execute(f"SELECT {', '.join(arm_cols)} FROM arms")],
        "workflow_ABSTRACT_ADJUDICATION_COMPLETE": live.execute(
            "SELECT status FROM workflow_state WHERE stage_name = 'ABSTRACT_ADJUDICATION_COMPLETE'"
        ).fetchone()[0],
    },
}, indent=1, sort_keys=True))
