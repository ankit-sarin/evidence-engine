"""12l-R612 step 4 — is live's extraction arm pinned? One mode=ro connection, reads only.
usage, from the repository root: pin_check.py <review.db> <spec.yaml>   (stdout: JSON)"""
import json, sqlite3, sys, yaml
db, spec_path = sys.argv[1:3]
spec = yaml.safe_load(open(spec_path, encoding="utf-8"))
arm = spec["extraction_models"]["arm"]
c = sqlite3.connect(f"file:{db}?mode=ro", uri=True); c.row_factory = sqlite3.Row
cols = [r["name"] for r in c.execute("PRAGMA table_info(arms)")]
arms = [dict(r) for r in c.execute("SELECT * FROM arms ORDER BY arm_name")]
row = [a for a in arms if a["arm_name"] == arm]
EXTRACT = ("extract_pass1", "extract_pass2", "extract_retry_snippet", "elicitation_pass1")
q = ",".join("?" * len(EXTRACT))
decl = [dict(r) for r in c.execute(
    f"SELECT run_id, stage, arm_name FROM run_stage_configs WHERE stage IN ({q}) OR stage LIKE 'cloud:%' ORDER BY run_id, stage", EXTRACT)]
out = {
    "database": db, "spec": spec_path, "spec_extraction_arm": arm,
    "spec_arms_declared": [a["name"] for a in spec.get("arms", [])],
    "arms_columns": cols,
    "arms_rows_as_stored": arms,
    "extraction_arm_row": row[0] if row else None,
    "extraction_arm_pinned": bool(row and (row[0].get("configuration_marker") == "pinned" or row[0].get("pinned_run_id") is not None)),
    "run_manifests_rows": c.execute("SELECT COUNT(*) FROM run_manifests").fetchone()[0],
    "run_stage_configs_rows": c.execute("SELECT COUNT(*) FROM run_stage_configs").fetchone()[0],
    "manifest_stage_rows_declaring_an_extraction_stage": decl,
    "field_events_rows": c.execute("SELECT COUNT(*) FROM field_events").fetchone()[0],
}
c.close()
print(json.dumps(out, indent=1, ensure_ascii=False, default=str))
