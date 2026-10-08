import sqlite3, json, hashlib, types, sys, os
sys.path.insert(0, os.getcwd())
DB = "data/surgical_autonomy/review.db"
c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True); c.row_factory = sqlite3.Row
q = lambda s: c.execute(s).fetchall()
print("receipts", q("select count(*) from schema_migrations")[0][0],
      [tuple(r) for r in q("select migration_id, file_sha256 from schema_migrations order by migration_id desc limit 1")])
print("arms", [tuple(r) for r in q("select arm_name, configuration_marker, pinned_sha256 from arms")])
for t in ("paper_events","parsed_text_refs","review_identities","field_events","run_manifests","run_stage_configs","run_calls","claim_inputs","audit_verdicts"):
    print(t, q(f"select count(*) from {t}")[0][0])
print("refs with sha", q("select count(*) from parsed_text_refs where parsed_text_sha256 is not null")[0][0])
print("multi-version papers", [tuple(r) for r in q("select paper_id, group_concat(parsed_text_version) from parsed_text_refs group by paper_id having count(*)>1")])
print("papers", [tuple(r) for r in q("select status, count(*) from papers group by status order by 2 desc")], q("select count(*) from papers")[0][0])
print("workflow_state", [tuple(r) for r in q("select * from workflow_state")])
from engine.exporters.prisma import generate_prisma_flow, validate_prisma_counts
db = types.SimpleNamespace(_conn=c, db_path=DB)
flow = generate_prisma_flow(db)
v = validate_prisma_counts(db, flow)
print("prisma", {k: flow.get(k) for k in ("eligible","extraction_in_progress","verification_pending","total_identified")}, "| keys:", sorted(flow)[:40])
print("prisma valid", v)
from engine.core.selection import select_for_extraction
s = select_for_extraction(c, arm="local_deepseek_r1_32b")
print("selection", len(s.to_extract), len(s.skipped_asserted), len(s.skipped_refused))
base = json.load(open("docs/session-reports/input-identity-01/parsed_text_hashes_20260923T214241Z.json"))
mm = 0
for e in base["entries"]:
    h = hashlib.sha256(open(e["parsed_text_path"], "rb").read()).hexdigest()
    mm += (h != e["sha256"])
print("hash baseline entries", len(base["entries"]), "mismatches", mm)
