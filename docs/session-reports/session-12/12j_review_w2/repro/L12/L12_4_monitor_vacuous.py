"""L12-4: the cloud run's post-extraction collapse gate reads the event store; store_result writes only
cloud_extractions / cloud_evidence_spans. 12 papers, one categorical field, one identical value."""
import logging, shutil, sqlite3, tempfile
from pathlib import Path
import yaml
from engine.cloud.base import CloudExtractorBase
from engine.cloud.schema import init_cloud_tables
from engine.validators.distribution_monitor import run_post_extraction_check, DistributionCollapseError
from tests._event_store_fixture import (ensure_event_store, fixture_run,
                                        seed_pre_manifest_paper_event, add_values)
logging.basicConfig(level=logging.CRITICAL)
REPO = Path("/home/ankitsarin/projects/evidence-engine")
root = Path(tempfile.mkdtemp(dir=".")) / "syn_review"; root.mkdir()
cb_path = root / "extraction_codebook.yaml"
shutil.copy(REPO / "data/surgical_autonomy/extraction_codebook.yaml", cb_path)   # a codebook file, not a database
field = next(f["name"] for f in yaml.safe_load(cb_path.read_text())["fields"] if f["type"] == "categorical")
db = root / "review.db"
ensure_event_store(db); init_cloud_tables(str(db))
conn = sqlite3.connect(db); conn.row_factory = sqlite3.Row; conn.execute("PRAGMA foreign_keys=ON")
for pid in range(1, 13):
    conn.execute("INSERT OR IGNORE INTO papers (id) VALUES (?)", (pid,))
    seed_pre_manifest_paper_event(conn, pid)          # eligible
fixture_run(conn, "cloud_syn")                        # the arm is registered, as open_run's pin would
conn.commit()
ex = object.__new__(CloudExtractorBase)
ex._conn = conn; ex.expected_fields = (field,); ex._last_salvage = None
ex.codebook_hash = ex.codebook_sha256 = "syn"
ex.db_path = str(db); ex._review_dir = root; ex.arm_name = "cloud_syn"
for pid in range(1, 13):
    ex.store_result(pid, "cloud_syn", "syn-model", {}, "", "", 0, 0, 0, 0.0,
        [{"field_name": field, "value": "SAME", "source_snippet": "q", "confidence": 1.0, "tier": 1}])
print("field:", field, "| cloud_evidence_spans:", conn.execute(
    "SELECT value, COUNT(*) FROM cloud_evidence_spans GROUP BY value").fetchall().__len__(), "distinct value over",
    conn.execute("SELECT COUNT(*) FROM cloud_evidence_spans").fetchone()[0], "rows",
    "| field_events for arm:", conn.execute("SELECT COUNT(*) FROM field_events WHERE arm='cloud_syn'").fetchone()[0])
out = ex.run_distribution_check({"extracted": 12, "failed": 0})      # exactly what run() calls
r = next(x for x in out["results"] if x["field_name"] == field)
print("cloud gate: skipped=%s collapsed=%d | %s total_non_null=%d status=%s" % (
    out["skipped"], out["collapsed"], field, r["total_non_null"], r["status"]))
conn.close()
# control: the same 12 identical values where the monitor looks
add_values(db, "ctl_arm", field, ["SAME"] * 12)
try:
    run_post_extraction_check(db, "syn_review", "ctl_arm", cb_path, extracted_count=12)
    print("control: no raise")
except DistributionCollapseError as e:
    print("control (same values in the event store): DistributionCollapseError raised")
