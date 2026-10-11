"""L12-5: (a) an unexpected field is logged 'dropped (R118)' and then stored; (b) telemetry says 'stored'
for a duplicate-field response that store_result then refuses. Stub DB, no client."""
import json, logging, sqlite3, tempfile, sys
from pathlib import Path
from engine.cloud.base import CloudExtractorBase
from engine.cloud.schema import init_cloud_tables
logging.basicConfig(level=logging.WARNING, stream=sys.stdout, format="LOG %(levelname)s %(message)s")
root = Path(tempfile.mkdtemp(dir="."))
db = root / "review.db"
c = sqlite3.connect(db); c.execute("CREATE TABLE papers (id INTEGER PRIMARY KEY)"); c.execute("INSERT INTO papers VALUES (1)"); c.commit(); c.close()
init_cloud_tables(str(db))
ex = object.__new__(CloudExtractorBase)
ex._conn = sqlite3.connect(db); ex.expected_fields = ("a", "b"); ex._last_salvage = None
ex.codebook_hash = ex.codebook_sha256 = "syn"; ex._review_dir = root; ex.arm_name = "syn_arm"
sp = lambda n: {"field_name": n, "value": "v", "source_snippet": "q", "confidence": 1.0, "tier": 1}
ex.store_result(1, "syn_arm", "m", {}, "", "", 0, 0, 0, 0.0, [sp("a"), sp("b"), sp("invented_field")])
print("(a) stored fields:", [r[0] for r in ex._conn.execute("SELECT field_name FROM cloud_evidence_spans")])
# (b)
ex.extract_paper = lambda pid, text: {"spans": [sp("a"), sp("b"), sp("b")]}
res = ex.extract_with_completeness(1, "text")
try:
    ex.store_result(1, "syn_arm", "m", {}, "", "", 0, 0, 0, 0.0, res["spans"])
except Exception as e:
    print("(b) store_result raised:", type(e).__name__)
rows = [json.loads(l) for p in root.rglob("*.jsonl") for l in p.read_text().splitlines()]
print("(b) telemetry outcomes:", [r.get("outcome") for r in rows], "| cloud_extractions rows:",
      ex._conn.execute("SELECT COUNT(*) FROM cloud_extractions").fetchone()[0])
