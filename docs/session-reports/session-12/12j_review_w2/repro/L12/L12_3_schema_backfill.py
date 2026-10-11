"""L12-3: init_cloud_tables on a pre-018-shape table with NULL confidence/tier invents 0.0 / 1."""
import sqlite3, tempfile, os
from engine.cloud.schema import init_cloud_tables
p = os.path.join(tempfile.mkdtemp(dir="."), "syn.db")
c = sqlite3.connect(p)
c.executescript("""
CREATE TABLE papers (id INTEGER PRIMARY KEY);
CREATE TABLE cloud_extractions (id INTEGER PRIMARY KEY, paper_id INTEGER NOT NULL, arm TEXT NOT NULL,
  model_string TEXT NOT NULL, extracted_data TEXT, reasoning_trace TEXT, prompt_text TEXT, input_tokens INTEGER,
  output_tokens INTEGER, reasoning_tokens INTEGER, cost_usd REAL, extraction_schema_hash TEXT,
  extracted_at TEXT NOT NULL, codebook_hash TEXT, codebook_sha256 TEXT);
CREATE TABLE cloud_evidence_spans (id INTEGER PRIMARY KEY,
  cloud_extraction_id INTEGER NOT NULL REFERENCES cloud_extractions(id), field_name TEXT NOT NULL,
  value TEXT, source_snippet TEXT, confidence REAL, tier INTEGER, notes TEXT,
  UNIQUE(cloud_extraction_id, field_name));
INSERT INTO papers VALUES (1);
INSERT INTO cloud_extractions (id, paper_id, arm, model_string, extracted_at) VALUES (1,1,'a','m','t');
INSERT INTO cloud_evidence_spans (cloud_extraction_id, field_name, value, confidence, tier) VALUES (1,'f1','v',NULL,NULL);
INSERT INTO cloud_evidence_spans (cloud_extraction_id, field_name, value, confidence, tier) VALUES (1,'f2','v',0.8,3);
""")
c.commit()
print("before:", c.execute("SELECT field_name, confidence, tier FROM cloud_evidence_spans").fetchall()); c.close()
init_cloud_tables(p)     # what --progress, --dry-run, every extractor constructor and migration 014 call
c = sqlite3.connect(p)
print("after :", c.execute("SELECT field_name, confidence, tier FROM cloud_evidence_spans").fetchall())
print("schema_migrations table present:", bool(c.execute(
    "SELECT 1 FROM sqlite_master WHERE name='schema_migrations'").fetchone()))
