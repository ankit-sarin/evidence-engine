"""F07 — methods_section on a SYNTHETIC in-memory db + stub spec. No ReviewDatabase,
no model, no network. Tables carry only the columns the module's own queries read."""
import sqlite3, logging
from types import SimpleNamespace as NS
from unittest.mock import patch
logging.disable(logging.CRITICAL)
import engine.exporters.methods_section as ms

conn = sqlite3.connect(":memory:"); conn.row_factory = sqlite3.Row
conn.executescript("""
CREATE TABLE ft_screening_decisions (paper_id INT, model TEXT);
CREATE TABLE run_stage_configs (run_id INT, stage TEXT, stage_kind TEXT, model_name TEXT);
CREATE TABLE run_calls (run_id INT, stage TEXT, paper_id INT);
CREATE TABLE paper_events (run_id INT, paper_id INT, to_state TEXT);
-- history: FT screening by an OLD model and the current one (all rows aggregate)
INSERT INTO ft_screening_decisions VALUES (1,'old-ft-model:1'),(2,'old-ft-model:1'),(2,'qwen3:32b');
-- run 1: really extracted 2 papers with model A
INSERT INTO run_stage_configs VALUES (1,'extract_pass1','extract_pass1','model-A'),(1,'extract_pass2','extract_pass2','model-A'),(1,'audit','audit','auditor-A');
INSERT INTO run_calls VALUES (1,'extract_pass1',1),(1,'extract_pass2',1),(1,'extract_pass1',2);
INSERT INTO paper_events VALUES (1,1,'audited_ai'),(1,2,'audited_ai');
-- run 2: declared extract+audit stages, made ZERO calls (nothing pending)
INSERT INTO run_stage_configs VALUES (2,'extract_pass1','extract_pass1','model-B'),(2,'extract_pass2','extract_pass2','model-B'),(2,'audit','audit','auditor-B');
-- run 3: export-only (--skip-to export): no stage rows at all
""")
print("run 2 extraction models:", ms._run_extraction_models(conn, 2))
print("run 2 audit models     :", ms._run_audit_models(conn, 2))
print("run 3 extraction models:", ms._run_extraction_models(conn, 3))

arms = {"openai_x": NS(provider="openai", model="o4-mini-2025-04-16")}
spec = NS(search_strategy=NS(databases=["PubMed"], date_range=(2015, 2025), query_terms=["q"]),
          screening_models=NS(primary="CURRENT-SPEC-screener"),
          ft_screening_models=NS(primary="x", verifier="y"),
          cloud=NS(enabled_arms=["openai_x"]), arm=lambda n: arms[n])
flow = dict(records_by_source={"pubmed": 10}, records_identified=10, duplicates_removed=0,
            studies_included=0, full_text_assessed=2, records_screened=10,
            records_excluded=5, screen_flagged=1)
db = NS(_conn=conn, db_path="unused")
with patch.object(ms, "generate_prisma_flow", lambda d: flow), \
     patch.object(ms, "load_codebook_beside", lambda p: NS(fields=[0] * 20)):
    for rid in (2, 3):
        print(f"\n--- run_id={rid} (no cloud call was ever made; cloud tables do not even exist) ---")
        print(ms.generate_methods_section(db, spec, run_id=rid))
