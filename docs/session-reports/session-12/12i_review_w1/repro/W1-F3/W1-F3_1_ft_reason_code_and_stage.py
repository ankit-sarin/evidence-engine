"""W1-F3 reproducer 1 — FT screener: (A) out-of-vocabulary reason_code ends the run and
wedges it; (B) decision/reason_code pairing is unchecked; (C) verify-only run completes
FULL_TEXT_SCREENING_COMPLETE while PARSED papers were never screened.
Synthetic DB under ./data_root; Ollama client faked; httpx.send refused; no git, no network."""
import json, shutil, sys, tempfile
from pathlib import Path
from types import SimpleNamespace

REPO = Path("/home/ankitsarin/projects/evidence-engine")
sys.path.insert(0, str(REPO / "tests"))          # for the _parsed_text_fixture helper only (read, not run)

import httpx
def _refuse(*a, **k): raise AssertionError("real HTTP attempted")
httpx.Client.send = _refuse
httpx.AsyncClient.send = _refuse

from engine.adjudication.workflow import ensure_workflow_table
from engine.agents import ft_screener as ft
from engine.core import run_manifest as rm
from engine.core.database import ReviewDatabase
from engine.core.review_paths import load_spec_for
from engine.search.models import Citation
from engine.utils import ollama_client as oc
from engine.utils import ollama_preflight as pf
from _parsed_text_fixture import write_parsed

CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
DIGESTS = {"qwen3:32b": "1" * 64, "gemma3:27b": "2" * 64}

class Fake:
    def __init__(self):
        self.calls = []; self.answers = {"qwen3:32b": [], "gemma3:27b": []}
        self._client = SimpleNamespace(base_url="http://repro.invalid")
    def show(self, model):
        return SimpleNamespace(modelinfo={"general.context_length": 131_072})
    def chat(self, **kw):
        self.calls.append(kw)
        content = "ok" if "format" not in kw else json.dumps(self.answers[kw["model"]].pop(0))
        chars = oc.message_chars(kw.get("messages"))
        return SimpleNamespace(message=SimpleNamespace(content=content, thinking=None),
                               prompt_eval_count=max(1, int(chars * 0.3)), done_reason="stop", eval_count=10)

fake = Fake()
oc._client = fake
pf.check_ollama_env = lambda: None
pf.ollama.ps = lambda: {"models": []}
import subprocess
def _nosub(*a, **k): raise AssertionError(f"subprocess attempted: {a}")
subprocess.run = _nosub

spec = load_spec_for("surgical_autonomy", str(REPO / "review_specs/surgical_autonomy.yaml"))
root = Path(tempfile.mkdtemp(prefix="data_root_", dir="."))

def newdb(name):
    d = ReviewDatabase(name, data_root=root)
    shutil.copy2(REPO / "data/surgical_autonomy/extraction_codebook.yaml",
                 Path(d.db_path).parent / "extraction_codebook.yaml")
    return d

def paper(db, n, upto="PARSED"):
    db.add_papers([Citation(title=f"Paper {n}", abstract="Autonomous suturing.", pmid=f"R{n}",
                            source="pubmed", authors=["A"], journal="J", year=2024)])
    pid = db._conn.execute("SELECT id FROM papers WHERE pmid=?", (f"R{n}",)).fetchone()[0]
    for s in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED"):
        db.update_status(pid, s)
    write_parsed(db, pid, "# Paper\n\nAutonomous robotic suturing in surgery. " * 40)
    if upto == "FT_ELIGIBLE":
        db.update_status(pid, "FT_ELIGIBLE")
    return pid

def invoke(db, **kw):
    return ft.run_ft_invocation(db, spec, git=CLEAN, digest_fn=DIGESTS.__getitem__, **kw)

def status(db): return dict(db._conn.execute("SELECT id, status FROM papers").fetchall())

print("vocabulary:", spec.eligibility.reason_codes())
fmt = __import__("engine.core.effective_config", fromlist=["stage_config"]).stage_config("ft_screen_primary", spec).kwargs().get("format")
print("format schema reason_code:", fmt["properties"]["reason_code"])

# ── A ──
db = newdb("a")
p1, p2 = paper(db, 1), paper(db, 2)
bad = {"decision": "FT_EXCLUDE", "reason_code": "not_surgical_robotics", "rationale": "r", "confidence": 0.9}
for attempt in (1, 2):
    fake.answers["qwen3:32b"] = [dict(bad), dict(bad)]
    try:
        invoke(db, screen_only=True)
        print(f"A run {attempt}: completed")
    except Exception as e:
        print(f"A run {attempt}: raised {type(e).__name__}: {str(e)[:90]}")
    print("   statuses:", status(db), "| ft_screening_decisions:",
          db._conn.execute("SELECT COUNT(*) FROM ft_screening_decisions").fetchone()[0],
          "| manifests:", [tuple(r) for r in db._conn.execute("SELECT run_id, end_status FROM run_manifests")],
          "| checkpoint exists:", ft._checkpoint_path(db).exists())
print("   run_calls outcomes:", [tuple(r) for r in db._conn.execute(
    "SELECT stage, outcome, COUNT(*) FROM run_calls GROUP BY stage, outcome")])
db.close()

# ── B ──
db = newdb("b")
p1, p2 = paper(db, 1), paper(db, 2)
fake.answers["qwen3:32b"] = [
    {"decision": "FT_EXCLUDE", "reason_code": "eligible", "rationale": "r", "confidence": 0.9},
    {"decision": "FT_ELIGIBLE", "reason_code": "wrong_specialty", "rationale": "r", "confidence": 0.9}]
invoke(db, screen_only=True)
print("B statuses:", status(db))
print("B decision rows:", [tuple(r) for r in db._conn.execute(
    "SELECT paper_id, decision, reason_code FROM ft_screening_decisions")])
print("B paper_events:", [tuple(r) for r in db._conn.execute(
    "SELECT paper_id, event_type, to_state FROM paper_events")])
db.close()

# ── C ──
db = newdb("c")
ensure_workflow_table(db._conn)
e1 = paper(db, 1, upto="FT_ELIGIBLE")
u2, u3 = paper(db, 2), paper(db, 3)
fake.answers["gemma3:27b"] = [{"decision": "FT_ELIGIBLE", "rationale": "r", "confidence": 0.9}]
invoke(db, verify_only=True)
print("C statuses:", status(db))
print("C workflow FULL_TEXT_SCREENING_COMPLETE:", tuple(db._conn.execute(
    "SELECT status, metadata FROM workflow_state WHERE stage_name='FULL_TEXT_SCREENING_COMPLETE'").fetchone()))
db.close()
