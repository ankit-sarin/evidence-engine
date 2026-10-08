"""F13: abstract-screener persistence across a failure and across re-invocation.
Synthetic DB under the scratch dir; screen_paper and preflight are stubbed (no model)."""
import json, shutil, sys, types
from pathlib import Path
from unittest.mock import patch
import engine.agents.screener as sc
from engine.core.database import ReviewDatabase
from engine.search.models import Citation

root = Path.cwd() / "f13_tmp"
shutil.rmtree(root, ignore_errors=True)
assert "evidence-engine" not in str(root.resolve())
db = ReviewDatabase("synth", data_root=root)
db.add_papers([Citation(title=f"P{i}", source="pubmed", pmid=str(i)) for i in (1, 2)])
spec = types.SimpleNamespace(screening_models=types.SimpleNamespace(primary="m-primary", verification="m-verifier"))
D = lambda d: sc.ScreeningDecision(decision=d, rationale="r", confidence=0.9)
rows = lambda t: [tuple(r) for r in db._conn.execute(f"SELECT * FROM {t} ORDER BY id")]
status = lambda: {r[0]: r[1] for r in db._conn.execute("SELECT id, status FROM papers")}

calls = {"n": 0}
def flaky(paper, spec, pass_number, model=None, role="primary"):
    calls["n"] += 1
    if calls["n"] == 2:                       # paper 1, pass 2: the service dies
        raise TimeoutError("simulated watchdog exhaustion")
    return D("include")

with patch("engine.utils.ollama_preflight.require_preflight"), patch.object(sc, "screen_paper", flaky):
    try: sc.run_screening(db, spec)
    except TimeoutError as e: print("run 1 died:", e)
    print("after run 1: status", status(), "| checkpoint exists:", sc._checkpoint_path(db).exists())
    print("  decisions (id, paper, pass, decision, rationale, model, at):")
    for r in rows("abstract_screening_decisions"): print("   ", r[:4], r[5])
    sc.run_screening(db, spec)
    print("after run 2: status", status())
    for r in rows("abstract_screening_decisions"): print("   ", r[:4], r[5])
n1 = db._conn.execute("SELECT COUNT(*) FROM abstract_screening_decisions WHERE paper_id=1 AND pass_number=1").fetchone()[0]
print("paper 1 pass-1 rows:", n1, "| columns:", [c[1] for c in db._conn.execute("PRAGMA table_info(abstract_screening_decisions)")])

vcalls = []
def verifier(paper, spec, pass_number, model=None, role="primary"):
    vcalls.append(paper["id"]); return D("include")
with patch("engine.utils.ollama_preflight.require_preflight"), patch.object(sc, "screen_paper", verifier):
    sc.run_verification(db, spec); a = len(vcalls)
    sc.run_verification(db, spec); b = len(vcalls) - a
print("verification: invocation 1 made", a, "calls; identical invocation 2 made", b, "calls")
print("  abstract_verification_decisions rows:", len(rows("abstract_verification_decisions")),
      "for", len(status()), "papers | verification checkpoint exists:",
      sc._checkpoint_path(db, suffix="_verification").exists())
print("  paper_events rows:", db._conn.execute("SELECT COUNT(*) FROM paper_events").fetchone()[0])
db.close()
