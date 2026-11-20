"""12ai-U41B 2f (E6): a default run's manifest key set — run_pipeline's own _open_run_manifest with no flag, on a scratch
review under a temporary data root (digest, git and the open's environment read injected; no server, no live database).
usage, from the repo root: python3 -I -B keyset_read.py"""
import json, shutil, sys, tempfile
from pathlib import Path
sys.path.insert(0, "."); sys.path.insert(0, "tests")
import scripts.run_pipeline as rp
from engine.core import run_manifest as rm
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.utils import ollama_preflight as pf
from _event_store_fixture import FIXTURE_DIGEST
pf.read_ollama_env = lambda: ("", {})
real = rm.open_run
rm.open_run = lambda *a, **k: real(*a, digest_fn=lambda m: FIXTURE_DIGEST,
                                   git=rm.GitState(commit="0" * 40, dirty=False, tag=None), host="fixture", **k)
tmp = Path(tempfile.mkdtemp())
db = ReviewDatabase("keys", data_root=tmp)
shutil.copy2("data/surgical_autonomy/extraction_codebook.yaml", Path(db.db_path).parent / "extraction_codebook.yaml")
spec = load_review_spec("review_specs/surgical_autonomy.yaml")
run_id = rp._open_run_manifest(db, spec, rp.STAGES.index("extract"))
body = json.loads(db._conn.execute("SELECT manifest_json FROM run_manifests WHERE run_id = ?", (run_id,)).fetchone()[0])
print("default run manifest keys:", sorted(body))
db.close(); shutil.rmtree(tmp)
