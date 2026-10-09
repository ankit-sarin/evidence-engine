"""Shared synthetic-review setup for the W1-F1 reproducers. No model, no network,
no live database: a ReviewDatabase under a temp dir here, a MagicMock Ollama client."""
import json, logging, shutil, sys, tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

REPO = Path("/home/ankitsarin/projects/evidence-engine")
sys.path.insert(0, str(REPO / "tests"))          # fixture helpers only (not test files)
logging.disable(logging.CRITICAL)

from engine.agents.models import EvidenceSpan, ExtractionOutput
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.utils import ollama_client as oc
from _event_store_fixture import seed_eligibility
from _parsed_text_fixture import write_parsed

oc.effective_ceiling = lambda model, options=None: 131_072
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"   # a YAML, read only
CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
EXTRACTION = ("extract_pass1", "extract_pass2", "extract_retry_snippet")
TEXT = "This RCT used the STAR robot for autonomous suturing."


def make_review():
    tmp = Path(tempfile.mkdtemp(dir=".", prefix="tmpdb_")).resolve()
    db = ReviewDatabase("w1f1", data_root=tmp)
    assert str(db.db_path).startswith("/home/ankitsarin/scratch/")
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    cb = load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    spec = load_review_spec(str(REPO / "review_specs" / "surgical_autonomy.yaml"))
    h = rm.open_run(db._conn, spec, kind="extraction", codebook=cb, stages=EXTRACTION,
                    git=CLEAN, digest_fn=lambda m: "a" * 64)
    db._conn.execute("INSERT INTO papers (id, title, source, status, created_at, updated_at) "
                     "VALUES (1, 't', 's', 'PARSED', 'n', 'n')")
    seed_eligibility(db._conn, 1)
    write_parsed(db, 1, TEXT)
    db._conn.commit()
    return tmp, db, cb, spec, h.run_id


def resp(content, thinking="The paper reports an RCT."):
    return SimpleNamespace(message=SimpleNamespace(content=content, thinking=thinking),
                           done_reason="stop", prompt_eval_count=8000, eval_count=5)


def fields(cb, value="A value", overrides=None):
    out = []
    for tier in (1, 2, 3, 4):
        for f in cb.fields_by_tier(tier):
            kw = dict(field_name=f["name"], value=value,
                      source_snippet=f"Snippet for {f['name']}.", confidence=0.9, tier=f["tier"])
            kw.update((overrides or {}).get(f["name"], {}))
            out.append(EvidenceSpan(**kw))
    return out


def pass2(cb, **kw):
    return resp(ExtractionOutput(fields=fields(cb, **kw)).model_dump_json())


def client(side_effect):
    c = MagicMock()
    c.chat.side_effect = side_effect
    return patch.object(oc, "_client", c)


def paper_events(db):
    return [(r["event_type"], r["to_state"], r["reason_code"]) for r in db._conn.execute(
        "SELECT event_type, to_state, reason_code FROM paper_events WHERE paper_id = 1 "
        "AND actor_name = 'extractor' ORDER BY event_id")]


def cleanup(tmp, db):
    db.close()
    shutil.rmtree(tmp)
