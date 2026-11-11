"""12af-U40B — FAKE post-build probe (step 2g; B9, C28; R798(c), (d)), on 12af_U40A/fake_probe_u40a.py's seams, run on the committed
engine tree. Two worlds are built identically, W0 without the cloud stores and W1 with them:
  setup  — a scratch ReviewDatabase (24 papers, each with an eligibility event and one recorded parsed text); the tracked codebook beside
           it; a spec copy whose `enabled_arms: []` names the openai arm; rm.git_state patched clean; the cloud run opened by
           rm.open_run(… cloud_arms=[arm] …) as scripts/run_cloud_extraction.run_arm opens it (both worlds, so run ids agree); the local run
           by tests/_event_store_fixture.open_extraction_run (digest a constant, no server); local claims for papers 15–24 in the local
           form (extractor.py:709, write_extraction_events with its default paper event).
  W1 only — the cloud arm stores papers 1–10 through the BUILT path (the OpenAI transport with its client step skipped and `_create`
           faked: its own load_parsed_text, extract_paper → send, store_result) before the local audit, and papers 15–18 — local papers
           the audit has just audited — after it: fourteen papers.
  both   — audit_run for the local arm (#1 before, #2 after the second cloud batch), then each consumer the read-out's §3 table marks under
           A1-β, driven once: the workflow gate's read (run_pipeline._processing_states' body), generate_prisma_flow, get_pipeline_stats,
           the evidence-table rows and the docx export for the local arm and the cloud arm, concordance.load_arm for both arms and
           run_concordance for the pair, judge_loader.load_grid, extraction_validator.validate_all for both arms.
Each line is printed for W0 and W1; every local-arm result is compared W0 == W1 and the comparison printed. The β column is confirmed or
contradicted line by line at the end. Refusers that count any call replace openai.OpenAI, anthropic.Anthropic, ollama_chat and
audit_events.semantic_verify (expected 0 each). No server, no provider, no key, no live database; the live data root is listed before and
after.
usage, from the repository root: .venv/bin/python fake_probe_u40b.py <tmp dir>"""
import contextlib, io, json, logging, shutil, sqlite3, sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
sys.path.insert(0, str(Path.cwd())); sys.path.insert(0, str(Path.cwd() / "tests"))
logging.disable(logging.CRITICAL)
import anthropic, openai
from engine.agents import audit_events
from engine.cloud.base import CloudExtractorBase
from engine.cloud.openai_extractor import OpenAIExtractor
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.effective import effective_state, eligible_paper_ids
from engine.core.effective_config import sha256_canonical
from engine.core.events import mint_extraction_uid
from engine.core.extraction_events import legacy_record, write_extraction_events
from engine.core.parsed_text import resolve_parsed_text
from engine.core.review_spec import load_review_spec
from engine.search.models import Citation
from engine.utils import ollama_client
from _event_store_fixture import open_extraction_run, seed_eligibility
from _parsed_text_fixture import write_parsed

TMP = Path(sys.argv[1]); TMP.mkdir(parents=True, exist_ok=True)
SPEC_PATH = Path("review_specs/surgical_autonomy.yaml")
CODEBOOK = Path("data/surgical_autonomy/extraction_codebook.yaml")
LIVE_DATA = Path("data")
CBK = load_codebook(CODEBOOK)
CALLS = {"openai.OpenAI": 0, "anthropic.Anthropic": 0, "ollama_chat": 0, "semantic_verify": 0}


def refuser(name):
    def _r(*a, **k):
        CALLS[name] += 1
        raise RuntimeError(f"probe: no {name}")
    return _r


class _RefuseSDK:
    name = ""

    def __init__(self, *a, **k):
        CALLS[self.name] += 1
        raise RuntimeError("probe: no SDK client")


class _O(_RefuseSDK): name = "openai.OpenAI"
class _A(_RefuseSDK): name = "anthropic.Anthropic"


def spans():
    return [{"field_name": f["name"], "value": "Original Research" if f["name"] == "study_type" else "NR",
             "source_snippet": "Snippet for study_type." if f["name"] == "study_type" else "",
             "confidence": 0.9, "tier": f["tier"]} for tier in (1, 2, 3, 4) for f in CBK.fields_by_tier(tier)]


class FakeTransport(OpenAIExtractor):
    def __init__(self, db_path, spec_path, arm_name):
        CloudExtractorBase.__init__(self, db_path, spec_path, arm_name=arm_name)

    def _create(self, **payload):
        msg = SimpleNamespace(content=json.dumps({"fields": spans()}), reasoning_content=None)
        return SimpleNamespace(choices=[SimpleNamespace(message=msg, finish_reason="stop")],
                               usage=SimpleNamespace(prompt_tokens=1, completion_tokens=1, completion_tokens_details=None))


def cloud_store(inst, pid):
    text = inst.load_parsed_text(pid)
    r = inst.extract_paper(pid, text)
    inst.store_result(paper_id=pid, arm=inst.arm_name, model_string=inst.model_string, extracted_data=r["extracted_data"],
                      reasoning_trace=r["reasoning_trace"], prompt_text=r["prompt_text"], input_tokens=r["input_tokens"],
                      output_tokens=r["output_tokens"], reasoning_tokens=r["reasoning_tokens"], cost_usd=r["cost_usd"],
                      spans=r["spans"])


def world(name, with_cloud):
    out = {}
    d = TMP / name; d.mkdir()
    db = ReviewDatabase("u40bprobe", data_root=d)
    db.add_papers([Citation(title=f"P{i}", source="pubmed", pmid=str(i)) for i in range(1, 25)])
    pids = [r["id"] for r in db.get_papers_by_status("INGESTED")]
    for pid in pids:
        seed_eligibility(db._conn, pid)
        write_parsed(db, pid, f"Paper {pid}.\n\nSnippet for study_type. Methods follow.\n", version=1)
    db._conn.commit()
    root = Path(db.db_path).parent; db_path = str(db.db_path)
    shutil.copy2(CODEBOOK, root / "extraction_codebook.yaml")
    spec0 = load_review_spec(SPEC_PATH)
    arm = next(a.name for a in spec0.arms if a.provider == "openai"); local_arm = spec0.extraction_models.arm
    text = SPEC_PATH.read_text(); assert text.count("enabled_arms: []") == 1
    spec_path = root / "spec.yaml"; spec_path.write_text(text.replace("enabled_arms: []", f"enabled_arms: [{arm}]"))
    spec = load_review_spec(spec_path); cb = load_codebook(root / "extraction_codebook.yaml")
    cloud_run = rm.open_run(db._conn, spec, kind="extraction", stages=(), codebook=cb, cloud_arms=[arm],
                            payload_description="probe").run_id
    local_run = open_extraction_run(db, spec)
    lconn = db._conn
    expected = None
    inst = FakeTransport(db_path, str(spec_path), arm); inst.run_id = cloud_run
    expected = tuple(inst.expected_fields)
    local_pids, batch1, batch2 = pids[14:24], pids[0:10], pids[14:18]
    for pid in local_pids:
        h = sha256_canonical({"probe_local": pid})
        write_extraction_events(lconn, legacy_record(
            paper_id=pid, arm=local_arm, run_id=local_run, extraction_uid=mint_extraction_uid(),
            parsed_text=resolve_parsed_text(lconn, pid), model="fake-local", model_digest="a" * 64, expected=expected, spans=spans(),
            attempts=1, presented_context_sha256=h, context_chain=(h,)), sentinels=frozenset(cb.absence_sentinels))
    out["setup"] = f"cloud run {cloud_run}, local run {local_run}; local claims on papers {local_pids}"
    if with_cloud:
        for pid in batch1:
            cloud_store(inst, pid)
    r1 = audit_events.audit_run(lconn, spec, run_id=local_run, arm=local_arm, review_dir=root)
    out["audit #1 (local)"] = (f"papers_audited={r1.papers_audited} located={r1.located} not_located={r1.not_located} "
                               f"skipped_by_state={list(r1.skipped_by_state)} skipped_refused={list(r1.skipped_refused)}")
    if with_cloud:
        for pid in batch2:
            cloud_store(inst, pid)
    r2 = audit_events.audit_run(lconn, spec, run_id=local_run, arm=local_arm, review_dir=root)
    out["audit #2 (local)"] = (f"papers_audited={r2.papers_audited} located={r2.located} not_located={r2.not_located} "
                               f"skipped_by_state={list(r2.skipped_by_state)}")
    held = {r[0] for r in lconn.execute("SELECT DISTINCT paper_id FROM field_events WHERE arm = ?", (local_arm,))}
    aud = sorted({r[0] for r in lconn.execute("SELECT paper_id FROM paper_events WHERE to_state = 'audited_ai'")})
    out["audited_ai papers"] = f"{aud}; of them, papers the local arm holds no claim on: {[p for p in aud if p not in held]}"
    out["cloud store counts"] = str({t: lconn.execute(q, (arm,)).fetchone()[0] for t, q in (
        ("cloud_extractions", "SELECT COUNT(*) FROM cloud_extractions WHERE arm = ?"),
        ("field_events", "SELECT COUNT(*) FROM field_events WHERE arm = ?"),
        ("claim_inputs", "SELECT COUNT(*) FROM claim_inputs WHERE arm = ?"),
        ("paper_events naming the arm", "SELECT COUNT(*) FROM paper_events WHERE json_extract(payload_json, '$.arm') = ?"))})
    states = {pid: effective_state(lconn, pid).processing for pid in eligible_paper_ids(lconn)}
    out["workflow gate's read (processing per corpus paper)"] = str(sorted(states.items()))
    out["papers at 'extracted' (AI_AUDIT_COMPLETE_STAGE pending)"] = str(sorted(p for p, s in states.items() if s == "extracted"))
    from engine.exporters.prisma import generate_prisma_flow
    with contextlib.redirect_stdout(io.StringIO()):
        pf = generate_prisma_flow(db)
    out["generate_prisma_flow"] = json.dumps(pf, default=str, sort_keys=True)
    st = db.get_pipeline_stats()
    out["get_pipeline_stats"] = json.dumps(st, default=str, sort_keys=True)
    from engine.exporters.evidence_table import _build_evidence_rows
    from engine.exporters.docx_export import export_evidence_docx
    for label, a in (("local", local_arm), ("cloud", arm)):
        headers, rows = _build_evidence_rows(db, spec, arm=a)
        out[f"evidence rows ({label})"] = json.dumps([headers] + [list(r) for r in rows], default=str)
        ix = {h: i for i, h in enumerate(headers)}
        out[f"evidence rows ({label}) summary"] = (f"{len(rows)} rows; study_type 'Original Research' "
                                                  f"{sum(1 for r in rows if r[ix['study_type']] == 'Original Research')}; "
                                                  f"processing column values {sorted({str(r[ix['processing']]) for r in rows})}")
        p = TMP / f"{name}_{label}.docx"
        export_evidence_docx(db, spec, str(p), arm=a)
        out[f"docx ({label})"] = f"written, {p.stat().st_size > 0}"
    from engine.analysis import concordance
    for label, a in (("local", local_arm), ("cloud", arm)):
        data = concordance.load_arm(db_path, a)
        out[f"concordance.load_arm ({label})"] = json.dumps({str(k): v for k, v in sorted(data.items())}, default=str, sort_keys=True)
        out[f"concordance.load_arm ({label}) summary"] = (f"{len(data)} papers; study_type values "
                                                         f"{sorted({str(v.get('study_type')) for v in data.values()})}")
    rc = concordance.run_concordance(db_path, local_arm, arm)
    out["concordance.run_concordance (local, cloud)"] = f"n_papers={rc.n_papers} a_only={rc.n_papers_a_only} b_only={rc.n_papers_b_only}"
    from analysis.paper1 import judge_loader
    ro = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    ins = judge_loader.load_grid(ro, root, judge_loader.load_codebook(root / "extraction_codebook.yaml"), root / "extraction_codebook.yaml")
    for label, a in (("local", local_arm), ("cloud", arm)):
        outs = sorted((i.paper_id, i.field_name, str(o.value)) for i in ins for o in i.arms if o.arm_name == a)
        out[f"judge_loader.load_grid ({label} arm outputs)"] = json.dumps(outs)
        out[f"judge_loader.load_grid ({label}) summary"] = (f"{len(ins)} JudgeInputs; {label} ArmOutputs {len(outs)}, with a value "
                                                           f"{sum(1 for x in outs if x[2] != 'None')}")
    from engine.validators import extraction_validator as ev
    for label, a in (("local", local_arm), ("cloud", arm)):
        issues, bleeds = ev.validate_all(ro, cb, arm=a)
        out[f"validate_all ({label})"] = f"issues {len(issues)}, bleeds {len(bleeds)}"
    ro.close(); inst.close(); db.close()
    return out


live_before = sorted(p.name for p in LIVE_DATA.iterdir())
with patch.object(openai, "OpenAI", _O), patch.object(anthropic, "Anthropic", _A), \
     patch.object(ollama_client, "ollama_chat", refuser("ollama_chat")), \
     patch.object(audit_events, "semantic_verify", refuser("semantic_verify")), \
     patch.object(rm, "git_state", lambda *a, **k: rm.GitState(commit="b" * 40, dirty=False, tag=None)):
    w0 = world("w0", with_cloud=False)
    w1 = world("w1", with_cloud=True)

LOCAL = ["audit #1 (local)", "audit #2 (local)", "audited_ai papers", "workflow gate's read (processing per corpus paper)",
         "papers at 'extracted' (AI_AUDIT_COMPLETE_STAGE pending)", "generate_prisma_flow", "get_pipeline_stats",
         "evidence rows (local)", "docx (local)", "concordance.load_arm (local)", "judge_loader.load_grid (local arm outputs)",
         "validate_all (local)"]
SHOW_FULL = {"audit #1 (local)", "audit #2 (local)", "audited_ai papers", "papers at 'extracted' (AI_AUDIT_COMPLETE_STAGE pending)",
             "concordance.run_concordance (local, cloud)", "cloud store counts", "setup", "validate_all (local)", "validate_all (cloud)",
             "docx (local)", "docx (cloud)"}
for k in w0:
    if k in SHOW_FULL or k.endswith("summary"):
        print(f"{k}\n  W0: {w0[k]}\n  W1: {w1[k]}")
    else:
        print(f"{k}\n  W0: {len(w0[k])} chars, W1: {len(w1[k])} chars (compared below){'' if k in LOCAL else ' — cloud-arm or shared: W1 differs by design' if w0[k] != w1[k] else ''}")
print("\n=== local-arm results, W0 == W1 (a difference is a STOP before the gate)")
diffs = []
for k in LOCAL:
    eq = w0[k] == w1[k]
    print(f"  {k}: equal {eq}")
    if not eq:
        diffs.append(k)
print(f"local-arm differences: {diffs or 'none'}")
print(f"\n=== the record\ncalls refused and counted: {CALLS}")
print(f"SDK client constructions: {CALLS['openai.OpenAI'] + CALLS['anthropic.Anthropic']}")
live_after = sorted(p.name for p in LIVE_DATA.iterdir())
print(f"live data root entries before {len(live_before)}, after {len(live_after)}, equal: {live_before == live_after}; "
      f"'u40bprobe' under it: {'u40bprobe' in live_after}")
