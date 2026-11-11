"""12af-U40A — FAKE probe for U40's Phase A (B9, C28; Wave 2 row 25; R797(f)), on 12ae_U28A/fake_probe_u28a.py's scratch setup.
  (i)   THE ENTRY'S TEST AT HEAD — twelve papers through store_result with one identical categorical value, then the gate as
        base.py calls it (base.run_distribution_check).
  (ii)  THE WRITER — the same values handed to write_extraction_events for the registered cloud arm, in the form Q1 found
        (engine.core.extraction_events.legacy_record, the run's own run_id): each refusal the form can meet, the accepted form,
        its paper event, a second write, the transaction it shares with store_result's INSERT, and the gate after.
  (iii) THE CONSUMERS — each grid consumer that runs on a scratch database with no model or provider call, driven once with the
        cloud arm holding claims; the local audit stage and the processing axis with cloud events interleaved.
  (iv)  U28's two leftovers — the monitor CLI's main for a registered arm with no claims, and the strict case.
Seams: a scratch ReviewDatabase under a temp directory (24 papers, each with an eligibility event and one recorded parsed text
written under the review's parsed_text/); the live codebook copied beside it (read, never written); a spec copy whose
`enabled_arms: []` names the openai arm; rm.git_state patched clean; the cloud run opened by rm.open_run(… cloud_arms=[arm] …) as
scripts/run_cloud_extraction.run_arm opens it; the local run by tests/_event_store_fixture.open_extraction_run (digest_fn a
constant, no server). store_result is called on a CloudExtractorBase (PROVIDER "" — no transport, no SDK client). Refusers that
count any call replace openai.OpenAI, anthropic.Anthropic, engine.utils.ollama_client.ollama_chat and
engine.agents.audit_events.semantic_verify (expected 0 each). The monitor CLI's main is driven with
engine.core.database.DATA_ROOT patched to the scratch root (U23 made it absolute, so no argument reaches it); the live data root
is listed before and after. No server, no provider, no key, no live database.
usage, from the repository root: .venv/bin/python fake_probe_u40a.py <tmp dir>   (stdout: one block per part, then the record)"""
import contextlib, hashlib, io, json, logging, shutil, sqlite3, sys
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path.cwd())); sys.path.insert(0, str(Path.cwd() / "tests"))
logging.disable(logging.CRITICAL)
import anthropic, openai
from engine.agents import audit_events
from engine.cloud.base import CloudExtractorBase
from engine.core import database as dbmod
from engine.core import paper_state as PS
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.effective import effective_state, eligible_paper_ids
from engine.core.effective_config import cloud_stage_config, sha256_canonical
from engine.core.events import mint_extraction_uid
from engine.core.extraction_events import PaperFailure, legacy_record, write_extraction_events
from engine.core.parsed_text import record_parsed_text, resolve_parsed_text
from engine.core.review_spec import load_review_spec
from engine.search.models import Citation
from engine.utils import ollama_client
from engine.validators import distribution_monitor as dm
from _event_store_fixture import open_extraction_run, register_pinned_arm, seed_claim, seed_eligibility

TMP = Path(sys.argv[1]); TMP.mkdir(parents=True, exist_ok=True)
SPEC_PATH = Path("review_specs/surgical_autonomy.yaml")
LIVE_CODEBOOK = Path("data/surgical_autonomy/extraction_codebook.yaml")
LIVE_DATA = Path("data")
CBK = load_codebook(LIVE_CODEBOOK)
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


def spans(study_type="Original Research"):
    return [{"field_name": f["name"], "value": study_type if f["name"] == "study_type" else "NR",
             "source_snippet": "Snippet for study_type." if f["name"] == "study_type" else "",
             "confidence": 0.9, "tier": f["tier"]} for tier in (1, 2, 3, 4) for f in CBK.fields_by_tier(tier)]


def counts(db_path, arm):
    c = sqlite3.connect(db_path)
    try:
        return {"cloud_extractions": c.execute("SELECT COUNT(*) FROM cloud_extractions WHERE arm = ?", (arm,)).fetchone()[0],
                "cloud_evidence_spans": c.execute("SELECT COUNT(*) FROM cloud_evidence_spans s JOIN cloud_extractions e "
                                                  "ON e.id = s.cloud_extraction_id WHERE e.arm = ?", (arm,)).fetchone()[0],
                "field_events": c.execute("SELECT COUNT(*) FROM field_events WHERE arm = ?", (arm,)).fetchone()[0],
                "claim_inputs": c.execute("SELECT COUNT(*) FROM claim_inputs WHERE arm = ?", (arm,)).fetchone()[0],
                "paper_events naming the arm": c.execute("SELECT COUNT(*) FROM paper_events WHERE json_extract(payload_json, '$.arm') = ?",
                                                         (arm,)).fetchone()[0]}
    finally:
        c.close()


def summ(s):
    st = [(r["status"], r["total_non_null"]) for r in s["results"] if r["field_name"] == "study_type"]
    return (f"skipped={s['skipped']} skip_reason={s['skip_reason']!r} ok={s['ok']} low_variance={s['low_variance']} "
            f"collapsed={s['collapsed']} study_type={st}")


def gate(base):
    try:
        return "returned: " + summ(base.run_distribution_check({"extracted": 12, "failed": 0}))
    except dm.DistributionCollapseError as e:
        return f"RAISED DistributionCollapseError: {str(e)[:110]}"


def attempt(label, fn):
    try:
        r = fn()
        print(f"  {label}: ACCEPTED — {r}")
        return True
    except BaseException as e:      # noqa: BLE001 — the probe reports every refusal's class
        print(f"  {label}: REFUSED {type(e).__name__}: {str(e)[:230]}")
        return False


def proc(conn, pid):
    s = effective_state(conn, pid)
    return f"processing={s.processing} reason={s.processing_reason} by_event={s.processing_provenance.get('event_type')}" \
        if hasattr(s, "processing_provenance") else f"processing={s.processing} reason={s.processing_reason}"


live_before = sorted(p.name for p in LIVE_DATA.iterdir())
with patch.object(openai, "OpenAI", _O), patch.object(anthropic, "Anthropic", _A), \
     patch.object(ollama_client, "ollama_chat", refuser("ollama_chat")), \
     patch.object(audit_events, "semantic_verify", refuser("semantic_verify")), \
     patch.object(rm, "git_state", lambda *a, **k: rm.GitState(commit="b" * 40, dirty=False, tag=None)):
    d = TMP / "root"; d.mkdir()
    db = ReviewDatabase("u40probe", data_root=d)
    db.add_papers([Citation(title=f"P{i}", source="pubmed", pmid=str(i)) for i in range(1, 25)])
    pids = [r["id"] for r in db.get_papers_by_status("INGESTED")]
    root = Path(db.db_path).parent
    for pid in pids:
        seed_eligibility(db._conn, pid)
        p = root / "parsed_text" / f"{pid}_v1.md"
        data = f"Paper {pid}.\n\nSnippet for study_type. Methods follow.\n".encode()
        p.write_bytes(data)
        record_parsed_text(db._conn, paper_id=pid, path=p, version=1, data=data, source_asset_id=None)
    db._conn.commit()
    db_path = str(db.db_path); shutil.copy2(LIVE_CODEBOOK, root / "extraction_codebook.yaml")
    spec0 = load_review_spec(SPEC_PATH)
    arm = next(a.name for a in spec0.arms if a.provider == "openai")
    local_arm = spec0.extraction_models.arm
    text = SPEC_PATH.read_text(); assert text.count("enabled_arms: []") == 1
    spec_path = root / "spec.yaml"; spec_path.write_text(text.replace("enabled_arms: []", f"enabled_arms: [{arm}]"))
    spec = load_review_spec(spec_path)
    cb = load_codebook(root / "extraction_codebook.yaml")
    sentinels = frozenset(cb.absence_sentinels)

    print("=== setup")
    handle = rm.open_run(db._conn, spec, kind="extraction", stages=(), codebook=cb, cloud_arms=[arm], payload_description="probe")
    cloud_run = handle.run_id
    print(f"  cloud run {cloud_run}: arms row {tuple(db._conn.execute('SELECT arm_name, arm_kind, configuration_marker, pinned_run_id FROM arms WHERE arm_name = ?', (arm,)).fetchone())}")
    print(f"  its run_stage_configs rows: {[tuple(r) for r in db._conn.execute('SELECT stage, stage_kind, arm_name, provider, model_name FROM run_stage_configs WHERE run_id = ?', (cloud_run,))]}")
    model_string = cloud_stage_config(spec, arm).model
    db.close()
    base = CloudExtractorBase(db_path, str(spec_path), arm_name=arm)
    base.run_id = cloud_run
    print(f"  CloudExtractorBase: PROVIDER={base.PROVIDER!r} arm_name={base.arm_name!r} run_id={base.run_id} "
          f"connection row_factory={base._conn.row_factory.__name__} isolation_level={base._conn.isolation_level!r}; "
          f"expected_fields {type(base.expected_fields).__name__} of {len(base.expected_fields)}")
    print(f"  activation outside any run: rm.active_field_names() = {rm.active_field_names()}")

    print("\n=== (i) the entry's test at HEAD — twelve papers through store_result, study_type identical")
    for pid in pids[:12]:
        base.store_result(paper_id=pid, arm=arm, model_string=model_string, extracted_data={"fields": []}, reasoning_trace="",
                          prompt_text="p", input_tokens=1, output_tokens=1, reasoning_tokens=0, cost_usd=0.0, spans=spans())
    print(f"  stored: {counts(db_path, arm)}")
    print(f"  base.run_distribution_check({{'extracted': 12, 'failed': 0}}): {gate(base)}")
    print(f"  arm_population: {dm.arm_population(Path(db_path), arm)}")
    print(f"  get_pending_papers({arm!r}): {len(base.get_pending_papers(arm))} of {len(pids)} corpus papers; "
          f"get_progress: {base.get_progress(arm)}")

    print("\n=== (ii) the writer — the same values for the registered cloud arm, legacy_record's form, the cloud run's run_id")
    conn = base._conn

    def rec(pid, *, ref="resolve", presented="hash", run_id=cloud_run, chain=None):
        r = resolve_parsed_text(conn, pid) if ref == "resolve" else ref
        h = sha256_canonical({"probe_request_for_paper": pid}) if presented == "hash" else presented
        return legacy_record(paper_id=pid, arm=arm, run_id=run_id, extraction_uid=mint_extraction_uid(), parsed_text=r,
                             model=model_string, model_digest=None, expected=tuple(base.expected_fields), spans=spans(),
                             attempts=1, stage_name=f"cloud:{arm}", presented_context_sha256=h,
                             context_chain=(chain if chain is not None else ((h,) if h else ())))

    def w(r):
        plan = write_extraction_events(conn, r, sentinels=sentinels)
        pe = plan.paper_event
        return (f"{len(plan.field_events)} field events ({sorted({e['event_type'] for e in plan.field_events})}); paper event "
                f"{pe['event_type']} -> {pe['to_state']} from {pe['from_state']} payload.arm={pe['payload'].get('arm')}")

    p0 = pids[0]
    attempt("(a) parsed_text=None (store_result holds no ParsedTextRef)", lambda: w(rec(p0, ref=None)))
    attempt("(b) presented_context_sha256=None, context_chain=() (store_result holds no request hash)",
            lambda: w(rec(p0, presented=None)))
    attempt("(c) run_id=None (CloudExtractorBase.run_id unset outside the runner)", lambda: w(rec(p0, run_id=None)))
    print(f"  after (a)–(c): {counts(db_path, arm)}; conn.in_transaction={conn.in_transaction}")
    attempt("(d) the full form: ref resolved, a request hash, chain (hash,), the cloud run", lambda: w(rec(p0)))
    print(f"  paper {p0}: {proc(conn, p0)}")
    row = conn.execute("SELECT event_type, actor_kind, actor_role, actor_name, run_id, extraction_uid IS NOT NULL, value, "
                       "source_snippet, json_extract(payload_json, '$.confidence') FROM field_events WHERE arm = ? AND paper_id = ? "
                       "AND field_name = 'study_type'", (arm, p0)).fetchone()
    print(f"  its study_type event: {tuple(row)}")
    ci = conn.execute("SELECT arm, paper_id, run_id, length(reuse_key), parsed_text_sha256 = (SELECT parsed_text_sha256 FROM "
                      "parsed_text_refs WHERE paper_id = ?) FROM claim_inputs WHERE arm = ? AND paper_id = ?", (p0, arm, p0)).fetchone()
    print(f"  its claim_inputs row: {tuple(ci)}")
    attempt("(e) a second write, same paper, same text", lambda: w(rec(p0)))
    for pid in pids[1:12]:
        w(rec(pid))
    print(f"  (d) for the other eleven: {counts(db_path, arm)}")
    print(f"  gate after: {gate(base)}")
    print(f"  arm_population: {dm.arm_population(Path(db_path), arm)}")

    print("\n--- the transaction it would share with store_result's INSERT (pids 15, 16)")
    p15, p16 = pids[14], pids[15]
    ins = ("INSERT INTO cloud_extractions (paper_id, arm, model_string, extracted_data, reasoning_trace, prompt_text, input_tokens, "
           "output_tokens, reasoning_tokens, cost_usd, extracted_at, codebook_hash, codebook_sha256) "
           "VALUES (?, ?, 'fake', '{}', '', 'p', 1, 1, 0, 0.0, 'now', 'h', 'h')")
    conn.execute(ins, (p15, arm)); before = conn.in_transaction
    w(rec(p15))
    other = sqlite3.connect(db_path)
    seen = other.execute("SELECT COUNT(*) FROM cloud_extractions WHERE paper_id = ? AND arm = ?", (p15, arm)).fetchone()[0]
    print(f"  INSERT pending (in_transaction={before}), writer accepted: in_transaction after={conn.in_transaction}; "
          f"the INSERT visible to a second connection: {seen} (the writer's commit committed it)")
    conn.execute(ins, (p16, arm)); before = conn.in_transaction
    ok = attempt("  writer refused after a pending INSERT", lambda: w(rec(p16, ref=None)))
    print(f"  INSERT pending (in_transaction={before}); after the refusal in_transaction={conn.in_transaction}; "
          f"visible to a second connection: {other.execute('SELECT COUNT(*) FROM cloud_extractions WHERE paper_id = ? AND arm = ?', (p16, arm)).fetchone()[0]}")
    conn.rollback()
    print(f"  after store_result's own rollback: cloud_extractions rows for paper {p16}: "
          f"{conn.execute('SELECT COUNT(*) FROM cloud_extractions WHERE paper_id = ? AND arm = ?', (p16, arm)).fetchone()[0]}")
    other.close()

    print("\n=== (iii-a) the processing axis with cloud events interleaved — the local audit stage (audit_events.audit_run)")
    db = ReviewDatabase("u40probe", data_root=d)
    local_run = open_extraction_run(db, spec)
    lconn = db._conn
    p13, p14 = pids[12], pids[13]
    for pid in (p13, p14):
        h = sha256_canonical({"probe_local": pid})
        write_extraction_events(lconn, legacy_record(
            paper_id=pid, arm=local_arm, run_id=local_run, extraction_uid=mint_extraction_uid(),
            parsed_text=resolve_parsed_text(lconn, pid), model="fake-local", model_digest="a" * 64,
            expected=tuple(base.expected_fields), spans=spans(), attempts=1, presented_context_sha256=h, context_chain=(h,)),
            sentinels=sentinels)
    print(f"  local run {local_run}; local claims written for papers {p13}, {p14} (the local form, extractor.py:709)")
    print(f"  before the audit: paper {p0} {proc(lconn, p0)}; paper {p13} {proc(lconn, p13)}; paper {pids[20]} {proc(lconn, pids[20])}")
    r1 = audit_events.audit_run(lconn, spec, run_id=local_run, arm=local_arm, review_dir=root)
    print(f"  audit_run(arm={local_arm!r}) #1: papers_audited={r1.papers_audited} located={r1.located} not_located={r1.not_located} "
          f"skipped_by_state={list(r1.skipped_by_state)} skipped_refused={list(r1.skipped_refused)}")
    vac = [r[0] for r in lconn.execute("SELECT paper_id FROM paper_events WHERE event_type = 'audited' AND run_id = ? AND "
                                       "json_extract(payload_json, '$.arm') = ? AND json_extract(payload_json, '$.located') = 0 "
                                       "ORDER BY paper_id", (local_run, local_arm))]
    held = {r[0] for r in lconn.execute("SELECT DISTINCT paper_id FROM field_events WHERE arm = ?", (local_arm,))}
    print(f"  audited_ai written for the local arm with located 0 on papers {vac}; of them, papers the local arm holds no claim on: "
          f"{[p for p in vac if p not in held]}")
    # the cloud arm writes again on the local papers: a stored paper (13) and a failed one (14)
    write_extraction_events(lconn, legacy_record(
        paper_id=p13, arm=arm, run_id=cloud_run, extraction_uid=mint_extraction_uid(), parsed_text=resolve_parsed_text(lconn, p13),
        model=model_string, model_digest=None, expected=tuple(base.expected_fields), spans=spans(), attempts=1,
        stage_name=f"cloud:{arm}", presented_context_sha256="d" * 64, context_chain=("d" * 64,)), sentinels=sentinels)
    write_extraction_events(lconn, PaperFailure(p14, arm, cloud_run, PS.REASON_MODEL_CALL_FAILED, f"cloud:{arm}",
                                                detail={"exception": "probe"}))
    print(f"  after a cloud store on {p13} and a cloud PaperFailure on {p14}: paper {p13} {proc(lconn, p13)}; paper {p14} {proc(lconn, p14)}")
    states = {pid: effective_state(lconn, pid).processing for pid in eligible_paper_ids(lconn)}   # run_pipeline._processing_states' body
    print(f"  _advance_extraction_workflow's read (run_pipeline.py:252): papers at 'extracted' (AI_AUDIT_COMPLETE_STAGE pending) "
          f"{sorted(p for p, s in states.items() if s == 'extracted')}")
    r2 = audit_events.audit_run(lconn, spec, run_id=local_run, arm=local_arm, review_dir=root)
    print(f"  audit_run(arm={local_arm!r}) #2: papers_audited={r2.papers_audited} located={r2.located} "
          f"skipped_by_state={list(r2.skipped_by_state)}")
    print(f"  get_pipeline_stats(): processing {db.get_pipeline_stats()['processing']} failures "
          f"{db.get_pipeline_stats()['processing_failures']} analysis_ready {db.get_pipeline_stats()['analysis_ready']}")
    from engine.exporters.prisma import generate_prisma_flow
    with contextlib.redirect_stdout(io.StringIO()):
        pf = generate_prisma_flow(db)
    print(f"  generate_prisma_flow: " + json.dumps({k: pf[k] for k in pf if k in ("studies_included", "extraction_in_progress",
          "extraction_failed", "failures", "extracted_incomplete")}, default=str)[:300])

    print("\n=== (iii-b) the grid consumers, driven once with the cloud arm holding claims")
    from engine.analysis import concordance
    data = concordance.load_arm(db_path, arm)
    print(f"  concordance.load_arm({arm!r}): {len(data)} papers; study_type values {sorted({v.get('study_type') for v in data.values()})}")
    attempt("concordance.run_concordance(local, cloud)", lambda: (lambda r: f"n_papers={r.n_papers} a_only={r.n_papers_a_only} "
            f"b_only={r.n_papers_b_only}")(concordance.run_concordance(db_path, local_arm, arm)))
    print(f"  concordance.check_schema_parity([{local_arm!r}, {arm!r}]): {concordance.check_schema_parity(db_path, [local_arm, arm])}")
    from engine.exporters.evidence_table import _build_evidence_rows
    headers, rows = _build_evidence_rows(db, spec, arm=arm)
    ix = {h: i for i, h in enumerate(headers)}
    print(f"  evidence_table._build_evidence_rows(arm={arm!r}): {len(rows)} rows; rows with study_type 'Original Research' "
          f"{sum(1 for r in rows if r[ix['study_type']] == 'Original Research')}; the processing column of paper {p14}'s row: "
          f"{next(r[ix['processing']] for r in rows if r[0] == p14)!r}")
    from engine.exporters.docx_export import export_evidence_docx
    attempt("docx_export.export_evidence_docx(arm=cloud)", lambda: (export_evidence_docx(db, spec, str(TMP / "e.docx"), arm=arm),
                                                                    f"{(TMP / 'e.docx').stat().st_size} B written")[1])
    from analysis.paper1 import judge_loader
    ro = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    def jl():
        ins = judge_loader.load_grid(ro, root, judge_loader.load_codebook(root / "extraction_codebook.yaml"),
                                     root / "extraction_codebook.yaml")
        arms_seen = sorted({a.arm_name for i in ins for a in i.arms})
        cloud = [a for i in ins for a in i.arms if a.arm_name == arm]
        return (f"{len(ins)} JudgeInputs; arms in them {arms_seen}; the cloud arm's ArmOutputs {len(cloud)}, with a value "
                f"{sum(1 for a in cloud if a.value is not None)}")
    attempt("judge_loader.load_grid", jl)
    from analysis.paper1 import export_disagreement_pairs as edp
    attempt("export_disagreement_pairs.arms_in_scope", lambda: edp.arms_in_scope(ro))
    from engine.validators import extraction_validator as ev
    issues, bleeds = ev.validate_all(ro, cb, arm=arm)
    print(f"  extraction_validator.validate_all(arm={arm!r}): issues {len(issues)}, bleeds {len(bleeds)}")
    print(f"  distribution_monitor.check_distribution: study_type "
          f"{[(r['status'], r['total_non_null']) for r in dm.check_distribution(Path(db_path), 'u40probe', arm, root / 'extraction_codebook.yaml') if r['field_name'] == 'study_type']}")
    ro.close()
    ra = audit_events.audit_run(lconn, spec, run_id=local_run, arm=arm, review_dir=root)
    print(f"  audit_events.audit_run(arm={arm!r}) — no caller passes a cloud arm; driven to show what it does: papers_audited="
          f"{ra.papers_audited} located={ra.located} not_located={ra.not_located} skipped_by_state={list(ra.skipped_by_state)}")
    db.close()

    print("\n=== (iv) U28's two leftovers")
    db = ReviewDatabase("u40probe", data_root=d)
    register_pinned_arm(db._conn, "empty_model_arm")
    register_pinned_arm(db._conn, "strict_model_arm")
    for i, pid in enumerate(pids[:22]):
        seed_claim(db._conn, arm="strict_model_arm", paper_id=pid, field_name="study_type",
                   value="Original Research" if i < 20 else "Review", source_snippet="S.")
    db._conn.commit(); db.close()
    for argv in (["dm", "--review", "u40probe", "--arm", "empty_model_arm"],
                 ["dm", "--review", "u40probe", "--arm", "strict_model_arm"],
                 ["dm", "--review", "u40probe", "--arm", "strict_model_arm", "--strict"]):
        out = io.StringIO()
        with patch.object(dbmod, "DATA_ROOT", d), patch.object(sys, "argv", argv), contextlib.redirect_stdout(out):
            try:
                dm.main(); code = 0
            except SystemExit as e:
                code = e.code
        lines = out.getvalue().splitlines()
        st = [l for l in lines if "study_type" in l][:1]
        print(f"  main {' '.join(argv[1:])}: exit {code}; {len(lines)} lines; study_type line {st}; last line {lines[-1] if lines else ''!r}")
    res = dm.check_distribution(root / "review.db", "u40probe", "strict_model_arm", root / "extraction_codebook.yaml")
    attempt("assert_no_collapse(strict=False) on strict_model_arm", lambda: dm.assert_no_collapse(res, strict=False))
    attempt("assert_no_collapse(strict=True) on strict_model_arm", lambda: dm.assert_no_collapse(res, strict=True))
    base.close()

live_after = sorted(p.name for p in LIVE_DATA.iterdir())
print("\n=== the record")
print(f"calls refused and counted: {CALLS}")
print(f"SDK client constructions (openai.OpenAI / anthropic.Anthropic): {CALLS['openai.OpenAI'] + CALLS['anthropic.Anthropic']}")
print(f"live data root entries before {len(live_before)}, after {len(live_after)}, equal: {live_before == live_after}; "
      f"'u40probe' under it: {'u40probe' in live_after}")
