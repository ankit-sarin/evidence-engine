"""12ae-U28A — FAKE probe for U28 (B9, C28; Wave 2 rows 25 and 27; N44). The entry's three claims, kept apart:
  (1) THE GATE — twelve papers stored through store_result with one identical categorical value; what the cloud gate reports.
  (2) N44 — check_distribution and run_post_extraction_check for a registered arm holding zero claims.
  (3) THE BOUNDARY — a non-sentinel value with no source_snippet through parse_response_to_spans then store_result.
Seams: a scratch ReviewDatabase under a temp directory (12 + 2 papers, each with an eligibility event); the live codebook copied
beside it (read, never written); a spec copy whose `enabled_arms: []` names the openai arm; rm.git_state patched clean;
rm.open_run(conn, spec, kind="extraction", stages=(), codebook=…, cloud_arms=[arm], payload_description=…) exactly as
scripts/run_cloud_extraction.run_arm calls it, which registers and pins the cloud arm (run_manifest.py:691–696). store_result is called on a
CloudExtractorBase (PROVIDER "" — no transport, no SDK client); openai.OpenAI and anthropic.Anthropic are replaced by recorders that count
any construction (expected 0). The monitor is called as base.py:510 calls it. A contrast arm (`contrast_model_arm`, registered pinned by
tests/_event_store_fixture.register_pinned_arm) holds the same twelve values as claim events (seed_claim), to show the monitor on the
store it reads. No server, no provider, no key, no live database.
usage, from the repository root: .venv/bin/python fake_probe_u28a.py <tmp dir>   (stdout: one block per claim, then the record)"""
import contextlib, io, json, logging, shutil, sqlite3, sys
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path.cwd())); sys.path.insert(0, str(Path.cwd() / "tests"))
logging.disable(logging.CRITICAL)
import anthropic, openai
from engine.cloud.base import CloudExtractorBase
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.search.models import Citation
from engine.validators import distribution_monitor as dm
from _event_store_fixture import register_pinned_arm, seed_claim, seed_eligibility

TMP = Path(sys.argv[1]); TMP.mkdir(parents=True, exist_ok=True)
SPEC_PATH = Path("review_specs/surgical_autonomy.yaml")
LIVE_CODEBOOK = Path("data/surgical_autonomy/extraction_codebook.yaml")
CBK = load_codebook(LIVE_CODEBOOK)
SDK = {"constructed": 0}


class _Refuse:
    def __init__(self, *a, **k):
        SDK["constructed"] += 1
        raise RuntimeError("probe: no SDK client")


def spans(study_type="Original Research"):
    return [{"field_name": f["name"], "value": study_type if f["name"] == "study_type" else "NR",
             "source_snippet": f"Snippet for {f['name']}." if f["name"] == "study_type" else "",
             "confidence": 0.9, "tier": f["tier"]} for tier in (1, 2, 3, 4) for f in CBK.fields_by_tier(tier)]


def counts(db_path, arm):
    c = sqlite3.connect(db_path)
    try:
        return {"cloud_extractions": c.execute("SELECT COUNT(*) FROM cloud_extractions WHERE arm = ?", (arm,)).fetchone()[0],
                "cloud_evidence_spans": c.execute("SELECT COUNT(*) FROM cloud_evidence_spans s JOIN cloud_extractions e "
                                                  "ON e.id = s.cloud_extraction_id WHERE e.arm = ?", (arm,)).fetchone()[0],
                "field_events": c.execute("SELECT COUNT(*) FROM field_events WHERE arm = ?", (arm,)).fetchone()[0]}
    finally:
        c.close()


def gate(db_path, arm, extracted):
    try:
        s = dm.run_post_extraction_check(db_path=Path(db_path), review_name=Path(db_path).parent.name, arm=arm,
                                         codebook_path=Path(db_path).parent / "extraction_codebook.yaml",
                                         extracted_count=extracted, failed_count=0)
        st = [(r["field_name"], r["status"], r["total_non_null"]) for r in s["results"] if r["field_name"] == "study_type"]
        return (f"returned: skipped={s['skipped']} skip_reason={s['skip_reason']!r} ok={s['ok']} low_variance={s['low_variance']} "
                f"collapsed={s['collapsed']} study_type={st}")
    except dm.DistributionCollapseError as e:
        return f"RAISED DistributionCollapseError: {str(e)[:110]}"


with patch.object(openai, "OpenAI", _Refuse), patch.object(anthropic, "Anthropic", _Refuse), \
     patch.object(rm, "git_state", lambda *a, **k: rm.GitState(commit="b" * 40, dirty=False, tag=None)):
    d = TMP / "review"; d.mkdir()
    db = ReviewDatabase("u28probe", data_root=d)
    db.add_papers([Citation(title=f"P{i}", source="pubmed", pmid=str(i)) for i in range(1, 15)])
    pids = [r["id"] for r in db.get_papers_by_status("INGESTED")]
    for pid in pids: seed_eligibility(db._conn, pid)
    db._conn.commit()
    db_path = str(db.db_path); shutil.copy2(LIVE_CODEBOOK, Path(db_path).parent / "extraction_codebook.yaml")
    spec0 = load_review_spec(SPEC_PATH)
    arm = next(a.name for a in spec0.arms if a.provider == "openai")
    text = SPEC_PATH.read_text(); assert text.count("enabled_arms: []") == 1
    spec_path = d / "spec.yaml"; spec_path.write_text(text.replace("enabled_arms: []", f"enabled_arms: [{arm}]"))
    spec = load_review_spec(spec_path)

    print("=== registration (I3)")
    unreg = dm._query_all_fields.__name__
    try:
        dm.check_distribution(Path(db_path), "u28probe", arm, Path(db_path).parent / "extraction_codebook.yaml")
        print(f"  before open_run, check_distribution({arm!r}): returned (unexpected)")
    except Exception as e:
        print(f"  before open_run, check_distribution({arm!r}): {type(e).__name__}: {str(e)[:120]}")
    handle = rm.open_run(db._conn, spec, kind="extraction", stages=(), codebook=load_codebook(Path(db_path).parent / "extraction_codebook.yaml"),
                         cloud_arms=[arm], payload_description="probe")
    row = db._conn.execute("SELECT arm_name, arm_kind, configuration_marker, pinned_run_id FROM arms WHERE arm_name = ?", (arm,)).fetchone()
    print(f"  after open_run (run {handle.run_id}): arms row {tuple(row)}")
    db.close()

    base = CloudExtractorBase(db_path, str(spec_path), arm_name=arm)
    print(f"  CloudExtractorBase constructed: PROVIDER={base.PROVIDER!r}, arm_name={base.arm_name!r}, SDK constructions so far {SDK['constructed']}")

    print("\n=== claim (2) N44 — the registered arm holding zero claims")
    res = dm.check_distribution(Path(db_path), "u28probe", arm, Path(db_path).parent / "extraction_codebook.yaml")
    print(f"  check_distribution: {len(res)} fields; statuses {sorted({r['status'] for r in res})}; total_non_null all 0: "
          f"{all(r['total_non_null'] == 0 for r in res)}; a 'skipped' key in any result: {any('skipped' in r for r in res)}")
    print(f"  run_post_extraction_check(extracted_count=12): {gate(db_path, arm, 12)}")
    print(f"  run_post_extraction_check(extracted_count=5):  {gate(db_path, arm, 5)}")
    print(f"  arm_population: {dm.arm_population(Path(db_path), arm)}")

    print("\n=== claim (1) THE GATE — twelve papers stored through store_result, study_type identical")
    for pid in pids[:12]:
        base.store_result(paper_id=pid, arm=arm, model_string="fake", extracted_data={"fields": []}, reasoning_trace="",
                          prompt_text="p", input_tokens=1, output_tokens=1, reasoning_tokens=0, cost_usd=0.0, spans=spans())
    print(f"  stored: {counts(db_path, arm)}")
    print(f"  base.run_distribution_check({{'extracted': 12, 'failed': 0}}): "
          + (lambda: (lambda s: f"returned: skipped={s['skipped']} ok={s['ok']} collapsed={s['collapsed']} "
                                 f"study_type={[(r['status'], r['total_non_null']) for r in s['results'] if r['field_name'] == 'study_type']}")
                     (base.run_distribution_check({"extracted": 12, "failed": 0})))())
    print(f"  run_post_extraction_check(extracted_count=12): {gate(db_path, arm, 12)}")
    print(f"  run_post_extraction_check(extracted_count=5):  {gate(db_path, arm, 5)}")
    print(f"  run_post_extraction_check(extracted_count=12, failed_count=1): "
          + str(dm.run_post_extraction_check(Path(db_path), "u28probe", arm, Path(db_path).parent / "extraction_codebook.yaml",
                                             extracted_count=12, failed_count=1)["skip_reason"]))
    print(f"  arm_population: {dm.arm_population(Path(db_path), arm)}")

    print("\n--- contrast: the same twelve values as claim events on a registered model arm")
    c = sqlite3.connect(db_path)
    register_pinned_arm(c, "contrast_model_arm")
    for pid in pids[:12]:
        seed_claim(c, arm="contrast_model_arm", paper_id=pid, field_name="study_type", value="Original Research",
                   source_snippet="Snippet.")
    c.commit(); c.close()
    print(f"  stored: {counts(db_path, 'contrast_model_arm')}")
    print(f"  run_post_extraction_check(extracted_count=12): {gate(db_path, 'contrast_model_arm', 12)}")

    print("\n--- an unregistered arm name")
    try:
        dm.check_distribution(Path(db_path), "u28probe", "no_such_arm", Path(db_path).parent / "extraction_codebook.yaml")
        print("  returned (unexpected)")
    except Exception as e:
        print(f"  check_distribution('no_such_arm'): {type(e).__name__}")

    print("\n=== claim (3) THE BOUNDARY — a non-sentinel value with no source_snippet")
    resp = {"fields": spans()}
    target = next(s for s in resp["fields"] if s["field_name"] == "surgical_domain")
    target["value"] = "Colorectal"; del target["source_snippet"]
    nulled = next(s for s in resp["fields"] if s["field_name"] == "task_monitor")
    nulled["value"] = None; nulled["source_snippet"] = "a quote the null branch clears"
    parsed = base.parse_response_to_spans(json.loads(json.dumps(resp)))
    p_t = next(s for s in parsed if s["field_name"] == "surgical_domain"); p_n = next(s for s in parsed if s["field_name"] == "task_monitor")
    print(f"  parse_response_to_spans: {len(parsed)} spans; surgical_domain -> value={p_t['value']!r} source_snippet={p_t['source_snippet']!r}; "
          f"task_monitor (null) -> value={p_n['value']!r} source_snippet={p_n['source_snippet']!r}")
    try:
        ext_id = base.store_result(paper_id=pids[12], arm=arm, model_string="fake", extracted_data=resp, reasoning_trace="",
                                   prompt_text="p", input_tokens=1, output_tokens=1, reasoning_tokens=0, cost_usd=0.0, spans=parsed)
        c = sqlite3.connect(db_path)
        r1 = c.execute("SELECT value, source_snippet FROM cloud_evidence_spans WHERE cloud_extraction_id = ? AND field_name = 'surgical_domain'",
                       (ext_id,)).fetchone()
        r2 = c.execute("SELECT value, source_snippet FROM cloud_evidence_spans WHERE cloud_extraction_id = ? AND field_name = 'task_monitor'",
                       (ext_id,)).fetchone()
        c.close()
        print(f"  store_result: INSERTED (cloud_extractions id {ext_id}); read back surgical_domain {tuple(r1)}; task_monitor {tuple(r2)}")
    except Exception as e:
        print(f"  store_result: REFUSED {type(e).__name__}: {str(e)[:120]}")
    base.close()

print("\n=== the record")
print(f"SDK client constructions (openai.OpenAI / anthropic.Anthropic): {SDK['constructed']}")
