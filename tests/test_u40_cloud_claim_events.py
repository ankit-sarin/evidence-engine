"""U40 (B9, C28; Wave 2 row 25; R798(c), (d)): the cloud store writes the arm's claim events and no paper event (A1-β).

Seams: 12af_U40A/fake_probe_u40a.py's. A scratch ReviewDatabase under tmp_path with fourteen papers, each with an eligibility event
(`seed_eligibility`) and one recorded parsed text (`_parsed_text_fixture.write_parsed`, through the engine's own `record_parsed_text`);
the tracked codebook copied beside it (read, never written); a spec copy whose `enabled_arms: []` names the openai arm; `rm.git_state`
patched clean; the cloud run opened by `rm.open_run(conn, spec, kind="extraction", stages=(), codebook=…, cloud_arms=[arm], …)` as
`scripts/run_cloud_extraction.run_arm` opens it, which registers and pins the arm, and its run_id set on the instance as the runner sets
it. The instance is the OpenAI transport with its constructor's client step skipped (`CloudExtractorBase.__init__` only) and its one
provider seam, `_create`, faked: so the instance's own `load_parsed_text`, `build_prompt`, `extract_paper`, `send` (which computes and
keeps the request hash) and `store_result` all run. `openai.OpenAI` and `anthropic.Anthropic` are replaced by recorders that refuse,
and every test asserts that neither was constructed. No server, no provider, no key.

Cases, by R798(f)'s numbering:
  (1) test_cloud_gate_sees_what_store_result_wrote — the entry's test: twelve papers stored with one identical categorical value; the
      gate raises DistributionCollapseError; the arm holds the claims, a claim_inputs row per paper; each claim's presented context is
      the paper's last request hash and its chain lists every send for that paper.
  (2) the processing axis is untouched — a preservation case, green on the parent.
  (3) refusals leave nothing in either store, each raising its EventRefused class.
  (4) the legacy rows and the claims agree per field and value; a sentinel's "" snippet is stored NULL in the claim.
  (5) the keyword — a PaperFailure with paper_event=False is refused with nothing written; an AST census finds the keyword passed by
      engine/cloud/base.py only.
"""

from __future__ import annotations

import ast
import json
import shutil
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import anthropic
import openai
import pytest

from engine.cloud.base import CloudExtractorBase
from engine.cloud.openai_extractor import OpenAIExtractor
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.effective import effective_state
from engine.core.events import ClaimWithoutInputIdentity, ClaimWithoutPresentedContext, EventRefused, RunLinkRefused
from engine.core.extraction_events import PaperFailure, ReuseKeyAlreadyClaimed, write_extraction_events
from engine.core import paper_state as PS
from engine.core.review_spec import load_review_spec
from engine.search.models import Citation
from engine.validators import distribution_monitor as dm
from _event_store_fixture import seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
SPEC_PATH = REPO / "review_specs" / "surgical_autonomy.yaml"
CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"


def _spans(cbk):
    """Every codebook field: study_type a value with a snippet, every other field the sentinel "NR" with an empty snippet."""
    return [{"field_name": f["name"], "value": "Original Research" if f["name"] == "study_type" else "NR",
             "source_snippet": "Snippet for study_type." if f["name"] == "study_type" else "",
             "confidence": 0.9, "tier": f["tier"]}
            for tier in (1, 2, 3, 4) for f in cbk.fields_by_tier(tier)]


class _FakeTransport(OpenAIExtractor):
    """The OpenAI transport with no client: the base constructor only, and `_create` answering from `self.reply`."""

    def __init__(self, db_path, spec_path, arm_name):
        CloudExtractorBase.__init__(self, db_path, spec_path, arm_name=arm_name)
        self.reply: list[dict] = []
        self.sends = 0

    def _create(self, **payload):
        self.sends += 1
        msg = SimpleNamespace(content=json.dumps({"fields": self.reply}), reasoning_content=None)
        return SimpleNamespace(choices=[SimpleNamespace(message=msg, finish_reason="stop")],
                               usage=SimpleNamespace(prompt_tokens=1, completion_tokens=1, completion_tokens_details=None))


@pytest.fixture
def cloud(tmp_path, monkeypatch):
    """(instance, db_path, arm, paper ids, codebook, SDK record) on a scratch review with the openai arm registered by open_run."""
    if not CODEBOOK.exists():
        pytest.skip("codebook not available")
    sdk = {"constructed": 0}

    class _Refuse:
        def __init__(self, *a, **k):
            sdk["constructed"] += 1
            raise RuntimeError("no SDK client in this test")

    monkeypatch.setattr(openai, "OpenAI", _Refuse)
    monkeypatch.setattr(anthropic, "Anthropic", _Refuse)
    monkeypatch.setattr(rm, "git_state", lambda *a, **k: rm.GitState(commit="b" * 40, dirty=False, tag=None))

    db = ReviewDatabase("u40", data_root=tmp_path)
    db.add_papers([Citation(title=f"P{i}", source="pubmed", pmid=str(i)) for i in range(1, 15)])
    pids = [r["id"] for r in db.get_papers_by_status("INGESTED")]
    for pid in pids:
        seed_eligibility(db._conn, pid)
        write_parsed(db, pid, f"Paper {pid}.\n\nSnippet for study_type. Methods follow.\n", version=1)
    db._conn.commit()
    db_path = Path(db.db_path)
    shutil.copy2(CODEBOOK, db_path.parent / "extraction_codebook.yaml")
    arm = next(a.name for a in load_review_spec(SPEC_PATH).arms if a.provider == "openai")
    text = SPEC_PATH.read_text()
    assert text.count("enabled_arms: []") == 1
    spec_path = tmp_path / "spec.yaml"
    spec_path.write_text(text.replace("enabled_arms: []", f"enabled_arms: [{arm}]"))
    cbk = load_codebook(db_path.parent / "extraction_codebook.yaml")
    handle = rm.open_run(db._conn, load_review_spec(spec_path), kind="extraction", stages=(), codebook=cbk,
                         cloud_arms=[arm], payload_description="test")
    db.close()
    inst = _FakeTransport(str(db_path), str(spec_path), arm)
    inst.run_id = handle.run_id
    inst.reply = _spans(cbk)
    yield inst, db_path, arm, pids, cbk, sdk
    inst.close()
    assert sdk["constructed"] == 0


def _extract(inst, pid, sends=1):
    """The transport's own path for one paper: load_parsed_text, then `sends` × extract_paper (each one send); returns the result
    of the last and the request hash of every send, in order."""
    text = inst.load_parsed_text(pid)
    hashes, result = [], None
    for _ in range(sends):
        result = inst.extract_paper(pid, text)
        hashes.append(inst.last_request_hash)
    return result, hashes


def _store(inst, pid, result):
    return inst.store_result(paper_id=pid, arm=inst.arm_name, model_string=inst.model_string,
                             extracted_data=result["extracted_data"], reasoning_trace=result["reasoning_trace"],
                             prompt_text=result["prompt_text"], input_tokens=result["input_tokens"],
                             output_tokens=result["output_tokens"], reasoning_tokens=result["reasoning_tokens"],
                             cost_usd=result["cost_usd"], spans=result["spans"])


def _counts(db_path, arm, pid=None):
    conn = sqlite3.connect(db_path)
    try:
        w, a = ("arm = ?", (arm,)) if pid is None else ("arm = ? AND paper_id = ?", (arm, pid))
        return {"cloud_extractions": conn.execute(f"SELECT COUNT(*) FROM cloud_extractions WHERE {w}", a).fetchone()[0],
                "cloud_evidence_spans": conn.execute(
                    f"SELECT COUNT(*) FROM cloud_evidence_spans s JOIN cloud_extractions e ON e.id = s.cloud_extraction_id "
                    f"WHERE e.{w.replace(' AND paper_id', ' AND e.paper_id')}", a).fetchone()[0],
                "field_events": conn.execute(f"SELECT COUNT(*) FROM field_events WHERE {w}", a).fetchone()[0],
                "claim_inputs": conn.execute(f"SELECT COUNT(*) FROM claim_inputs WHERE {w}", a).fetchone()[0]}
    finally:
        conn.close()


def _axis(db_path, pids):
    conn = sqlite3.connect(db_path)
    try:
        return {pid: (effective_state(conn, pid).processing,
                      conn.execute("SELECT COUNT(*) FROM paper_events WHERE paper_id = ?", (pid,)).fetchone()[0]) for pid in pids}
    finally:
        conn.close()


def test_cloud_gate_sees_what_store_result_wrote(cloud):
    """(1), the entry's test (B9, Wave 2 row 25): twelve papers through the instance's own load_parsed_text, send and store_result
    with one identical categorical value — the even-numbered papers sent twice, as a completeness re-issue sends — and the cloud gate
    raises DistributionCollapseError. The arm holds the claims, one claim_inputs row per paper; each claim's presented context is its
    paper's last request hash and its context chain lists every send for that paper, in order."""
    inst, db_path, arm, pids, cbk, _ = cloud
    sent = {}
    for i, pid in enumerate(pids[:12]):
        result, hashes = _extract(inst, pid, sends=2 if i % 2 else 1)
        sent[pid] = hashes
        _store(inst, pid, result)
    c = _counts(db_path, arm)
    assert c["cloud_extractions"] == 12
    assert c["field_events"] == 12 * len(inst.expected_fields)
    assert c["claim_inputs"] == 12
    conn = sqlite3.connect(db_path)
    try:
        for pid, hashes in sent.items():
            rows = conn.execute("SELECT presented_context_sha256, payload_json, event_type FROM field_events "
                                "WHERE arm = ? AND paper_id = ?", (arm, pid)).fetchall()
            assert len(rows) == len(inst.expected_fields)
            for presented, payload, event_type in rows:
                assert event_type == "asserted"
                assert presented == hashes[-1]
                assert json.loads(payload)["context_chain"] == hashes
            assert conn.execute("SELECT COUNT(*) FROM claim_inputs WHERE arm = ? AND paper_id = ?", (arm, pid)).fetchone()[0] == 1
    finally:
        conn.close()
    assert dm.arm_population(Path(db_path), arm) == 12
    with pytest.raises(dm.DistributionCollapseError) as exc_info:
        inst.run_distribution_check({"extracted": 12, "failed": 0})
    assert "study_type" in str(exc_info.value)


def test_the_processing_axis_is_untouched_by_cloud_stores(cloud):
    """(2), preservation (R798(b), (c)): for every paper, effective_state's processing and the count of its paper_events rows are the
    same before and after the twelve stores. Green on the parent, which writes no event; green after, which writes no paper event."""
    inst, db_path, arm, pids, cbk, _ = cloud
    before = _axis(db_path, pids)
    for pid in pids[:12]:
        result, _ = _extract(inst, pid)
        _store(inst, pid, result)
    assert _counts(db_path, arm)["cloud_extractions"] == 12
    assert _axis(db_path, pids) == before


@pytest.mark.parametrize("case,refusal", [
    ("no_reference_kept", ClaimWithoutInputIdentity),
    ("no_send_made", ClaimWithoutPresentedContext),
    ("state_kept_for_another_paper", ClaimWithoutInputIdentity),
    ("no_run_id", RunLinkRefused),
    ("a_second_store_of_the_same_text", ReuseKeyAlreadyClaimed),
])
def test_a_refused_cloud_store_leaves_nothing_in_either_store(cloud, case, refusal):
    """(3), R798(d)(iv): a reference or a chain kept for another paper is absent, never used; the writer's own refusal fires and
    nothing is stored for the paper in either store. Each refusal is an EventRefused, so the transports' handlers pass it up."""
    inst, db_path, arm, pids, cbk, _ = cloud
    pid, other = pids[0], pids[1]
    if case == "no_reference_kept":
        result = inst.extract_paper(pid, "Paper text the instance did not load.")
    elif case == "no_send_made":
        inst.load_parsed_text(pid)
        result = {"extracted_data": {"fields": []}, "reasoning_trace": "", "prompt_text": "p", "input_tokens": 1,
                  "output_tokens": 1, "reasoning_tokens": 0, "cost_usd": 0.0, "spans": _spans(cbk)}
    elif case == "state_kept_for_another_paper":
        result, _ = _extract(inst, other)
    elif case == "no_run_id":
        result, _ = _extract(inst, pid)
        inst.run_id = None
    else:
        result, _ = _extract(inst, pid)
        _store(inst, pid, result)
        result, _ = _extract(inst, pid)
    before = _counts(db_path, arm, pid)
    with pytest.raises(refusal) as exc_info:
        _store(inst, pid, result)
    assert isinstance(exc_info.value, EventRefused)
    assert _counts(db_path, arm, pid) == before
    if case != "a_second_store_of_the_same_text":
        assert before == {"cloud_extractions": 0, "cloud_evidence_spans": 0, "field_events": 0, "claim_inputs": 0}


def test_the_legacy_rows_and_the_claims_agree(cloud):
    """(4), R798(d)(iii): for a stored paper, the cloud_evidence_spans rows and the arm's asserted claims hold the same field and value,
    field for field; a sentinel's "" snippet is "" in the legacy row and NULL in the claim (legacy_record's `or None`)."""
    inst, db_path, arm, pids, cbk, _ = cloud
    pid = pids[0]
    result, _ = _extract(inst, pid)
    _store(inst, pid, result)
    conn = sqlite3.connect(db_path)
    try:
        legacy = {r[0]: (r[1], r[2]) for r in conn.execute(
            "SELECT s.field_name, s.value, s.source_snippet FROM cloud_evidence_spans s JOIN cloud_extractions e "
            "ON e.id = s.cloud_extraction_id WHERE e.arm = ? AND e.paper_id = ?", (arm, pid))}
        claims = {r[0]: (r[1], r[2]) for r in conn.execute(
            "SELECT field_name, value, source_snippet FROM field_events WHERE arm = ? AND paper_id = ? AND event_type = 'asserted'",
            (arm, pid))}
    finally:
        conn.close()
    assert set(legacy) == set(claims) == set(inst.expected_fields)
    assert {f: v for f, (v, _) in legacy.items()} == {f: v for f, (v, _) in claims.items()}
    assert claims["study_type"] == ("Original Research", "Snippet for study_type.")
    sentinel_fields = [f for f, (v, _) in legacy.items() if v == "NR"]
    assert sentinel_fields
    for f in sentinel_fields:
        assert legacy[f][1] == "" and claims[f][1] is None


@pytest.mark.parametrize("half", ["paper_failure_refused", "census_base_only"])
def test_the_paper_event_keyword(cloud, half):
    """(5), R798(c): write_extraction_events(…, paper_event=False) refuses a PaperFailure before anything is read or written; and the
    keyword is passed by engine/cloud/base.py and by no other production file (an AST census over engine/, scripts/, analysis/)."""
    inst, db_path, arm, pids, cbk, _ = cloud
    if half == "census_base_only":
        passers = set()
        for root in ("engine", "scripts", "analysis"):
            for p in sorted((REPO / root).rglob("*.py")):
                for n in ast.walk(ast.parse(p.read_text(encoding="utf-8"))):
                    if isinstance(n, ast.Call) and ast.unparse(n.func).rsplit(".", 1)[-1] == "write_extraction_events" \
                            and any(k.arg == "paper_event" for k in n.keywords):
                        passers.add(str(p.relative_to(REPO)))
        assert passers == {"engine/cloud/base.py"}
        return
    pid = pids[0]
    conn = sqlite3.connect(db_path)
    try:
        before = (conn.execute("SELECT COUNT(*) FROM paper_events").fetchone()[0],
                  conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0])
        failure = PaperFailure(pid, arm, inst.run_id, PS.REASON_MODEL_CALL_FAILED, f"cloud:{arm}")
        with pytest.raises(ValueError):
            write_extraction_events(conn, failure, paper_event=False)
        assert not conn.in_transaction
        assert (conn.execute("SELECT COUNT(*) FROM paper_events").fetchone()[0],
                conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0]) == before
    finally:
        conn.close()
