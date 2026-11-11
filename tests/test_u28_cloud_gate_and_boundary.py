"""U28 (B9, C28; R794(d), (e), (f)): the cloud gate skips honestly, an arm with no values is reported skipped, and the cloud write
boundary refuses an uncited or empty value.

Seams: 12ae_U28A/fake_probe_u28a.py's. A scratch ReviewDatabase under tmp_path with fourteen papers, each with an eligibility event
(`seed_eligibility`) and, since U40, a recorded parsed text (`write_parsed`); the live codebook copied beside it (read, never written); a spec copy whose `enabled_arms: []` names the openai arm;
`rm.git_state` patched clean; `rm.open_run(conn, spec, kind="extraction", stages=(), codebook=…, cloud_arms=[arm], …)` exactly as
`scripts/run_cloud_extraction.run_arm` calls it, which registers and pins the arm, its run_id set on the instance as the runner sets it;
`store_result` called on a `CloudExtractorBase` (PROVIDER "" — no transport), given the paper's reference through its own
`load_parsed_text` and one request hash at its chain (U40, R798(d), (f)). `openai.OpenAI` and `anthropic.Anthropic` are replaced by recorders that refuse, and every test asserts that
neither was constructed. No server, no provider, no key, no gitignored fixture.

Claims, by the U28A read-out's numbering and R794's letters:
  (i)   D — the cloud gate (`CloudExtractorBase.run_distribution_check`) skips by the arm's live-claim population, a failed paper recorded,
        not a veto: a cloud run that stored no paper leaves the arm's population at 0.
  (ii)  B, claim (2) / N44 — `run_post_extraction_check` reports an arm holding no non-null value for any categorical field as skipped;
        `check_distribution`'s per-field contract (OK with zeros, the B5 rewrite under A12) stands.
  (iii) C, claim (3) — `store_result` refuses a non-sentinel value without a snippet, and an empty value, with UncitedValueError, inserting
        nothing; a sentinel with an empty snippet is still stored (R132, exempt in LEGACY).
"""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import anthropic
import openai
import pytest

from engine.cloud.base import CloudExtractorBase
from engine.core import run_manifest as rm
from engine.core.citation_guard import BLANK_VALUE, VALUE_WITHOUT_CITATION, UncitedValueError
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.effective_config import sha256_canonical
from engine.core.review_spec import load_review_spec
from engine.search.models import Citation
from engine.validators import distribution_monitor as dm
from _event_store_fixture import seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
SPEC_PATH = REPO / "review_specs" / "surgical_autonomy.yaml"
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
NO_VALUES = "arm holds no non-null value for any categorical field"


def _spans(cbk, **override):
    """Every codebook field: study_type a value with a snippet, every other field the sentinel "NR" with an empty snippet.
    `override` maps a field to (value, snippet)."""
    out = []
    for tier in (1, 2, 3, 4):
        for f in cbk.fields_by_tier(tier):
            value, snippet = ("Original Research", "Snippet for study_type.") if f["name"] == "study_type" else ("NR", "")
            value, snippet = override.get(f["name"], (value, snippet))
            out.append({"field_name": f["name"], "value": value, "source_snippet": snippet, "confidence": 0.9, "tier": f["tier"]})
    return out


@pytest.fixture
def cloud(tmp_path, monkeypatch):
    """(base, db_path, arm, paper ids, codebook, SDK record) on a scratch review with the openai arm registered by open_run."""
    if not LIVE_CODEBOOK.exists():
        pytest.skip("codebook not available")
    sdk = {"constructed": 0}

    class _Refuse:
        def __init__(self, *a, **k):
            sdk["constructed"] += 1
            raise RuntimeError("no SDK client in this test")

    monkeypatch.setattr(openai, "OpenAI", _Refuse)
    monkeypatch.setattr(anthropic, "Anthropic", _Refuse)
    monkeypatch.setattr(rm, "git_state", lambda *a, **k: rm.GitState(commit="b" * 40, dirty=False, tag=None))

    db = ReviewDatabase("u28", data_root=tmp_path)
    db.add_papers([Citation(title=f"P{i}", source="pubmed", pmid=str(i)) for i in range(1, 15)])
    pids = [r["id"] for r in db.get_papers_by_status("INGESTED")]
    for pid in pids:
        seed_eligibility(db._conn, pid)
        write_parsed(db, pid, f"Paper {pid}.\n\nSnippet for study_type.\n", version=1)
    db._conn.commit()
    db_path = Path(db.db_path)
    shutil.copy2(LIVE_CODEBOOK, db_path.parent / "extraction_codebook.yaml")
    arm = next(a.name for a in load_review_spec(SPEC_PATH).arms if a.provider == "openai")
    text = SPEC_PATH.read_text()
    assert text.count("enabled_arms: []") == 1
    spec_path = tmp_path / "spec.yaml"
    spec_path.write_text(text.replace("enabled_arms: []", f"enabled_arms: [{arm}]"))
    cbk = load_codebook(db_path.parent / "extraction_codebook.yaml")
    handle = rm.open_run(db._conn, load_review_spec(spec_path), kind="extraction", stages=(), codebook=cbk,
                         cloud_arms=[arm], payload_description="test")
    db.close()
    base = CloudExtractorBase(str(db_path), str(spec_path), arm_name=arm)
    base.run_id = handle.run_id
    yield base, db_path, arm, pids, cbk, sdk
    base.close()
    assert sdk["constructed"] == 0


def _store(base, arm, pid, spans):
    base.load_parsed_text(pid)
    base._request_chain = (pid, [sha256_canonical({"fixture_request_for_paper": pid})])
    return base.store_result(paper_id=pid, arm=arm, model_string="fake", extracted_data={"fields": []}, reasoning_trace="",
                             prompt_text="p", input_tokens=1, output_tokens=1, reasoning_tokens=0, cost_usd=0.0, spans=spans)


def _rows(db_path, arm, pid=None):
    conn = sqlite3.connect(db_path)
    try:
        if pid is None:
            return conn.execute("SELECT COUNT(*) FROM cloud_extractions WHERE arm = ?", (arm,)).fetchone()[0]
        return conn.execute("SELECT COUNT(*) FROM cloud_extractions WHERE arm = ? AND paper_id = ?", (arm, pid)).fetchone()[0]
    finally:
        conn.close()


@pytest.mark.parametrize("failed", [0, 1], ids=["no_failed_paper", "one_failed_paper"])
def test_a_cloud_run_with_no_claims_is_reported_skipped_not_ok(cloud, failed):
    """(i), D (R794(d)): a cloud run that stored no paper; the cloud gate skips by the arm's live-claim population (0), with or
    without a failed paper — never eleven OK rows over a store it did not read."""
    base, db_path, arm, pids, cbk, _ = cloud
    assert _rows(db_path, arm) == 0
    summary = base.run_distribution_check({"extracted": 0, "failed": failed})
    assert summary["skipped"] is True, summary
    assert summary["skip_reason"] == f"only 0 eligible papers hold a live claim for arm {arm} (minimum 10)"
    assert summary["arm_population"] == 0


@pytest.mark.parametrize("half", ["summary_reports_skipped", "per_field_contract_kept"])
def test_an_arm_holding_no_non_null_values_is_reported_skipped(cloud, half):
    """(ii), B (R794(e); claim (2), N44): on the registered arm holding nothing, `run_post_extraction_check` with
    min_population="run", extracted_count=12 and raise_on_collapse=True reports the arm skipped with the no-values reason and does not
    raise; `check_distribution` itself still returns OK with zeros on every field (the per-field contract, B5 under A12)."""
    base, db_path, arm, pids, cbk, _ = cloud
    codebook_path = db_path.parent / "extraction_codebook.yaml"
    if half == "per_field_contract_kept":
        results = dm.check_distribution(db_path, "u28", arm, codebook_path)
        assert results and all(r["status"] == "OK" and r["total_non_null"] == 0 for r in results)
        return
    summary = dm.run_post_extraction_check(db_path, "u28", arm, codebook_path, extracted_count=12, failed_count=0,
                                           min_population="run", raise_on_collapse=True)
    assert summary["skipped"] is True, summary
    assert summary["skip_reason"] == NO_VALUES
    assert len(summary["results"]) == len(dm._load_categorical_fields(codebook_path))


@pytest.mark.parametrize("case", ["uncited_non_sentinel", "empty_value_with_a_snippet", "sentinel_with_an_empty_snippet"])
def test_store_result_refuses_an_uncited_or_empty_value(cloud, case):
    """(iii), C (R794(f); claim (3)): store_result runs enforce_citations in LEGACY mode before its INSERTs. A non-sentinel value with
    an empty snippet (VALUE_WITHOUT_CITATION) and an empty value with a snippet (BLANK_VALUE) raise UncitedValueError and insert nothing;
    the sentinel with an empty snippet is stored."""
    base, db_path, arm, pids, cbk, _ = cloud
    pid = pids[0]
    if case == "sentinel_with_an_empty_snippet":
        _store(base, arm, pid, _spans(cbk))
        assert _rows(db_path, arm, pid) == 1
        return
    override, code = ({"surgical_domain": ("Colorectal", "")}, VALUE_WITHOUT_CITATION) if case == "uncited_non_sentinel" \
        else ({"surgical_domain": ("", "Snippet for surgical_domain.")}, BLANK_VALUE)
    with pytest.raises(UncitedValueError) as exc_info:
        _store(base, arm, pid, _spans(cbk, **override))
    assert ("surgical_domain", code) in exc_info.value.offenders
    assert _rows(db_path, arm, pid) == 0
