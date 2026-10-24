"""12w-U17A Q9 — U17's acceptance test (12l_P1A.md §3 U17), written against HEAD as a reproducer.

Kept here as evidence under R695(b)'s form, not under tests/: it enters tests/ with the fix.
Run from the repository root with an explicit path:

    .venv/bin/python -m pytest docs/session-reports/session-12/12w_U17A/repro/test_u17_cross_family_repro.py -v

Outside tests/, so tests/conftest.py's fences do not load; nothing here needs them. The database is a
fresh `ReviewDatabase` under tmp_path, git state and the digest function are injected (the pattern of
tests/test_run_manifest.py), and no model server, git or network is reached.

The same-family case uses the extractor's own model as the auditor. That model is of the extractor's
family under every reading R702(e) names (vendor of the weights, lineage of the base), so the test
presupposes no ruling on what "family" means. The reading-dependent case (an auditor that is a
different vendor on the same base lineage) is listed in the read-out, not tested here.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.review_paths import load_spec_for

REPO = Path(__file__).resolve().parents[5]
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
EXTRACTION_AND_AUDIT = ("extract_pass1", "extract_pass2", "extract_retry_snippet", "audit")

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")


@pytest.fixture
def review(tmp_path):
    from engine.core.database import ReviewDatabase
    db = ReviewDatabase("u17repro", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    cb = load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    yield db, cb
    db.close()


@pytest.fixture
def spec():
    return load_spec_for("surgical_autonomy")


def _open(db, spec, cb):
    return rm.open_run(db._conn, spec, kind="extraction", stages=EXTRACTION_AND_AUDIT,
                       codebook=cb, git=CLEAN, digest_fn=lambda m: "a" * 64)


def _rows(conn):
    return {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            for t in ("run_manifests", "run_stage_configs", "arms")}


def test_open_run_refuses_a_same_family_auditor(review, spec):
    db, cb = review
    same = spec.model_copy(update={"auditor_model": spec.extraction_models.extractor})
    before = _rows(db._conn)
    with pytest.raises(rm.RunRefused) as exc:
        _open(db, same, cb)
    assert _rows(db._conn) == before, "a refused open writes no manifest row"
    text = str(exc.value)
    assert spec.extraction_models.extractor in text and "audit" in text


def test_open_run_records_both_families_for_a_cross_family_auditor(review, spec):
    db, cb = review
    h = _open(db, spec, cb)
    body = json.loads(db._conn.execute(
        "SELECT manifest_json FROM run_manifests WHERE run_id = ?", (h.run_id,)).fetchone()[0])
    stages = body["stages"]
    assert "family" in stages["audit"], "the manifest records the auditor's family"
    assert "family" in stages["extract_pass1"], "the manifest records the extractor's family"
    assert stages["audit"]["family"] != stages["extract_pass1"]["family"]
