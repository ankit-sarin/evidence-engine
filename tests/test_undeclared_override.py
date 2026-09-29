"""R223 / R223a — `with_options` refuses an undeclared option override under an
active manifest (10a-C7).

The manifest is the contract: a caller's option merge that disagrees with the
stage's declared `run_stage_configs` row is refused before any call is built,
never left to `run_calls.request_hash` alone to notice after the fact.

R223a defers the FT screener's half (B2) to session 11's eligibility-bridge
commit, because no production driver opens a manifest naming
`ft_screen_primary` / `ft_screen_verifier` today (10a-C7 Phase A, M1) — T4
below pins that deferral rather than exercising a refusal that cannot fire yet.
The vision path's `num_predict` / `num_ctx` override needed no code change
(B3): it is already resolver-supplied (M2) and is now simply guarded by
`with_options` whenever it runs under a manifest that declared `vision_parse`.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fpdf import FPDF

from engine.agents.ft_screener import _ft_config
from engine.core import effective_config as ec
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.review_paths import load_spec_for
from engine.parsers.pdf_parser import parse_with_vision

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")

CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
DIGEST = "a" * 64


def _digest(model):
    return DIGEST


@pytest.fixture
def review(tmp_path):
    db = ReviewDatabase("undeclared_override", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    cb = load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    yield db, cb
    db.close()


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


def _open_vision_run(db, spec, cb):
    return rm.open_run(db._conn, spec, kind="screening", stages=("vision_parse",),
                       codebook=cb, git=CLEAN, digest_fn=_digest)


@pytest.fixture
def three_page_pdf(tmp_path):
    pdf = FPDF()
    for i in range(3):
        pdf.add_page()
        pdf.set_font("Helvetica", size=12)
        pdf.multi_cell(w=0, text=f"Page {i + 1} body text. " * 30)
    p = tmp_path / "three.pdf"
    pdf.output(str(p))
    return p


# ── T1 — a differing option under a declared stage is refused ─────────
def test_T1_a_differing_option_under_a_declared_stage_is_refused(review, spec):
    db, cb = review
    h = _open_vision_run(db, spec, cb)
    with rm.active_run(db._conn, h.run_id):
        cfg = ec.stage_config("vision_parse", spec)
        with pytest.raises(ec.UndeclaredOverride) as exc:
            cfg.with_options({"num_ctx": 4096})
    assert exc.value.stage == "vision_parse"
    assert exc.value.differing["num_ctx"] == {
        "recorded": spec.pdf_parsing.vision_num_ctx, "requested": 4096}
    # No call was ever built, let alone sent (R223: refused before any call).
    assert db._conn.execute("SELECT COUNT(*) FROM run_calls").fetchone()[0] == 0


# ── T2 — an equal option under a declared stage passes through ────────
def test_T2_an_equal_option_under_a_declared_stage_passes_through(review, spec):
    db, cb = review
    h = _open_vision_run(db, spec, cb)
    with rm.active_run(db._conn, h.run_id):
        cfg = ec.stage_config("vision_parse", spec)
        merged = cfg.with_options({"num_ctx": spec.pdf_parsing.vision_num_ctx})
    assert merged.options["num_ctx"] == spec.pdf_parsing.vision_num_ctx


# ── T3 — no active run is today's merge behaviour ──────────────────────
def test_T3_no_active_run_is_todays_merge_behaviour(spec):
    cfg = ec.stage_config("vision_parse", spec)
    merged = cfg.with_options({"num_ctx": 4096})
    assert merged.options["num_ctx"] == 4096


# ── T4' — the FT deferral (R223a): no manifest ever declares FT today ─
def test_T4_ft_config_with_temperature_none_is_the_resolver_unchanged(spec):
    cfg = _ft_config("ft_screen_primary", spec, None, None, None)
    resolver = ec.stage_config("ft_screen_primary", spec, model=None)
    assert cfg.options == resolver.options


def test_T4_ft_config_with_a_differing_temperature_and_no_manifest_still_merges(spec):
    """Pins the R223a deferral: FT screening runs with no active manifest today
    (10a-C7 Phase A, M1), so `with_options` returns today's merged config
    rather than refusing — there is no declared row to refuse against."""
    cfg = _ft_config("ft_screen_primary", spec, None, None, 0.7)
    assert cfg.options["temperature"] == 0.7


# ── T5 — the vision path, guarded by B1, with no code change of its own ─
def test_T5_vision_under_a_declared_run_refuses_a_differing_override(
        review, spec, three_page_pdf, monkeypatch):
    db, cb = review
    h = _open_vision_run(db, spec, cb)
    chat = Mock()
    monkeypatch.setattr("engine.parsers.pdf_parser.ollama_chat", chat)
    with rm.active_run(db._conn, h.run_id):
        with pytest.raises(ec.UndeclaredOverride):
            parse_with_vision(str(three_page_pdf), num_ctx=4096,
                              cfg=ec.stage_config("vision_parse", spec))
    chat.assert_not_called()


def test_T5_vision_under_a_declared_run_sends_the_rows_values(
        review, spec, three_page_pdf, monkeypatch):
    db, cb = review
    h = _open_vision_run(db, spec, cb)
    seen = {}

    def fake_chat(**kw):
        seen["kw"] = kw
        return SimpleNamespace(message=SimpleNamespace(content="page text", thinking=None),
                               done_reason="stop", eval_count=10, prompt_eval_count=10)

    monkeypatch.setattr("engine.parsers.pdf_parser.ollama_chat", fake_chat)
    with rm.active_run(db._conn, h.run_id):
        parse_with_vision(str(three_page_pdf), cfg=ec.stage_config("vision_parse", spec))
    assert seen["kw"]["options"]["num_predict"] == spec.pdf_parsing.vision_num_predict
    assert seen["kw"]["options"]["num_ctx"] == spec.pdf_parsing.vision_num_ctx


# ── T6 — open_run records vision_parse's declared options ─────────────
def test_T6_open_run_records_vision_parses_declared_options(review, spec):
    db, cb = review
    h = _open_vision_run(db, spec, cb)
    row = db._conn.execute(
        "SELECT options_json, options_hash FROM run_stage_configs "
        "WHERE run_id = ? AND stage = ?", (h.run_id, "vision_parse")).fetchone()
    assert row is not None
    recorded = json.loads(row["options_json"])
    assert recorded["num_predict"] == spec.pdf_parsing.vision_num_predict
    assert recorded["num_ctx"] == spec.pdf_parsing.vision_num_ctx
    assert row["options_hash"] == ec.stage_config("vision_parse", spec).options_hash


# T7 — the 7a invariant, unmoved by this task, is named and asserted in the
# closing full-suite run: tests/test_effective_config.py::
# test_no_site_builds_an_options_dict_or_names_a_model_literal
