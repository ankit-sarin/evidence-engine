"""12c-C53 — the PARSE stage resolves vision_parse and the parse thresholds from the spec.

`parse_all_pdfs` took no spec, so `parse_pdf` built
`stage_config("vision_parse", None)` and the engine-default thresholds while the
run's manifest recorded `vision_parse` resolved WITH the spec — a provenance
mismatch (C53's nature, 12c-C53-R1 R-4). The spec is now threaded
`run_pipeline` → `_stage_parse` → `parse_all_pdfs` → `parse_pdf`, and
`reparse_papers`; each takes it as a required argument, and `parse_pdf` reads it
unconditionally.

No model is called and no parse runs: a spy at `compute_pdf_hash` — the first
call after `parse_pdf` resolves its configuration — records what was resolved
and stops the run.
"""

from __future__ import annotations

import inspect
from pathlib import Path
from unittest.mock import patch

import pytest

from engine.core.database import ReviewDatabase
from engine.core.review_paths import load_spec_for
from engine.parsers import pdf_parser as PP
from engine.search.models import Citation


class _Resolved(BaseException):
    """Raised by the spy; a BaseException so parse_all_pdfs' per-paper
    `except Exception` cannot swallow it."""


@pytest.fixture()
def db(tmp_path):
    rdb = ReviewDatabase("test_c53", data_root=tmp_path)
    yield rdb
    rdb.close()


def _acquired_paper(db) -> int:
    db.add_papers([Citation(title="Paper", source="pubmed", pmid="1")])
    pid = db.get_papers_by_status("INGESTED")[-1]["id"]
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    pdfs = Path(db.db_path).parent / "pdfs"
    pdfs.mkdir(parents=True, exist_ok=True)
    (pdfs / f"{pid}.pdf").write_bytes(b"%PDF-1.4\n")
    return pid


def _non_default_spec():
    spec = load_spec_for("surgical_autonomy")
    spec.pdf_parsing.vision_num_predict = 1234        # a vision option
    spec.pdf_parsing.scanned_text_threshold = 4321    # a parse threshold
    return spec


def test_a_non_default_pdf_parsing_reaches_parse_pdfs_resolution(db):
    _acquired_paper(db)
    seen = {}

    def spy(pdf_path):
        frame = inspect.currentframe().f_back           # parse_pdf's frame
        seen.update(cfg=frame.f_locals["vision_opts"]["cfg"],
                    scanned=frame.f_locals["scanned_threshold"])
        raise _Resolved

    with patch.object(PP, "compute_pdf_hash", spy), pytest.raises(_Resolved):
        PP.parse_all_pdfs(db, "test_c53", spec=_non_default_spec())

    assert seen["cfg"].options["num_predict"] == 1234
    assert seen["scanned"] == 4321


def test_stage_parse_passes_the_runs_spec(db):
    import scripts.run_pipeline as rp
    _acquired_paper(db)                 # _stage_parse returns early with none
    spec = _non_default_spec()
    calls = []
    with patch.object(rp, "parse_all_pdfs",
                      lambda *a, **kw: calls.append(kw) or {"total": 0}):
        rp._stage_parse(db, "test_c53", spec=spec, run_id=1)
    assert len(calls) == 1 and calls[0]["spec"] is spec


@pytest.mark.parametrize("call", [
    lambda db: PP.parse_all_pdfs(db, "test_c53"),
    lambda db: PP.parse_pdf("no-such.pdf", 1, "test_c53", db),
    lambda db: PP.reparse_papers(db, [1]),
], ids=["parse_all_pdfs", "parse_pdf", "reparse_papers"])
def test_spec_is_required(db, call):
    with pytest.raises(TypeError, match="missing 1 required .*argument: 'spec'"):
        call(db)


def test_parse_pdf_with_a_none_spec_fails_at_the_first_read(db):
    """12c-C53-R1 R-2: no separate None check — a None spec fails where the
    spec is first read. Observed: AttributeError, before any hash is taken."""
    def no_hash(pdf_path):
        raise AssertionError("parse_pdf went past its configuration with spec=None")

    with patch.object(PP, "compute_pdf_hash", no_hash), \
            pytest.raises(AttributeError, match="'NoneType' object has no attribute 'pdf_parsing'"):
        PP.parse_pdf("no-such.pdf", 1, "test_c53", db, spec=None)
