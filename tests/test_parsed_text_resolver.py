"""The one parsed-text resolver (S3e) — INPUT-IDENTITY-01 gates G2 and G5,
the R95 read-time verification, and R66's "nothing handed on changes"."""

from __future__ import annotations

import hashlib
import importlib
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from engine.core import parsed_text as pt
from engine.core.database import ReviewDatabase
from _parsed_text_fixture import write_parsed
from _event_store_fixture import open_extraction_run, seed_eligibility

REPO = Path(__file__).resolve().parent.parent
m021 = importlib.import_module("engine.migrations.021_parsed_text_sha256")


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("resolver", data_root=tmp_path)
    rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, updated_at) "
                      "VALUES (7, 't', 's', 'PARSED', 'n', 'n')")
    rdb._conn.commit()
    yield rdb
    rdb.close()


# ── G2: _v10 beats _v9 ────────────────────────────────────────────────
def test_g2_version_10_beats_version_9(db):
    for v in range(1, 11):
        write_parsed(db, 7, f"text of version {v}\n", version=v)
    ref = pt.resolve_parsed_text(db._conn, 7)
    assert ref.version == 10
    assert ref.path.name == "7_v10.md"
    assert pt.read_parsed_text(ref) == "text of version 10\n"
    # the rule it replaced, for contrast: lexical order picks version 9
    lexical = sorted(ref.path.parent.glob("7_v*.md"), reverse=True)[0]
    assert lexical.name == "7_v9.md"


def test_the_resolver_lists_no_directory(db, monkeypatch):
    write_parsed(db, 7, "x\n")

    def refuse(*a, **k):
        raise AssertionError("the resolver must not list the filesystem")

    monkeypatch.setattr(Path, "glob", refuse)
    monkeypatch.setattr(Path, "iterdir", refuse)
    assert pt.load_parsed_text(db._conn, 7) == "x\n"


# ── R95: verified on every read; three distinct refusals ──────────────
def test_a_one_character_change_is_refused_with_both_hashes_named(db):
    path = write_parsed(db, 7, "The robot sutured.\n")
    recorded = hashlib.sha256(path.read_bytes()).hexdigest()
    path.write_text("The robot sutureD.\n")
    observed = hashlib.sha256(path.read_bytes()).hexdigest()
    ref = pt.resolve_parsed_text(db._conn, 7)
    with pytest.raises(pt.ParsedTextModified) as exc:
        pt.read_parsed_text(ref)
    msg = str(exc.value)
    assert ref.parsed_text_uid in msg and recorded in msg and observed in msg
    assert "NEW version" in msg


def test_a_missing_file_is_its_own_refusal(db):
    path = write_parsed(db, 7, "x\n")
    path.unlink()
    with pytest.raises(pt.ParsedTextMissing):
        pt.load_parsed_text(db._conn, 7)


def test_no_reference_is_its_own_refusal(db):
    with pytest.raises(pt.NoParsedText):
        pt.load_parsed_text(db._conn, 7)


def test_the_three_refusals_are_distinct():
    kinds = (pt.NoParsedText, pt.ParsedTextMissing, pt.ParsedTextModified)
    for a in kinds:
        assert issubclass(a, pt.ParsedTextError)
        for b in kinds:
            assert a is b or not issubclass(a, b)


# ── R66: what a caller is handed is what the file holds ───────────────
TEXT = "# Title\n\nUnicode — ok: μ, ±, 95%.\nSecond line.\n"


def test_text_handed_on_equals_the_file_bytes(db):
    path = write_parsed(db, 7, TEXT)
    got = pt.load_parsed_text(db._conn, 7)
    assert got == path.read_text()
    assert got.encode("utf-8") == path.read_bytes()


def test_ft_screener_site_hands_on_the_file(db):
    from engine.agents.ft_screener import _load_parsed_text
    path = write_parsed(db, 7, TEXT)
    assert _load_parsed_text(db, 7).encode("utf-8") == path.read_bytes()
    assert _load_parsed_text(db, 8) is None  # no reference → None, as "no file" was


def test_cloud_site_hands_on_the_file(db):
    from engine.cloud.base import CloudExtractorBase
    path = write_parsed(db, 7, TEXT)
    got = CloudExtractorBase.load_parsed_text(SimpleNamespace(_conn=db._conn), 7)
    assert got.encode("utf-8") == path.read_bytes()
    with pytest.raises(FileNotFoundError):
        CloudExtractorBase.load_parsed_text(SimpleNamespace(_conn=db._conn), 8)


def test_extractor_site_hands_on_the_file(db, tmp_path):
    from engine.agents.extractor import run_extraction
    from engine.core.review_spec import load_review_spec
    path = write_parsed(db, 7, TEXT)
    seed_eligibility(db._conn, 7)   # 9b-2a: selection reads the eligibility axis
    seen = []

    def capture(pid, paper_text, *a, **k):
        seen.append(paper_text)
        raise RuntimeError("captured")

    spec = load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")
    run_id = open_extraction_run(db, spec)   # 9b-2b: run_id is required (R116)
    with patch("engine.utils.ollama_preflight.require_preflight"), \
         patch("engine.utils.ollama_client.fetch_model_digest", return_value="d" * 64), \
         patch("engine.agents.extractor.extract_paper_with_completeness", side_effect=capture):
        run_extraction(db, spec, "resolver", experiment_lock=False, restart_every=0,
                       run_id=run_id)
    assert [s.encode("utf-8") for s in seen] == [path.read_bytes()]


@pytest.mark.parametrize("module", [
    "engine.agents.extractor", "engine.agents.auditor", "engine.agents.ft_screener",
    "engine.cloud.base", "engine.review.human_review"])
def test_each_former_glob_site_reads_through_the_resolver(module):
    mod = importlib.import_module(module)
    if module == "engine.agents.extractor":
        # 9b-2a: selection resolves the reference; the loop reads it through
        # the resolver's verified read (R95).
        assert mod.read_parsed_text is pt.read_parsed_text
    else:
        assert mod.load_parsed_text is pt.load_parsed_text


# ── G5: no engine module resolves a parsed text by glob ───────────────
def test_g5_no_parsed_text_glob_under_engine_outside_analysis():
    hits = []
    for f in (REPO / "engine").rglob("*.py"):
        rel = f.relative_to(REPO).as_posix()
        if rel.startswith("engine/analysis/"):
            continue
        for n, line in enumerate(f.read_text().splitlines(), 1):
            if re.search(r"_v\*", line):
                hits.append(f"{rel}:{n}")
    assert hits == []


# ── R35 copy, one rule: 021's canonical_path agrees with the engine's ─
@pytest.mark.parametrize("stored", [
    "data/x/parsed_text/1_v1.md", "data/x/../x/parsed_text/1_v2.md",
    "/somewhere/else/1_v1.md", "./data/a.md"])
def test_migration_021_and_the_resolver_canonicalise_alike(stored):
    assert m021.canonical_path(stored, pt.REPO_ROOT) == pt.canonical_path(stored)
    under = str(pt.REPO_ROOT / "data" / "q.md")
    assert m021.canonical_path(under, pt.REPO_ROOT) == pt.canonical_path(under) == "data/q.md"
