"""STAGED-ENTRY-01 11c — the extraction-entry import (R204; 11c R-b..R-e, R-j..R-m).

A screened corpus enters an EMPTY review at FT_ELIGIBLE: per paper a papers row
through `insert_paper_at_status` (R-d), its supplied text placed under the
review's `parsed_text/` with a ref (`source_asset_id=None`, R-b), and an
`adjudicated` → `eligible` event under one `import` manifest whose `inputs` pin
the JSON file and every text (R-c); then the eight screening workflow stages
complete. No test reaches git or a model server: the manifest is opened with a
fixed git state and a null digest.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from engine.adjudication import import_extraction_entry as ee
from engine.adjudication.import_extraction_entry import (
    ExtractionEntryRejected,
    import_extraction_entry,
)
from engine.adjudication.workflow import WORKFLOW_STAGES
from engine.core import run_manifest as rm
from engine.core.database import (
    IMPORT_ENTRY_STATUSES,
    RETIRED_TOKENS,
    SCREENING_TOKENS,
    ImportStatusRefused,
    RetiredTransition,
    ReviewDatabase,
    insert_paper_at_status,
)
from engine.core.effective import eligible_paper_ids
from engine.core.review_paths import load_spec_for
from engine.core.selection import select_for_extraction
from engine.exporters.prisma import generate_prisma_flow, validate_prisma_counts
from engine.search.models import Citation

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")

CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)

TEXTS = [b"# Paper one\n\nMethods: a robot sutured.\n",
         b"# Paper two\n\nResults: \xc3\xa9l\xc3\xa8ve autonomy level 2.\n",
         b"# Paper three\n\nDiscussion.\n"]


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


@pytest.fixture
def db(tmp_path):
    d = ReviewDatabase("ee_import", data_root=tmp_path / "data")
    shutil.copy2(LIVE_CODEBOOK, Path(d.db_path).parent / "extraction_codebook.yaml")
    # I-R2 (11c-EE-R2): the constructor leaves parsed_text/ absent or empty.
    ptd = Path(d.db_path).parent / "parsed_text"
    assert not (ptd.exists() and any(ptd.iterdir())), \
        "STOP (I-R2): a fresh ReviewDatabase created files under parsed_text/"
    yield d
    d.close()


def _entries(n=3):
    return [{"title": f"Paper {i}", "pmid": f"EE{i}", "doi": f"10.1/ee.{i}",
             "abstract": "a", "authors": ["A", "B"], "journal": "J", "year": 2024,
             "text_path": f"texts/t{i}.md"} for i in range(1, n + 1)]


def _input(tmp_path, papers=None, *, source="smoke", texts=TEXTS, doc=None):
    d = tmp_path / "in"
    (d / "texts").mkdir(parents=True, exist_ok=True)
    for i, data in enumerate(texts, 1):
        (d / "texts" / f"t{i}.md").write_bytes(data)
    p = d / "entry.json"
    p.write_text(json.dumps(doc if doc is not None else
                            {"source": source, "papers": papers if papers is not None
                             else _entries(len(texts))}))
    return p


def _import(db, spec, path):
    return import_extraction_entry(db, path, spec=spec, git=CLEAN, digest_fn=lambda m: "")


def _n(db, table):
    return db._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _files(db):
    ptd = Path(db.db_path).parent / "parsed_text"
    return sorted(p.name for p in ptd.iterdir()) if ptd.exists() else []


def _manifests(db):
    return db._conn.execute(
        "SELECT run_id, run_kind, end_status, end_reason, manifest_json "
        "FROM run_manifests ORDER BY run_id").fetchall()


def _nothing_written(db):
    assert _n(db, "papers") == 0
    assert _n(db, "paper_events") == 0
    assert _n(db, "parsed_text_refs") == 0
    assert _files(db) == []


# ── T1 — happy path ──────────────────────────────────────────────────
def test_t1_three_papers_enter_at_ft_eligible_with_refs_events_and_one_manifest(
        db, spec, tmp_path):
    path = _input(tmp_path)
    out = _import(db, spec, path)
    assert out["imported"] == 3 and len(out["paper_ids"]) == 3
    c = db._conn

    rows = c.execute("SELECT id, status, source, pmid, doi, title FROM papers "
                     "ORDER BY id").fetchall()
    assert [r["status"] for r in rows] == ["FT_ELIGIBLE"] * 3
    assert {r["source"] for r in rows} == {"smoke"}
    assert [r["pmid"] for r in rows] == ["EE1", "EE2", "EE3"]

    ev = c.execute("SELECT paper_id, event_type, to_state, from_state, actor_kind, "
                   "actor_role, actor_name, run_id, reason_code, stage_name "
                   "FROM paper_events ORDER BY paper_id").fetchall()
    assert len(ev) == 3
    for r in ev:
        assert (r["event_type"], r["to_state"], r["from_state"]) == ("adjudicated", "eligible", None)
        assert (r["actor_kind"], r["actor_role"]) == ("human", "reviewer")
        assert r["actor_name"] == str(path.resolve())
        assert r["run_id"] == out["run_id"]
        assert r["reason_code"] is None
        assert r["stage_name"] == ee.STAGE_NAME

    refs = c.execute("SELECT paper_id, parsed_text_path, parsed_text_version, "
                     "parsed_text_sha256, source_full_text_assets_id "
                     "FROM parsed_text_refs ORDER BY paper_id").fetchall()
    assert len(refs) == 3
    for r, data, pid in zip(refs, TEXTS, out["paper_ids"]):
        assert r["paper_id"] == pid
        assert r["parsed_text_sha256"] == hashlib.sha256(data).hexdigest()
        assert r["parsed_text_version"] == 1
        assert r["source_full_text_assets_id"] is None
        placed = Path(db.db_path).parent / "parsed_text" / f"{pid}_v1.md"
        assert placed.read_bytes() == data
    assert _files(db) == sorted(f"{pid}_v1.md" for pid in out["paper_ids"])
    assert _n(db, "full_text_assets") == 0

    m = _manifests(db)
    assert len(m) == 1
    assert (m[0]["run_kind"], m[0]["end_status"], m[0]["end_reason"]) == \
        ("import", "completed", None)
    assert _n(db, "run_calls") == 0
    assert _n(db, "run_stage_configs") == 0


# ── T2 — PRISMA ─────────────────────────────────────────────────────
def test_t2_prisma_seam_holds_on_the_imported_review(db, spec, tmp_path):
    _import(db, spec, _input(tmp_path))
    flow = generate_prisma_flow(db)
    res = validate_prisma_counts(db, flow)
    remainder = [r[0] for r in db._conn.execute("SELECT status FROM papers")
                 if r[0] not in SCREENING_TOKENS]
    assert flow["n_eligible"] == 3
    assert len(remainder) == 3
    assert res["verification_pending"] == 0
    assert res["valid"], res["details"]
    assert res["total_db"] == 3


# ── T3 — extraction selects exactly the imported papers ─────────────
def test_t3_select_for_extraction_selects_the_three_with_no_refusal(db, spec, tmp_path):
    out = _import(db, spec, _input(tmp_path))
    sel = select_for_extraction(db._conn, arm=spec.extraction_models.arm)
    assert sorted(pid for pid, _ in sel.to_extract) == sorted(out["paper_ids"])
    assert sel.skipped_refused == () and sel.skipped_asserted == ()
    assert sorted(eligible_paper_ids(db._conn)) == sorted(out["paper_ids"])


# ── T4 — one test per validation rule ───────────────────────────────
def _bad_entries(mutate):
    papers = _entries()
    mutate(papers)
    return papers


_RULES = [
    ("title", lambda p: p[1].update(title="  "), "entry 2: title must be a non-empty string"),
    ("no_id", lambda p: p[0].update(pmid=None, doi=""), "entry 1: at least one of pmid and doi"),
    ("dup_pmid", lambda p: p[2].update(pmid=" EE1 "), "entry 3: duplicate pmid"),
    ("dup_doi_case", lambda p: p[1].update(doi="10.1/EE.1"), "entry 2: duplicate doi"),
    ("missing_text", lambda p: p[0].update(text_path="texts/nope.md"),
     "entry 1: text_path 'texts/nope.md' is not an existing regular file"),
    ("dir_text", lambda p: p[0].update(text_path="texts"), "is not an existing regular file"),
    ("dup_text", lambda p: p[2].update(text_path="texts/t1.md"), "is also entry 1's"),
    ("pmid_type", lambda p: p[0].update(pmid=123), "entry 1: pmid must be a string or null"),
    ("year_type", lambda p: p[0].update(year="2024"), "entry 1: year must be int or null"),
    ("authors_type", lambda p: p[0].update(authors="A"),
     "entry 1: authors must be a list of strings or null"),
]


@pytest.mark.parametrize("mutate,message", [(m, msg) for _, m, msg in _RULES],
                         ids=[r[0] for r in _RULES])
def test_t4_an_entry_rule_rejects_the_file_and_writes_nothing(db, spec, tmp_path,
                                                              mutate, message):
    path = _input(tmp_path, _bad_entries(mutate))
    with pytest.raises(ExtractionEntryRejected, match=message):
        _import(db, spec, path)
    _nothing_written(db)
    assert _manifests(db) == []


@pytest.mark.parametrize("texts,message", [
    ([b"", TEXTS[1], TEXTS[2]], "entry 1: text_path 'texts/t1.md' is empty"),
    ([TEXTS[0], b"\xff\xfe bad", TEXTS[2]], "entry 2: .* is not UTF-8 decodable"),
], ids=["empty_text", "non_utf8_text"])
def test_t4_a_text_file_rule_rejects(db, spec, tmp_path, texts, message):
    with pytest.raises(ExtractionEntryRejected, match=message):
        _import(db, spec, _input(tmp_path, texts=texts))
    _nothing_written(db)
    assert _manifests(db) == []


@pytest.mark.parametrize("doc,message", [
    ([1, 2], "top-level shape"),
    ({"source": "s"}, "top-level shape"),
    ({"source": "s", "papers": [], "extra": 1}, "top-level shape"),
    ({"source": " ", "papers": _entries()}, "source: must be a non-empty string"),
    ({"source": "s", "papers": []}, "papers: must be a non-empty list"),
    ({"source": "s", "papers": ["x"]}, "entry 1: must be an object"),
], ids=["not_object", "missing_papers", "extra_key", "empty_source", "empty_papers",
        "entry_not_object"])
def test_t4_a_file_shape_rule_rejects(db, spec, tmp_path, doc, message):
    with pytest.raises(ExtractionEntryRejected, match=message):
        _import(db, spec, _input(tmp_path, doc=doc))
    _nothing_written(db)
    assert _manifests(db) == []


def test_t4_unreadable_json_rejects(db, spec, tmp_path):
    path = _input(tmp_path)
    path.write_text("{not json")
    with pytest.raises(ExtractionEntryRejected, match="unreadable as JSON"):
        _import(db, spec, path)
    _nothing_written(db)
    assert _manifests(db) == []


def test_t4_a_non_empty_parsed_text_directory_rejects(db, spec, tmp_path):
    """R-l: the placement target starts empty; a stray file refuses the import."""
    stray = Path(db.db_path).parent / "parsed_text" / "stray.md"
    stray.parent.mkdir(exist_ok=True)
    stray.write_bytes(b"x")
    with pytest.raises(ExtractionEntryRejected, match="parsed_text.* is not empty"):
        _import(db, spec, _input(tmp_path))
    assert _n(db, "papers") == 0 and _n(db, "parsed_text_refs") == 0
    assert _files(db) == ["stray.md"] and stray.read_bytes() == b"x"
    assert _manifests(db) == []


# ── T5 — R-e: an existing paper refuses the import ──────────────────
def test_t5_a_review_with_one_paper_is_refused_before_any_manifest(db, spec, tmp_path):
    db.add_papers([Citation(title="Existing", pmid="X1", source="pubmed")])
    with pytest.raises(ExtractionEntryRejected, match=r"holds 1 papers row\(s\).*\(R-e\)"):
        _import(db, spec, _input(tmp_path))
    assert _n(db, "papers") == 1
    assert _n(db, "paper_events") == 0 and _n(db, "parsed_text_refs") == 0
    assert _manifests(db) == []


# ── T6a / T6b — mid-file failure ────────────────────────────────────
def test_t6a_a_generic_failure_at_entry_2_rolls_back_and_closes_failed(db, spec, tmp_path):
    real = ee.write_paper_event
    calls = {"n": 0}

    def boom(*a, **kw):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("injected")
        return real(*a, **kw)

    with patch.object(ee, "write_paper_event", boom), \
            pytest.raises(RuntimeError, match="injected"):
        _import(db, spec, _input(tmp_path))
    _nothing_written(db)   # no md and no tmp under parsed_text/
    m = _manifests(db)
    assert len(m) == 1 and m[0]["end_status"] == "failed"
    assert m[0]["end_reason"].startswith("entry 2 (")
    assert m[0]["end_reason"] == "entry 2 (EE2): injected"
    assert all(s == "pending" for (s,) in db._conn.execute(
        "SELECT status FROM workflow_state"))


def test_t6b_a_pre_placed_target_at_entry_2_aborts_and_is_left_untouched(db, spec, tmp_path):
    real = ee.insert_paper_at_status
    calls = {"n": 0}
    ptd = Path(db.db_path).parent / "parsed_text"
    pre = {}

    def seam(conn, **kw):
        pid = real(conn, **kw)
        calls["n"] += 1
        if calls["n"] == 2:     # after the insert, before placement
            pre["path"] = ptd / f"{pid}_v1.md"
            pre["path"].write_bytes(b"pre-placed")
        return pid

    with patch.object(ee, "insert_paper_at_status", seam), \
            pytest.raises(FileExistsError):
        _import(db, spec, _input(tmp_path))
    assert _n(db, "papers") == 0 and _n(db, "paper_events") == 0
    assert _n(db, "parsed_text_refs") == 0
    assert _files(db) == [pre["path"].name]
    assert pre["path"].read_bytes() == b"pre-placed"
    m = _manifests(db)
    assert len(m) == 1 and m[0]["end_status"] == "aborted"
    assert m[0]["end_reason"].startswith("entry 2 (EE2): ")


def test_t6_an_exception_after_the_loop_closes_failed_unprefixed(db, spec, tmp_path):
    """R-k: outside the per-paper loop the reason is str(exc), unprefixed."""
    def boom(*a, **kw):
        raise RuntimeError("stage write failed")

    with patch.object(ee, "complete_stage", boom), \
            pytest.raises(RuntimeError, match="stage write failed"):
        _import(db, spec, _input(tmp_path))
    _nothing_written(db)
    m = _manifests(db)
    assert (m[0]["end_status"], m[0]["end_reason"]) == ("failed", "stage write failed")


# ── T7 — the manifest pins every input ──────────────────────────────
def test_t7_manifest_inputs_hold_the_json_and_every_text_by_stored_path(db, spec, tmp_path):
    path = _input(tmp_path)
    _import(db, spec, path)
    inputs = json.loads(_manifests(db)[0]["manifest_json"])["inputs"]
    texts = path.parent / "texts"
    expected = {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest()}
    for i, data in enumerate(TEXTS, 1):
        expected[str((texts / f"t{i}.md").resolve())] = hashlib.sha256(data).hexdigest()
    assert inputs == expected


# ── T8 — workflow ───────────────────────────────────────────────────
def test_t8_the_eight_screening_stages_complete_and_no_later_stage_moves(db, spec, tmp_path):
    _import(db, spec, _input(tmp_path))
    status = dict(db._conn.execute("SELECT stage_name, status FROM workflow_state"))
    assert all(status[s] == "complete" for s in WORKFLOW_STAGES[:8])
    assert all(status[s] == "pending" for s in WORKFLOW_STAGES[8:])
    assert ee.SCREENING_WORKFLOW_STAGES == WORKFLOW_STAGES[:8]


# ── T9 — the R-d helper ─────────────────────────────────────────────
def test_t9_the_helper_refuses_a_status_outside_the_set(db):
    with pytest.raises(ImportStatusRefused):
        insert_paper_at_status(db._conn, status="PARSED", title="t", source="s")
    assert _n(db, "papers") == 0


@pytest.mark.parametrize("token", sorted(RETIRED_TOKENS))
def test_t9_the_helper_refuses_a_retired_token(db, token):
    with pytest.raises(RetiredTransition):
        insert_paper_at_status(db._conn, status=token, title="t", source="s")
    assert _n(db, "papers") == 0


@pytest.mark.parametrize("status", sorted(IMPORT_ENTRY_STATUSES))
def test_t9_the_helper_does_not_commit(db, status):
    pid = insert_paper_at_status(db._conn, status=status, title="t", source="s",
                                 pmid="P1", authors=["A"])
    row = db._conn.execute("SELECT status, authors, created_at, updated_at FROM papers "
                           "WHERE id = ?", (pid,)).fetchone()
    assert row["status"] == status and json.loads(row["authors"]) == ["A"]
    assert row["created_at"] == row["updated_at"]
    assert db._conn.in_transaction
    db._conn.rollback()
    assert _n(db, "papers") == 0


# ── T10 — pin ───────────────────────────────────────────────────────
def test_t10_import_entry_statuses_are_pinned_and_disjoint_from_retired():
    assert IMPORT_ENTRY_STATUSES == frozenset({"INGESTED", "FT_ELIGIBLE"})
    assert not (IMPORT_ENTRY_STATUSES & RETIRED_TOKENS)


# ── T11 — R-c: an imported ref is identifiable as imported ──────────
def test_t11_every_imported_ref_sha_appears_under_its_import_manifest_inputs(
        db, spec, tmp_path):
    out = _import(db, spec, _input(tmp_path))
    c = db._conn
    rows = c.execute(
        "SELECT r.parsed_text_sha256, r.source_full_text_assets_id, m.run_kind, "
        "m.manifest_json FROM parsed_text_refs r "
        "JOIN paper_events e ON e.paper_id = r.paper_id "
        "JOIN run_manifests m ON m.run_id = e.run_id").fetchall()
    assert len(rows) == 3
    for r in rows:
        assert r["run_kind"] == "import"
        assert r["source_full_text_assets_id"] is None
        assert r["parsed_text_sha256"] in json.loads(r["manifest_json"])["inputs"].values()
    assert {r[0] for r in c.execute("SELECT run_id FROM paper_events")} == {out["run_id"]}
