"""STAGED-ENTRY-01 11c — the screening-entry import (R202; 11c R-r, R-s, R-t).

An identified citation set enters an EMPTY review at INGESTED: one papers row per
entry through `insert_paper_at_status`, under one `import` manifest whose
`inputs` pin the file. No paper event, no ref, no workflow write, no model call.
Every within-file duplicate is refused (R-s). No test reaches git or a model
server: the manifest is opened with a fixed git state and a null digest.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from engine.adjudication import import_screening_entry as se
from engine.adjudication.import_screening_entry import (
    ScreeningEntryRejected,
    import_screening_entry,
    normalise_title,
)
from engine.core import run_manifest as rm
from engine.core.database import ReviewDatabase
from engine.core.review_paths import load_spec_for
from engine.exporters.prisma import generate_prisma_flow, validate_prisma_counts
from engine.search.models import Citation

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")

CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


@pytest.fixture
def db(tmp_path):
    d = ReviewDatabase("se_import", data_root=tmp_path / "data")
    shutil.copy2(LIVE_CODEBOOK, Path(d.db_path).parent / "extraction_codebook.yaml")
    yield d
    d.close()


def _entries():
    """Four entries: two identified, one without an abstract, one with neither id."""
    return [
        {"title": "Autonomous suturing", "pmid": "SE1", "doi": "10.1/se.1",
         "abstract": "a", "authors": ["A"], "journal": "J", "year": 2024},
        {"title": "Robot camera control", "pmid": None, "doi": "10.1/se.2",
         "abstract": "b", "authors": ["B"], "journal": "J", "year": 2023},
        {"title": "Needle steering", "pmid": "SE3", "doi": None, "journal": "K"},
        {"title": "A conference abstract with no identifiers", "abstract": "c",
         "year": 2022},
    ]


def _input(tmp_path, papers=None, *, source="smoke-se", doc=None):
    d = tmp_path / "in"
    d.mkdir(exist_ok=True)
    p = d / "entry.json"
    p.write_text(json.dumps(doc if doc is not None else
                            {"source": source,
                             "papers": papers if papers is not None else _entries()}))
    return p


def _import(db, spec, path):
    return import_screening_entry(db, path, spec=spec, git=CLEAN, digest_fn=lambda m: "")


def _n(db, table):
    return db._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _manifests(db):
    return db._conn.execute(
        "SELECT run_id, run_kind, end_status, end_reason, manifest_json "
        "FROM run_manifests ORDER BY run_id").fetchall()


def _workflow(db):
    return [tuple(r) for r in db._conn.execute(
        "SELECT stage_name, status, completed_at, metadata FROM workflow_state ORDER BY id")]


# ── T1 — happy path ──────────────────────────────────────────────────
def test_t1_four_entries_enter_at_ingested_under_one_manifest(db, spec, tmp_path):
    before = _workflow(db)
    path = _input(tmp_path)
    out = _import(db, spec, path)
    assert out["imported"] == 4 and len(out["paper_ids"]) == 4

    rows = db._conn.execute("SELECT id, status, source, pmid, doi, title, abstract, "
                            "authors, journal, year FROM papers ORDER BY id").fetchall()
    assert [r["status"] for r in rows] == ["INGESTED"] * 4
    assert {r["source"] for r in rows} == {"smoke-se"}
    assert [(r["pmid"], r["doi"]) for r in rows] == [
        ("SE1", "10.1/se.1"), (None, "10.1/se.2"), ("SE3", None), (None, None)]
    assert rows[2]["abstract"] is None and rows[3]["abstract"] == "c"
    assert json.loads(rows[0]["authors"]) == ["A"] and json.loads(rows[3]["authors"]) == []

    m = _manifests(db)
    assert len(m) == 1
    assert (m[0]["run_kind"], m[0]["end_status"], m[0]["end_reason"]) == \
        ("import", "completed", None)
    inputs = json.loads(m[0]["manifest_json"])["inputs"]
    assert inputs == {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest()}

    for table in ("paper_events", "parsed_text_refs", "full_text_assets", "run_calls",
                  "run_stage_configs", "field_events"):
        assert _n(db, table) == 0, table
    assert _workflow(db) == before


# ── T2 — PRISMA (I2) ────────────────────────────────────────────────
def test_t2_prisma_reads_an_ingested_only_review(db, spec, tmp_path):
    _import(db, spec, _input(tmp_path))
    flow = generate_prisma_flow(db)
    res = validate_prisma_counts(db, flow)
    assert flow["records_identified"] == 4
    assert flow["records_by_source"] == {"smoke-se": 4}
    assert flow["records_screened"] == 0
    assert flow["n_eligible"] == 0
    assert flow["screening_in_progress"] == 0
    assert res["valid"], res["details"]
    assert (res["total_db"], res["total_prisma"], res["verification_pending"]) == (4, 4, 0)


# ── T3 — abstract screening's own selection sees exactly the imported rows ──
def test_t3_the_screening_selection_returns_exactly_the_four(db, spec, tmp_path):
    out = _import(db, spec, _input(tmp_path))
    assert sorted(p["id"] for p in db.get_papers_by_status("INGESTED")) == \
        sorted(out["paper_ids"])


# ── T4 — one rejection test per rule ────────────────────────────────
def _mutated(mutate):
    papers = _entries()
    mutate(papers)
    return papers


_RULES = [
    ("dup_pmid", lambda p: p[2].update(pmid=" SE1 "), "entry 3: duplicate pmid"),
    ("dup_doi_case", lambda p: p[2].update(doi=" 10.1/SE.2"), "entry 3: duplicate doi"),
    ("idless_title_vs_idless",
     lambda p: p.append({"title": "  A CONFERENCE abstract   with no identifiers "}),
     "entry 5: has neither pmid nor doi and its title duplicates entry 4's"),
    ("idless_title_vs_identified", lambda p: p[3].update(title="autonomous  SUTURING"),
     "entry 4: has neither pmid nor doi and its title duplicates entry 1's"),
    ("missing_title", lambda p: p[1].pop("title"), "entry 2: title must be a non-empty string"),
    ("blank_title", lambda p: p[1].update(title="   "), "entry 2: title must be a non-empty string"),
    ("pmid_type", lambda p: p[0].update(pmid=17), "entry 1: pmid must be a string or null"),
    ("doi_type", lambda p: p[0].update(doi=["x"]), "entry 1: doi must be a string or null"),
    ("year_type", lambda p: p[0].update(year="2024"), "entry 1: year must be int or null"),
    ("year_bool", lambda p: p[0].update(year=True), "entry 1: year must be int or null"),
    ("abstract_type", lambda p: p[0].update(abstract=1), "entry 1: abstract must be str or null"),
    ("journal_type", lambda p: p[0].update(journal=2), "entry 1: journal must be str or null"),
    ("authors_type", lambda p: p[0].update(authors=[1]),
     "entry 1: authors must be a list of strings or null"),
    ("entry_not_object", lambda p: p.append("x"), "entry 5: must be an object"),
]


@pytest.mark.parametrize("mutate,message", [(m, msg) for _, m, msg in _RULES],
                         ids=[r[0] for r in _RULES])
def test_t4_an_entry_rule_rejects_the_file_and_writes_nothing(db, spec, tmp_path,
                                                              mutate, message):
    with pytest.raises(ScreeningEntryRejected, match=message):
        _import(db, spec, _input(tmp_path, _mutated(mutate)))
    assert _n(db, "papers") == 0
    assert _manifests(db) == []


@pytest.mark.parametrize("doc,message", [
    ([1], "top-level shape"),
    ({"papers": _entries()}, "top-level shape"),
    ({"source": "", "papers": _entries()}, "source: must be a non-empty string"),
    ({"source": None, "papers": _entries()}, "source: must be a non-empty string"),
    ({"source": "s", "papers": []}, "papers: must be a non-empty list"),
], ids=["not_object", "missing_source", "empty_source", "null_source", "empty_papers"])
def test_t4_a_file_shape_rule_rejects(db, spec, tmp_path, doc, message):
    with pytest.raises(ScreeningEntryRejected, match=message):
        _import(db, spec, _input(tmp_path, doc=doc))
    assert _n(db, "papers") == 0
    assert _manifests(db) == []


def test_t4_unreadable_json_rejects(db, spec, tmp_path):
    path = _input(tmp_path)
    path.write_bytes(b"\xff{")
    with pytest.raises(ScreeningEntryRejected, match="unreadable as JSON"):
        _import(db, spec, path)
    assert _n(db, "papers") == 0
    assert _manifests(db) == []


def test_t4_every_failed_rule_is_reported_in_one_rejection(db, spec, tmp_path):
    def two(p):
        p[0].update(year="x")
        p[2].update(pmid="SE1")
    with pytest.raises(ScreeningEntryRejected) as info:
        _import(db, spec, _input(tmp_path, _mutated(two)))
    assert len(info.value.errors) == 2


# ── T4b — admission: identified entries are not title-deduplicated ──
def test_t4b_two_identified_entries_with_the_same_title_are_admitted(db, spec, tmp_path):
    papers = [{"title": "Same title", "doi": "10.1/a"},
              {"title": "same  TITLE", "doi": "10.1/b"}]
    out = _import(db, spec, _input(tmp_path, papers))
    assert out["imported"] == 2
    assert normalise_title(" Same \t title ") == "same title"


# ── T5 — R-r: an existing paper refuses the import ──────────────────
def test_t5_a_review_with_one_paper_is_refused_before_any_manifest(db, spec, tmp_path):
    db.add_papers([Citation(title="Existing", pmid="X1", source="pubmed")])
    with pytest.raises(ScreeningEntryRejected, match=r"holds 1 papers row\(s\).*\(R-r\)"):
        _import(db, spec, _input(tmp_path))
    assert _n(db, "papers") == 1
    assert _manifests(db) == []


# ── T6 — mid-file failure ───────────────────────────────────────────
def test_t6_a_generic_failure_at_entry_2_rolls_back_and_closes_failed(db, spec, tmp_path):
    real = se.insert_paper_at_status
    calls = {"n": 0}

    def boom(conn, **kw):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("injected")
        return real(conn, **kw)

    with patch.object(se, "insert_paper_at_status", boom), \
            pytest.raises(RuntimeError, match="injected"):
        _import(db, spec, _input(tmp_path))
    assert _n(db, "papers") == 0
    m = _manifests(db)
    assert len(m) == 1 and m[0]["end_status"] == "failed"
    assert m[0]["end_reason"] == "entry 2 (10.1/se.2): injected"


def test_t6_an_id_less_entry_is_labelled_by_its_title(db, spec, tmp_path):
    """I3: an entry with neither pmid nor doi is named by its title's first 40 chars."""
    real = se.insert_paper_at_status
    calls = {"n": 0}

    def boom(conn, **kw):
        calls["n"] += 1
        if calls["n"] == 4:
            raise RuntimeError("injected")
        return real(conn, **kw)

    with patch.object(se, "insert_paper_at_status", boom), pytest.raises(RuntimeError):
        _import(db, spec, _input(tmp_path))
    reason = _manifests(db)[0]["end_reason"]
    assert reason == "entry 4 (title: A conference abstract with no identifier): injected"


def test_t6_a_helper_refusal_closes_aborted(db, spec, tmp_path, monkeypatch):
    """R-k: the helper's refusal is recognised and closes the manifest 'aborted'."""
    monkeypatch.setattr(se, "ENTRY_STATUS", "PARSED")
    with pytest.raises(se.ImportStatusRefused):
        _import(db, spec, _input(tmp_path))
    assert _n(db, "papers") == 0
    m = _manifests(db)
    assert m[0]["end_status"] == "aborted"
    assert m[0]["end_reason"].startswith("entry 1 (SE1): insert at 'PARSED' refused")
