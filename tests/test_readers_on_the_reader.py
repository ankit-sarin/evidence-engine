"""The four migrated readers, on the reader (READERS-01 Phase 2a).

Concordance, the evidence-table exporter, the judge loader and the distribution
monitor no longer touch `extractions` / `evidence_spans` / `cloud_*` and no
longer route on an arm NAME. Everything here runs on a constructed event history
(standing constraint from READERS-01 contradiction 7).
"""

from __future__ import annotations

import re
import shutil
import sqlite3
import subprocess
from pathlib import Path

import pytest

from engine.core import events
from engine.core.effective import UnknownArm

REPO = Path(__file__).resolve().parent.parent
CODEBOOK = REPO / "data/surgical_autonomy/extraction_codebook.yaml"


@pytest.fixture
def review(tmp_path):
    """A real review directory: schema through the runner, codebook beside it."""
    from engine.core.database import ReviewDatabase

    root = tmp_path / "rev"
    root.mkdir()
    shutil.copy(CODEBOOK, root / "extraction_codebook.yaml")
    db = ReviewDatabase(str(root))

    conn = db._conn
    for pid in (1, 2, 3):
        conn.execute(
            "INSERT INTO papers (id,title,source,status,created_at,updated_at,"
            "pmid,doi,authors,year,journal) VALUES (?,?,'manual','INGESTED',"
            "'x','x',?,?,'[]',2026,'J')", (pid, f"paper {pid}", str(pid), f"10.1/{pid}"))
    conn.commit()

    events.register_arm(conn, "local", "model")
    events.register_arm(conn, "openai_o4_mini_high", "model")
    events.register_arm(conn, "human_A", "human_extractor")
    for pid in (1, 2, 3):
        events.write_paper_event(
            conn, event_type="adjudicated", paper_id=pid, to_state="eligible",
            actor_kind="engine", actor_role="system", actor_name="fixture")
    conn.commit()
    yield db, root
    db.close()


def _assert_value(conn, paper_id, field, arm, value, *, located=True):
    """Returns the CLAIM ID, not the event id — an against-reference names
    claims (R20), and passing an event id would read as a reference to a claim
    that does not exist, which v2.1 resolves as "needs re-review"."""
    uid = events.mint_extraction_uid()
    events.write_field_event(
        conn, event_type="asserted", paper_id=paper_id, field_name=field,
        arm=arm, value=value, extraction_uid=uid, source_snippet=value,
        actor_kind="model", actor_role="extractor", actor_name="m")
    if located:
        events.write_field_event(
            conn, event_type="citation_located", paper_id=paper_id,
            field_name=field, arm=arm, extraction_uid=uid,
            actor_kind="engine", actor_role="system", actor_name="locator",
            payload={"located": True, "snippet": value})
    conn.commit()
    return events.make_claim_id(arm, uid, field)


def _decline(conn, paper_id, field, arm):
    events.write_field_event(
        conn, event_type="declined", paper_id=paper_id, field_name=field,
        arm=arm, extraction_uid=events.mint_extraction_uid(),
        actor_kind="model", actor_role="extractor", actor_name="m")
    conn.commit()


# ── G6 / A12: the registry is the routing predicate ──────────────────

def test_load_arm_returns_nothing_for_a_registered_human_arm_with_no_claims(review):
    """A12's shape. The old code sent `human_*` into the cloud branch and got
    `{}` with no error — indistinguishable from "this arm has no values". Now
    the arm resolves, the grid enumerates its cells, and every one reads
    `missing`, which is a different fact from "no such arm"."""
    from engine.analysis.concordance import load_arm

    db, root = review
    _assert_value(db._conn, 1, "study_type", "local", "RCT")
    assert load_arm(str(db.db_path), "human_A") == {}
    assert load_arm(str(db.db_path), "local") == {1: {"study_type": "RCT"}}


def test_load_arm_raises_on_a_name_that_is_not_in_the_registry(review):
    from engine.analysis.concordance import load_arm

    db, root = review
    with pytest.raises(UnknownArm, match="not in this review's registry"):
        load_arm(str(db.db_path), "openai_o4_mini")     # a real typo: missing _high


def test_query_values_raises_on_an_unregistered_arm(review):
    from engine.validators.distribution_monitor import _query_values

    db, root = review
    with pytest.raises(UnknownArm):
        _query_values(db._conn, "study_type", "human_B")


def test_query_values_and_load_arm_agree_on_a_human_arm(review):
    """A12 closed. The two files disagreed BY CONSTRUCTION before — `load_arm`
    had two branches and `_query_values` three, so one sent a `human_*` arm to
    `cloud_evidence_spans` and the other to `human_extractions`. There is one
    predicate now, and the answer they agree on is R13's: a human-extractor arm
    is assigned nothing until the assignment table arrives in session 12, so its
    cells are OUT OF SCOPE (rule row 0) — not missing, and not an error."""
    from engine.analysis.concordance import load_arm
    from engine.core.effective import effective_value
    from engine.validators.distribution_monitor import _query_values

    db, root = review
    _assert_value(db._conn, 1, "study_type", "human_A", "Original Research")

    ev = effective_value(db._conn, 1, "study_type", "human_A",
                         sentinels=frozenset())
    assert (ev.rule_row, ev.state) == (0, "out of scope"), "R13"

    assert _query_values(db._conn, "study_type", "human_A") == []
    assert load_arm(str(db.db_path), "human_A") == {}


def test_load_arm_does_not_fold_every_extraction(review):
    """A7: the old local branch had no run selection, so the LAST row of
    `ORDER BY paper_id, field_name` won — neither newest nor oldest by design."""
    from engine.analysis.concordance import load_arm

    db, root = review
    _assert_value(db._conn, 1, "study_type", "local", "old")
    _assert_value(db._conn, 1, "study_type", "local", "new")
    got = load_arm(str(db.db_path), "local")[1]["study_type"]
    # two pre-manifest claims on one cell do not supersede (R10) — v2.1 sends the
    # cell to "unresolved (needs re-review)" and no value is exported. What it
    # does NOT do is silently pick one, which is what the fold did.
    assert got != "old", "the older claim must not simply stand (A7)"


# ── G4 / D1-4: declined and withdrawn through the exporter ───────────

def test_a_declined_field_reports_as_declined_in_the_evidence_table(review):
    from engine.exporters.evidence_table import _build_evidence_rows

    db, root = review
    _assert_value(db._conn, 1, "study_type", "local", "RCT")
    _decline(db._conn, 1, "sample_size", "local")

    headers, rows = _build_evidence_rows(db, None, arm="local")
    row = dict(zip(headers, rows[0]))
    assert row["study_type"] == "RCT"
    assert row["study_type_state"] == "asserted with evidence"
    assert row["sample_size"] == ""
    assert row["sample_size_state"] == "declined"
    assert row["sample_size_rule_row"] == 14


def test_a_withdrawn_value_is_no_value_in_the_evidence_table(review):
    """D1-4."""
    from engine.exporters.evidence_table import _build_evidence_rows

    db, root = review
    claim = _assert_value(db._conn, 1, "study_type", "local", "RCT")
    events.write_field_event(
        db._conn, event_type="human_withdrew", paper_id=1,
        field_name="study_type", arm="local", against_claims=[claim],
        actor_kind="human", actor_role="reviewer", actor_name="PI")
    db._conn.commit()

    headers, rows = _build_evidence_rows(db, None, arm="local")
    row = dict(zip(headers, rows[0]))
    assert row["study_type"] == ""
    assert row["study_type_state"] == "withdrawn"
    assert row["study_type_rule_row"] == 4


def test_the_evidence_table_carries_both_axes_and_no_confidence_column(review):
    """R36: `{field}_confidence` is gone; `{field}_state` and `_rule_row` replace
    it. R29/R39: the paper's two axes are columns of their own."""
    from engine.exporters.evidence_table import _build_evidence_rows

    db, root = review
    _assert_value(db._conn, 1, "study_type", "local", "RCT")
    headers, rows = _build_evidence_rows(db, None, arm="local")

    assert not [h for h in headers if h.endswith("_confidence")]
    assert "study_type_state" in headers and "study_type_rule_row" in headers
    for col in ("eligibility", "processing", "processing_reason", "analysis_ready"):
        assert col in headers


def test_a_failed_paper_is_exported_with_its_reason_and_is_not_analysis_ready(review):
    """G3 through the exporter: eligible, failed, reported by reason."""
    from engine.exporters.evidence_table import _build_evidence_rows

    db, root = review
    _assert_value(db._conn, 2, "study_type", "local", "RCT")
    events.write_paper_event(
        db._conn, event_type="extraction_failed", paper_id=2,
        to_state="extraction_failed", reason_code="extraction failed after retries",
        actor_kind="engine", actor_role="system", actor_name="fixture")
    db._conn.commit()

    headers, rows = _build_evidence_rows(db, None, arm="local")
    by_pid = {r[0]: dict(zip(headers, r)) for r in rows}
    assert 2 in by_pid, "a processing failure must not remove a paper from the export"
    assert by_pid[2]["eligibility"] == "eligible"
    assert by_pid[2]["processing"] == "extraction_failed"
    assert by_pid[2]["processing_reason"] == "extraction failed after retries"
    assert by_pid[2]["analysis_ready"] is False


def test_the_exporter_refuses_an_unregistered_arm(review):
    from engine.exporters.evidence_table import _build_evidence_rows

    db, root = review
    with pytest.raises(UnknownArm):
        _build_evidence_rows(db, None, arm="not_an_arm")


# ── G5 / S1c: the verdict is a feature, not a filter ─────────────────

def test_the_judge_loader_attempts_every_cell_whatever_the_verdict(review):
    """R28. On a fixture of mixed verdicts — including cells where all arms AGREE
    and cells no arm claimed — the loader emits one JudgeInput per cell."""
    from analysis.paper1.judge_loader import load_codebook, load_grid

    db, root = review
    (root / "parsed_text").mkdir(exist_ok=True)
    for pid in (1, 2, 3):
        (root / "parsed_text" / f"{pid}_v1.md").write_text("the paper text " * 50)

    # cell A: the two model arms AGREE (a scorer MATCH — never judged before)
    _assert_value(db._conn, 1, "study_type", "local", "RCT")
    _assert_value(db._conn, 1, "study_type", "openai_o4_mini_high", "RCT")
    # cell B: they DISAGREE (a scorer MISMATCH — the only kind ever judged)
    _assert_value(db._conn, 1, "sample_size", "local", "51")
    _assert_value(db._conn, 1, "sample_size", "openai_o4_mini_high", "150")
    # every other cell: no claim at all

    codebook = load_codebook(root / "extraction_codebook.yaml")
    inputs = load_grid(db._conn, root, codebook, root / "extraction_codebook.yaml")

    from engine.core.effective import (
        eligible_paper_ids, registered_arms,
    )
    from engine.core.codebook import load_codebook as load_cb
    n_papers = len(eligible_paper_ids(db._conn))
    n_fields = len(load_cb(root / "extraction_codebook.yaml").field_names)
    # a DERIVATION, not a literal: |papers| x |fields|, one JudgeInput per cell
    assert len(inputs) == n_papers * n_fields

    by_cell = {(i.paper_id, i.field_name): i for i in inputs}
    assert by_cell[("1", "study_type")].arms[0].arm_name in registered_arms(db._conn)
    # the AGREEING cell is present — this is exactly what B3 dropped
    agree = by_cell[("1", "study_type")]
    assert {a.value for a in agree.arms if a.value} == {"RCT"}
    # and so is a cell nobody claimed
    empty = by_cell[("3", "country")]
    assert all(a.value is None for a in empty.arms)
    # every arm in the registry is represented on every cell
    assert all(len(i.arms) == len(registered_arms(db._conn)) for i in inputs)


def test_the_scorer_verdict_rides_as_a_feature(review):
    from analysis.paper1.judge_loader import load_codebook, load_grid

    db, root = review
    (root / "parsed_text").mkdir(exist_ok=True)
    for pid in (1, 2, 3):
        (root / "parsed_text" / f"{pid}_v1.md").write_text("text " * 50)
    _assert_value(db._conn, 1, "study_type", "local", "RCT")

    codebook = load_codebook(root / "extraction_codebook.yaml")
    inputs = load_grid(db._conn, root, codebook, root / "extraction_codebook.yaml",
                       verdicts={(1, "study_type"): "MATCH"})
    got = {(i.paper_id, i.field_name): i.scorer_verdict for i in inputs}
    assert got[("1", "study_type")] == "MATCH"
    assert got[("1", "sample_size")] is None


# ── G8: no read-write path survives ──────────────────────────────────

@pytest.mark.parametrize("rel", [
    "engine/analysis/concordance.py",
    "analysis/paper1/judge_loader.py",
])
def test_no_bare_sqlite3_connect_survives(rel):
    """I5. `mode=ro`, never `immutable=1` — `review.db` is live, so `immutable`
    would be a promise the file cannot keep and the reader could see a torn page."""
    text = (REPO / rel).read_text()
    for line in text.splitlines():
        if "sqlite3.connect(" in line and not line.lstrip().startswith("#"):
            assert "mode=ro" in line or "_ro_uri" in line, (
                f"{rel}: {line.strip()} opens the live database read-write")
    # `immutable=1` may be NAMED in prose (both files explain why it is refused);
    # what must not exist is a connect string that uses it.
    assert not [ln for ln in text.splitlines()
                if "immutable=1" in ln and "connect" in ln]


def test_the_judge_loader_no_longer_reaches_through_a_private_attribute():
    """I13 (R38). `db._conn` on an open read-write `ReviewDatabase` was a
    read-write path the I5 grep could not see, because it is not a
    `sqlite3.connect` call."""
    text = (REPO / "analysis/paper1/judge_loader.py").read_text()
    code = [ln for ln in text.splitlines()
            if not ln.lstrip().startswith("#") and "db._conn" in ln]
    # the one surviving mention is in a docstring explaining the removal
    assert not [ln for ln in code if "db._conn" in ln and '"""' not in ln
                and "`db._conn`" not in ln], code


def test_no_migrated_reader_still_selects_the_latest_extraction():
    """R30: the direct-table path is removed, not retained beside the reader."""
    migrated = [
        "engine/analysis/concordance.py",
        "engine/exporters/evidence_table.py",
        "analysis/paper1/judge_loader.py",
        "engine/validators/distribution_monitor.py",
    ]
    # CODE, not prose: three of the four files explain in a docstring what they
    # used to do, and a guard that counted those is a guard nobody can document
    # past. Docstrings and comments are stripped before the scan.
    import ast
    import io
    import tokenize

    def _code_only(path: Path) -> str:
        src = path.read_text()
        drop = set()
        for node in ast.walk(ast.parse(src)):
            if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                                 ast.AsyncFunctionDef)):
                doc = ast.get_docstring(node, clean=False)
                if doc is not None and node.body:
                    d = node.body[0]
                    drop.update(range(d.lineno, (d.end_lineno or d.lineno) + 1))
        comments = {tok.start[0] for tok in
                    tokenize.generate_tokens(io.StringIO(src).readline)
                    if tok.type == tokenize.COMMENT}
        drop |= comments
        return "\n".join(ln for i, ln in enumerate(src.splitlines(), 1)
                          if i not in drop)

    pattern = re.compile(
        r"ORDER BY id DESC LIMIT 1|MAX\(e2\.id\)|MAX\(ce2\.id\)|"
        r"FROM\s+evidence_spans|FROM\s+cloud_evidence_spans|FROM\s+human_extractions")
    hits = {rel for rel in migrated if pattern.search(_code_only(REPO / rel))}
    assert not hits, (
        f"a migrated reader still reads a result table directly: {sorted(hits)}. "
        "R30 removes the direct-table path rather than keeping it beside the "
        "reader.")

    # Phase 2b's two remaining copies, named so they are scheduled not forgotten.
    out2 = subprocess.run(
        ["git", "grep", "-l", "-E", r"ORDER BY id DESC LIMIT 1|MAX\(e2\.id\)",
         "--", "engine/exporters/"],
        cwd=REPO, capture_output=True, text=True)
    assert {ln for ln in out2.stdout.split() if ln} == {
        "engine/exporters/docx_export.py",
        "engine/exporters/trace_exporter.py",
    }, "Phase 2b's scope moved; update this pin with the ruling that moved it"
