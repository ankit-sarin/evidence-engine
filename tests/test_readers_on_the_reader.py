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
from tests._event_store_fixture import run_for
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
        conn, run_id=run_for(conn), event_type="adjudicated", paper_id=pid, to_state="eligible",
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
        conn, run_id=run_for(conn), event_type="asserted", paper_id=paper_id, field_name=field,
        arm=arm, value=value, extraction_uid=uid, source_snippet=value,
        actor_kind="model", actor_role="extractor", actor_name="m")
    if located:
        events.write_field_event(
        conn, run_id=run_for(conn), event_type="citation_located", paper_id=paper_id,
            field_name=field, arm=arm, extraction_uid=uid,
            actor_kind="engine", actor_role="system", actor_name="locator",
            payload={"located": True, "snippet": value})
    conn.commit()
    return events.make_claim_id(arm, uid, field)


def _decline(conn, paper_id, field, arm):
    events.write_field_event(
        conn, run_id=run_for(conn), event_type="declined", paper_id=paper_id, field_name=field,
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
        db._conn, run_id=run_for(db._conn), event_type="human_withdrew", paper_id=1,
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
        db._conn, run_id=run_for(db._conn), event_type="extraction_failed", paper_id=2,
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
    "engine/exporters/docx_export.py",
    "analysis/paper1/judge_loader.py",
    "analysis/paper1/export_disagreement_pairs.py",
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
        "engine/exporters/docx_export.py",
        "analysis/paper1/judge_loader.py",
        "analysis/paper1/export_disagreement_pairs.py",
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

    # Phase 2b closed the last two: `docx_export.py` migrated, `trace_exporter.py`
    # RETIRED (R46) rather than migrated — it reported on reasoning traces and
    # auditor verdicts, none of which the event store carries.
    # Same code-only rule: `docx_export.py`'s docstring quotes the query it no
    # longer runs, and a guard that counted that is a guard nobody can document
    # past.
    latest = re.compile(r"ORDER BY id DESC LIMIT 1|MAX\(e2\.id\)")
    exporters = sorted((REPO / "engine/exporters").glob("*.py"))
    still = [f.relative_to(REPO).as_posix() for f in exporters
             if latest.search(_code_only(f))]
    assert not still, f"an exporter still selects the latest extraction (R33): {still}"
    assert not (REPO / "engine/exporters/trace_exporter.py").exists()
    assert not (REPO / "tests/test_trace_exporter.py").exists()


# ── Phase 2b: the DOCX exporter ──────────────────────────────────────

def _docx_cells(path):
    """Every cell of the first table, as a list of rows."""
    from docx import Document

    doc = Document(str(path))
    return [[c.text for c in row.cells] for row in doc.tables[0].rows]


def _docx_paragraphs(path):
    from docx import Document

    return [p.text for p in Document(str(path)).paragraphs]


def test_the_docx_reports_declined_and_shows_no_value_for_withdrawn(review, tmp_path):
    """G3 / D1-4 through the DOCX exporter.

    R1 makes a withdrawal "no value in the current result", so a withdrawn field
    renders EMPTY. A declined field is a different fact — the model was asked and
    abstained — and an empty cell there would make a submission table say "not
    reported" where the record says "declined".
    """
    from engine.core.review_spec import load_review_spec
    from engine.exporters.docx_export import export_evidence_docx

    db, root = review
    spec = load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")

    _assert_value(db._conn, 1, "study_type", "local", "RCT")
    _decline(db._conn, 1, "sample_size", "local")
    claim = _assert_value(db._conn, 1, "country", "local", "USA")
    events.write_field_event(
        db._conn, run_id=run_for(db._conn), event_type="human_withdrew", paper_id=1, field_name="country",
        arm="local", against_claims=[claim],
        actor_kind="human", actor_role="reviewer", actor_name="PI")
    db._conn.commit()

    out = tmp_path / "evidence.docx"
    export_evidence_docx(db, spec, str(out), arm="local")

    rows = _docx_cells(out)
    header = rows[0]
    body = {r[0]: r for r in rows[1:]}
    row = next(iter(body.values()))
    col = {name: i for i, name in enumerate(header)}

    assert row[col["Study Type"]] == "RCT"
    assert row[col["Sample Size"]] == "[declined]"
    assert row[col["Country"]] == "", "a withdrawal is no value (R1)"


def test_the_docx_refuses_an_unregistered_arm(review, tmp_path):
    from engine.core.review_spec import load_review_spec
    from engine.exporters.docx_export import export_evidence_docx

    db, root = review
    spec = load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")
    with pytest.raises(UnknownArm):
        export_evidence_docx(db, spec, str(tmp_path / "x.docx"), arm="not_an_arm")


def test_the_docx_keeps_a_failed_paper_and_reports_it_by_reason(review, tmp_path):
    """S3h through a submission artifact: the paper is not silently dropped, and
    the reason is stated rather than left to be inferred from an empty row."""
    from engine.core.review_spec import load_review_spec
    from engine.exporters.docx_export import export_evidence_docx

    db, root = review
    spec = load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")
    _assert_value(db._conn, 1, "study_type", "local", "RCT")
    events.write_paper_event(
        db._conn, run_id=run_for(db._conn), event_type="extraction_failed", paper_id=2,
        to_state="extraction_failed", reason_code="extraction failed after retries",
        actor_kind="engine", actor_role="system", actor_name="fixture")
    db._conn.commit()

    out = tmp_path / "evidence.docx"
    export_evidence_docx(db, spec, str(out), arm="local")

    assert len(_docx_cells(out)) == 1 + 3      # header + the three corpus papers
    note = " ".join(_docx_paragraphs(out))
    assert "extraction failed after retries" in note
    assert "1 paper(s)" in note


def test_the_docx_has_no_note_when_nothing_failed(review, tmp_path):
    from engine.core.review_spec import load_review_spec
    from engine.exporters.docx_export import export_evidence_docx

    db, root = review
    spec = load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")
    _assert_value(db._conn, 1, "study_type", "local", "RCT")
    out = tmp_path / "evidence.docx"
    export_evidence_docx(db, spec, str(out), arm="local")
    assert "Processing outcomes" not in " ".join(_docx_paragraphs(out))


# ── Phase 2b: the disagreement-pairs universe (R48) ──────────────────

def test_disagreement_pairs_emits_every_grid_cell_none_filtered(review, tmp_path):
    """G4 / R48. `if not any_disagree: continue` was THE universe role, and it is
    B3 itself: this file's output WAS the judge's input, so a cell every arm
    agreed on could never be judged. A MATCH is now a row with MATCH in it."""
    from analysis.paper1.export_disagreement_pairs import build_disagreement_rows
    from engine.core.codebook import load_codebook
    from engine.core.effective import eligible_paper_ids, registered_arms

    db, root = review
    # one cell where the two model arms AGREE, one where they DISAGREE,
    # and every other cell empty
    _assert_value(db._conn, 1, "study_type", "local", "RCT")
    _assert_value(db._conn, 1, "study_type", "openai_o4_mini_high", "RCT")
    _assert_value(db._conn, 1, "sample_size", "local", "51")
    _assert_value(db._conn, 1, "sample_size", "openai_o4_mini_high", "150")

    rows, summaries = build_disagreement_rows(
        str(db.db_path), str(REPO / "review_specs" / "surgical_autonomy.yaml"))

    n_papers = len(eligible_paper_ids(db._conn))
    n_fields = len(load_codebook(root / "extraction_codebook.yaml").field_names)
    # a DERIVATION, never a literal
    assert len(rows) == n_papers * n_fields

    by_cell = {(r["paper_id"], r["field_name"]): r for r in rows}
    agree = by_cell[(1, "study_type")]
    assert agree["local_vs_o4mini_score"] == "MATCH", "an agreeing cell is a ROW now"
    assert agree["local_value"] == "RCT" and agree["o4mini_value"] == "RCT"
    disagree = by_cell[(1, "sample_size")]
    assert disagree["local_vs_o4mini_score"] != "MATCH"
    # a cell nobody claimed is present too
    empty = by_cell[(3, "country")]
    assert empty["local_value"] is None and empty["o4mini_value"] is None


def test_disagreement_pairs_row_format_is_unchanged(review, tmp_path):
    """R48: the row builder and row format are unchanged — the CSV writer's
    header is the contract, and every key it names must still be produced."""
    from analysis.paper1.export_disagreement_pairs import (
        build_disagreement_rows, write_csv,
    )

    db, root = review
    _assert_value(db._conn, 1, "study_type", "local", "RCT")
    rows, _ = build_disagreement_rows(
        str(db.db_path), str(REPO / "review_specs" / "surgical_autonomy.yaml"))

    for key in ("paper_id", "paper_label", "paper_title", "field_name",
                "field_tier", "field_type", "local_value", "o4mini_value",
                "sonnet_value", "local_vs_o4mini_score", "local_vs_sonnet_score",
                "o4mini_vs_sonnet_score"):
        assert key in rows[0], f"{key} left the row format"

    out = tmp_path / "pairs.csv"
    write_csv(rows, out)
    header = out.read_text().splitlines()[0].split(",")
    assert header == [
        "paper_id", "paper_label", "paper_title", "field_name", "field_tier",
        "field_type", "local_value", "o4mini_value", "sonnet_value",
        "local_vs_o4mini_score", "local_vs_sonnet_score", "o4mini_vs_sonnet_score",
    ]


def test_an_arm_the_row_format_cannot_name_is_refused(review):
    """The registry is the universe now, so a fourth arm is a FORMAT decision —
    and a silent omission is the failure this refusal exists to prevent."""
    from analysis.paper1.export_disagreement_pairs import arms_in_scope

    db, root = review
    events.register_arm(db._conn, "some_new_model_arm", "model")
    db._conn.commit()
    with pytest.raises(RuntimeError, match="no column for"):
        arms_in_scope(db._conn)


def test_arms_in_scope_reads_the_registry_not_a_literal(review):
    """R12/R48: a human arm is not a column here, and the model arms come from
    the registry rather than from a module list."""
    from analysis.paper1.export_disagreement_pairs import arms_in_scope

    db, root = review
    assert arms_in_scope(db._conn) == ["local", "openai_o4_mini_high"]
