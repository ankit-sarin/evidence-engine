"""Tests for analysis.paper1.adjudication — concordance adjudication interface."""

import json
import sqlite3
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from analysis.paper1.adjudication import (
    _load_field_types,
    _get_snippet,
    _has_table,
    export_ambiguous_pairs,
    generate_adjudication_html,
    import_adjudication_decisions,
)


CODEBOOK_PATH = Path("data/surgical_autonomy/extraction_codebook.yaml")


# ── Helpers ──────────────────────────────────────────────────────────


def _make_review_db(tmp_path: Path) -> Path:
    """Create a minimal review DB with papers, extractions, evidence_spans,
    cloud_extractions, and cloud_evidence_spans tables."""
    db_path = tmp_path / "review.db"
    # A review directory carries a codebook (CODEBOOK-AUTH-01).
    import shutil as _shutil
    _shutil.copy2(
        Path(__file__).resolve().parent.parent
        / "data" / "surgical_autonomy" / "extraction_codebook.yaml",
        tmp_path / "extraction_codebook.yaml",
    )
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE papers (id INTEGER PRIMARY KEY, title TEXT);
        INSERT INTO papers VALUES (1, 'Paper 1');
        INSERT INTO papers VALUES (2, 'Paper 2');

        CREATE TABLE extractions (
            id INTEGER PRIMARY KEY,
            paper_id INTEGER,
            extraction_schema_hash TEXT,
            extracted_data TEXT DEFAULT '[]',
            model TEXT DEFAULT 'test',
            extracted_at TEXT DEFAULT '2026-01-01'
        );
        INSERT INTO extractions VALUES (1, 1, 'abc', '[]', 'test', '2026-01-01');
        INSERT INTO extractions VALUES (2, 2, 'abc', '[]', 'test', '2026-01-01');

        CREATE TABLE evidence_spans (
            id INTEGER PRIMARY KEY,
            extraction_id INTEGER,
            field_name TEXT,
            value TEXT,
            source_snippet TEXT,
            confidence REAL DEFAULT 0.9,
            audit_status TEXT DEFAULT 'pending'
        );
        INSERT INTO evidence_spans VALUES (1, 1, 'study_type', 'Original Research', 'local snippet 1', 0.9, 'pending');
        INSERT INTO evidence_spans VALUES (2, 1, 'key_limitation', 'Small sample', 'local lim snippet', 0.9, 'pending');
        INSERT INTO evidence_spans VALUES (3, 2, 'study_type', 'Review', 'local snippet 2', 0.9, 'pending');
        INSERT INTO evidence_spans VALUES (4, 2, 'key_limitation', 'Short follow-up', 'local lim2', 0.9, 'pending');

        CREATE TABLE cloud_extractions (
            id INTEGER PRIMARY KEY,
            paper_id INTEGER,
            arm TEXT,
            model_string TEXT DEFAULT 'test',
            extraction_schema_hash TEXT,
            extracted_at TEXT DEFAULT '2026-01-01'
        );
        INSERT INTO cloud_extractions VALUES (1, 1, 'openai_o4_mini', 'test', 'abc', '2026-01-01');
        INSERT INTO cloud_extractions VALUES (2, 2, 'openai_o4_mini', 'test', 'abc', '2026-01-01');

        CREATE TABLE cloud_evidence_spans (
            id INTEGER PRIMARY KEY,
            cloud_extraction_id INTEGER,
            field_name TEXT,
            value TEXT,
            source_snippet TEXT,
            confidence REAL,
            tier INTEGER
        );
        INSERT INTO cloud_evidence_spans VALUES (1, 1, 'study_type', 'Original Research', 'cloud snippet 1', 0.9, 1);
        INSERT INTO cloud_evidence_spans VALUES (2, 1, 'key_limitation', 'Limited sample size and short follow-up', 'cloud lim snippet', 0.9, 4);
        INSERT INTO cloud_evidence_spans VALUES (3, 2, 'study_type', 'Review', 'cloud snippet 2', 0.9, 1);
        INSERT INTO cloud_evidence_spans VALUES (4, 2, 'key_limitation', 'Brief follow-up period', 'cloud lim2', 0.9, 4);
    """)
    conn.commit()
    conn.close()
    # B5 (READERS-01 Phase 2a): `load_arm` reads through `effective_value` now,
    # so the fixture's declarations are mirrored into the event store. What the
    # fixture SAYS is unchanged; where the reader LOOKS is what moved.
    _mirror(db_path)
    return db_path


def _mirror(db_path) -> None:
    from engine.core import events
    from tests._event_store_fixture import run_for
    from tests._event_store_fixture import ensure_event_store

    ensure_event_store(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        rows = [("local", r[0], r[1], r[2], r[3]) for r in conn.execute(
            "SELECT e.paper_id, es.field_name, es.value, es.source_snippet "
            "FROM evidence_spans es JOIN extractions e ON e.id = es.extraction_id")]
        rows += [(r[0], r[1], r[2], r[3], r[4]) for r in conn.execute(
            "SELECT ce.arm, ce.paper_id, cs.field_name, cs.value, cs.source_snippet "
            "FROM cloud_evidence_spans cs "
            "JOIN cloud_extractions ce ON ce.id = cs.cloud_extraction_id")]

        seen_arms, seen_papers = set(), set()
        for arm, pid, field, value, snippet in rows:
            if arm not in seen_arms:
                if not conn.execute("SELECT COUNT(*) FROM arms WHERE arm_name = ?",
                                    (arm,)).fetchone()[0]:
                    events.register_arm(conn, arm, "model")
                seen_arms.add(arm)
            if pid not in seen_papers:
                conn.execute("INSERT OR IGNORE INTO papers (id) VALUES (?)", (pid,))
                if not conn.execute(
                    "SELECT COUNT(*) FROM paper_events WHERE paper_id = ?", (pid,)
                ).fetchone()[0]:
                    events.write_paper_event(
        conn, run_id=run_for(conn), event_type="state_at_migration", paper_id=pid,
                        to_state="eligible", actor_kind="engine",
                        actor_role="system", actor_name="fixture")
                seen_papers.add(pid)
            uid = events.mint_extraction_uid()
            events.write_field_event(
        conn, run_id=run_for(conn), event_type="asserted", paper_id=pid, field_name=field,
                arm=arm, value=value, extraction_uid=uid, source_snippet=snippet,
                actor_kind="model", actor_role="extractor", actor_name="fixture")
            events.write_field_event(
        conn, run_id=run_for(conn), event_type="citation_located", paper_id=pid,
                field_name=field, arm=arm, extraction_uid=uid,
                actor_kind="engine", actor_role="system", actor_name="locator",
                payload={"located": True, "snippet": snippet or ""})
        conn.commit()
    finally:
        conn.close()


def _make_pair(overrides: dict | None = None) -> dict:
    """Build a minimal AMBIGUOUS pair dict."""
    pair = {
        "paper_id": 1,
        "field_name": "key_limitation",
        "arm_a_name": "local",
        "arm_a_value": "Small sample",
        "arm_a_snippet": "local snippet",
        "arm_b_name": "openai_o4_mini",
        "arm_b_value": "Limited sample size",
        "arm_b_snippet": "cloud snippet",
        "field_type": "free_text",
        "score_detail": "Jaccard=0.45",
    }
    if overrides:
        pair.update(overrides)
    return pair


# ── Unit: field type loading ─────────────────────────────────────────


class TestFieldTypes:

    def test_loads_from_real_codebook(self):
        ft = _load_field_types(CODEBOOK_PATH)
        assert ft["study_type"] == "categorical"
        assert ft["key_limitation"] == "free_text"
        assert ft["sample_size"] == "numeric"

    def test_all_20_fields_present(self):
        ft = _load_field_types(CODEBOOK_PATH)
        # Codebook has 18 fields (country and sample_size counted)
        assert len(ft) >= 18


# ── Unit: snippet routing ────────────────────────────────────────────


class TestSnippetRouting:

    def test_local_snippet(self, tmp_path):
        db_path = _make_review_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        snip = _get_snippet(conn, 1, "study_type", "local")
        assert snip == "local snippet 1"
        conn.close()

    def test_cloud_snippet(self, tmp_path):
        db_path = _make_review_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        snip = _get_snippet(conn, 1, "study_type", "openai_o4_mini")
        assert snip == "cloud snippet 1"
        conn.close()

    def test_human_snippet(self, tmp_path):
        db_path = _make_review_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE human_extractions (
                id INTEGER PRIMARY KEY, paper_id TEXT, extractor_id TEXT,
                field_name TEXT, value TEXT, source_quote TEXT, notes TEXT,
                imported_at TEXT
            )
        """)
        conn.execute(
            "INSERT INTO human_extractions VALUES (1, 'EE-001', 'A', 'study_type', 'Original Research', 'human quote', NULL, '2026-01-01')"
        )
        conn.commit()
        snip = _get_snippet(conn, 1, "study_type", "human_A")
        assert snip == "human quote"
        conn.close()

    def test_missing_snippet_returns_none(self, tmp_path):
        db_path = _make_review_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        snip = _get_snippet(conn, 999, "study_type", "local")
        assert snip is None
        conn.close()


# ── Unit: export_ambiguous_pairs ─────────────────────────────────────


class TestExportAmbiguousPairs:

    def test_collects_ambiguous_only(self, tmp_path):
        """Should return only AMBIGUOUS-scored pairs."""
        db_path = _make_review_db(tmp_path)
        pairs = export_ambiguous_pairs(
            db_path, "test_review",
            arms=["local", "openai_o4_mini"],
            codebook_path=CODEBOOK_PATH,
        )
        # study_type matches exactly (MATCH) → excluded
        # key_limitation has partial overlap → AMBIGUOUS
        assert all(p["field_name"] != "study_type" or
                   p["arm_a_value"] != p["arm_b_value"]
                   for p in pairs)
        # At least the key_limitation pairs should be AMBIGUOUS
        lim_pairs = [p for p in pairs if p["field_name"] == "key_limitation"]
        assert len(lim_pairs) >= 1

    def test_includes_snippets(self, tmp_path):
        db_path = _make_review_db(tmp_path)
        pairs = export_ambiguous_pairs(
            db_path, "test_review",
            arms=["local", "openai_o4_mini"],
            codebook_path=CODEBOOK_PATH,
        )
        for p in pairs:
            # At least one snippet should be non-None for pairs with data
            assert p["arm_a_snippet"] is not None or p["arm_b_snippet"] is not None

    def test_includes_field_type(self, tmp_path):
        db_path = _make_review_db(tmp_path)
        pairs = export_ambiguous_pairs(
            db_path, "test_review",
            arms=["local", "openai_o4_mini"],
            codebook_path=CODEBOOK_PATH,
        )
        for p in pairs:
            assert p["field_type"] in ("categorical", "free_text", "numeric", "unknown")

    def test_an_arm_that_is_not_in_the_registry_raises(self, tmp_path):
        """B5 rewrite (A12, R12).

        This pinned `RuntimeError("No extraction data")` — the old `load_arm`
        returned `{}` for a name nobody had declared, and the caller turned that
        silence into a message about missing DATA. The name was the problem, not
        the data, and the two are now different errors: routing is the registry,
        so an unregistered arm raises `UnknownArm` before any query runs.
        """
        from engine.core.effective import UnknownArm

        db_path = _make_review_db(tmp_path)
        with pytest.raises(UnknownArm, match="not in this review's registry"):
            export_ambiguous_pairs(
                db_path, "test_review",
                arms=["local", "nonexistent_arm"],
                codebook_path=CODEBOOK_PATH,
            )

    def test_a_registered_arm_holding_nothing_still_raises_no_extraction_data(
            self, tmp_path):
        """The other half: an arm that exists and is empty is a DATA problem,
        and keeps the message it always had."""
        from engine.core import events
        from tests._event_store_fixture import ensure_event_store

        db_path = _make_review_db(tmp_path)
        ensure_event_store(db_path)
        conn = sqlite3.connect(str(db_path))
        events.register_arm(conn, "empty_arm", "model")
        conn.commit()
        conn.close()

        with pytest.raises(RuntimeError, match="No extraction data"):
            export_ambiguous_pairs(
                db_path, "test_review",
                arms=["local", "empty_arm"],
                codebook_path=CODEBOOK_PATH,
            )

    def test_multiple_arm_pairs(self, tmp_path):
        """With 3 arms, should check all 3 combinations."""
        db_path = _make_review_db(tmp_path)
        # Add a second cloud arm
        conn = sqlite3.connect(str(db_path))
        conn.executescript("""
            INSERT INTO cloud_extractions VALUES (3, 1, 'anthropic_sonnet', 'test', 'abc', '2026-01-01');
            INSERT INTO cloud_extractions VALUES (4, 2, 'anthropic_sonnet', 'test', 'abc', '2026-01-01');
            INSERT INTO cloud_evidence_spans VALUES (5, 3, 'study_type', 'Original Research', 'anth snippet', 0.9, 1);
            INSERT INTO cloud_evidence_spans VALUES (6, 3, 'key_limitation', 'Very small sample size noted by authors', 'anth lim', 0.9, 4);
            INSERT INTO cloud_evidence_spans VALUES (7, 4, 'study_type', 'Review', 'anth snippet 2', 0.9, 1);
            INSERT INTO cloud_evidence_spans VALUES (8, 4, 'key_limitation', 'Limited follow-up duration', 'anth lim2', 0.9, 4);
        """)
        conn.commit()
        conn.close()
        _mirror(db_path)     # B5: the third arm reaches the reader, not a table

        pairs = export_ambiguous_pairs(
            db_path, "test_review",
            arms=["local", "openai_o4_mini", "anthropic_sonnet"],
            codebook_path=CODEBOOK_PATH,
        )
        # Check we get pairs from multiple arm combinations
        arm_combos = {(p["arm_a_name"], p["arm_b_name"]) for p in pairs}
        assert len(arm_combos) >= 1  # at least some ambiguous across combos


# ── Unit: generate_adjudication_html ─────────────────────────────────


class TestGenerateHTML:

    def test_creates_html_file(self, tmp_path):
        pairs = [_make_pair(), _make_pair({"paper_id": 2, "field_name": "study_design"})]
        out = tmp_path / "test.html"
        generate_adjudication_html(pairs, out, review_name="test_review")
        assert out.exists()
        content = out.read_text()
        assert "<!DOCTYPE html>" in content

    def test_contains_pair_data(self, tmp_path):
        pairs = [_make_pair()]
        out = tmp_path / "test.html"
        generate_adjudication_html(pairs, out)
        content = out.read_text()
        assert "key_limitation" in content
        assert "Small sample" in content
        assert "Limited sample size" in content
        assert "local" in content

    def test_groups_by_paper(self, tmp_path):
        pairs = [
            _make_pair({"paper_id": 1}),
            _make_pair({"paper_id": 1, "field_name": "study_design"}),
            _make_pair({"paper_id": 2}),
        ]
        out = tmp_path / "test.html"
        generate_adjudication_html(pairs, out)
        content = out.read_text()
        assert "Paper 1" in content
        assert "Paper 2" in content

    def test_radio_buttons_present(self, tmp_path):
        pairs = [_make_pair()]
        out = tmp_path / "test.html"
        generate_adjudication_html(pairs, out)
        content = out.read_text()
        assert 'value="AGREE"' in content
        assert 'value="DISAGREE"' in content

    def test_download_json_button(self, tmp_path):
        pairs = [_make_pair()]
        out = tmp_path / "test.html"
        generate_adjudication_html(pairs, out, review_name="test_review")
        content = out.read_text()
        assert "exportJSON" in content
        assert "Download" in content or "Export Final" in content

    def test_html_escapes_values(self, tmp_path):
        pairs = [_make_pair({"arm_a_value": '<script>alert("xss")</script>'})]
        out = tmp_path / "test.html"
        generate_adjudication_html(pairs, out)
        content = out.read_text()
        # The injected script tag must be escaped in the arm-value div
        assert '&lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt;' in content
        # Must NOT appear as a raw executable script
        assert 'alert("xss")' not in content

    def test_handles_none_snippets(self, tmp_path):
        pairs = [_make_pair({"arm_a_snippet": None, "arm_b_snippet": None})]
        out = tmp_path / "test.html"
        generate_adjudication_html(pairs, out)
        content = out.read_text()
        assert "snippet" not in content or "class=\"snippet\"" not in content

    def test_empty_pairs_still_valid_html(self, tmp_path):
        out = tmp_path / "test.html"
        generate_adjudication_html([], out)
        content = out.read_text()
        assert "<!DOCTYPE html>" in content
        assert "0 AMBIGUOUS pairs" in content

    def test_brand_styling(self, tmp_path):
        pairs = [_make_pair()]
        out = tmp_path / "test.html"
        generate_adjudication_html(pairs, out)
        content = out.read_text()
        assert "#0A5E56" in content  # forest teal
        assert "Fraunces" in content


# ── Unit: import_adjudication_decisions ──────────────────────────────


class TestImportDecisions:

    def test_basic_import(self, tmp_path):
        db_path = tmp_path / "import_test.db"
        decisions = [
            {"paper_id": 1, "field_name": "key_limitation",
             "arm_a": "local", "arm_b": "openai", "decision": "AGREE"},
            {"paper_id": 1, "field_name": "study_design",
             "arm_a": "local", "arm_b": "openai", "decision": "DISAGREE"},
        ]
        json_path = tmp_path / "decisions.json"
        json_path.write_text(json.dumps(decisions))

        inserted = import_adjudication_decisions(json_path, db_path)
        assert inserted == 2

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT * FROM concordance_adjudications").fetchall()
        assert len(rows) == 2
        conn.close()

    def test_skips_null_decisions(self, tmp_path):
        db_path = tmp_path / "skip_test.db"
        decisions = [
            {"paper_id": 1, "field_name": "f1",
             "arm_a": "a", "arm_b": "b", "decision": "AGREE"},
            {"paper_id": 2, "field_name": "f2",
             "arm_a": "a", "arm_b": "b", "decision": None},
        ]
        json_path = tmp_path / "decisions.json"
        json_path.write_text(json.dumps(decisions))

        inserted = import_adjudication_decisions(json_path, db_path)
        assert inserted == 1

    def test_invalid_decision_raises(self, tmp_path):
        db_path = tmp_path / "invalid_test.db"
        decisions = [
            {"paper_id": 1, "field_name": "f1",
             "arm_a": "a", "arm_b": "b", "decision": "MAYBE"},
        ]
        json_path = tmp_path / "decisions.json"
        json_path.write_text(json.dumps(decisions))

        with pytest.raises(ValueError, match="Invalid decision 'MAYBE'"):
            import_adjudication_decisions(json_path, db_path)

    def test_duplicate_import_raises(self, tmp_path):
        db_path = tmp_path / "dup_test.db"
        decisions = [
            {"paper_id": 1, "field_name": "f1",
             "arm_a": "a", "arm_b": "b", "decision": "AGREE"},
        ]
        json_path = tmp_path / "decisions.json"
        json_path.write_text(json.dumps(decisions))

        import_adjudication_decisions(json_path, db_path)
        with pytest.raises(RuntimeError, match="Duplicate adjudication"):
            import_adjudication_decisions(json_path, db_path)

    def test_stores_timestamp(self, tmp_path):
        db_path = tmp_path / "ts_test.db"
        decisions = [
            {"paper_id": 1, "field_name": "f1",
             "arm_a": "a", "arm_b": "b", "decision": "AGREE"},
        ]
        json_path = tmp_path / "decisions.json"
        json_path.write_text(json.dumps(decisions))

        import_adjudication_decisions(json_path, db_path)
        conn = sqlite3.connect(str(db_path))
        ts = conn.execute(
            "SELECT adjudicated_at FROM concordance_adjudications LIMIT 1"
        ).fetchone()[0]
        assert ts is not None
        assert "T" in ts
        conn.close()

    def test_non_array_json_raises(self, tmp_path):
        db_path = tmp_path / "bad_json.db"
        json_path = tmp_path / "decisions.json"
        json_path.write_text('{"not": "an array"}')

        with pytest.raises(ValueError, match="Expected a JSON array"):
            import_adjudication_decisions(json_path, db_path)


# ── Integration: round-trip ──────────────────────────────────────────


class TestRoundTrip:

    def test_export_generate_structure(self, tmp_path):
        """Export pairs → generate HTML → verify structure is consistent."""
        db_path = _make_review_db(tmp_path)
        pairs = export_ambiguous_pairs(
            db_path, "test_review",
            arms=["local", "openai_o4_mini"],
            codebook_path=CODEBOOK_PATH,
        )

        html_path = tmp_path / "adj.html"
        generate_adjudication_html(pairs, html_path, review_name="test_review")

        content = html_path.read_text()
        # Every pair's field_name should appear in the HTML
        for p in pairs:
            assert p["field_name"] in content
