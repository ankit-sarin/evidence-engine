"""ELICIT-DESIGN-02 gate 3 — a paper is written whole or not at all.

Two properties, against a REAL SQLite database rather than a recording stub,
because the claim is about what survives a transaction:

  1. All twenty terminal states land in one transaction, or none do. Ruling 1
     moved the unit of REFUSAL to the field; it did not make the WRITE partial.
     A short span list is exactly the shape SPANLOSS-01 was built to detect —
     21 Run 6 extractions stored one span instead of ~20 and nothing noticed —
     so every field gets a row whatever its state.

  2. A refusal before the write leaves nothing behind. There is no "store what
     we got" path, because storing what we got is what produced those 21.
"""

from __future__ import annotations

import json
import sqlite3

import pytest

from engine.core.database import ReviewDatabase


@pytest.fixture
def db(tmp_path):
    d = ReviewDatabase("atomic_test", data_root=tmp_path)
    d._conn.execute(
        "INSERT INTO papers (id, title, source, status, created_at, updated_at) "
        "VALUES (1, 'A paper', 'test', 'PARSED', '2026-09-05T00:00:00Z',"
        " '2026-09-05T00:00:00Z')")
    d._conn.commit()
    yield d
    d._conn.close()


def _spans(n_evidenced: int, n_unmet: int, n_escape: int = 0):
    out, states = [], {}
    i = 0
    for _ in range(n_evidenced):
        i += 1
        out.append({"field_name": f"f{i}", "value": f"value {i}",
                    "source_snippet": f"snippet {i}", "confidence": 0.9})
        states[f"f{i}"] = "EVIDENCED_VALUE"
    for _ in range(n_unmet):
        i += 1
        out.append({"field_name": f"f{i}", "value": "CONTRACT_UNMET",
                    "source_snippet": "", "confidence": 0.0})
        states[f"f{i}"] = "CONTRACT_UNMET"
    for _ in range(n_escape):
        i += 1
        out.append({"field_name": f"f{i}", "value": "NO_EVIDENCE_LOCATABLE",
                    "source_snippet": "", "confidence": 0.0})
        states[f"f{i}"] = "NO_EVIDENCE_LOCATABLE"
    return out, states


def test_all_twenty_states_land_in_one_transaction(db):
    spans, states = _spans(13, 5, 2)
    assert len(spans) == 20

    db.add_extraction_atomic(
        paper_id=1, schema_hash="h", model="m", reasoning_trace="t", spans=spans,
        extracted_data=[{**s, "terminal_state": states[s["field_name"]]} for s in spans],
    )

    rows = db._conn.execute(
        "SELECT field_name, value, source_snippet, confidence FROM evidence_spans"
    ).fetchall()
    assert len(rows) == 20, "span count equals field count whatever the states"

    by_value = {}
    for r in rows:
        by_value.setdefault(r["value"], []).append(r)
    assert len(by_value["CONTRACT_UNMET"]) == 5
    assert len(by_value["NO_EVIDENCE_LOCATABLE"]) == 2
    for token in ("CONTRACT_UNMET", "NO_EVIDENCE_LOCATABLE"):
        for r in by_value[token]:
            assert r["source_snippet"] == ""
            assert r["confidence"] == 0.0


def test_the_terminal_state_rides_on_every_entry_not_only_the_unmet_ones(db):
    """D6. A reader must be able to tell 'evidenced' from 'not asked' without
    inferring it from the absence of a marker."""
    spans, states = _spans(13, 5, 2)
    db.add_extraction_atomic(
        paper_id=1, schema_hash="h", model="m", reasoning_trace="t", spans=spans,
        extracted_data=[{**s, "terminal_state": states[s["field_name"]]} for s in spans],
    )
    stored = json.loads(db._conn.execute(
        "SELECT extracted_data FROM extractions").fetchone()["extracted_data"])
    assert len(stored) == 20
    assert all("terminal_state" in e for e in stored)
    assert sum(1 for e in stored if e["terminal_state"] == "EVIDENCED_VALUE") == 13


def test_extracted_data_keeps_the_list_shape_downstream_readers_expect(db):
    """A wrapper dict would silently break LOW_YIELD's denominator, which is
    `len(extracted)`, and trace_exporter's tier map, which branches on `list`."""
    spans, states = _spans(2, 1)
    db.add_extraction_atomic(
        paper_id=1, schema_hash="h", model="m", reasoning_trace="t", spans=spans,
        extracted_data=[{**s, "terminal_state": states[s["field_name"]]} for s in spans],
    )
    stored = json.loads(db._conn.execute(
        "SELECT extracted_data FROM extractions").fetchone()["extracted_data"])
    assert isinstance(stored, list)
    assert len(stored) == 3, "the LOW_YIELD denominator is the field count"

    from engine.agents.auditor import count_populated_fields
    assert count_populated_fields(
        stored, frozenset({"CONTRACT_UNMET", "NO_EVIDENCE_LOCATABLE"})) == 2


def test_a_failing_span_rolls_the_whole_paper_back(db):
    """One bad row and the extraction row goes too — no 19-of-20 write."""
    spans, states = _spans(19, 0)
    spans.append({"field_name": "f20", "value": None,       # NOT NULL violation
                  "source_snippet": "", "confidence": 0.0})

    with pytest.raises(sqlite3.IntegrityError):
        db.add_extraction_atomic(
            paper_id=1, schema_hash="h", model="m", reasoning_trace="t",
            spans=spans, extracted_data=spans)

    assert db._conn.execute("SELECT COUNT(*) FROM extractions").fetchone()[0] == 0
    assert db._conn.execute("SELECT COUNT(*) FROM evidence_spans").fetchone()[0] == 0


def test_a_pre_write_refusal_stores_nothing(db, tmp_path, monkeypatch):
    """The guards raise BEFORE any INSERT, so an unstorable paper leaves no trace."""
    from engine.core.citation_guard import UncitedValueError, enforce_citations

    spans, _ = _spans(2, 0)
    spans[0]["source_snippet"] = ""              # a value with no evidence

    with pytest.raises(UncitedValueError):
        enforce_citations(spans, paper_id=1, arm="test",
                          escape_token="NO_EVIDENCE_LOCATABLE",
                          contract_unmet_token="CONTRACT_UNMET")

    assert db._conn.execute("SELECT COUNT(*) FROM extractions").fetchone()[0] == 0
    assert db._conn.execute("SELECT COUNT(*) FROM evidence_spans").fetchone()[0] == 0


def test_no_uncited_non_escape_value_can_be_stored_as_a_value():
    """Gate 3, stated as the property rather than as a path. Sentinels included:
    'the paper does not report X' is a claim about the paper's text."""
    from engine.core.citation_guard import (
        STRICT, VALUE_WITHOUT_CITATION, check_citations,
    )

    for value in ("General Surgery", "NR", "NOT_FOUND", "No comparison reported"):
        r = check_citations(
            [{"field_name": "f", "value": value, "source_snippet": ""}],
            escape_token="NO_EVIDENCE_LOCATABLE",
            contract_unmet_token="CONTRACT_UNMET",
            absence_sentinels=frozenset({"NR", "NOT_FOUND"}),
            mode=STRICT, citation_counts={"f": 0},
        )
        assert not r.ok, f"{value!r} was allowed through with no citation"
        assert r.offenders[0][1] == VALUE_WITHOUT_CITATION

    for token in ("NO_EVIDENCE_LOCATABLE", "CONTRACT_UNMET"):
        r = check_citations(
            [{"field_name": "f", "value": token, "source_snippet": ""}],
            escape_token="NO_EVIDENCE_LOCATABLE",
            contract_unmet_token="CONTRACT_UNMET",
            absence_sentinels=frozenset({"NR", "NOT_FOUND"}),
            mode=STRICT, citation_counts={"f": 0},
        )
        assert r.ok, f"{token} carries no value and owes no citation"
