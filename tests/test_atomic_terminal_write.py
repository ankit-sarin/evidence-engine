"""ELICIT-DESIGN-02 gate 3 — a paper is written whole or not at all.

Two properties, against a REAL SQLite database rather than a recording stub,
because the claim is about what survives a transaction:

  1. (Retired 2026-09-25 with the legacy atomic writer it tested — 9c-C5,
     R160a. The event writer's one-transaction property,
     `engine/core/extraction_events.write_extraction_events`, has no test yet:
     an open finding, not a covered case.)

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
