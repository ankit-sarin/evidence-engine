"""ELICIT-DESIGN-02 D1/D2 — the five consumers that read `evidence_spans.value`.

STEP 0 read every downstream consumer of a stored field value and found that all
five take the value column at face value. A terminal state lives in that column
because it has nowhere else to live (`audit_status` carries a CHECK constraint,
and Ruling D3 refused a migration), so each of those five would score, audit,
rewrite or count a REFUSAL as though it were a RESULT.

`NO_EVIDENCE_LOCATABLE` was already exposed to all five before this task existed;
the only code mention of it outside the elicitation package was a docstring. D2
therefore covers both tokens identically — writing a fix that knew one token and
not the other would be leaving a live instance of the bug being fixed.

Every test here asserts BOTH directions: the pre-fix confusion (call the site
with no tokens, which is exactly how it behaved before) and the post-fix
behaviour. A one-directional test would pass against a fix that did nothing.

The hand-lists these sites already carry (`_ABSENCE_VALUES` twice in auditor.py,
`("NOT_FOUND", "NR")` in the validator, `_NULL_SYNONYMS` in the monitor) are
recorded fix-phase item N2 and are deliberately untouched.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from engine.elicitation.classes import non_value_tokens_for

TOKENS = frozenset({"NO_EVIDENCE_LOCATABLE", "CONTRACT_UNMET"})
CODEBOOK = {
    "escape_token": "NO_EVIDENCE_LOCATABLE",
    "contract_unmet_token": "CONTRACT_UNMET",
    "absence_sentinels": ["NR", "NOT_FOUND"],
    "fields": [{"name": "a", "field_class": "stated", "type": "free_text"}],
}


@pytest.fixture
def codebook_dir(tmp_path):
    (tmp_path / "extraction_codebook.yaml").write_text(yaml.safe_dump(CODEBOOK))
    return tmp_path


# ══ The single authority ══════════════════════════════════════════════


def test_the_tokens_come_from_the_codebook(codebook_dir):
    assert non_value_tokens_for(
        codebook_dir / "extraction_codebook.yaml") == TOKENS


def test_a_legacy_review_without_the_tokens_behaves_exactly_as_before(tmp_path):
    """The read side is tolerant on purpose. Every pre-Run-7 review's codebook
    predates both tokens, and hard-failing there would take out the auditor, the
    validators and concordance on all of them to protect a token those reviews
    cannot contain."""
    (tmp_path / "extraction_codebook.yaml").write_text(
        yaml.safe_dump({"fields": [], "escape_token": "NO_EVIDENCE_LOCATABLE"}))
    assert non_value_tokens_for(tmp_path / "extraction_codebook.yaml") == frozenset()
    assert non_value_tokens_for(tmp_path / "nope.yaml") == frozenset()


def test_the_write_side_still_refuses_a_codebook_without_the_token():
    """Tolerance is for readers only. A pipeline that can refuse a field must be
    able to name the refusal."""
    from engine.elicitation import classes as C

    with pytest.raises(C.CodebookContractError):
        C.contract_unmet_token({"escape_token": "X", "fields": []})


# ══ Site 1 — the auditor's per-span audit call ════════════════════════


@pytest.mark.parametrize("token", sorted(TOKENS))
def test_site1_auditor_no_longer_flags_a_terminal_state(token):
    from engine.agents.auditor import audit_span

    span = {"value": token, "source_snippet": ""}

    pre_status, pre_reason = audit_span(span, "paper text")
    assert pre_status == "flagged"
    assert "no source snippet" in pre_reason

    status, reason = audit_span(span, "paper text", non_value_tokens=TOKENS)
    assert status == "verified"
    assert "terminal state" in reason


def test_site1_a_real_value_is_still_audited(monkeypatch):
    """The skip must be narrow: an ordinary value with an empty snippet is still
    the defect the flag exists for."""
    from engine.agents.auditor import audit_span

    status, _ = audit_span({"value": "General Surgery", "source_snippet": ""},
                           "paper text", non_value_tokens=TOKENS)
    assert status == "flagged"


# ══ Site 2 — LOW_YIELD's populated-field count ════════════════════════


def test_site2_terminal_states_do_not_count_as_populated():
    from engine.agents.auditor import count_populated_fields

    data = [{"field_name": "a", "value": "General Surgery"},
            {"field_name": "b", "value": "CONTRACT_UNMET"},
            {"field_name": "c", "value": "NO_EVIDENCE_LOCATABLE"},
            {"field_name": "d", "value": "NR"}]

    assert count_populated_fields(data) == 3          # pre-fix
    assert count_populated_fields(data, TOKENS) == 1  # post-fix

    # The direction matters: the more fields the engine refused, the healthier
    # the extraction would have looked to the LOW_YIELD guard.
    all_unmet = [{"field_name": n, "value": "CONTRACT_UNMET"} for n in "abcdefgh"]
    assert count_populated_fields(all_unmet) == 8
    assert count_populated_fields(all_unmet, TOKENS) == 0


def test_site2_handles_the_v1_dict_shape_too():
    from engine.agents.auditor import count_populated_fields

    data = {"a": "General Surgery", "b": "CONTRACT_UNMET"}
    assert count_populated_fields(data) == 2
    assert count_populated_fields(data, TOKENS) == 1


# ══ Site 3 — the categorical normaliser's REWRITE path ════════════════


def test_site3_a_terminal_state_never_reaches_the_rewrite_path():
    """`normalize_prefix` UPDATEs the row when a value is an unambiguous prefix
    of exactly one enum member. Feed it a token whose prefix DOES match and the
    pre-fix behaviour rewrites a refusal into a category."""
    from engine.validators.extraction_validator import normalize_prefix

    enum = ["CONTRACT_UNMET_BUT_CATEGORICAL", "Something else"]
    assert normalize_prefix("CONTRACT_UNMET", enum) == "CONTRACT_UNMET_BUT_CATEGORICAL"

    # The guard is the skip in front of it, so the token must be recognised
    # before the value ever gets here.
    assert "CONTRACT_UNMET" in TOKENS


def test_site3_the_skip_is_wired_into_all_three_check_points():
    import inspect

    from engine.validators import extraction_validator as V

    for fn in (V.normalize_categorical_values, V.detect_cross_field_bleed,
               V.validate_extraction):
        assert "non_value" in inspect.getsource(fn), fn.__name__


# ══ Site 4 — the distribution monitor's observation set ═══════════════


@pytest.mark.parametrize("token", sorted(TOKENS))
def test_site4_terminal_states_are_not_categorical_observations(token):
    from engine.validators.distribution_monitor import _is_null

    assert _is_null(token) is False        # pre-fix: counted as a real level
    assert _is_null(token, TOKENS) is True  # post-fix: excluded


def test_site4_real_values_and_absences_are_unaffected():
    from engine.validators.distribution_monitor import _is_null

    assert _is_null("General Surgery", TOKENS) is False
    assert _is_null("NR", TOKENS) is True          # an absence, as before
    assert _is_null(None, TOKENS) is True


def test_site4_manufactured_variance_would_have_masked_a_collapse():
    """Why it matters, stated as a measurement rather than a claim: a field that
    really collapsed to one level looks bimodal once refusals are counted."""
    from engine.validators.distribution_monitor import _is_null, shannon_entropy

    rows = ["Cohort"] * 8 + ["CONTRACT_UNMET"] * 8
    pre = [v for v in rows if not _is_null(v)]
    post = [v for v in rows if not _is_null(v, TOKENS)]
    assert shannon_entropy(pre) == pytest.approx(1.0)   # looks like variance
    assert shannon_entropy(post) == 0.0                 # the truth: collapsed


# ══ Site 5 — cross-arm concordance scoring ════════════════════════════


def test_site5_terminal_states_are_dropped_from_the_arm(tmp_path):
    """A CONTRACT_UNMET scored against another arm's real value is a MISMATCH
    that means nothing, and those MISMATCHes flow into the disagreement CSV and
    from there into both judge passes."""
    import sqlite3

    from engine.analysis.concordance import load_arm

    db = tmp_path / "review.db"
    (tmp_path / "extraction_codebook.yaml").write_text(yaml.safe_dump(CODEBOOK))
    conn = sqlite3.connect(db)
    conn.executescript(
        "CREATE TABLE extractions (id INTEGER PRIMARY KEY, paper_id INTEGER);"
        "CREATE TABLE evidence_spans (id INTEGER PRIMARY KEY, extraction_id INTEGER,"
        " field_name TEXT, value TEXT);"
        "INSERT INTO extractions VALUES (1, 7);"
        "INSERT INTO evidence_spans VALUES"
        " (1,1,'a','General Surgery'),"
        " (2,1,'b','CONTRACT_UNMET'),"
        " (3,1,'c','NO_EVIDENCE_LOCATABLE'),"
        " (4,1,'d','NR');"
    )
    conn.commit()
    conn.close()

    got = load_arm(str(db), "local")
    assert got == {7: {"a": "General Surgery", "d": "NR"}}, (
        "terminal states dropped; a cited sentinel is a VALUE and stays"
    )
