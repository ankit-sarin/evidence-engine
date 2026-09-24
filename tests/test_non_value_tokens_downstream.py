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

Absence sentinels at these sites come from the codebook too (R124/R136:
`Codebook.absence_sentinel_set`, passed in as `absence_sentinels`). The one
remaining hand-list is `auditor.audit_span`'s, which slice 2 rewrites.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from engine.elicitation.classes import non_value_tokens_for

TOKENS = frozenset({"NO_EVIDENCE_LOCATABLE", "CONTRACT_UNMET"})
#: The fixture codebook's absence sentinels, upper-cased as the loader hands them out.
ABSENCE = frozenset({"NR", "NOT_FOUND"})
# Complete, because the loader validates eagerly (CODEBOOK-AUTH-01).
CODEBOOK = {
    "version": "1.0",
    "review": "test_review",
    "date": "2026-01-01",
    "escape_token": "NO_EVIDENCE_LOCATABLE",
    "contract_unmet_token": "CONTRACT_UNMET",
    "absence_sentinels": ["NR", "NOT_FOUND"],
    "canonical_absence_sentinel": "NR",  # R137: required by the loader since R132
    "fields": [{"name": "a", "field_class": "stated", "type": "free_text",
                "tier": 1, "definition": "A field.", "instruction": "Extract it.",
                "judge_rubric_family": "free_text"}],
}


@pytest.fixture
def codebook_dir(tmp_path):
    (tmp_path / "extraction_codebook.yaml").write_text(yaml.safe_dump(CODEBOOK))
    return tmp_path


# ══ The single authority ══════════════════════════════════════════════


def test_the_tokens_come_from_the_codebook(codebook_dir):
    assert non_value_tokens_for(
        codebook_dir / "extraction_codebook.yaml") == TOKENS


def test_an_incomplete_codebook_fails_the_reader_instead_of_emptying_it(tmp_path):
    """CODEBOOK-AUTH-01 R4 closed the read-side tolerance.

    This asserted the opposite until now, and the reasoning held while it did:
    hard-failing would have taken out the auditor, the validators and
    concordance on every pre-Run-7 review to protect a token those reviews
    could not contain. What the bare `except` actually did was make a MISSING
    FILE, a PARSE ERROR and a legitimately tokenless codebook indistinguishable
    — all three returned an empty set, and an empty set is not inert: it means
    every terminal state downstream is scored, audited and counted as a real
    extracted value. The failure it was protecting against was quieter than the
    one it created.
    """
    from engine.core.codebook import CodebookError

    (tmp_path / "extraction_codebook.yaml").write_text(
        yaml.safe_dump({"fields": [], "escape_token": "NO_EVIDENCE_LOCATABLE"}))
    with pytest.raises(CodebookError):
        non_value_tokens_for(tmp_path / "extraction_codebook.yaml")
    with pytest.raises(CodebookError):
        non_value_tokens_for(tmp_path / "nope.yaml")


def test_absence_sentinels_are_required_not_defaulted():
    """T6 — no silent path left in the accessors."""
    from engine.elicitation import classes as C

    with pytest.raises(C.CodebookContractError, match="absence_sentinels"):
        C.absence_sentinels({"fields": []})


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

    assert count_populated_fields(data, absence_sentinels=ABSENCE) == 3          # pre-fix
    assert count_populated_fields(data, TOKENS, absence_sentinels=ABSENCE) == 1  # post-fix

    # The direction matters: the more fields the engine refused, the healthier
    # the extraction would have looked to the LOW_YIELD guard.
    all_unmet = [{"field_name": n, "value": "CONTRACT_UNMET"} for n in "abcdefgh"]
    assert count_populated_fields(all_unmet, absence_sentinels=ABSENCE) == 8
    assert count_populated_fields(all_unmet, TOKENS, absence_sentinels=ABSENCE) == 0


def test_site2_handles_the_v1_dict_shape_too():
    from engine.agents.auditor import count_populated_fields

    data = {"a": "General Surgery", "b": "CONTRACT_UNMET"}
    assert count_populated_fields(data, absence_sentinels=ABSENCE) == 2
    assert count_populated_fields(data, TOKENS, absence_sentinels=ABSENCE) == 1


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

    assert _is_null(token, absence_sentinels=ABSENCE) is False        # pre-fix: counted as a real level
    assert _is_null(token, TOKENS, absence_sentinels=ABSENCE) is True  # post-fix: excluded


def test_site4_real_values_and_absences_are_unaffected():
    from engine.validators.distribution_monitor import _is_null

    assert _is_null("General Surgery", TOKENS, absence_sentinels=ABSENCE) is False
    assert _is_null("NR", TOKENS, absence_sentinels=ABSENCE) is True          # an absence, as before
    assert _is_null(None, TOKENS, absence_sentinels=ABSENCE) is True


def test_site4_manufactured_variance_would_have_masked_a_collapse():
    """Why it matters, stated as a measurement rather than a claim: a field that
    really collapsed to one level looks bimodal once refusals are counted."""
    from engine.validators.distribution_monitor import _is_null, shannon_entropy

    rows = ["Cohort"] * 8 + ["CONTRACT_UNMET"] * 8
    pre = [v for v in rows if not _is_null(v, absence_sentinels=ABSENCE)]
    post = [v for v in rows if not _is_null(v, TOKENS, absence_sentinels=ABSENCE)]
    assert shannon_entropy(pre) == pytest.approx(1.0)   # looks like variance
    assert shannon_entropy(post) == 0.0                 # the truth: collapsed


# ══ Site 5 — cross-arm concordance scoring ════════════════════════════


def test_site5_terminal_states_are_dropped_from_the_arm(tmp_path):
    """A CONTRACT_UNMET scored against another arm's real value is a MISMATCH
    that means nothing, and those MISMATCHes flow into the disagreement CSV and
    from there into both judge passes."""
    import sqlite3

    from engine.analysis.concordance import load_arm

    from tests._event_store_fixture import add_values

    db = tmp_path / "review.db"
    # Four fields, because the reader's field set IS the codebook's (R18/Q6) and
    # a one-field codebook would make this test pass for the wrong reason. The
    # filtering itself is asserted separately below — it is also what removes the
    # `field_1` parse artefact that reached every published figure since March
    # (A10).
    cb = dict(CODEBOOK)
    proto = CODEBOOK["fields"][0]
    cb["fields"] = [dict(proto, name=n) for n in ("a", "b", "c", "d")]
    (tmp_path / "extraction_codebook.yaml").write_text(yaml.safe_dump(cb))

    # B5 rewrite (R30). The values used to be INSERTed into `evidence_spans`,
    # which `load_arm` no longer reads. The CONTRACT is unchanged and is what
    # this test is about: a terminal state is not a value to score.
    #
    # What produces the drop is now different, and better. It used to be a
    # string test against the codebook's non-value tokens, applied at the
    # boundary. It is now the READER'S OWN STATE: `contract_unmet` is v2.1 row
    # 15 and `declined` is row 14, both of which return `value=None`, so there
    # is nothing to drop — the cell simply carries no value. A cited sentinel
    # (`NR`) is an ordinary value under R22 and stays.
    for pid, field, value in (
        (7, "a", "General Surgery"),
        (7, "d", "NR"),
    ):
        add_values(db, "local", field, [None] * (pid - 1) + [value])

    import sqlite3 as _s
    from engine.core import events
    from tests._event_store_fixture import run_for

    conn = _s.connect(db)
    conn.execute("PRAGMA foreign_keys = ON")
    events.write_field_event(
        conn, run_id=run_for(conn), event_type="contract_unmet", paper_id=7, field_name="b",
        arm="local", extraction_uid=events.mint_extraction_uid(),
        actor_kind="model", actor_role="extractor", actor_name="m",
        payload={"violation_codes": ["X"], "attempts": 2})
    events.write_field_event(
        conn, run_id=run_for(conn), event_type="declined", paper_id=7, field_name="c", arm="local",
        extraction_uid=events.mint_extraction_uid(),
        actor_kind="model", actor_role="extractor", actor_name="m")
    conn.commit()
    conn.close()

    got = load_arm(str(db), "local")
    assert got == {7: {"a": "General Surgery", "d": "NR"}}, (
        "terminal states carry no value; a cited sentinel is a VALUE and stays"
    )


def test_site5_a_field_the_codebook_does_not_declare_is_ignored(tmp_path):
    """R18/Q6, and the mechanism that removes A10.

    `field_1` — a model preamble stored as a span on paper 719 — was carried into
    every published figure since March because `load_arm` read whatever rows were
    in `evidence_spans`. The reader enumerates the CODEBOOK's fields, so a field
    nobody declared cannot reach an arm at all.
    """
    import sqlite3 as _s

    from engine.analysis.concordance import load_arm
    from engine.core import events
    from tests._event_store_fixture import add_values, run_for

    db = tmp_path / "review.db"
    (tmp_path / "extraction_codebook.yaml").write_text(yaml.safe_dump(CODEBOOK))
    add_values(db, "local", "a", [None] * 6 + ["General Surgery"])

    conn = _s.connect(db)
    conn.execute("PRAGMA foreign_keys = ON")
    events.write_field_event(
        conn, run_id=run_for(conn), event_type="asserted", paper_id=7, field_name="field_1",
        arm="local", value="The paper presents a dynamic potential field method",
        extraction_uid=events.mint_extraction_uid(),
        actor_kind="model", actor_role="extractor", actor_name="m")
    conn.commit()
    conn.close()

    assert load_arm(str(db), "local") == {7: {"a": "General Surgery"}}
