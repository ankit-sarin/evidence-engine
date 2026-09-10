"""CODEBOOK-AUTH-01 Phase 2 — the codebook loader.

T1 resolution · T2 eager validation · T3 identity · T4 the two hashes.
"""

from __future__ import annotations

import copy
from pathlib import Path

import pytest
import yaml

from engine.core import codebook as CB
from engine.core.codebook import (
    CodebookError,
    CodebookIdentityError,
    codebook_path_for,
    compute_codebook_sha256,
    load_codebook,
    load_codebook_for,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
LIVE = REPO_ROOT / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

VALID = {
    "version": "1.0",
    "review": "alpha",
    "date": "2026-01-01",
    "escape_token": "NO_EVIDENCE_LOCATABLE",
    "contract_unmet_token": "CONTRACT_UNMET",
    "absence_sentinels": ["NR", "NOT_FOUND"],
    "fields": [
        {"name": "robot_platform", "type": "free_text", "tier": 1,
         "definition": "The robot.", "instruction": "Name it.",
         "field_class": "stated", "judge_rubric_family": "free_text"},
        {"name": "study_type", "type": "categorical", "tier": 1,
         "definition": "The design.", "instruction": "Classify it.",
         "field_class": "inferable", "judge_rubric_family": "categorical",
         "valid_values": [{"value": "RCT", "definition": "Randomised."},
                          {"value": "Cohort", "definition": "Not randomised."}]},
    ],
}


@pytest.fixture(autouse=True)
def _clear_cache():
    CB.clear_cache()
    yield
    CB.clear_cache()


def _write(tmp_path, doc=None, name="extraction_codebook.yaml"):
    p = tmp_path / name
    p.write_text(yaml.safe_dump(doc if doc is not None else VALID))
    return p


# ── T1: resolution ───────────────────────────────────────────────────


def test_path_derives_from_the_review_id():
    assert codebook_path_for("surgical_autonomy") == Path(
        "data/surgical_autonomy/extraction_codebook.yaml")
    assert codebook_path_for("other_review") == Path(
        "data/other_review/extraction_codebook.yaml")


def test_live_codebook_loads_from_its_review_id():
    cb = load_codebook_for("surgical_autonomy")
    assert cb.review == "surgical_autonomy"
    assert len(cb.fields) == 20


def test_override_is_honoured(tmp_path):
    p = _write(tmp_path)
    assert load_codebook_for("alpha", p).path == p


def test_missing_file_error_names_the_expected_path():
    with pytest.raises(CodebookError) as exc:
        load_codebook_for("no_such_review")
    assert "data/no_such_review/extraction_codebook.yaml" in str(exc.value)


def test_the_glob_is_gone():
    """T1's real claim.

    `_find_codebook_path` returned the first `data/*/extraction_codebook.yaml`
    in glob order with no identity check. On a two-review box it could hand one
    review's codebook to another review's extraction, and nothing raised.
    """
    from engine.agents import extractor

    assert not hasattr(extractor, "_find_codebook_path")
    assert not hasattr(extractor, "_load_codebook")
    for rel in ("engine/agents/extractor.py", "engine/elicitation/classes.py",
                "engine/elicitation/pipeline.py", "engine/core/codebook.py"):
        src = (REPO_ROOT / rel).read_text()
        assert 'glob("*/extraction_codebook' not in src, rel
        assert 'glob(f"*/extraction_codebook' not in src, rel


def test_parse_is_cached_per_path(tmp_path):
    p = _write(tmp_path)
    assert load_codebook("alpha" and p) is load_codebook(p)


# ── T2: eager validation ─────────────────────────────────────────────


@pytest.mark.parametrize("key", CB.REQUIRED_TOP_LEVEL)
def test_missing_top_level_key_rejected_and_named(tmp_path, key):
    doc = copy.deepcopy(VALID)
    del doc[key]
    with pytest.raises(CodebookError) as exc:
        load_codebook(_write(tmp_path, doc))
    assert key in str(exc.value)


def test_unknown_top_level_key_rejected_and_named(tmp_path):
    doc = copy.deepcopy(VALID)
    doc["reviw"] = "typo"
    with pytest.raises(CodebookError) as exc:
        load_codebook(_write(tmp_path, doc))
    assert "reviw" in str(exc.value)


@pytest.mark.parametrize("key", CB.REQUIRED_FIELD_KEYS)
def test_missing_field_key_rejected_and_named(tmp_path, key):
    doc = copy.deepcopy(VALID)
    del doc["fields"][0][key]
    with pytest.raises(CodebookError) as exc:
        load_codebook(_write(tmp_path, doc))
    msg = str(exc.value)
    assert key in msg
    if key != "name":
        assert "robot_platform" in msg


def test_unknown_field_key_rejected_and_named(tmp_path):
    doc = copy.deepcopy(VALID)
    doc["fields"][0]["definiton"] = "typo"
    with pytest.raises(CodebookError) as exc:
        load_codebook(_write(tmp_path, doc))
    assert "definiton" in str(exc.value) and "robot_platform" in str(exc.value)


def test_duplicate_field_name_rejected(tmp_path):
    doc = copy.deepcopy(VALID)
    doc["fields"].append(copy.deepcopy(doc["fields"][0]))
    with pytest.raises(CodebookError, match="duplicate"):
        load_codebook(_write(tmp_path, doc))


def test_type_vocabulary_comes_from_the_relocated_authority(tmp_path):
    doc = copy.deepcopy(VALID)
    doc["fields"][0]["type"] = "text"          # the spec's old spelling
    with pytest.raises(CodebookError) as exc:
        load_codebook(_write(tmp_path, doc))
    assert "text" in str(exc.value)
    assert "free_text" in str(exc.value)


def test_field_class_vocabulary_comes_from_classes_not_a_copy(tmp_path):
    from engine.elicitation.classes import CLASSES

    doc = copy.deepcopy(VALID)
    doc["fields"][0]["field_class"] = "asserted"
    with pytest.raises(CodebookError) as exc:
        load_codebook(_write(tmp_path, doc))
    assert "asserted" in str(exc.value)
    for cls in CLASSES:
        assert cls in str(exc.value)
    # and the set is not duplicated in the loader
    src = (REPO_ROOT / "engine/core/codebook.py").read_text()
    assert '"stated"' not in src and "'stated'" not in src


def test_empty_absence_sentinels_rejected(tmp_path):
    doc = copy.deepcopy(VALID)
    doc["absence_sentinels"] = []
    with pytest.raises(CodebookError, match="absence_sentinels"):
        load_codebook(_write(tmp_path, doc))


def test_categorical_without_valid_values_rejected(tmp_path):
    doc = copy.deepcopy(VALID)
    del doc["fields"][1]["valid_values"]
    with pytest.raises(CodebookError) as exc:
        load_codebook(_write(tmp_path, doc))
    assert "study_type" in str(exc.value) and "valid_values" in str(exc.value)


def test_valid_values_shape_enforced(tmp_path):
    doc = copy.deepcopy(VALID)
    doc["fields"][1]["valid_values"] = ["RCT", "Cohort"]
    with pytest.raises(CodebookError, match="valid_values"):
        load_codebook(_write(tmp_path, doc))


def test_bad_tier_rejected(tmp_path):
    doc = copy.deepcopy(VALID)
    doc["fields"][0]["tier"] = 5
    with pytest.raises(CodebookError, match="tier"):
        load_codebook(_write(tmp_path, doc))


def test_invalid_yaml_is_a_codebook_error(tmp_path):
    p = tmp_path / "extraction_codebook.yaml"
    p.write_text("fields: [\n  broken: yaml:\n")
    with pytest.raises(CodebookError, match="invalid YAML"):
        load_codebook(p)


# ── T3: identity ─────────────────────────────────────────────────────


def test_identity_mismatch_raises(tmp_path):
    p = _write(tmp_path)                       # declares review: alpha
    with pytest.raises(CodebookIdentityError) as exc:
        load_codebook_for("beta", p)
    assert "alpha" in str(exc.value) and "beta" in str(exc.value)


def test_identity_mismatch_is_a_codebook_error():
    assert issubclass(CodebookIdentityError, CodebookError)


def test_identity_is_checked_on_the_derived_path_too(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data" / "beta").mkdir(parents=True)
    (tmp_path / "data" / "beta" / "extraction_codebook.yaml").write_text(
        yaml.safe_dump(VALID))            # declares alpha, sitting in beta/
    with pytest.raises(CodebookIdentityError):
        load_codebook_for("beta")


# ── T4: the two hashes ───────────────────────────────────────────────


def test_semantic_hash_ignores_comments_and_whitespace(tmp_path):
    a = tmp_path / "a.yaml"
    a.write_text(yaml.safe_dump(VALID))
    b = tmp_path / "b.yaml"
    b.write_text("# a comment nobody reads\n\n" + yaml.safe_dump(VALID) + "\n\n")
    ca, cbk = load_codebook(a), load_codebook(b)
    assert ca.semantic_hash == cbk.semantic_hash
    assert ca.sha256 != cbk.sha256, "the byte hash must still move"


def test_semantic_hash_ignores_version_date_and_review(tmp_path):
    """They reach no prompt and no code (pre-flight P2).

    A provenance value that moves when nothing the model saw changed is a false
    alarm, and false alarms are how staleness detection gets ignored.
    """
    doc = copy.deepcopy(VALID)
    doc["version"], doc["date"] = "9.9", "2030-12-31"
    a = load_codebook(_write(tmp_path, VALID, "a.yaml"))
    b = load_codebook(_write(tmp_path, doc, "b.yaml"))
    assert a.semantic_hash == b.semantic_hash
    assert a.sha256 != b.sha256


def test_semantic_hash_moves_on_a_definition_edit(tmp_path):
    doc = copy.deepcopy(VALID)
    doc["fields"][0]["definition"] = "The robotic platform, named precisely."
    a = load_codebook(_write(tmp_path, VALID, "a.yaml"))
    b = load_codebook(_write(tmp_path, doc, "b.yaml"))
    assert a.semantic_hash != b.semantic_hash
    assert a.sha256 != b.sha256


def test_semantic_hash_moves_on_a_token_edit(tmp_path):
    doc = copy.deepcopy(VALID)
    doc["absence_sentinels"] = ["NR"]
    a = load_codebook(_write(tmp_path, VALID, "a.yaml"))
    b = load_codebook(_write(tmp_path, doc, "b.yaml"))
    assert a.semantic_hash != b.semantic_hash


def test_byte_hash_is_the_same_function_the_judge_lane_uses():
    from analysis.paper1.judge_loader import compute_codebook_sha256 as judge_fn

    assert judge_fn is compute_codebook_sha256
    assert load_codebook_for("surgical_autonomy").sha256 == compute_codebook_sha256(LIVE)


def test_lint_runs_at_load_and_does_not_raise():
    cb = load_codebook_for("surgical_autonomy")
    assert cb.lint_findings == ()


def test_lint_findings_are_attached_not_raised(tmp_path):
    """A JUDGMENT field demanding a quotable passage contradicts its contract."""
    doc = copy.deepcopy(VALID)
    doc["fields"][0]["field_class"] = "judgment"
    doc["fields"][0]["source_quote_required"] = True
    cb = load_codebook(_write(tmp_path, doc))       # loads, does not raise
    assert cb.lint_findings
    assert "robot_platform" in cb.lint_findings[0]


# ── C8: one reader ───────────────────────────────────────────────────


def test_only_the_two_loaders_parse_yaml_directly():
    """Eleven readers opened the codebook with a bare yaml.safe_load and none
    of them validated anything. Each reached for the keys it happened to need,
    so a codebook missing one of them failed at a different place in each.

    Three raw-YAML sites survive, and each is the sole authority for its own
    file type: this loader, the Review Spec loader, and the adjudication
    category config.
    """
    import re

    allowed = {
        "engine/core/codebook.py",
        "engine/core/review_spec.py",
        "engine/adjudication/categorizer.py",
    }
    offenders = []
    for root in ("engine", "scripts", "analysis"):
        for path in sorted((REPO_ROOT / root).rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            rel = path.relative_to(REPO_ROOT).as_posix()
            if rel in allowed or rel == "engine/tools/inventory.py":
                continue
            for i, line in enumerate(path.read_text().split("\n"), 1):
                if re.search(r"\byaml\.(safe_)?load\s*\(", line):
                    offenders.append(f"{rel}:{i}")
    assert offenders == [], (
        "these parse YAML directly instead of going through a loader: "
        + ", ".join(offenders)
    )


def test_the_codebook_file_is_read_never_written():
    """Nothing in the engine writes a codebook."""
    import re

    for root in ("engine", "scripts"):
        for path in sorted((REPO_ROOT / root).rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            src = path.read_text()
            for i, line in enumerate(src.split("\n"), 1):
                if "extraction_codebook" in line and re.search(
                    r"write_text|yaml\.(safe_)?dump|open\([^)]*[\"']w", line
                ):
                    raise AssertionError(
                        f"{path.relative_to(REPO_ROOT)}:{i} writes a codebook"
                    )


# ── T1: the accessors that replace ExtractionSchema / ExtractionField ─


def test_fields_by_tier_covers_every_field_exactly_once():
    cb = load_codebook_for("surgical_autonomy")
    seen = [f["name"] for t in (1, 2, 3, 4) for f in cb.fields_by_tier(t)]
    assert sorted(seen) == sorted(cb.field_names)
    assert len(seen) == 20


def test_fields_by_tier_preserves_codebook_order():
    """The prompt renders in this order, so it must not move."""
    cb = load_codebook_for("surgical_autonomy")
    for tier in (1, 2, 3, 4):
        got = [f["name"] for f in cb.fields_by_tier(tier)]
        expected = [n for n in cb.field_names if cb.field(n)["tier"] == tier]
        assert got == expected


def test_fields_by_tier_is_empty_for_an_unused_tier():
    cb = load_codebook_for("surgical_autonomy")
    assert cb.fields_by_tier(9) == ()


def test_enum_values_for_every_categorical_and_none_otherwise():
    cb = load_codebook_for("surgical_autonomy")
    categorical = [n for n in cb.field_names if cb.field(n)["type"] == "categorical"]
    # ELEVEN, not the eight the Phase 2b brief carried — eight is study_type's
    # VALUE count, not the number of categorical fields.
    assert len(categorical) == 11, categorical
    for n in categorical:
        vals = cb.enum_values(n)
        assert vals and all(isinstance(v, str) for v in vals)
        assert vals == [v["value"] for v in cb.field(n)["valid_values"]]
    for n in cb.field_names:
        if n not in categorical:
            assert cb.enum_values(n) is None, n


def test_enum_values_is_none_not_empty_for_a_field_without_a_list():
    """None and [] mean different things to a caller: 'no value list' is not
    'an empty value list'."""
    cb = load_codebook_for("surgical_autonomy")
    assert cb.enum_values("country") is None


def test_view_exposes_name_type_tier_and_values():
    cb = load_codebook_for("surgical_autonomy")
    v = cb.view("sample_size")
    assert (v.name, v.type, v.tier, v.enum_values) == ("sample_size", "numeric", 1, None)
    v = cb.view("study_type")
    assert v.type == "categorical" and v.enum_values[0] == "Original Research"
    assert len(cb.views) == 20


def test_view_and_enum_values_reject_an_unknown_field():
    cb = load_codebook_for("surgical_autonomy")
    for call in (cb.view, cb.enum_values, cb.field):
        with pytest.raises(CodebookError):
            call("no_such_field")
