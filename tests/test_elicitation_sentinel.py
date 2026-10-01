"""12c-E-SENTINEL — a codebook the elicited prompt-hash sentinels cannot fill is refused by name.

The sentinels added at E-PIN (84faf0d) take field names from the review's own
codebook, one per record, by class. A codebook with too few fields of a class
used to raise an unnamed IndexError from inside the render — which `open_run`
reaches through `resolve_run`, before any write. It is now a `RunRefused`
naming each short class, the count required and present, and the codebook.
Non-elicited renders never reach the sentinels, so never reach the check.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from engine.core import effective_config as ec
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.review_paths import load_spec_for
from engine.elicitation import classes as C
from engine.elicitation import pipeline as PL

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


@pytest.fixture(scope="module")
def elicited(spec):
    em = spec.extraction_models.model_copy(update={"elicitation": True})
    return spec.model_copy(update={"extraction_models": em})


def _trimmed(dest: Path, *, keep_stated: int | None = None, drop_class: str | None = None) -> Path:
    """The live codebook with fields removed; nothing else changed."""
    raw = yaml.safe_load(LIVE_CODEBOOK.read_text())
    fields, n_stated = [], 0
    for f in raw["fields"]:
        if f["field_class"] == drop_class:
            continue
        if f["field_class"] == C.STATED and keep_stated is not None:
            n_stated += 1
            if n_stated > keep_stated:
                continue
        fields.append(f)
    raw["fields"] = fields
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / "extraction_codebook.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return path


def _hash(stage, s, path):
    return ec.prompt_hash(stage, s, ec.stage_config(stage, s), codebook_path=path)


def test_three_stated_fields_are_refused_naming_stated_four_and_three(elicited, tmp_path):
    path = _trimmed(tmp_path, keep_stated=3)
    with pytest.raises(rm.RunRefused) as exc:
        _hash("elicitation_pass1", elicited, path)
    assert type(exc.value).__name__ == "ElicitationSentinelUnsatisfiable"
    msg = str(exc.value)
    assert msg.startswith("run refused: ")
    assert "STATED: 4 required, 3 present" in msg
    assert str(path) in msg and "E-SENTINEL-ADAPT" in msg
    assert "INFERABLE" not in msg and "JUDGMENT" not in msg


def test_no_judgment_field_is_refused_naming_judgment(elicited, tmp_path):
    path = _trimmed(tmp_path, drop_class=C.JUDGMENT)
    for stage in ("elicitation_pass1", "extract_pass2"):
        with pytest.raises(rm.RunRefused) as exc:
            _hash(stage, elicited, path)
        assert type(exc.value).__name__ == "ElicitationSentinelUnsatisfiable"
        assert "JUDGMENT: 1 required, 0 present" in str(exc.value)
        assert "STATED" not in str(exc.value)


def test_open_run_refuses_before_any_write(elicited, tmp_path):
    from engine.core.database import ReviewDatabase
    db = ReviewDatabase("sentinel", data_root=tmp_path)
    try:
        path = _trimmed(Path(db.db_path).parent, keep_stated=3)
        cb = load_codebook(path)
        with pytest.raises(rm.RunRefused) as exc:
            rm.open_run(db._conn, elicited, kind="extraction",
                        stages=("elicitation_pass1", "extract_pass2", "extract_retry_snippet"),
                        codebook=cb, git=rm.GitState(commit="b" * 40, dirty=False, tag=None),
                        digest_fn=lambda m: "a" * 64)
        assert type(exc.value).__name__ == "ElicitationSentinelUnsatisfiable"
        for table in ("run_manifests", "run_stage_configs", "arms"):
            n = db._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert n == 0, f"{table} was written before the refusal"
    finally:
        db.close()


def test_the_live_codebook_satisfies_the_sentinels(elicited):
    cb = load_codebook(LIVE_CODEBOOK)
    names = PL.expected_field_names(elicited, LIVE_CODEBOOK)
    need = PL.sentinel_requirement()
    assert set(need) == set(C.CLASSES)
    assert need[C.STATED] == 4 and need[C.INFERABLE] == 1 and need[C.JUDGMENT] == 1
    PL.check_sentinel_fields(cb.raw, names, LIVE_CODEBOOK)          # does not raise
    for stage in ("elicitation_pass1", "extract_pass2"):
        _hash(stage, elicited, LIVE_CODEBOOK)


def test_a_non_elicited_render_never_calls_the_check(spec, tmp_path):
    path = _trimmed(tmp_path, keep_stated=3, drop_class=C.JUDGMENT)

    def must_not_run(*a, **kw):
        raise AssertionError("the sentinel check ran for a non-elicited render")

    with patch.object(PL, "check_sentinel_fields", must_not_run):
        for stage in ("extract_pass1", "extract_pass2", "extract_retry_snippet"):
            _hash(stage, spec, path)
