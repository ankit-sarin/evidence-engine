"""U25 (INT-i12-31): an unquoted YAML 1.1 boolean, null or number is refused at codebook load (R788(b), (d)).

The five fixtures are committed under `docs/session-reports/session-12/12ae_U25A/fixtures/`, each the live
codebook with one line edited (12ae-U25A's probe and 12ae-U25B's derivation): an unquoted `Yes` as a
`valid_values` value, an unquoted `NO` and `null` as absence sentinels, an unquoted `1` as a value (F3), and an
unquoted `No` as a field's `instruction` (R788(b)(iv)). They are read from the repository, never re-authored,
and the test writes nothing. The live codebook must still load with its byte hash and its semantic hash
unchanged: a preservation pin, green before and after the fix.
"""
import pytest

from engine.core.codebook import CodebookError, clear_cache, load_codebook
from engine.core.repo_root import REPO_ROOT

FIXTURES = REPO_ROOT / "docs/session-reports/session-12/12ae_U25A/fixtures"
LIVE = REPO_ROOT / "data/surgical_autonomy/extraction_codebook.yaml"
# 12ad_close/codebook_spec_sha256.txt
LIVE_SHA256 = "89dbfa91b5033f2b3dff713e36c7ecd0aa086739b5832059948b6f55b29ead82"
# 12ae_U24B/pin_tuples_u24b.json, codebook_semantic_hash
LIVE_SEMANTIC_HASH = "f261af76feadd650f3c58b36498e2d140ef2dc4ffec6a80a16c06741eb2f5646"

#: fixture directory -> the key its refusal must name
CASES = {
    "value_yes": "value",
    "sentinel_no": "absence_sentinels",
    "sentinel_null": "absence_sentinels",
    "value_one": "value",
    "instruction_no": "instruction",
}


@pytest.fixture(autouse=True)
def _fresh_cache():
    clear_cache()
    yield
    clear_cache()


@pytest.mark.parametrize("case", sorted(CASES))
def test_unquoted_yaml_booleans_and_nulls_are_refused_at_load(case):
    path = FIXTURES / case / "extraction_codebook.yaml"
    assert path.is_file(), path
    with pytest.raises(CodebookError) as refused:
        load_codebook(path)
    message = str(refused.value)
    assert CASES[case] in message
    assert "quote" in message


def test_the_live_codebook_loads_unchanged():
    cb = load_codebook(LIVE)
    assert cb.sha256 == LIVE_SHA256
    assert cb.semantic_hash == LIVE_SEMANTIC_HASH
