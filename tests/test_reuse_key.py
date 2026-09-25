"""S3d's reuse key (R91) and gate G1 (INPUT-IDENTITY-01).

G1 is R92's sentence one: a one-character change to a recorded parsed text is
refused by the resolver with both hashes named; recording it as a new version
makes the resolver return the new hash and a different reuse key.
"""

from __future__ import annotations

import hashlib

import pytest

from engine.core import parsed_text as pt
from engine.core.database import ReviewDatabase
from engine.core.effective_config import sha256_canonical
from engine.core.reuse_key import SCHEME, reuse_key
from _parsed_text_fixture import write_parsed

ARM = "local_deepseek_r1_32b"
H1, H2 = "a" * 64, "b" * 64


def test_the_key_is_deterministic_and_follows_its_documented_scheme():
    k = reuse_key(ARM, 7, H1)
    assert k == reuse_key(ARM, 7, H1)
    assert k == f"{SCHEME}:" + sha256_canonical(
        {"arm": ARM, "paper_id": 7, "parsed_text_sha256": H1})
    assert k.startswith("rk1:") and len(k) == 4 + 64


@pytest.mark.parametrize("other", [
    ("openai_o4_mini_2025_04_16_high", 7, H1),   # arm
    (ARM, 8, H1),                                # paper
    (ARM, 7, H2),                                # parsed text
])
def test_each_component_changes_the_key(other):
    assert reuse_key(*other) != reuse_key(ARM, 7, H1)


@pytest.mark.parametrize("bad", [
    ("", 7, H1), (None, 7, H1), (ARM, "7", H1), (ARM, True, H1), (ARM, 7.0, H1),
    (ARM, 7, "A" * 64), (ARM, 7, "a" * 63), (ARM, 7, None),
])
def test_a_malformed_component_is_refused_not_hashed(bad):
    with pytest.raises(ValueError):
        reuse_key(*bad)


def test_selection_and_the_event_mapping_are_the_callers_of_the_key():
    """R92 held the key uncalled through session 8; 9b-2a gave it one caller,
    extraction selection (9b-2a R5). 9b-2c adds the second the 2(a) ruling R3
    anticipated: the event mapping stamps it on every claim it writes. Both call
    the one function — shared, never copied."""
    from pathlib import Path
    repo = Path(__file__).resolve().parent.parent
    callers = [f.relative_to(repo).as_posix() for f in (repo / "engine").rglob("*.py")
               if "reuse_key(" in f.read_text() and f.name != "reuse_key.py"]
    assert sorted(callers) == ["engine/core/extraction_events.py", "engine/core/selection.py"]


# ── G1 ────────────────────────────────────────────────────────────────
@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("g1", data_root=tmp_path)
    rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, updated_at) "
                      "VALUES (7, 't', 's', 'PARSED', 'n', 'n')")
    rdb._conn.commit()
    yield rdb
    rdb.close()


def test_g1_a_one_character_change_is_refused_then_recorded_as_a_new_version(db):
    original = "The robot completed 20 of 20 sutures.\n"
    path = write_parsed(db, 7, original)
    before = pt.resolve_parsed_text(db._conn, 7)
    key_before = reuse_key(ARM, 7, before.sha256)

    edited = original.replace("20 of 20", "20 of 21")
    assert sum(a != b for a, b in zip(original, edited)) == 1
    path.write_text(edited)
    observed = hashlib.sha256(path.read_bytes()).hexdigest()

    with pytest.raises(pt.ParsedTextModified) as exc:
        pt.load_parsed_text(db._conn, 7)
    assert before.sha256 in str(exc.value) and observed in str(exc.value)

    # The remedy the refusal names: record the changed text as a new version.
    path.write_text(original)  # the recorded v1 is restored, untouched
    write_parsed(db, 7, edited)
    after = pt.resolve_parsed_text(db._conn, 7)
    assert (after.version, after.sha256) == (2, observed)
    assert pt.load_parsed_text(db._conn, 7) == edited
    assert reuse_key(ARM, 7, after.sha256) != key_before
