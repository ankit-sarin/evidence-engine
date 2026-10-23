"""U37 (INT-r12-1, R694(e)) — attempt 2's feedback names the whole schema and repeats the output
instruction after itself; attempt 1 and the non-elicited pin do not move.

12v-RSA measured the shape these tests are about: across every elicited attempt 2 on disk, attempt 2
came back STATED-only in 4 of the 6 that followed feedback naming only STATED fields, and in 0 of the
19 that followed any other feedback (12v_RSA.md F6). The block today lists only the failed fields and
asks to "re-emit EVERY field in the schema" without the count or the unlisted fields, after the whole
paper text, while "Emit exactly one entry per field (20 total)" sits before it (F3, F4).

R694(e) fixes it with options R-1 and R-2 of 12v_RSA.md §Q8 in one commit, attempt 2 only (R670(b)).
Phase 1 (12v-U37A) wrote these against the unmodified tree: T1 and T2 failed there, T3–T5 passed and
are the guards (12v_U37A/phase1/). They entered tests/ with the fix (R695(b)). Every assertion is on rendered text through the runtime's own builders — no model,
no database.
"""

from __future__ import annotations

import hashlib
import json
import re

import pytest

from engine.core.codebook import load_codebook
from engine.core.completeness import expected_field_names
from engine.core.review_paths import load_spec_for
from engine.elicitation import classes as C
from engine.elicitation import pipeline as PL
from engine.elicitation.contracts import (
    FIELD_MISSING, STEPS_MISSING, VALUE_WITHOUT_CITATION, FieldRecord, Pass1Result, Step,
)
from engine.elicitation.prompts import build_feedback_block
from engine.elicitation.units import build_unit_map

LIVE_CODEBOOK = "data/surgical_autonomy/extraction_codebook.yaml"
OUTPUT_SENTENCE = "Emit exactly one entry per field"
PAPER_HEADING = "## Numbered Paper Text"
FEEDBACK_HEADING = "## Your previous response did not meet the contract"
#: A fixed paper for the rendered prompt: four units, every one of MIN_UNIT_TOKENS or more.
TEXT = ("The robot sutured the bowel in twelve pigs. Leak pressure was measured in every case.\n\n"
        "No human comparison group was reported. The study was performed in Baltimore, Maryland.")

#: Captured at the unmodified tree (12v-U37A Phase 1, HEAD 1e15b50; 12v_U37A/phase1/…/fixtures_head.json).
ATTEMPT1_MESSAGES_SHA256 = "d3c047de668aa59bcf6e486a747843a8cb7c3d724fe67ccb69ebfa10e4101e31"
MIXED_ENTRIES_SHA256 = "5f8c11217eec7daebe25d5654c2cd50b6a8145c8822860bd35559df346d590b7"
#: The non-elicited arm's prompt hash per stage, as `12l_U04/pin_tuples.py` read them at the unmodified
#: tree (12v_U37A/phase1/…/pin_preview_head.json, whose whole pin is d3672ab6…ff5c).
NON_ELICITED_PROMPT_HASHES = {
    "audit": "d972c46dde944f1381c4f2d5472c6264c3a1053bd23bdd3801bdf0fdc2a5838d",
    "extract_pass1": "c0083f30e0d7a4ff3e8036ac6e6feeb0e79319dab7693ec246aee3809d054f34",
    "extract_pass2": "a4f3cb35706fcc1974494f0e746e10a6bc21499a661bc9a49bcbf2d51b6e8e78",
    "extract_retry_snippet": "05a981814969ec38c5785343a403d02f6c5c8ff2724490cae3a602da069b55aa",
    "preflight:deepseek-r1:32b": "de95332f63517c2c6a6558c76892369173dd3a55cf71944cabd0850ed44d322a",
    "preflight:gemma3:27b": "de95332f63517c2c6a6558c76892369173dd3a55cf71944cabd0850ed44d322a",
}


@pytest.fixture(scope="module")
def cb():
    return load_codebook(LIVE_CODEBOOK)


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


@pytest.fixture(scope="module")
def names(spec, cb):
    return expected_field_names(spec, cb.path)


@pytest.fixture(scope="module")
def classes(cb, names):
    by = {f["name"]: f["field_class"] for f in cb.raw["fields"]}
    return {n: by[n] for n in names}


def _ok(name, cls):
    if cls == C.INFERABLE:
        return FieldRecord(name, cls, "V", False, indices=(1,), inference="I")
    if cls == C.JUDGMENT:
        return FieldRecord(name, cls, "V", False, indices=(1,), steps=(Step("T", (1,)),))
    return FieldRecord(name, cls, "V", False, indices=(1,))


def _result(records, n_units):
    return Pass1Result(records={r.field_name: r for r in records}, parse_path="direct", n_units=n_units)


def _stated_only_failure(names, classes, n_units):
    """12r paper 8's shape (12v_RSA.md §Q2): every field answered, one STATED field uncited."""
    failed = next(n for n in names if classes[n] == C.STATED)
    recs = [FieldRecord(n, C.STATED, "NR", False, violations=(VALUE_WITHOUT_CITATION,)) if n == failed
            else _ok(n, classes[n]) for n in names]
    return _result(recs, n_units), failed


def _mixed_failure(names, classes, n_units):
    """One STATED uncited, one JUDGMENT without steps, one STATED missing; the rest pass."""
    st = [n for n in names if classes[n] == C.STATED]
    jd = [n for n in names if classes[n] == C.JUDGMENT]
    bad = {st[0]: FieldRecord(st[0], C.STATED, "12 patients", False, violations=(VALUE_WITHOUT_CITATION,)),
           jd[0]: FieldRecord(jd[0], C.JUDGMENT, "Small cohort", False, indices=(2,), violations=(STEPS_MISSING,)),
           st[1]: FieldRecord(st[1], C.STATED, "", False, violations=(FIELD_MISSING,))}
    return _result([bad.get(n) or _ok(n, classes[n]) for n in names], n_units), set(bad)


def _entries(block):
    """The failed-field list: each '- **name** [CLASS]' line and the indented lines under it."""
    out, inside = [], False
    for line in block.split("\n"):
        if line.startswith("- **"):
            inside = True
        elif not line.startswith("    "):
            inside = False
        if inside:
            out.append(line)
    return out


def _attempt1_messages(cb, names):
    um = build_unit_map(0, TEXT)
    return PL.pass1_messages(PL.pass1_prompt(um, cb.raw, names)), um


def test_u37_the_feedback_header_names_every_schema_field(cb, names, classes):
    """INT-r12-1, 12v_RSA.md §11 R-1 ("the block contains the count 20 and the three class names") as
    R694(e) widens it ("the feedback header names the whole schema (every field, with the count)"): for a
    STATED-only failure the block names every codebook field, the count and the three classes, not only
    the failed field and its class. Fails on the unmodified tree, which names one field."""
    um = build_unit_map(0, TEXT)
    result, failed = _stated_only_failure(names, classes, um.n)
    block = build_feedback_block(result, cb.raw)
    assert block, "a failing field must produce a block"
    missing = [n for n in names if n not in block]
    assert not missing, f"the block names {len(names) - len(missing)} of {len(names)} fields; missing {missing}"
    assert re.search(rf"\b{len(names)}\b", block), f"the block does not state the field count {len(names)}"
    for cls in C.CLASSES:                         # §11 R-1: "the three class names"
        assert cls.upper() in block, f"the block does not name the {cls.upper()} class"
    assert len(names) == 20                       # the live codebook's 9 / 6 / 5


def test_u37_the_output_instruction_is_repeated_after_the_feedback(cb, names, classes):
    """INT-r12-1, 12v_RSA.md §11 R-2: attempt 2's user turn carries the output instruction after the
    feedback block, and still carries it before the paper text. Fails on the unmodified tree, where it
    appears once, before the paper."""
    um = build_unit_map(0, TEXT)
    result, _ = _stated_only_failure(names, classes, um.n)
    feedback = build_feedback_block(result, cb.raw)
    user = PL.pass1_messages(PL.pass1_prompt(um, cb.raw, names, feedback))[1]["content"]
    i_paper, i_fb = user.index(PAPER_HEADING), user.index(FEEDBACK_HEADING)
    assert i_paper < i_fb, "the feedback follows the paper text"
    assert OUTPUT_SENTENCE in user[:i_paper], "the instruction before the paper text is kept"
    assert OUTPUT_SENTENCE in user[i_fb:], "the output instruction is repeated after the feedback block"


def test_u37_attempt_1_prompt_text_is_unchanged(cb, names, classes):
    """INT-r12-1, 12v_RSA.md §11 R-2 ("Attempt 1's prompt is a byte-identical prefix of attempt 2's.
    Attempt 1's request hash is unchanged"), R670(b): the attempt-1 system message and user turn render
    byte-identical to the unmodified tree's, and attempt 2 begins with them. Passes before and after U37."""
    msgs, um = _attempt1_messages(cb, names)
    got = hashlib.sha256(json.dumps(msgs, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    assert got == ATTEMPT1_MESSAGES_SHA256
    result, _ = _stated_only_failure(names, classes, um.n)
    a2 = PL.pass1_messages(PL.pass1_prompt(um, cb.raw, names, build_feedback_block(result, cb.raw)))
    assert a2[0] == msgs[0]
    assert a2[1]["content"].startswith(msgs[1]["content"]) and len(a2[1]["content"]) > len(msgs[1]["content"])


def test_u37_the_non_elicited_pin_is_unchanged(spec, cb):
    """INT-r12-1, 12v_RSA.md §11 R-1 ("The non-elicited pin tuple is byte-identical"), R694(e) ("the
    non-elicited pin must not [move]"), narrowed by R695(d): every stage's prompt hash of the
    non-elicited arm, resolved as 12l_U04/pin_tuples.py resolves them — digests fixed, no fetch — equals
    the unmodified tree's. The whole tuple also carries the installed client-library versions, so its
    comparison stays the session-close check by pin_tuples.py, not this test."""
    from engine.agents.extractor import extraction_stages
    from engine.core import run_manifest as rm
    digests = {"deepseek-r1:32b": "edba8017331d15236e57480eb45406c0d721db77a4cdcf234df500fc2ad3960c",
               "gemma3:27b": "a418f5838eaf7fe2cfe0a3046c8384b68ba43a4435542c942f9db00a5f342203"}
    s = spec.model_copy(update={"extraction_models": spec.extraction_models.model_copy(update={"elicitation": False})})
    stages = [*extraction_stages(s), "audit", "preflight"]
    arms = {st: a for st in stages if (a := rm._stage_arm(s, st))}
    res = rm.resolve_run(s, stages, digest_fn=digests.__getitem__, preflight_models=sorted(digests),
                         codebook_path=cb.path, arms_by_stage=arms)
    assert {k: r.prompt_hash for k, r in sorted(res.items())} == NON_ELICITED_PROMPT_HASHES


def test_u37_a_non_stated_feedback_block_still_lists_only_its_failed_fields_codes(cb, names, classes):
    """INT-r12-1, R694(e) ("only the header and the trailing instruction differ"): for a mixed failure
    the failed-field list — each '- **name** [CLASS]' entry with its value, indices and '✗ code —
    requirement' lines — is byte-identical to the unmodified tree's, and it carries exactly the failed
    fields. Passes before and after U37; U37 must not render passing fields in this entry form."""
    um = build_unit_map(0, TEXT)
    result, failed = _mixed_failure(names, classes, um.n)
    entries = _entries(build_feedback_block(result, cb.raw))
    listed = {m.group(1) for line in entries if (m := re.match(r"- \*\*(.+?)\*\* \[", line))}
    assert listed == failed
    got = hashlib.sha256("\n".join(entries).encode()).hexdigest()
    assert got == MIXED_ENTRIES_SHA256
