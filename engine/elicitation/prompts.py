"""Pass-1 elicitation prompt and Pass-2 priming message.

Pass 1 asks for evidence FIRST and the value second, under a contract that
differs by field class. Pass 2 is primed with the materialized evidence rather
than with a free-form reasoning trace.

**Why this exists at all.** PRIME-01 measured Run 6's quote-rich priming at
37.9-42.9% verbatim-window richness in the Pass-1 draft channel against 0.4% in
the thinking channel, and Run 6's 58.3% anchoring turned out to be an artifact of
a parser bug that routed the draft into Pass 2. The lever is not to restore the
bug; it is to elicit the quotes deliberately, which is what these prompts do.

**Field order.** Pass 1 groups fields BY CLASS, because the contract is per
class and interleaving three contracts through a tier ordering would make each
field's obligations a lookup. Within a class, codebook order is preserved. Pass 2
is untouched and keeps its tier ordering (`build_extraction_prompt`).

**No hand-curated lists** (acceptance gate 2). The class of every field, the
membership of every class, and the escape token all come from the codebook.
Adding a field to the codebook adds it to its class's contract with no code
change; adding a class would not, and that is deliberate -- a fourth class needs
a contract written for it, not inferred.
"""

from __future__ import annotations

from engine.agents.extractor import _build_field_block
from engine.elicitation import classes as C
from engine.elicitation.contracts import (
    DIRECTLY_STATED, FIELD_MISSING, INDEX_MALFORMED, INDEX_OUT_OF_RANGE,
    INFERENCE_MALFORMED, INFERENCE_MISSING, KEY_FIELD, KEY_INDICES,
    KEY_INFERENCE, KEY_STEPS, KEY_STEP_CRITERIA, KEY_STEP_TEXT, KEY_VALUE,
    ESCAPE_WITH_CITATION, INFERENCE_MAX_SENTENCES, INFERENCE_MIN_SENTENCES,
    STEPS_MISSING, STEP_WITHOUT_BASIS, VALUE_MISSING, VALUE_WITHOUT_CITATION,
    Pass1Result,
)
from engine.elicitation.units import UnitMap

SYSTEM_PASS1 = (
    "You are a systematic review data extractor. For each field, first cite the "
    "numbered sentence units that carry the evidence, then state the value. Cite "
    "before you answer, and never copy text — cite unit numbers only."
)

SYSTEM_PASS2 = (
    "You are a systematic review data extractor. "
    "Use the cited evidence to produce accurate structured output. "
    "Respond ONLY with the requested JSON."
)

_CLASS_TITLE = {
    C.STATED: "STATED — the paper asserts it",
    C.INFERABLE: "INFERABLE — the paper fixes it without asserting it",
    C.JUDGMENT: "JUDGMENT — no single passage states it",
}

_CLASS_CONTRACT = {
    C.STATED: (
        "For each field below: cite at least one unit that asserts the value, "
        "then state the value.\n"
        f'  {{"{KEY_FIELD}": "...", "{KEY_INDICES}": [12, 13], "{KEY_VALUE}": "..."}}'
    ),
    C.INFERABLE: (
        "The paper contains material that fixes these values but does not state "
        "them. For each field below: cite at least one unit carrying that "
        "material, then declare the inference in "
        f"{INFERENCE_MIN_SENTENCES}–{INFERENCE_MAX_SENTENCES} sentences naming "
        "the step from the cited evidence to the value, then state the value.\n"
        f'  {{"{KEY_FIELD}": "...", "{KEY_INDICES}": [4], '
        f'"{KEY_INFERENCE}": "The affiliation names a Vancouver institution, so the '
        f'country is Canada.", "{KEY_VALUE}": "..."}}\n'
        f"If a cited unit states the value OUTRIGHT and you made no inferential "
        f"step, put the literal `{DIRECTLY_STATED}` in `{KEY_INFERENCE}` instead "
        f"of inventing one. The citation is still required — only the "
        f"declaration changes.\n"
        f'  {{"{KEY_FIELD}": "...", "{KEY_INDICES}": [4], '
        f'"{KEY_INFERENCE}": "{DIRECTLY_STATED}", "{KEY_VALUE}": "..."}}'
    ),
    C.JUDGMENT: (
        "These values are syntheses that no single passage states. For each "
        "field below: reason in steps, where EVERY step either cites at least "
        "one unit or is explicitly marked as applying the codebook criteria with "
        "no textual basis claimed; then state the value. A step's "
        f"`{KEY_INDICES}` takes bare integers, exactly like a field's — "
        "`[7]`, never `[S7]`.\n"
        f'  {{"{KEY_FIELD}": "...", "{KEY_STEPS}": ['
        f'{{"{KEY_STEP_TEXT}": "...", "{KEY_INDICES}": [7]}}, '
        f'{{"{KEY_STEP_TEXT}": "...", "{KEY_STEP_CRITERIA}": true}}], '
        f'"{KEY_VALUE}": "..."}}'
    ),
}


def group_by_class(codebook: dict, field_names: tuple[str, ...]
                   ) -> dict[str, list[dict]]:
    """{class: [codebook entry, ...]} for the requested fields, codebook order.

    Derived from the codebook's own `field_class`; raises if a requested field
    has no class, because a prompt cannot state a contract it does not know.
    """
    known = C.classes_by_field(codebook)
    wanted = set(field_names)
    missing = sorted(wanted - set(known))
    if missing:
        raise C.CodebookContractError(
            f"fields requested by the spec but absent from the codebook: {missing}"
        )
    out: dict[str, list[dict]] = {c: [] for c in C.CLASSES}
    for f in codebook["fields"]:
        if f["name"] in wanted:
            out[known[f["name"]]].append(f)
    return out


def prompt_field_order(codebook: dict, field_names: tuple[str, ...]) -> tuple[str, ...]:
    """The order Pass 1 presents fields in: class blocks, codebook order within."""
    grouped = group_by_class(codebook, field_names)
    return tuple(f["name"] for c in C.CLASSES for f in grouped[c])


def _escape_line(escape: str) -> str:
    """The escape alternative, restated on EVERY field block (Ruling 2(a)).

    ELICIT-DESIGN-01's F1 is the reason: `NO_EVIDENCE_LOCATABLE` was used 0 times
    in 180 field entries while `NR` was used 23 times, 19 of them uncited. The
    token existed only in a preamble two hundred lines above the field the model
    was answering, and the sentinel habit won. A response-format line is where
    the model is deciding what to emit, so that is where the alternative has to
    be. Uniform across classes on purpose: the escape shape is the same for all
    three, and that sameness is itself the teaching.
    """
    return (f'\n  *Nothing citable for this field?* '
            f'{{"{KEY_FIELD}": "...", "{KEY_INDICES}": [], "{KEY_VALUE}": "{escape}"}}')


def _worked_example(escape: str) -> str:
    """One compact worked example of correct escape use (Ruling 2(b)).

    Deliberately built on `funding_source`, which is NOT a field in this or any
    review's codebook, and on invented sentences. An example drawn from a corpus
    paper would be a demonstration answer for a field the model is about to be
    asked, and the three-case contrast — nothing citable, the paper reports it,
    the paper reports its absence — is exactly the distinction F1 says is not
    landing.
    """
    return f"""## Worked example — the three cases, on a field that is not in this schema

Say the field were `funding_source`.

1. You read every unit and find no sentence naming a funder, and no sentence
   saying funding was not received. Nothing to cite either way — this is the
   escape, and it takes an EMPTY citation list:

     {{"{KEY_FIELD}": "funding_source", "{KEY_INDICES}": [], "{KEY_VALUE}": "{escape}"}}

2. Unit [S88] reads "The authors received no external funding." The paper DOES
   report the item, so this is an ordinary value and it cites the unit:

     {{"{KEY_FIELD}": "funding_source", "{KEY_INDICES}": [88], "{KEY_VALUE}": "None"}}

3. Unit [S88] instead reads "Funding information was not available." The paper is
   reporting the ABSENCE, so a sentinel is right — and it cites the unit that
   reports that absence:

     {{"{KEY_FIELD}": "funding_source", "{KEY_INDICES}": [88], "{KEY_VALUE}": "NR"}}

Case 1 is the one that gets missed. If you find yourself about to write a
sentinel with an empty citation list, the answer you want is case 1."""


def _sentinel_rule(escape: str, sentinels: str) -> str:
    """The explicit sentinel rule (Ruling 2(c)), at the ruling's stated intent."""
    return (f"**The sentinel rule.** A sentinel such as NR is a claim about the "
            f"paper and must cite the sentence stating that absence. If no such "
            f"sentence exists, output {escape}.")


def build_pass1_prompt(unit_map: UnitMap, codebook: dict,
                       field_names: tuple[str, ...]) -> str:
    """The elicitation prompt: numbered paper text plus per-class contracts."""
    grouped = group_by_class(codebook, field_names)
    escape = C.escape_token(codebook)
    sentinels = ", ".join(sorted(C.absence_sentinels(codebook)))

    sections = []
    for cls in C.CLASSES:
        entries = grouped[cls]
        if not entries:
            continue
        blocks = "\n".join(
            _build_field_block(f) + _escape_line(escape) for f in entries
        )
        sections.append(
            f"### {_CLASS_TITLE[cls]}  ({len(entries)} fields)\n\n"
            f"{_CLASS_CONTRACT[cls]}\n\n{blocks}"
        )
    schema_text = "\n\n".join(sections)
    n_fields = sum(len(v) for v in grouped.values())

    return f"""Extract structured data from the following paper for a systematic review.

The paper text below is split into numbered sentence units, each prefixed with a
marker of the form [S1], [S2], ... [S{unit_map.n}]. You cite evidence by unit
number. Do not quote or copy any text: name the unit numbers and nothing else.

Every index must be a bare JSON integer between 1 and {unit_map.n}. **Use the
integer only, not the "[S12]" marker** — write `[12, 13]`, never `[S12, S13]`.
This applies to every `{KEY_INDICES}` list in your response, including the ones
nested inside reasoning steps. A marker written into an index list is not valid
JSON and the whole response is discarded.

Section headings are numbered units too and may be cited.

The fields are grouped into three classes. Each class states what must accompany
its citations. Read the contract at the top of a class block before its fields.

## Extraction Schema
{schema_text}

## Escape and absence — these are two different things
- **{escape}** — you searched and could locate nothing in this paper to cite for
  the field. Emit it as the value with an EMPTY citation list. It is a statement
  about your search, so it never carries a citation.
- **{sentinels}** — the paper itself does not report the item. That is a claim
  about the paper's text, so it is a value like any other and REQUIRES at least
  one citation showing where the paper would have reported it and does not.

{_sentinel_rule(escape, sentinels)}

Never return a value of any other kind without at least one citation.

{_worked_example(escape)}

## Output
Emit exactly one entry per field ({n_fields} total), in the order the fields are
listed above, with the evidence keys BEFORE the value key, as JSON:

{{"fields": [ ...one object per field, shaped by its class... ]}}

## Numbered Paper Text
{unit_map.render()}"""


def build_pass2_priming_message(priming: str) -> str:
    """The Pass-2 user message that replaces the free-form reasoning trace.

    Pass 2 still receives the full paper text as its first user message
    (ELICIT-DESIGN-01 C6-Q4: this task changes elicitation, not context
    composition), so the evidence below is priming, not the model's only view of
    the paper.
    """
    return (
        "Here is the evidence you cited for this paper in your first pass. Each "
        "quote is the verbatim text of a numbered unit you named, resolved by the "
        "engine — not text you retyped.\n\n"
        f"{priming}\n\n"
        "Now output the structured extraction as JSON matching the schema. Include "
        "all fields from the extraction schema. For each field, use the cited "
        "evidence above as your source_snippet wherever it supports the value, and "
        "keep the value consistent with the evidence you cited."
    )


# ── Ruling 4: the typed feedback block that makes attempt 2 not-identical ──

FEEDBACK_ECHO_CAP = 200
FEEDBACK_TRUNCATION_MARKER = "…[truncated]"


def _echo(text: str, cap: int = FEEDBACK_ECHO_CAP) -> str:
    """Quote back what the model emitted, bounded and VISIBLY bounded.

    The cap exists so one pathological Pass-1 value cannot make attempt 2
    unsizable. The marker exists because a silently shortened echo would show
    the model a doctored artifact of its own output and invite it to "fix"
    wording it never wrote (ELICIT-DESIGN-02 D5).
    """
    text = str(text or "")
    return text if len(text) <= cap else text[:cap] + FEEDBACK_TRUNCATION_MARKER


def _requirement(code: str, cls: str, escape: str, n_units: int) -> str:
    """What the contract requires, for one violation code on one field class.

    Keyed by the closed violation vocabulary and the class, never by field name:
    the builder stays field-agnostic exactly like the prompt builder, and gate 2
    holds for the retry path too.
    """
    accompaniment = {
        C.STATED: "cite at least one unit that asserts the value, before the value",
        C.INFERABLE: (
            f"cite at least one unit, then declare the inference in "
            f"{INFERENCE_MIN_SENTENCES}–{INFERENCE_MAX_SENTENCES} sentences "
            f"(or the literal {DIRECTLY_STATED} if a cited unit states it "
            f"outright), then the value"
        ),
        C.JUDGMENT: (
            "reason in steps where EVERY step either cites at least one unit or "
            "sets criteria_application: true, then the value"
        ),
    }[cls]
    return {
        FIELD_MISSING:
            "no entry for this field appeared in your response. Every field in "
            "the schema needs exactly one entry.",
        VALUE_MISSING:
            "the entry carried no value. State one, or emit "
            f"{escape} with an empty citation list.",
        VALUE_WITHOUT_CITATION:
            f"a value was asserted with nothing cited behind it. The contract is: "
            f"{accompaniment}. If nothing in this paper can be cited for it, the "
            f"value must be {escape} with an EMPTY citation list.",
        ESCAPE_WITH_CITATION:
            f"{escape} is a statement about your search, not about the paper, so "
            f"it takes an EMPTY citation list. If you can cite something, the "
            f"field has a value and is not an escape.",
        INDEX_MALFORMED:
            "an index was not a bare JSON integer. Write `[12]`, never `[S12]` "
            "and never `\"S12\"`. Nothing was dropped to rescue the rest — the "
            "whole field failed.",
        INDEX_OUT_OF_RANGE:
            f"an index named a unit that does not exist. Valid indices for this "
            f"paper are 1 to {n_units}.",
        INFERENCE_MISSING:
            f"an INFERABLE field needs a declared inference: {accompaniment}.",
        INFERENCE_MALFORMED:
            f"the declared inference must be "
            f"{INFERENCE_MIN_SENTENCES}–{INFERENCE_MAX_SENTENCES} sentences, or "
            f"the literal {DIRECTLY_STATED}.",
        STEPS_MISSING:
            f"a JUDGMENT field needs `{KEY_STEPS}`, not a bare value with a "
            f"citation. The contract is: {accompaniment}.",
        STEP_WITHOUT_BASIS:
            "at least one reasoning step neither cited a unit nor set "
            f"`{KEY_STEP_CRITERIA}`: true. Every step needs one or the other.",
    }.get(code, f"the field did not satisfy its {cls.upper()} contract.")


def build_feedback_block(result: Pass1Result, codebook: dict,
                         cap: int = FEEDBACK_ECHO_CAP) -> str:
    """Attempt 2's appended feedback, listing every field that failed.

    ELICIT-DESIGN-01's F7 measured that re-issuing the identical request bought
    nothing over three attempts and cost p604 three clean fields. The failure it
    was retrying is response CONTENT at temperature 0, not response SHAPE, and an
    identical request is the right instrument only for shape. This block is what
    makes attempt 2 a different request.

    Deterministic by construction: fields in the response's own order, codes in
    the order the checker recorded them, echoes bounded and marked.
    """
    escape = C.escape_token(codebook)
    lines: list[str] = []
    for name, rec in result.records.items():
        if rec.ok:
            continue
        lines.append(f"- **{name}** [{rec.field_class.upper()}]")
        lines.append(f"    you returned value: {_echo(rec.value, cap)!r}")
        lines.append(f"    with {KEY_INDICES}: {list(rec.indices)}")
        if rec.bad_indices:
            lines.append(f"    indices that did not resolve: "
                         f"{[repr(b) for b in rec.bad_indices]}")
        if rec.field_class == C.INFERABLE:
            lines.append(f"    with {KEY_INFERENCE}: {_echo(rec.inference, cap)!r}")
        if rec.field_class == C.JUDGMENT:
            lines.append(f"    with {len(rec.steps)} reasoning step(s)")
        for code in rec.fatal:
            lines.append(f"    ✗ {code} — "
                         f"{_requirement(code, rec.field_class, escape, result.n_units)}")

    if not lines:
        return ""

    n = len(result.failed_fields)
    return (
        f"\n\n## Your previous response did not meet the contract on {n} field(s)\n\n"
        "Each one below shows what you returned and what its contract requires. "
        "Fix these, and re-emit EVERY field in the schema — this is a full "
        "replacement response, not a patch.\n\n"
        + "\n".join(lines)
    )
