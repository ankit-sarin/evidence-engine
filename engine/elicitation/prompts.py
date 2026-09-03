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
    KEY_FIELD, KEY_INDICES, KEY_INFERENCE, KEY_STEPS, KEY_STEP_CRITERIA,
    KEY_STEP_TEXT, KEY_VALUE, INFERENCE_MAX_SENTENCES, INFERENCE_MIN_SENTENCES,
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
        f'country is Canada.", "{KEY_VALUE}": "..."}}'
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
        blocks = "\n".join(_build_field_block(f) for f in entries)
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

Never return a value of any other kind without at least one citation.

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
