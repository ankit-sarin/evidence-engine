"""ELICIT-01 — sibling prompt builders for the COPY and INDEX conditions.

Both reuse the production codebook traversal (`_load_codebook` +
`_build_field_block` from `engine.agents.extractor`) so field definitions,
per-value descriptions and examples are byte-identical to what production Pass 1
shows the model. `build_extraction_prompt` itself is NOT modified and NOT called:
these are siblings, not wrappers.

The field set is **derived**, never hand-listed: `field_class3.fields_by_class(STATED)`
intersected with the codebook's own field order. If the ratified classification
changes, these prompts change with it, and the test asserts the derivation rather
than a literal list.

The two conditions differ in exactly one respect -- how evidence is elicited:

  COPY   value + one or more verbatim quotes copied from the paper.
  INDEX  value + a list of [Sn] unit indices; the engine materializes the quotes
         afterwards from the persisted unit map.

Everything else -- field set, definitions, ordering, absence handling, output
container -- is held identical, so a difference in the results is attributable to
the elicitation mode.
"""

from __future__ import annotations

import json
from pathlib import Path

from analysis.provenance.field_class3 import STATED, fields_by_class
from engine.agents.extractor import _build_field_block, _find_codebook_path, _load_codebook

CONDITION_COPY = "COPY"
CONDITION_INDEX = "INDEX"
CONDITIONS = (CONDITION_COPY, CONDITION_INDEX)

# Byte-identical to production Pass 1's system message (extract_pass1_reasoning),
# duplicated as a literal so a production prompt edit cannot silently move this study.
SYSTEM_PASS1 = (
    "You are a systematic review data extractor. Read the paper "
    "carefully and reason through each extraction field step by step. "
    "Think about what the paper says for each field before extracting."
)


def stated_fields(codebook_path: str | Path | None = None) -> list[dict]:
    """The 9 STATED codebook entries, in codebook order. Derived, not listed."""
    cb = _load_codebook(str(Path(codebook_path) if codebook_path else _find_codebook_path()))
    wanted = set(fields_by_class(STATED))
    out = [f for f in cb["fields"] if f["name"] in wanted]
    missing = wanted - {f["name"] for f in out}
    if missing:
        raise RuntimeError(f"STATED fields absent from codebook: {sorted(missing)}")
    return out


def _schema_block(codebook_path=None) -> tuple[str, list[str]]:
    fields = stated_fields(codebook_path)
    blocks = [_build_field_block(f) for f in fields]
    return "\n".join(blocks), [f["name"] for f in fields]


def build_copy_prompt(paper_text: str, codebook_path=None) -> str:
    schema, names = _schema_block(codebook_path)
    return f"""Extract structured data from the following paper for a systematic review.

## Extraction Schema
{schema}

## Instructions
For each of the {len(names)} fields above, emit one entry with:
- **field_name**: exactly as listed above.
- **value**: the extracted data, or "NOT_FOUND" if the paper does not state it.
- **quotes**: a list of one or more VERBATIM quotes copied character-for-character
  from the paper text that support the value. Copy exactly — do not paraphrase,
  summarise, correct, or join distant passages with ellipses. Each quote must be one
  continuous passage that appears in the paper. If value is "NOT_FOUND", emit an
  empty list.

Emit exactly one entry per field ({len(names)} total), as JSON:

{{"fields": [{{"field_name": "...", "value": "...", "quotes": ["...", "..."]}}]}}

## Paper Text
{paper_text}"""


def build_index_prompt(numbered_text: str, n_units: int, codebook_path=None) -> str:
    schema, names = _schema_block(codebook_path)
    return f"""Extract structured data from the following paper for a systematic review.

The paper text below is split into numbered sentence units, each prefixed with a
marker of the form [S1], [S2], ... [S{n_units}]. You will cite evidence by unit
number rather than by copying text.

## Extraction Schema
{schema}

## Instructions
For each of the {len(names)} fields above, emit one entry with:
- **field_name**: exactly as listed above.
- **value**: the extracted data, or "NOT_FOUND" if the paper does not state it.
- **unit_indices**: a list of one or more integers naming the numbered units that
  support the value — for example [12, 13]. Use the integer only, not the "[S12]"
  marker. Every index must be between 1 and {n_units}. Do not quote or copy any
  text; cite the unit numbers and nothing else. If value is "NOT_FOUND", emit an
  empty list.

Emit exactly one entry per field ({len(names)} total), as JSON:

{{"fields": [{{"field_name": "...", "value": "...", "unit_indices": [1, 2]}}]}}

## Numbered Paper Text
{numbered_text}"""


def parse_fields(raw: str) -> tuple[list[dict], str]:
    """Best-effort container parse. Returns (entries, parse_path).

    Deliberately shallow: it recovers the JSON container and nothing more. Per-field
    validity (missing keys, bad index types, out-of-range indices) is measured by the
    analysis, not repaired here.
    """
    text = (raw or "").strip()
    if "```" in text:
        seg = text.split("```")
        for part in seg:
            p = part.strip()
            if p.startswith("json"):
                p = p[4:].strip()
            if p.startswith("{") or p.startswith("["):
                text = p
                break
    try:
        obj = json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            return [], "unparseable"
        try:
            obj = json.loads(text[start:end + 1])
        except Exception:
            return [], "unparseable"
        return _entries(obj), "recovered_braces"
    return _entries(obj), "direct"


def _entries(obj) -> list[dict]:
    if isinstance(obj, dict):
        for key in ("fields", "entries", "extractions", "results", "data"):
            v = obj.get(key)
            if isinstance(v, list):
                return [e for e in v if isinstance(e, dict)]
        if "field_name" in obj:
            return [obj]
        return []
    if isinstance(obj, list):
        return [e for e in obj if isinstance(e, dict)]
    return []
