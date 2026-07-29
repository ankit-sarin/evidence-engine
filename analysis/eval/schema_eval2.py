"""SCHEMA-EVAL-02 — local response-contract decision at n=40, fixed pipeline.

Three conditions, byte-identical prompts, `think` per Review Spec policy:

    A  no format constraint           — prompt-instructed JSON only
    B  current production array schema — format=ExtractionOutput.model_json_schema()
    C  20-required-slot schema         — one object, 20 named properties in prompt
                                        field order, all required, additionalProperties
                                        false; per-field object keeps the current key
                                        shape and order (value, source_snippet,
                                        confidence, tier) with field_name promoted to
                                        the property key

Field order is held identical across conditions because schema field order acts as
prompt order — varying it would confound the contract question with a prompt-order
question.

Runs the post-REGRESSION-01 pipeline, so Pass 1 consumes the native thinking
channel and `parse_branch` is recorded per call. Writes to the eval store only;
never to review.db's extraction tables.

**Sample arithmetic.** The brief specifies "the 10 SCHEMA-EVAL-01 papers + the 3
REGRESSION-01 smoke papers + 27 new". The 3 smoke papers (39, 466, 629) are a
strict subset of the EVAL-01 ten, so that expression yields 37 distinct papers,
not 40. This module therefore carries the 10 forward and draws **30** new, giving
40 as intended. The correction is stated in the report.
"""

from __future__ import annotations

import json
import logging
import random
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

SEED = 20260729
N_TOTAL = 40

COND_A = "A_unconstrained"
COND_B = "B_array_schema"
COND_C = "C_required_slots"
CONDITIONS = (COND_A, COND_B, COND_C)

# Carried forward from SCHEMA-EVAL-01 (which itself contains the three
# REGRESSION-01 smoke papers 39, 466, 629).
CARRIED = (39, 386, 466, 498, 547, 629, 691, 694, 708, 799)


@dataclass(frozen=True)
class Paper:
    paper_id: int
    chars: int
    length_stratum: str
    study_type: str
    source: str  # "carried" | "new"


def _length_stratum(chars: int, cuts: tuple[int, int]) -> str:
    if chars <= cuts[0]:
        return "short"
    if chars <= cuts[1]:
        return "medium"
    return "long"


def select_sample(review_dir: Path, n_total: int = N_TOTAL, seed: int = SEED) -> list[Paper]:
    """40 papers: the 10 carried forward, plus 30 new stratified by length x study type.

    Stratification uses the Run 6 stored `study_type` where one exists (it is the
    only study-type label available for the corpus) and "unknown" otherwise.
    Selection is round-robin over the (length, study_type) cells in a seeded
    shuffle, so no cell can dominate and the draw is reproducible.
    """
    parsed = review_dir / "parsed_text"
    sizes: dict[int, int] = {}
    for f in parsed.glob("*_v*.md"):
        pid = int(f.stem.split("_v")[0])
        sizes[pid] = max(sizes.get(pid, 0), len(f.read_text()))
    if not sizes:
        raise RuntimeError(f"no parsed text under {parsed}")

    conn = sqlite3.connect(f"file:{review_dir / 'review.db'}?mode=ro", uri=True)
    try:
        study_types = {
            r[0]: (r[1] or "unknown")
            for r in conn.execute(
                "SELECT e.paper_id, s.value FROM evidence_spans s "
                "JOIN extractions e ON e.id = s.extraction_id "
                "WHERE s.field_name = 'study_type'"
            )
        }
        eligible = {
            r[0] for r in conn.execute(
                "SELECT id FROM papers WHERE status IN "
                "('FT_ELIGIBLE','EXTRACTED','AI_AUDIT_COMPLETE','HUMAN_AUDIT_COMPLETE')"
            )
        }
    finally:
        conn.close()

    ordered = sorted(sizes.items(), key=lambda kv: kv[1])
    cuts = (ordered[len(ordered) // 3][1], ordered[2 * len(ordered) // 3][1])

    def make(pid: int, source: str) -> Paper:
        return Paper(pid, sizes[pid], _length_stratum(sizes[pid], cuts),
                     study_types.get(pid, "unknown"), source)

    picked = [make(p, "carried") for p in CARRIED if p in sizes]

    pool = sorted(p for p in sizes if p not in set(CARRIED) and p in eligible)
    cells: dict[tuple[str, str], list[int]] = {}
    for pid in pool:
        cells.setdefault((_length_stratum(sizes[pid], cuts),
                          study_types.get(pid, "unknown")), []).append(pid)

    rng = random.Random(seed)
    for v in cells.values():
        rng.shuffle(v)
    keys = sorted(cells)
    rng.shuffle(keys)

    need = n_total - len(picked)
    while need > 0 and any(cells[k] for k in keys):
        for k in keys:
            if need == 0:
                break
            if cells[k]:
                picked.append(make(cells[k].pop(), "new"))
                need -= 1
    picked.sort(key=lambda p: p.paper_id)
    return picked


# ── condition C schema ───────────────────────────────────────────────────


def required_slot_schema(field_names: tuple[str, ...]) -> dict:
    """One object, one required property per prompted field, in prompt order.

    Same construction SCHEMA-EVAL-01 used for the cloud strict condition. The
    per-field object keeps the production key shape and order; `field_name` is
    promoted to the property key, which is the only structural change and is what
    makes cardinality expressible at all — a JSON array carries no minItems, so
    the production array schema cannot require 20 entries.
    """
    slot = {
        "type": "object",
        "properties": {
            "value": {"type": "string"},
            "source_snippet": {"type": "string"},
            "confidence": {"type": "number"},
            "tier": {"type": "integer"},
        },
        "required": ["value", "source_snippet", "confidence", "tier"],
        "additionalProperties": False,
    }
    return {
        "type": "object",
        "properties": {name: dict(slot) for name in field_names},
        "required": list(field_names),
        "additionalProperties": False,
    }


def slots_to_spans(payload: dict, field_names: tuple[str, ...]) -> list[dict]:
    """Flatten a condition-C object into the engine's span-list shape."""
    spans = []
    for name in field_names:
        entry = payload.get(name)
        if not isinstance(entry, dict):
            continue
        spans.append({
            "field_name": name,
            "value": str(entry.get("value", "") or ""),
            "source_snippet": entry.get("source_snippet", "") or "",
            "confidence": float(entry.get("confidence", 0.0) or 0.0),
            "tier": int(entry.get("tier", 1) or 1),
        })
    return spans


# ── result record ────────────────────────────────────────────────────────


@dataclass
class Result:
    condition: str
    paper_id: int
    length_stratum: str
    study_type: str
    ok: bool
    n_spans: int
    fields_expected: int
    complete: bool
    missing: list
    parse_path: str | None
    parse_branch: str | None
    thinking_chars: int | None
    pass1_latency_s: float
    pass2_latency_s: float
    total_latency_s: float
    prompt_eval_count: int | None
    eval_count: int | None
    done_reason: str | None
    retries: int
    spans: list
    raw_content: str | None
    error: str | None = None

    def to_json(self) -> dict:
        return asdict(self)


def store_dir(review_dir: Path) -> Path:
    p = Path(review_dir) / "eval" / "schema_eval2"
    p.mkdir(parents=True, exist_ok=True)
    return p


def append_result(review_dir: Path, label: str, result: Result) -> Path:
    """Append one result immediately — a 15-hour batch must not lose work to a crash."""
    out = store_dir(review_dir) / f"{label}.jsonl"
    with out.open("a") as fh:
        fh.write(json.dumps(result.to_json(), default=str) + "\n")
    return out


def read_results(review_dir: Path, pattern: str = "*.jsonl") -> list[dict]:
    rows = []
    for p in sorted(store_dir(review_dir).glob(pattern)):
        for line in p.read_text().splitlines():
            if line.strip():
                rows.append(json.loads(line))
    return rows
