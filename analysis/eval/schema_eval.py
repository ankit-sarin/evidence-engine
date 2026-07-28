"""SCHEMA-EVAL-01 — A/B diagnostic: schema-constrained vs unconstrained extraction.

Decision input for the Run 7 response contract. Measures the format-constraint
tax on *our* task and *our* models rather than importing it from the literature.

**Framing correction found in pre-flight and carried through this module.** The
task brief describes condition A as "current production path … no grammar
constraint". That is true of the cloud arms but *not* of the local arm:
`extract_pass2_structured` already passes
`format=ExtractionOutput.model_json_schema()` to Ollama
(`engine/agents/extractor.py`). So for the local arm the labels are:

    A = UNCONSTRAINED  (novel condition — what production would be without format=)
    B = CONSTRAINED    (current production)

and the question the A/B answers is "should the existing local constraint be
kept?", not "should one be added". For the cloud arms A is genuinely the
production path (`json_object` / free text) and B is the novel strict schema.

Writes nothing to review.db's extraction tables. All output lands in a separate
eval store under the gitignored review directory.
"""

from __future__ import annotations

import json
import logging
import random
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

EVAL_STORE = "eval/schema_eval"
SEED = 20260728

CONDITION_A = "A_unconstrained"
CONDITION_B = "B_constrained"


# ── strict 20-field schema (cloud condition B) ───────────────────────────


def strict_extraction_schema(field_names: tuple[str, ...]) -> dict:
    """An object with one required property per prompted field.

    This is what "all fields required" has to mean at the schema level: the
    production `{"fields": [...]}` shape cannot express cardinality, because a
    JSON array carries no minItems in `ExtractionOutput.model_json_schema()` —
    a one-element array is schema-valid, which is why constrained decoding as
    currently configured could not have prevented SPANLOSS-01.

    Field order and span shape are preserved exactly; no enums (option-set
    constraint is a separate contract change and is deliberately not mixed in).
    """
    span = {
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
        "properties": {name: dict(span) for name in field_names},
        "required": list(field_names),
        "additionalProperties": False,
    }


def strict_response_to_spans(payload: dict, field_names: tuple[str, ...]) -> list[dict]:
    """Flatten a strict-schema object into the engine's span-list shape."""
    spans = []
    for name in field_names:
        entry = payload.get(name)
        if not isinstance(entry, dict):
            continue
        spans.append({
            "field_name": name,
            "value": str(entry.get("value", "")),
            "source_snippet": entry.get("source_snippet", "") or "",
            "confidence": float(entry.get("confidence", 0.0) or 0.0),
            "tier": int(entry.get("tier", 1) or 1),
        })
    return spans


# ── sample ───────────────────────────────────────────────────────────────

# The 17 openai single-span collapses from SPANLOSS-01. Sampled from, not
# treated as a population — they are here because the brief asks the eval to
# include 277-class offenders, not because they are representative.
COLLAPSE_PAPERS = (277, 386, 457, 460, 466, 477, 489, 498, 514,
                   519, 526, 553, 610, 614, 683, 694, 699)


@dataclass
class SamplePaper:
    paper_id: int
    stratum: str
    chars: int


def select_sample(review_dir: Path, n_local: int = 10, n_cloud: int = 5,
                  seed: int = SEED) -> tuple[list[SamplePaper], list[SamplePaper]]:
    """Stratified fixed-seed sample: collapse offenders, long papers, ordinary."""
    parsed = review_dir / "parsed_text"
    sizes: dict[int, int] = {}
    for f in parsed.glob("*_v*.md"):
        pid = int(f.stem.split("_v")[0])
        sizes[pid] = max(sizes.get(pid, 0), len(f.read_text()))
    if not sizes:
        raise RuntimeError(f"No parsed text under {parsed}")

    rng = random.Random(seed)
    by_size = sorted(sizes.items(), key=lambda kv: kv[1])
    long_cut = by_size[int(len(by_size) * 0.9)][1]

    collapse = sorted(p for p in COLLAPSE_PAPERS if p in sizes)
    longs = sorted(p for p, c in sizes.items() if c >= long_cut and p not in collapse)
    ordinary = sorted(p for p in sizes if p not in collapse and p not in longs)

    picked: list[SamplePaper] = []
    picked += [SamplePaper(p, "collapse", sizes[p]) for p in rng.sample(collapse, min(4, len(collapse)))]
    picked += [SamplePaper(p, "long", sizes[p]) for p in rng.sample(longs, min(2, len(longs)))]
    remaining = n_local - len(picked)
    picked += [SamplePaper(p, "ordinary", sizes[p]) for p in rng.sample(ordinary, remaining)]
    picked.sort(key=lambda s: s.paper_id)

    cloud = sorted(rng.sample(picked, min(n_cloud, len(picked))), key=lambda s: s.paper_id)
    return picked, cloud


# ── result records ───────────────────────────────────────────────────────


@dataclass
class CallResult:
    arm: str
    condition: str
    paper_id: int
    stratum: str
    ok: bool
    n_spans: int
    fields_expected: int
    missing: list[str] = field(default_factory=list)
    complete: bool = False
    latency_s: float = 0.0
    input_tokens: int | None = None
    output_tokens: int | None = None
    reasoning_tokens: int | None = None
    finish_reason: str | None = None
    think_chars: int | None = None
    parse_path: str | None = None
    error: str | None = None
    spans: list[dict] = field(default_factory=list)
    raw_content: str | None = None

    def to_json(self) -> dict:
        return asdict(self)


def store_path(review_dir: Path, name: str) -> Path:
    p = Path(review_dir) / EVAL_STORE
    p.mkdir(parents=True, exist_ok=True)
    return p / name


def write_results(review_dir: Path, results: list[CallResult], label: str) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = store_path(review_dir, f"{label}_{ts}.jsonl")
    with out.open("w") as fh:
        for r in results:
            fh.write(json.dumps(r.to_json(), default=str) + "\n")
    logger.info("Wrote %d results -> %s", len(results), out)
    return out


def read_results(review_dir: Path, pattern: str = "*.jsonl") -> list[dict]:
    out = []
    for p in sorted((Path(review_dir) / EVAL_STORE).glob(pattern)):
        for line in p.read_text().splitlines():
            if line.strip():
                out.append(json.loads(line))
    return out
