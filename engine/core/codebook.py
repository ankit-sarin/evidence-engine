"""The extraction codebook: located, validated, identified and hashed.

CODEBOOK-AUTH-01. The codebook is the field authority — it is what the prompt
is actually built from, for every arm. Before this module it was located five
different ways (a review-dir join, a `data/` glob taking the first hit, an
`args.codebook or data_dir/…`, a required flag, a module constant), parsed by
eleven bare `yaml.safe_load` calls, validated by none of them, and hashed into
no extraction's provenance. A codebook could be edited and every stored
extraction's hash stayed byte-identical, because `extraction_schema_hash` is
computed from the *spec*.

One loader now. The path derives from the review id; the document is validated
eagerly; its `review` key is checked against the review it was asked for; and
two hashes travel with it — a semantic hash over the content that reaches a
prompt, and a byte hash of the file.

**The glob is gone deliberately.** `_find_codebook_path` returned the first
`data/*/extraction_codebook.yaml` in glob order with no identity check, so on a
two-review box it could hand one review's codebook to another review's
extraction and nothing raised.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

CODEBOOK_FILENAME = "extraction_codebook.yaml"

#: The field-type vocabulary. Relocated here from
#: `analysis/paper1/judge_loader._VALID_FIELD_TYPES` so that engine code does
#: not import from analysis/ to learn what a field type is. The judge loader
#: and `ReviewSpec.ExtractionField` both validate against this same tuple, so
#: the two files cannot drift into separate vocabularies again.
VALID_FIELD_TYPES = ("categorical", "numeric", "free_text")

#: Top-level keys. Every one is required; nothing else is allowed.
REQUIRED_TOP_LEVEL = (
    "version", "review", "date",
    "absence_sentinels", "escape_token", "contract_unmet_token",
    "fields",
)

#: Per-field keys that must be present on every field.
REQUIRED_FIELD_KEYS = (
    "name", "type", "tier", "definition", "instruction",
    "field_class", "judge_rubric_family",
)

#: Per-field keys that may be present. `source_quote_required` is here and not
#: in the required set on purpose: it is the evidence-modality flag the lint
#: reads, and a field that does not set it simply makes no such demand.
OPTIONAL_FIELD_KEYS = (
    "valid_values", "decision_criteria", "examples",
    "ordered_values", "dimension", "source_quote_required",
    # Numeric agreement tolerance, read by the judge loader
    # (`field.get("tolerance", 0.0)`). Absent from the live codebook, which is
    # why it defaults there — but it is a real key of this document type and
    # rejecting it would refuse a codebook the judge lane can already read.
    "tolerance",
)

ALLOWED_FIELD_KEYS = frozenset(REQUIRED_FIELD_KEYS) | frozenset(OPTIONAL_FIELD_KEYS)

#: The projection that is semantically hashed: everything that reaches a prompt.
#: Measured in CODEBOOK-AUTH-01 Phase 2 pre-flight P2 — `fields[*]` (whose keys
#: feed `extractor._build_field_block`, shared by the extraction prompt and the
#: elicitation Pass-1 prompt, plus `field_class` which groups Pass 1's blocks)
#: and the three token/sentinel keys, two of which are rendered into Pass-1 text.
#: `version`, `date` and `review` reach no prompt and no code, so an edit to any
#: of them must NOT move the semantic hash — a provenance value that changes
#: when nothing the model saw changed is a false alarm, and false alarms are how
#: staleness detection gets ignored.
SEMANTIC_KEYS = ("fields", "absence_sentinels", "escape_token", "contract_unmet_token")


class CodebookError(ValueError):
    """Raised when a codebook cannot be loaded, parsed or validated."""


class CodebookIdentityError(CodebookError):
    """Raised when a codebook's `review` is not the review that was asked for."""


@dataclass(frozen=True)
class FieldView:
    """The three attributes consumers used to read off `ExtractionField`.

    A view, not a second authority: it is built from the codebook entry on
    demand and holds no state of its own. It exists so a consumer that wants
    `f.name` / `f.type` / `f.tier` does not have to index a dict, and so the
    annotations that named `ExtractionField` have something to name.
    """

    name: str
    type: str
    tier: int
    enum_values: list[str] | None = None


@dataclass(frozen=True)
class Codebook:
    """A validated codebook, with the two hashes that identify its content."""

    review: str
    version: str
    date: str
    fields: tuple[dict[str, Any], ...]
    escape_token: str
    absence_sentinels: tuple[str, ...]
    contract_unmet_token: str
    path: Path
    semantic_hash: str
    sha256: str
    #: Evidence-modality lint findings, run at load. Advisory by design — they
    #: describe a codebook that will elicit worse answers, not one the engine
    #: cannot run against — but they are attached here so a run can record them
    #: instead of a human having to remember to look.
    lint_findings: tuple[str, ...] = ()

    @property
    def raw(self) -> dict[str, Any]:
        """The document as the legacy readers expect it: a plain dict."""
        return {
            "version": self.version,
            "review": self.review,
            "date": self.date,
            "absence_sentinels": list(self.absence_sentinels),
            "escape_token": self.escape_token,
            "contract_unmet_token": self.contract_unmet_token,
            "fields": [dict(f) for f in self.fields],
        }

    def field(self, name: str) -> dict[str, Any]:
        for f in self.fields:
            if f["name"] == name:
                return f
        raise CodebookError(f"{name!r} is not a field in {self.path}")

    @property
    def field_names(self) -> tuple[str, ...]:
        return tuple(f["name"] for f in self.fields)

    def fields_by_tier(self, tier: int) -> tuple[dict[str, Any], ...]:
        """The tier's fields, in CODEBOOK order.

        Replaces `ExtractionSchema.fields_by_tier`. Order is the codebook's
        file order, which was already the spec's order — the parity test held
        them equal before the spec's copy was removed — so the prompt's field
        sequence does not move.
        """
        return tuple(f for f in self.fields if f["tier"] == tier)

    def enum_values(self, name: str) -> list[str] | None:
        """Allowed values for a categorical field, else None.

        Replaces `ExtractionField.enum_values`. None rather than [] for a
        non-categorical field, because the two mean different things to a
        caller: "this field has no value list" is not "this field's value
        list is empty".
        """
        vv = self.field(name).get("valid_values")
        if not vv:
            return None
        return [v["value"] for v in vv]

    def view(self, name: str) -> "FieldView":
        """A small typed view for consumers that annotated `ExtractionField`."""
        f = self.field(name)
        return FieldView(
            name=f["name"], type=f["type"], tier=f["tier"],
            enum_values=self.enum_values(name),
        )

    @property
    def views(self) -> tuple["FieldView", ...]:
        return tuple(self.view(n) for n in self.field_names)


# ── Hashing ──────────────────────────────────────────────────────────


def compute_codebook_sha256(path: str | Path) -> str:
    """SHA-256 hex digest of the codebook file bytes (no normalization).

    Relocated from `analysis/paper1/judge_loader`, which keeps importing it from
    here so `judge_runs.codebook_sha256` and the new extraction-provenance
    column are produced by one function rather than two that agree today.
    """
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def compute_semantic_hash(doc: dict[str, Any]) -> str:
    """SHA-256 over the canonical JSON of the prompt-reaching projection.

    Field order is preserved (it is file order, and it is the order the prompt
    renders within a class), but keys inside each field are sorted, so a
    reordering of a field's keys — which no prompt can see — does not move the
    hash while an edit to a definition does.
    """
    projection = {k: doc[k] for k in SEMANTIC_KEYS if k in doc}
    blob = json.dumps(projection, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()


# ── Validation ───────────────────────────────────────────────────────


def _require_mapping(doc: Any, path: Path) -> dict:
    if not isinstance(doc, dict):
        raise CodebookError(
            f"Codebook at {path} must be a YAML mapping, got {type(doc).__name__}."
        )
    return doc


def _validate_top_level(doc: dict, path: Path) -> None:
    missing = [k for k in REQUIRED_TOP_LEVEL if k not in doc]
    if missing:
        raise CodebookError(
            f"Codebook at {path} is missing required top-level key(s): "
            f"{', '.join(sorted(missing))}."
        )
    unknown = sorted(set(doc) - set(REQUIRED_TOP_LEVEL))
    if unknown:
        raise CodebookError(
            f"Codebook at {path} declares unknown top-level key(s): "
            f"{', '.join(unknown)}. Remove them or correct the spelling."
        )
    if not isinstance(doc["fields"], list) or not doc["fields"]:
        raise CodebookError(f"Codebook at {path}: `fields` must be a non-empty list.")
    if not isinstance(doc["absence_sentinels"], list) or not doc["absence_sentinels"]:
        raise CodebookError(
            f"Codebook at {path}: `absence_sentinels` must be a non-empty list. "
            f"An empty one silently turns every absence claim into an ordinary "
            f"value for the auditor, the validators and concordance."
        )
    for key in ("escape_token", "contract_unmet_token", "review", "version"):
        if not str(doc[key] or "").strip():
            raise CodebookError(f"Codebook at {path}: `{key}` must be a non-empty string.")


def _validate_fields(doc: dict, path: Path) -> None:
    # Deferred: engine.elicitation.classes imports this module, so importing it
    # at module scope would close a cycle. The class vocabulary has exactly one
    # authority and this is not it.
    from engine.elicitation.classes import CLASSES

    seen: set[str] = set()
    for i, f in enumerate(doc["fields"]):
        where = f"field #{i + 1}"
        if not isinstance(f, dict):
            raise CodebookError(f"Codebook at {path}: {where} must be a mapping, got {f!r}.")
        name = f.get("name")
        if not name or not isinstance(name, str):
            raise CodebookError(f"Codebook at {path}: {where} has no usable `name`.")
        where = f"field {name!r}"
        if name in seen:
            raise CodebookError(f"Codebook at {path}: duplicate field name {name!r}.")
        seen.add(name)

        missing = [k for k in REQUIRED_FIELD_KEYS if k not in f]
        if missing:
            raise CodebookError(
                f"Codebook at {path}: {where} is missing required key(s): "
                f"{', '.join(sorted(missing))}."
            )
        unknown = sorted(set(f) - ALLOWED_FIELD_KEYS)
        if unknown:
            raise CodebookError(
                f"Codebook at {path}: {where} declares unknown key(s): "
                f"{', '.join(unknown)}. Remove them or correct the spelling."
            )
        if f["type"] not in VALID_FIELD_TYPES:
            raise CodebookError(
                f"Codebook at {path}: {where} has type {f['type']!r}; "
                f"expected one of {', '.join(VALID_FIELD_TYPES)}."
            )
        cls = str(f["field_class"]).strip().lower()
        if cls not in CLASSES:
            raise CodebookError(
                f"Codebook at {path}: {where} has field_class {f['field_class']!r}; "
                f"expected one of {', '.join(CLASSES)}."
            )
        if not isinstance(f["tier"], int) or not 1 <= f["tier"] <= 4:
            raise CodebookError(
                f"Codebook at {path}: {where} has tier {f['tier']!r}; expected 1-4."
            )
        for key in ("definition", "instruction", "judge_rubric_family"):
            if not str(f[key] or "").strip():
                raise CodebookError(
                    f"Codebook at {path}: {where} has an empty `{key}`."
                )
        _validate_valid_values(f, path, where)


def _validate_valid_values(f: dict, path: Path, where: str) -> None:
    vv = f.get("valid_values")
    if vv is None:
        if f["type"] == "categorical":
            raise CodebookError(
                f"Codebook at {path}: {where} is categorical but declares no "
                f"`valid_values`; the prompt would list no allowed values."
            )
        return
    if not isinstance(vv, list) or not vv:
        raise CodebookError(
            f"Codebook at {path}: {where} has a `valid_values` that is not a "
            f"non-empty list."
        )
    seen: set[str] = set()
    for item in vv:
        if not isinstance(item, dict) or "value" not in item:
            raise CodebookError(
                f"Codebook at {path}: {where} has a `valid_values` entry that is "
                f"not a mapping with a `value`: {item!r}. A bare string is "
                f"refused deliberately: `extractor._build_field_block` renders "
                f"`v[\"value\"]` and `v[\"definition\"]` for every entry, so a "
                f"codebook written that way does not degrade — it raises while "
                f"building the prompt."
            )
        unknown = sorted(set(item) - {"value", "definition"})
        if unknown:
            raise CodebookError(
                f"Codebook at {path}: {where} valid_values entry {item['value']!r} "
                f"declares unknown key(s): {', '.join(unknown)}."
            )
        if item["value"] in seen:
            raise CodebookError(
                f"Codebook at {path}: {where} lists {item['value']!r} twice in "
                f"`valid_values`."
            )
        seen.add(item["value"])


# ── Resolution and load ──────────────────────────────────────────────


def codebook_path_for(review_id: str) -> Path:
    """The codebook for a review: `data/<review_id>/extraction_codebook.yaml`."""
    # Deferred: review_paths imports review_spec, and review_spec imports
    # VALID_FIELD_TYPES from here. Importing it at module scope would close
    # that loop; nothing else in this module needs engine imports at all.
    from engine.core.review_paths import data_root_for

    return data_root_for(review_id) / CODEBOOK_FILENAME


_CACHE: dict[str, Codebook] = {}


def load_codebook(path: str | Path) -> Codebook:
    """Load and validate the codebook at `path`. Cached by resolved path.

    No identity check: the caller named a file and no review to check it
    against. Prefer `load_codebook_for` wherever a review id is in scope —
    which is everywhere in the pipeline.
    """
    path = Path(path)
    key = str(path.resolve()) if path.exists() else str(path)
    cached = _CACHE.get(key)
    if cached is None:
        cached = _parse(path)
        _CACHE[key] = cached
    return cached


def load_codebook_for(
    review_id: str, override: str | Path | None = None
) -> Codebook:
    """Load, validate and identity-check the codebook for `review_id`.

    Raises `CodebookIdentityError` if the document names a different review,
    override or not — the same rule the Review Spec resolver applies, for the
    same reason: a codebook belonging to another review does not fail, it
    silently prompts for the wrong study.
    """
    path = Path(override) if override else codebook_path_for(review_id)
    cb = load_codebook(path)
    if cb.review != review_id:
        raise CodebookIdentityError(
            f"Codebook at {path} declares review {cb.review!r}, but the run "
            f"asked for {review_id!r}. Refusing before it is used — building a "
            f"prompt from another review's codebook does not fail, it extracts "
            f"the wrong study's fields with the wrong definitions."
        )
    return cb


def _parse(path: Path) -> Codebook:
    try:
        text = path.read_text()
    except FileNotFoundError:
        raise CodebookError(
            f"Codebook not found at {path}. Every review carries one at "
            f"data/<review_id>/{CODEBOOK_FILENAME}."
        ) from None
    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise CodebookError(f"Codebook at {path} contains invalid YAML: {exc}") from exc

    doc = _require_mapping(doc, path)
    _validate_top_level(doc, path)
    _validate_fields(doc, path)

    findings = _lint(doc)
    for finding in findings:
        logger.warning("Codebook lint (%s): %s", path, finding)

    return Codebook(
        review=str(doc["review"]).strip(),
        version=str(doc["version"]).strip(),
        date=str(doc["date"]),
        fields=tuple(dict(f) for f in doc["fields"]),
        escape_token=str(doc["escape_token"]).strip(),
        absence_sentinels=tuple(str(s) for s in doc["absence_sentinels"]),
        contract_unmet_token=str(doc["contract_unmet_token"]).strip(),
        path=path,
        semantic_hash=compute_semantic_hash(doc),
        sha256=hashlib.sha256(text.encode()).hexdigest(),
        lint_findings=tuple(findings),
    )


def _lint(doc: dict) -> list[str]:
    """Run the evidence-modality lint. Advisory: it logs, it never raises."""
    from engine.elicitation.classes import check_evidence_modality

    return list(check_evidence_modality(doc))


def clear_cache() -> None:
    """Drop the parse cache. For tests that rewrite a codebook in place."""
    _CACHE.clear()
