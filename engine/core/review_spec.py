"""Review Spec: YAML parser, Pydantic models, and protocol hashing."""

import hashlib
import json
import re
from datetime import date
from pathlib import Path
from typing import Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator


# ── Strictness ───────────────────────────────────────────────────────


class _SpecModel(BaseModel):
    """Base for every Review Spec model.

    `extra='forbid'` at every level, deliberately. Under the previous
    `extra='ignore'` a misspelled key was dropped with no error and no
    warning, so a review could declare configuration that never took
    effect and read as if it had. A spec is the review's configuration
    authority; a key it does not recognise is a defect in the spec, not
    a comment.
    """

    model_config = ConfigDict(extra="forbid")


# ── PICO ─────────────────────────────────────────────────────────────


class PICO(_SpecModel):
    """Population, Intervention, Comparator, Outcomes."""

    population: str
    intervention: str
    comparator: str
    outcomes: list[str]


# ── Search Strategy ──────────────────────────────────────────────────


class SearchStrategy(_SpecModel):
    """Databases and query parameters for literature search."""

    databases: list[str]
    query_terms: list[str]
    date_range: list[int] = Field(
        min_length=2, max_length=2, description="[start_year, end_year]"
    )

    @field_validator("date_range")
    @classmethod
    def valid_date_range(cls, v: list[int]) -> list[int]:
        if v[0] > v[1]:
            raise ValueError(
                f"Start year ({v[0]}) must be <= end year ({v[1]})"
            )
        return v


# ── Screening Criteria ───────────────────────────────────────────────


class ScreeningModels(_SpecModel):
    """Model configuration for dual-model screening."""

    primary: str = Field(default="qwen3:8b", description="Fast high-recall primary screener")
    verification: str = Field(default="qwen3:32b", description="Larger model for verification of includes")


class FTScreeningModels(_SpecModel):
    """Model configuration for full-text screening."""

    primary: str = Field(default="qwen3.5:27b", description="Full-text primary screener")
    verifier: str = Field(default="gemma3:27b", description="Full-text verification model")
    think: bool = Field(default=False, description="Enable thinking mode (slow, not recommended)")
    temperature: float = Field(default=0.0, ge=0.0, le=1.0)


class ExtractionModels(_SpecModel):
    """Model configuration for the two-pass local extractor.

    `think` is declared per pass and always passed explicitly to Ollama.
    REGRESSION-01: relying on the Ollama default is unsafe — 0.21.0 auto-enables
    thinking for deepseek-r1 and moves it from inline `<think>` tags in content
    to a separate `message.thinking` field, which silently changed what Pass 1
    returned as its "reasoning trace".

    Pass 1 exists to produce a reasoning trace, so thinking must be ON.
    Pass 2 emits schema-constrained JSON, so thinking must be OFF.
    """

    extractor: str = Field(default="deepseek-r1:32b", description="Two-pass extraction model")
    pass1_think: bool = Field(default=True, description="Pass 1 is the reasoning pass — thinking ON")
    pass2_think: bool = Field(default=False, description="Pass 2 emits structured JSON — thinking OFF")
    temperature: float = Field(default=0.0, ge=0.0, le=1.0)
    elicitation: bool = Field(
        default=False,
        description=(
            "Pass 1 elicits sentence-unit citations under per-class contracts and "
            "Pass 2 is primed with the materialized evidence (ELICIT-DESIGN-01). "
            "Defaults OFF: the design is smoke-gated and Run 7 flips it "
            "deliberately, so no existing review changes behaviour by upgrading."
        ),
    )


#: Every surface at which eligibility content is rendered. The four model
#: stages are prompts; the two adjudication stages are the human reference
#: sheets, which render the same criteria under different markers. A stage is
#: named here rather than inferred so a criterion can declare where it applies.
Stage = Literal[
    "abstract_primary",
    "abstract_verifier",
    "ft_primary",
    "ft_verifier",
    "abstract_adjudication",
    "ft_adjudication",
]


#: Reason codes that apply to ANY systematic review. They belong to the engine,
#: not to a spec, and each has exactly one description here. A spec may point a
#: criterion at one of them — the evidence-sufficiency criterion points at
#: `insufficient_data` — but the engine's description is the authority for it.
STRUCTURAL_REASON_CODES: dict[str, str] = {
    "eligible": "Meets all inclusion criteria and no exclusion criterion applies.",
    "protocol_only": "Study protocol without results.",
    "duplicate_cohort": "Reports the same cohort or dataset as another included paper.",
    "insufficient_data": (
        "The full text does not contain enough information to determine "
        "eligibility against the criteria."
    ),
}

#: The code a paper that passes carries. It names no rule, so nothing may declare it.
ELIGIBLE_CODE = "eligible"

#: The evidence-sufficiency code. The criterion that declares it is the rule a
#: prompt states when there is nothing to judge.
EVIDENCE_CODE = "insufficient_data"

#: Stages at which a model decides. The two adjudication stages are human
#: surfaces; their policy is derived from the model stage they resolve.
MODEL_STAGES: tuple[str, ...] = (
    "abstract_primary", "abstract_verifier", "ft_primary", "ft_verifier",
)
VERIFIER_STAGES: tuple[str, ...] = ("abstract_verifier", "ft_verifier")

#: The one placeholder a verifier test may carry. The renderer fills it with what
#: the stage is reading, so one wording serves the abstract and full-text passes.
EVIDENCE_PLACEHOLDER = "evidence"

_REASON_CODE_PATTERN = r"^[a-z][a-z0-9_]*$"
_PLACEHOLDER_RE = re.compile(r"\{([^{}]*)\}")


class Criterion(_SpecModel):
    """One eligibility rule, and the single authority for its wording.

    `text` renders verbatim at every stage in `stages`; there is no per-stage
    wording. `examples` render after it as "e.g., …" wherever it renders — they
    illustrate the rule and never widen it. An exclusion declares the
    `reason_code` a full-text decision records when this rule is the reason.
    """

    id: str = Field(pattern=r"^[a-z][a-z0-9-]*$", max_length=64)
    kind: Literal["inclusion", "exclusion"]
    text: str = Field(min_length=1)
    stages: list[Stage] = Field(min_length=1)
    reason_code: Optional[str] = Field(
        default=None, pattern=_REASON_CODE_PATTERN, max_length=64
    )
    examples: Optional[list[str]] = None

    @model_validator(mode="after")
    def _check(self) -> "Criterion":
        if self.kind == "inclusion" and self.reason_code is not None:
            raise ValueError(
                f"criterion {self.id!r}: reason_code is for exclusions only. "
                "An inclusion criterion is not a reason to exclude a paper, and "
                "a code on one would have no emitter."
            )
        if self.kind == "exclusion" and self.reason_code is None:
            raise ValueError(
                f"criterion {self.id!r}: an exclusion criterion must declare a "
                "reason_code. Without one, a paper excluded under this rule cannot "
                "record why, and the stored decision names no rule."
            )
        if self.reason_code == ELIGIBLE_CODE:
            raise ValueError(
                f"criterion {self.id!r}: {ELIGIBLE_CODE!r} is the code a paper that "
                "passes carries; it cannot be the reason a rule excludes one."
            )
        if self.examples is not None and (
            not self.examples or any(not e.strip() for e in self.examples)
        ):
            raise ValueError(
                f"criterion {self.id!r}: examples, when present, must be a "
                "non-empty list of non-empty strings. An empty example renders as "
                "'e.g., ' with nothing after it."
            )
        return self


class VerifierTest(_SpecModel):
    """A numbered test a verification pass applies, and what it derives from.

    `derives_from` is the audit trail from a test back to the rules it enforces.
    It may name criterion ids or the specialty scope's id; it may not be empty,
    because a test that enforces nothing declared is topic content smuggled in
    through the back door.

    `text` may carry one placeholder, `{evidence}`, which the renderer fills with
    what the stage is reading — "abstract" or "full text" — so a test applied at
    both verifier stages has one wording, not two.
    """

    id: str = Field(pattern=r"^[a-z][a-z0-9-]*$", max_length=64)
    text: str = Field(min_length=1)
    stages: list[Stage] = Field(min_length=1)
    derives_from: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def _check(self) -> "VerifierTest":
        unknown = sorted(
            {p for p in _PLACEHOLDER_RE.findall(self.text) if p != EVIDENCE_PLACEHOLDER}
        )
        if unknown:
            raise ValueError(
                f"verifier test {self.id!r}: unknown placeholder(s) {unknown}. Only "
                "{evidence} is filled by the renderer; anything else would reach "
                "the model as literal braces."
            )
        return self


class StagePolicy(_SpecModel):
    """How a model stage resolves the two cases its criteria cannot settle.

    `when_uncertain`: the evidence is partial. `when_evidence_absent`: there is
    nothing to judge. The prose a stage receives is derived from these two values
    by the renderer. There is deliberately no free-text instruction field: free
    text is how the recall-first rule and the evidence-sufficiency criterion came
    to contradict each other inside a single prompt.
    """

    when_uncertain: Optional[Literal["include", "exclude"]] = None
    when_evidence_absent: Optional[Literal["include", "exclude"]] = None


# ── Specialty Scope ──────────────────────────────────────────────────


class SpecialtyScope(_SpecModel):
    """Surgical specialty inclusion/exclusion scope for screening.

    It carries an `id` so a VerifierTest can cite it in `derives_from` the way
    it cites a criterion, and a `reason_code` because it is the authority behind
    the single largest exclusion category.
    """

    id: str = Field(pattern=r"^[a-z][a-z0-9-]*$", max_length=64)
    reason_code: str = Field(pattern=_REASON_CODE_PATTERN, max_length=64)
    included: list[str] = Field(min_length=1)
    excluded: list[str] = Field(min_length=1)
    notes: Optional[str] = None

    def format_for_prompt(self) -> str:
        """Format specialty scope as a string for inclusion in screening prompts."""
        lines = ["SPECIALTY SCOPE:"]
        lines.append("  Included specialties:")
        for s in self.included:
            lines.append(f"    - {s}")
        lines.append("  Excluded specialties:")
        for s in self.excluded:
            lines.append(f"    - {s}")
        if self.notes:
            lines.append(f"  Notes: {self.notes.strip()}")
        return "\n".join(lines)


# ── Eligibility ──────────────────────────────────────────────────────


class Eligibility(_SpecModel):
    """The review's eligibility authority: what makes a paper in or out.

    Every surface that screens a paper — four model requests and two human
    adjudication sheets — renders from here through `eligibility_render`, and
    none of them carries wording of its own. Before SCREEN-AUTH-01 the same rules
    lived as prose at seven literal sites that had drifted apart. Phase 2b moved
    them here behind per-stage overrides so the move could be proved
    byte-identical; Phase 2c removed the overrides. A criterion now says one
    thing everywhere it renders.
    """

    criteria: list[Criterion] = Field(min_length=1)
    specialty_scope: SpecialtyScope
    verifier_tests: list[VerifierTest] = Field(default_factory=list)
    stage_policies: dict[Stage, StagePolicy] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check(self) -> "Eligibility":
        ids = [c.id for c in self.criteria]
        dupes = sorted({i for i in ids if ids.count(i) > 1})
        if dupes:
            raise ValueError(
                f"duplicate criterion id(s) {dupes}. An id is how a verifier "
                "test and a stored decision name a rule; two rules answering to "
                "one id make that reference ambiguous."
            )
        known = set(ids) | {self.specialty_scope.id}
        for t in self.verifier_tests:
            unknown = sorted(set(t.derives_from) - known)
            if unknown:
                raise ValueError(
                    f"verifier test {t.id!r}: derives_from names unknown id(s) "
                    f"{unknown}. A test may cite a criterion id or the specialty "
                    "scope id; anything else is a reference to a rule that does "
                    "not exist."
                )
        owners: dict[str, str] = {}
        declared = [(c.id, c.reason_code) for c in self.criteria if c.reason_code]
        declared.append((self.specialty_scope.id, self.specialty_scope.reason_code))
        for owner, code in declared:
            if code in owners:
                raise ValueError(
                    f"reason_code {code!r} is declared by both {owners[code]!r} and "
                    f"{owner!r}. A decision records one code; if two rules share it, "
                    "the record cannot say which rule excluded the paper."
                )
            owners[code] = owner
        if self.specialty_scope.reason_code == ELIGIBLE_CODE:
            raise ValueError(
                f"specialty_scope: {ELIGIBLE_CODE!r} is the code a paper that passes "
                "carries; it cannot be the reason the scope excludes one."
            )
        declared_derived = sorted(set(self.stage_policies) - set(MODEL_STAGES))
        if declared_derived:
            raise ValueError(
                f"stage_policies declares {declared_derived}. An adjudication "
                "stage's policy is derived from the model stage it resolves, so a "
                "declared one would be silently ignored."
            )
        for stage in VERIFIER_STAGES:
            policy = self.stage_policies.get(stage)
            if policy is not None and policy.when_uncertain == "include":
                raise ValueError(
                    f"stage_policies[{stage!r}].when_uncertain is 'include'. A "
                    "verification pass exists to catch false positives; one that "
                    "includes on uncertainty cannot."
                )
        return self

    def reason_codes(self) -> tuple[str, ...]:
        """The effective reason-code vocabulary: structural ∪ spec-declared.

        Order: `eligible`; each exclusion criterion's code in declaration order;
        the specialty scope's code; then any structural code not already named.
        Prompts list codes in this order, so it is part of what they render.
        """
        out = [ELIGIBLE_CODE]
        for c in self.criteria:
            if c.reason_code and c.reason_code not in out:
                out.append(c.reason_code)
        if self.specialty_scope.reason_code not in out:
            out.append(self.specialty_scope.reason_code)
        for code in STRUCTURAL_REASON_CODES:
            if code not in out:
                out.append(code)
        return tuple(out)

    def evidence_criterion(self) -> Optional[Criterion]:
        """The criterion declaring the evidence-sufficiency code, if any."""
        return next((c for c in self.criteria if c.reason_code == EVIDENCE_CODE), None)

    def criteria_for(self, stage: str, kind: Optional[str] = None) -> list[Criterion]:
        """Criteria rendered at `stage`, in declaration order."""
        return [
            c for c in self.criteria
            if stage in c.stages and (kind is None or c.kind == kind)
        ]

    def tests_for(self, stage: str) -> list[VerifierTest]:
        """Verifier tests rendered at `stage`, in declaration order."""
        return [t for t in self.verifier_tests if stage in t.stages]

    def policy_for(self, stage: str) -> StagePolicy:
        """The stage's policy, or an empty one if the spec declares none."""
        return self.stage_policies.get(stage) or StagePolicy()


# ── PDF Parsing ─────────────────────────────────────────────────────


class ParseQuality(_SpecModel):
    """Absolute thresholds for the parse-quality gate.

    Defaults mirror `engine.parsers.parse_quality.Thresholds` exactly; a test
    pins the two together, because a spec default that silently disagreed with
    the engine default would make the gate behave differently depending on
    whether a review happened to declare the section.
    """

    short_unit_share_pct_max: float = Field(
        default=50.0, ge=0.0,
        description="Max share of sentence units under 3 tokens (half of SHATTERED)",
    )
    chars_per_unit_min: float = Field(
        default=20.0, ge=0.0,
        description="Min characters per sentence unit (other half of SHATTERED)",
    )
    glyph_density_per_kchar_max: float = Field(
        default=5.0, ge=0.0,
        description="Max GLYPH<...> artifacts per 1000 characters",
    )
    replacement_density_per_kchar_max: float = Field(
        default=1.0, ge=0.0,
        description="Max U+FFFD replacement characters per 1000 characters",
    )
    font_exposure_per_kchar_max: float = Field(
        default=5.0, ge=0.0,
        description=(
            "Max font-damaged characters per 1000 exported characters — markers "
            "plus silent wrong characters placed in the text. Equal to "
            "glyph_density_per_kchar_max by design, and note the two are COUPLED: "
            "this criterion counts the same markers, so relaxing the glyph limit "
            "alone does not relax the gate."
        ),
    )


class PDFParsing(_SpecModel):
    """Configuration for PDF parsing thresholds and models."""

    scanned_text_threshold: int = Field(
        default=100, ge=0,
        description="Characters per page below which a PDF is considered scanned",
    )
    vision_model: str = Field(
        default="qwen2.5vl:7b",
        description="Ollama vision model for OCR of scanned PDFs",
    )
    vision_max_pages: int = Field(
        default=60, ge=1,
        description=(
            "Page cap for the vision fallback. Above this the attempt is recorded "
            "as SKIPPED rather than run: vision renders and sends every page, so "
            "a 728-page proceedings volume would be a very long, very expensive "
            "call to recover a document whose problem is not its parse."
        ),
    )
    vision_num_predict: int = Field(
        default=2048, ge=1,
        description=(
            "Output-token cap per page for the vision model. Without it the "
            "model can generate to the 128k context: PARSE-GATE-04 measured a "
            "211-character loop running at 42.6 tok/s, ~50 minutes for one page. "
            "A good page stops well under this (page 1 of p455: 724 tokens)."
        ),
    )
    vision_num_ctx: int = Field(
        default=8192, ge=512,
        description=(
            "Context window for the vision model. A page image costs ~2,630 "
            "prompt tokens, so 8k is ample; the model's 128k default inflates "
            "the KV cache to 6.8 GiB for no benefit."
        ),
    )
    vision_page_timeout_s: int = Field(
        default=240, ge=1,
        description=(
            "Wall-clock budget for one page's vision call. A healthy page takes "
            "~33 s. NOTE: this bounds the CALLER, not the request -- the HTTP "
            "read timeout still runs to 900 s and the abandoned generation keeps "
            "the runner slot (OLLAMA-CLIENT-01)."
        ),
    )
    ocr_engine: str = Field(
        default="rapidocr",
        description=(
            "OCR engine for the deterministic docling_ocr tier. Only 'rapidocr' "
            "is wired; it ships with docling and its PP-OCRv4 ONNX models are "
            "already on disk, so the tier needs no install. Any other value "
            "raises rather than silently falling back."
        ),
    )
    ocr_max_pages: int = Field(
        default=100, ge=1,
        description=(
            "Page cap for the docling_ocr tier. Higher than the vision cap "
            "because OCR is ~6 s/page on CPU and deterministic, where vision is "
            "a per-page model call; over the cap the attempt is recorded as "
            "SKIPPED rather than run."
        ),
    )
    parse_quality: ParseQuality = Field(
        default_factory=ParseQuality,
        description="Absolute thresholds for the parse-quality gate.",
    )


# ── Cloud Models ────────────────────────────────────────────────────


class CloudModelConfig(_SpecModel):
    """Configuration for a single cloud extraction arm."""

    model: str = Field(description="Model identifier (e.g., 'o4-mini-2025-04-16')")
    cost_input_per_m: float = Field(description="Cost per 1M input tokens (USD)")
    cost_output_per_m: float = Field(description="Cost per 1M output tokens (USD)")


class CloudModels(_SpecModel):
    """Configuration for cloud extraction arms (optional)."""

    openai: Optional[CloudModelConfig] = None
    anthropic: Optional[CloudModelConfig] = None


# ── PDF Quality Check ───────────────────────────────────────────────


class DistributionMonitorConfig(_SpecModel):
    """Thresholds for post-extraction distribution collapse detection."""

    collapsed_min_papers: int = Field(
        default=10, ge=1,
        description="Minimum non-null papers to flag COLLAPSED (single-value field).",
    )
    low_variance_threshold: float = Field(
        default=0.85, gt=0.0, le=1.0,
        description="Top-value fraction above which a field is LOW_VARIANCE.",
    )
    low_variance_min_papers: int = Field(
        default=20, ge=1,
        description="Minimum non-null papers to flag LOW_VARIANCE.",
    )


class PDFQualityCheck(_SpecModel):
    """Configuration for AI-based PDF quality classification."""

    enabled: bool = Field(default=True, description="Enable PDF quality check")
    ai_model: str = Field(
        default="qwen2.5vl:7b",
        description="Ollama vision model for first-page classification",
    )
    dpi: int = Field(
        default=150, ge=72, le=600,
        description="Render DPI for first-page image",
    )
    timeout: int = Field(
        default=120, ge=10, le=600,
        description="Ollama request timeout in seconds",
    )
    exclude_reasons: list[str] = Field(
        default=["NON_ENGLISH", "NOT_MANUSCRIPT", "INACCESSIBLE", "OTHER"],
        description="Valid exclusion reason codes for disposition",
    )


# ── Review Spec (top-level) ──────────────────────────────────────────


class ReviewSpec(_SpecModel):
    """Top-level model for a systematic review specification."""

    review_id: str = Field(
        pattern=r"^[a-z][a-z0-9_]*$",
        max_length=64,
        description=(
            "The review's identity, and the single one. The spec file is "
            "review_specs/<review_id>.yaml and the data root is "
            "data/<review_id>; neither is passed independently. Lowercase "
            "slug so it is safe as both a filename and a directory name on "
            "every filesystem this runs on."
        ),
    )
    title: str
    version: str
    authors: list[str]
    date: date
    prospero_id: Optional[str] = None
    pico: PICO
    search_strategy: SearchStrategy
    screening_models: ScreeningModels = Field(default_factory=ScreeningModels)
    ft_screening_models: FTScreeningModels = Field(default_factory=FTScreeningModels)
    extraction_models: ExtractionModels = Field(default_factory=ExtractionModels)
    eligibility: Eligibility = Field(
        description=(
            "The eligibility authority: criteria, specialty scope, verifier "
            "tests and per-stage policy. Replaces the former "
            "`screening_criteria` and top-level `specialty_scope` sections, "
            "which are refused by `extra='forbid'` rather than ignored."
        ),
    )
    low_yield_threshold: int = Field(
        default=4,
        ge=1,
        description=(
            "Minimum number of non-null extracted fields required. Papers below "
            "this threshold are flagged as LOW_YIELD for PI review."
        ),
    )
    auditor_model: Optional[str] = Field(
        default=None,
        description="Ollama model for extraction audit. Defaults to gemma3:27b if not set.",
    )
    unpaywall_email: Optional[str] = Field(
        default=None,
        description="Email for Unpaywall API queries (required for OA checking).",
    )
    institutional_proxy_pattern: Optional[str] = Field(
        default=None,
        description=(
            "Institutional proxy URL pattern with {doi} placeholder for manual downloads. "
            "Proxy URL patterns vary by institution (e.g., libproxy, EZproxy) and typically "
            "require browser-level VPN or SSO authentication to work. The manual download "
            "list uses this as one of several link options alongside Google Scholar, Direct "
            "DOI, and PubMed."
        ),
    )
    pdf_quality_check: PDFQualityCheck = Field(
        default_factory=PDFQualityCheck,
        description="Configuration for AI-based PDF quality classification.",
    )
    cloud_models: Optional[CloudModels] = Field(
        default=None,
        description="Cloud extraction arm configuration (model names and cost rates).",
    )
    pdf_parsing: PDFParsing = Field(
        default_factory=PDFParsing,
        description="PDF parsing thresholds and vision model configuration.",
    )
    distribution_monitor: DistributionMonitorConfig = Field(
        default_factory=DistributionMonitorConfig,
        description="Thresholds for post-extraction distribution collapse detection.",
    )

    # ── Protocol hashing ─────────────────────────────────────────

    #: There is no `extraction_hash`. The extraction schema left this file in
    #: SCHEMA-DERIVE-01: the codebook is the field authority and always was the
    #: one the prompt was built from, so an extraction's provenance is the
    #: codebook's hash (`extractions.codebook_hash`), not a hash of a parallel
    #: copy that could — and did — disagree with it.

    def screening_hash(self) -> str:
        """SHA-256 of the eligibility section (canonical JSON).

        Everything a screening surface renders from the spec is inside this
        object — criteria and their examples and codes, the specialty scope,
        verifier tests, and stage policy — so two specs with the same hash render
        the same prompts and sheets.
        """
        return _canonical_hash(self.eligibility.model_dump())



# ── Helpers ──────────────────────────────────────────────────────────


def _canonical_hash(data: dict) -> str:
    """Deterministic SHA-256 hash of a dict via sorted-key JSON."""
    blob = json.dumps(data, sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()


class ReviewSpecError(ValueError):
    """Raised when a review spec cannot be loaded or parsed."""


def _format_validation_error(path: Path, exc: ValidationError) -> str:
    """Render a Pydantic ValidationError naming the offending key path.

    Every model is `extra='forbid'`, so the commonest failure is a key the
    spec does not declare. A reader needs the dotted path to it, not a
    traceback: `pdf_parsing.vision_modle` says where to look, `ReviewSpec`
    does not.
    """
    lines = [f"Review spec at {path} is invalid ({exc.error_count()} error(s)):"]
    for err in exc.errors():
        loc = ".".join(str(part) for part in err["loc"]) or "<root>"
        if err["type"] == "extra_forbidden":
            lines.append(
                f"  {loc}: unknown key — the Review Spec declares no such field. "
                f"Remove it or correct the spelling."
            )
        else:
            lines.append(f"  {loc}: {err['msg']}")
    return "\n".join(lines)


def load_review_spec(path: str | Path) -> ReviewSpec:
    """Load a YAML Review Spec from disk and return a validated model.

    Raises ReviewSpecError with a descriptive message on file-not-found,
    YAML parse errors, or schema validation failure (including an unknown
    key, which every model rejects — see `_SpecModel`).
    """
    path = Path(path)
    try:
        with open(path) as f:
            raw = yaml.safe_load(f)
    except FileNotFoundError:
        raise ReviewSpecError(
            f"Review spec not found at {path}. "
            f"Expected a YAML file (e.g., review_specs/<review_id>.yaml)."
        )
    except yaml.YAMLError as exc:
        raise ReviewSpecError(
            f"Review spec at {path} contains invalid YAML: {exc}"
        )
    try:
        return ReviewSpec.model_validate(raw)
    except ValidationError as exc:
        raise ReviewSpecError(_format_validation_error(path, exc)) from exc
