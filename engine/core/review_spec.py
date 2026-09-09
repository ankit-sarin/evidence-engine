"""Review Spec: YAML parser, Pydantic models, and protocol hashing."""

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Optional

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


# ── Extraction Schema ────────────────────────────────────────────────


class ExtractionField(_SpecModel):
    """Single field to extract from a study's full text."""

    name: str
    description: str
    type: str = Field(
        description="Data type: str, int, float, bool, list[str], enum, etc."
    )
    tier: int = Field(ge=1, le=4, description="1=explicit, 2=interpretive, 3=numeric, 4=judgment")
    enum_values: Optional[list[str]] = Field(
        default=None, description="Allowed values when type is 'enum'"
    )


class ExtractionSchema(_SpecModel):
    """Full extraction schema organized by tier."""

    fields: list[ExtractionField]

    @field_validator("fields")
    @classmethod
    def at_least_one_tier1(cls, v: list[ExtractionField]) -> list[ExtractionField]:
        if not any(f.tier == 1 for f in v):
            raise ValueError("Extraction schema must have at least one tier-1 field")
        return v

    def fields_by_tier(self, tier: int) -> list[ExtractionField]:
        return [f for f in self.fields if f.tier == tier]


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


class ScreeningCriteria(_SpecModel):
    """Inclusion/exclusion rules for title-abstract screening."""

    inclusion: list[str]
    exclusion: list[str]


# ── Specialty Scope ──────────────────────────────────────────────────


class SpecialtyScope(_SpecModel):
    """Surgical specialty inclusion/exclusion scope for screening."""

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
    screening_criteria: ScreeningCriteria
    extraction_schema: ExtractionSchema
    specialty_scope: Optional[SpecialtyScope] = Field(
        default=None,
        description="Surgical specialty inclusion/exclusion scope. If absent, no specialty filtering.",
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

    def screening_hash(self) -> str:
        """SHA-256 of the screening criteria section (canonical JSON)."""
        return _canonical_hash(self.screening_criteria.model_dump())

    def extraction_hash(self) -> str:
        """SHA-256 of the extraction schema section (canonical JSON)."""
        return _canonical_hash(self.extraction_schema.model_dump())


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
