"""PROPOSED extractive-vs-interpretive classification of the 20 codebook fields.

*** PROPOSED — ARCHITECT RATIFICATION REQUIRED. ***
Nothing downstream should treat this as canonical until it is ratified. It is
kept in code (rather than in the codebook) precisely so that ratifying it is a
deliberate act, not an accident of importing a YAML file.

The distinction the taxonomy needs:

  EXTRACTIVE    the answer exists somewhere in the paper as text. A correct
                extraction can, in principle, quote one contiguous passage that
                states it. Failure to anchor is therefore a compliance problem.

  INTERPRETIVE  the answer is a synthesized judgment *about* the paper. No
                passage states it, because the coded value belongs to the
                codebook's vocabulary, not the paper's. Failure to anchor may
                be structural rather than a compliance problem.

Derivation inputs, per field: codebook `type`, `judge_rubric_family`,
`source_quote_required`, the per-value `definition` texts, and the field
`instruction` wording. Where the instruction itself contains an inference verb
("infer", "your judgment", "synthesize", "select most advanced", "use the
decision tree"), that is treated as decisive evidence of INTERPRETIVE.
"""

from __future__ import annotations

EXTRACTIVE = "extractive"
INTERPRETIVE = "interpretive"

# field -> (class, one-sentence justification)
FIELD_CLASS: dict[str, tuple[str, str]] = {
    # ── Tier 1 ────────────────────────────────────────────────────────────
    "study_type": (
        EXTRACTIVE,
        "Codebook instruction leads with 'Look for explicit statements like \"prospective study,\" "
        "\"case series\"' — the label is normally the paper's own word, with inference only as a "
        "documented fallback.",
    ),
    "robot_platform": (
        EXTRACTIVE,
        "A proper noun naming hardware; if the paper used a robot it names it.",
    ),
    "task_performed": (
        EXTRACTIVE,
        "The task is described in the methods in the paper's own words; the field asks for that "
        "description, not a category.",
    ),
    "sample_size": (
        EXTRACTIVE,
        "Counts are stated in the paper; summing across groups is arithmetic over quoted numbers, "
        "not interpretation of them.",
    ),
    "surgical_domain": (
        INTERPRETIVE,
        "Bench, phantom and simulation studies have no stated specialty, so values like "
        "'Non-clinical Bench / Phantom' are assignments made by the extractor rather than terms "
        "the paper uses.",
    ),
    # ── Tier 2 ────────────────────────────────────────────────────────────
    "autonomy_level": (
        INTERPRETIVE,
        "The codebook supplies a five-step decision tree for the common case where the paper never "
        "references the Yang levels, so the coded level is derived, not quoted.",
    ),
    "validation_setting": (
        EXTRACTIVE,
        "Whether work was done in vivo, ex vivo, on a phantom or in simulation is stated in the "
        "methods; the 'select most advanced' rule ranks stated facts rather than inferring new ones.",
    ),
    "task_monitor": (
        INTERPRETIVE,
        "No paper writes 'R' or 'Shared'; agency for observation must be read off a system "
        "description and mapped to the codebook's three-way vocabulary.",
    ),
    "task_generate": (
        INTERPRETIVE,
        "Same agency decomposition as task_monitor — who authors the plan is inferred from an "
        "architecture description, never labelled as such in the source.",
    ),
    "task_select": (
        INTERPRETIVE,
        "Plan selection is frequently not described at all, so the value is often a judgment that "
        "the paper is silent, which no passage can state.",
    ),
    "task_execute": (
        INTERPRETIVE,
        "Same agency decomposition; 'Shared' in particular summarizes a cooperative-control "
        "arrangement that is described across several sentences, not named.",
    ),
    "system_maturity": (
        INTERPRETIVE,
        "Technology-readiness categories such as 'Algorithm on existing platform' are the "
        "codebook's frame for the work, not a claim the authors make about themselves.",
    ),
    "study_design": (
        INTERPRETIVE,
        "The codebook's design vocabulary is finer than authors' self-description, so 'single "
        "best-fit' selection is a judgment call in most papers.",
    ),
    "country": (
        EXTRACTIVE,
        "Affiliation text is present on page 1; the documented fallback to the first author's "
        "institution still resolves to text in the paper.",
    ),
    # ── Tier 3 ────────────────────────────────────────────────────────────
    "primary_outcome_metric": (
        EXTRACTIVE,
        "Defined positionally as the first quantitative outcome in results — a rule for locating "
        "text, explicitly 'no judgment' in the codebook.",
    ),
    "primary_outcome_value": (
        EXTRACTIVE,
        "The codebook says to copy the exact numeric reporting including units, CIs and p-values.",
    ),
    "comparison_to_human": (
        EXTRACTIVE,
        "Either the paper reports a robot-vs-human comparison in text or it does not; the absence "
        "case is a claim about the text, still adjudicable against it.",
    ),
    "secondary_outcomes": (
        EXTRACTIVE,
        "Additional reported outcomes are quoted metric/value pairs drawn from the results section.",
    ),
    # ── Tier 4 ────────────────────────────────────────────────────────────
    "key_limitation": (
        INTERPRETIVE,
        "The instruction explicitly forbids copying the authors' own limitations section and asks "
        "for the extractor's judgment; source_quote_required is set precisely because the answer "
        "is not itself quotable.",
    ),
    "clinical_readiness_assessment": (
        INTERPRETIVE,
        "The codebook states outright that 'there is no right answer in the text' and asks the "
        "extractor to synthesize results, limitations and validation setting.",
    ),
}

STATUS = "PROPOSED — ARCHITECT RATIFICATION REQUIRED"


def field_class(name: str) -> str:
    """Return the proposed class for a field, or '' if the field is unknown."""
    entry = FIELD_CLASS.get(name)
    return entry[0] if entry else ""


def justification(name: str) -> str:
    entry = FIELD_CLASS.get(name)
    return entry[1] if entry else ""
