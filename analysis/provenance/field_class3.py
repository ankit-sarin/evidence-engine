"""Three-way field classification: where each field's answer lives.

STATED / INFERABLE / JUDGMENT. Criteria and per-field justifications are pinned
in FIELD_CLASSES.md (`prov-fieldclass-1`); this module is the machine-readable
mirror of §2 of that document and must not drift from it — the test suite
asserts that every field here appears there with the same class.

Supersedes the binary extractive/interpretive split in `field_class.py`, which
is deliberately left in place: TAXONOMY-CENSUS-01 and -02 were reported under it
and the revision disclosure needs both axes side by side.

The two axes are NOT nested. Every JUDGMENT field was `interpretive` and every
STATED field was `extractive`, but the INFERABLE six are drawn from both:
`country` was extractive; `surgical_domain` and the four `task_*` fields were
interpretive.

This module is an axis for tabulation. It changes no span's taxonomy class —
the provenance taxonomy is frozen at v1.1.
"""

from __future__ import annotations

STATED = "stated"
INFERABLE = "inferable"
JUDGMENT = "judgment"

CLASSES = (STATED, INFERABLE, JUDGMENT)
VERSION = "prov-fieldclass-1"

# Basis for each assignment: a declared corpus sample, or reasoning from the
# codebook against the pinned criteria. Recorded so the report can separate
# "measured" from "argued" without re-reading the prose.
MEASURED = "measured"
REASONED = "reasoned"

# Fields whose class genuinely varies by paper (FIELD_CLASSES.md §1.5). These
# are the hard cases for any design that wants to declare a field's class once,
# at setup time, rather than per paper.
PAPER_VARIABLE = frozenset({"sample_size", "surgical_domain"})

# field -> (class, basis, one-sentence justification)
FIELD_CLASS3: dict[str, tuple[str, str, str]] = {
    "study_type": (
        STATED, REASONED,
        "The codebook leads with \"Look for explicit statements like 'prospective study,' "
        "'case series'\"; papers name their own type, and the inference fallback is the "
        "minority case.",
    ),
    "robot_platform": (
        STATED, REASONED,
        "Hardware is named in prose; a paper that used a robot says which one.",
    ),
    "task_performed": (
        STATED, REASONED,
        "The methods describe the task in the authors' own words, which is what the field asks for.",
    ),
    "sample_size": (
        STATED, MEASURED,
        "Counts are asserted ('n = 5 pigs'); summing across groups is arithmetic over stated "
        "numbers. Paper-variable: 30.4% of values are NR and the sum-across-groups rule needs "
        "a derivation in a real minority.",
    ),
    "surgical_domain": (
        INFERABLE, REASONED,
        "Clinical papers name the specialty, but this corpus is dominated by bench, phantom and "
        "simulation work where no specialty is asserted and the value is assigned from the setup. "
        "Paper-variable; the split was not measured.",
    ),
    "autonomy_level": (
        JUDGMENT, REASONED,
        "The codebook supplies a five-step decision tree precisely because papers routinely do "
        "not reference the Yang levels, and applying it is an evaluative call.",
    ),
    "validation_setting": (
        STATED, REASONED,
        "The methods assert in vivo / ex vivo / phantom / simulation directly; 'select most "
        "advanced' ranks stated facts rather than deriving new ones.",
    ),
    "task_monitor": (
        INFERABLE, REASONED,
        "No paper writes 'R' or 'Shared'; the architecture description fixes who observes "
        "mechanically, but never states it.",
    ),
    "task_generate": (
        INFERABLE, REASONED,
        "Who authors the plan is read off the system description and is not a claim the paper makes.",
    ),
    "task_select": (
        INFERABLE, REASONED,
        "Same decomposition; the material is in the control-flow description, and the NR rate "
        "(5.3%, highest of the four) is still low enough that the answer is normally derivable.",
    ),
    "task_execute": (
        INFERABLE, REASONED,
        "Same decomposition; 'Shared' summarizes a cooperative-control arrangement described "
        "across several sentences rather than named.",
    ),
    "system_maturity": (
        JUDGMENT, REASONED,
        "Technology-readiness categories are the codebook's frame for the work, not a claim the "
        "authors make about themselves.",
    ),
    "study_design": (
        JUDGMENT, REASONED,
        "The codebook's design vocabulary is finer than authors' self-description, so "
        "'single best-fit' selection is evaluative.",
    ),
    "country": (
        INFERABLE, MEASURED,
        "19 of 20 sampled papers fix the country only through affiliations, correspondence "
        "addresses or funding notes — paratext, not assertion.",
    ),
    "primary_outcome_metric": (
        STATED, REASONED,
        "Defined positionally as the first quantitative outcome; the codebook calls it "
        "'positional order, no judgment' and the metric is named in the results prose.",
    ),
    "primary_outcome_value": (
        STATED, MEASURED,
        "14 of 15 sampled values are locatable in body prose rather than table-only; the earlier "
        "table-dominant impression came from a no-basis-biased sample.",
    ),
    "comparison_to_human": (
        STATED, REASONED,
        "Where the comparison exists the paper reports it in text; where it does not, "
        "'no comparison reported' is a checkable claim about the body text.",
    ),
    "secondary_outcomes": (
        STATED, REASONED,
        "Additional reported outcomes are quoted metric/value pairs from the results section.",
    ),
    "key_limitation": (
        JUDGMENT, REASONED,
        "The instruction forbids copying the authors' own limitations and asks for the "
        "extractor's judgment.",
    ),
    "clinical_readiness_assessment": (
        JUDGMENT, REASONED,
        "The codebook states outright that 'there is no right answer in the text'.",
    ),
}


def field_class3(name: str) -> str:
    """Three-way class for a field, or '' if the field is unknown.

    Unknown returns '' rather than raising: the Run 6 corpus contains two spans
    with field names that are not codebook fields at all ('Title', 'field_1' —
    collapsed Pass 2 outputs), and a census must be able to tabulate them
    without crashing or silently attributing them to a real class.
    """
    entry = FIELD_CLASS3.get(name)
    return entry[0] if entry else ""


def basis(name: str) -> str:
    entry = FIELD_CLASS3.get(name)
    return entry[1] if entry else ""


def justification(name: str) -> str:
    entry = FIELD_CLASS3.get(name)
    return entry[2] if entry else ""


def fields_by_class(cls: str) -> list[str]:
    return sorted(n for n, v in FIELD_CLASS3.items() if v[0] == cls)
