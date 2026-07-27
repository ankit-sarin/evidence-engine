"""Unit tests for the evidence-provenance taxonomy classifier.

One fixture per taxonomy class, including the two cases the taxonomy exists to
separate: an ellipsis-stitched snippet (real text, non-contiguous) and a
model-authored rationale (no textual basis).
"""

import pytest

from analysis.provenance import classifier as C
from analysis.provenance.field_class import FIELD_CLASS, field_class
from analysis.provenance.normalize import normalize
from analysis.provenance.segment import sentences

PAPER = """
## METHODS

The treatment planning was performed on the tablet PC by clicking on the target
position for the needle tip inside the US image. Using the transformations of
the system control the 2D click position was represented as the corresponding
3D position in the coordinate space of iiwa2.

## RESULTS

A pair of industrial CMOS cameras is used for online visual feedback data
acquisition with 640 x 480 resolution and 30 frames per second. Positioning
precision is acceptable (&lt;1mm), while accuracy was not.

## DISCUSSION

The surgeon's decision and supervision remains critical to reduce uncertainties
of target recognition in supervised robotic surgery. The system was evaluated on
a tissue phantom in a bench setup.
"""


@pytest.fixture(scope="module")
def paper():
    return C.PaperIndex.build(1, PAPER, with_sentences=True)


def test_anchored_single_contiguous_passage(paper):
    snippet = (
        "A pair of industrial CMOS cameras is used for online visual feedback data "
        "acquisition with 640 x 480 resolution and 30 frames per second."
    )
    res = C.classify_span(snippet, "R", paper)
    assert res.taxonomy_class == C.ANCHORED
    assert res.min_ratio == 1.0


def test_anchored_survives_line_break_and_html_entity(paper):
    # "(&lt;1mm)" in the paper vs "(<1mm)" in the snippet (T1), and the paper
    # wraps this sentence across a line break (T9).
    snippet = "Positioning precision is acceptable (<1mm), while accuracy was not."
    assert C.classify_span(snippet, "x", paper).taxonomy_class == C.ANCHORED


def test_stitched_two_real_passages_joined_silently(paper):
    snippet = (
        "The treatment planning was performed on the tablet PC by clicking on the "
        "target position for the needle tip inside the US image. The system was "
        "evaluated on a tissue phantom in a bench setup."
    )
    res = C.classify_span(snippet, "Shared", paper)
    assert res.taxonomy_class == C.STITCHED
    assert res.n_exact == res.n_evaluated == 2


def test_stitched_via_ellipsis_bridge(paper):
    """The T-seg-0 case: an explicit '...' bridge between two verbatim passages.

    Without the ellipsis pre-split this is one unmatchable unit and would be
    misclassified UNTRACEABLE_NO_BASIS.
    """
    snippet = (
        "A pair of industrial CMOS cameras is used for online visual feedback data "
        "acquisition with 640 x 480 resolution and 30 frames per second... "
        "The surgeon's decision and supervision remains critical to reduce "
        "uncertainties of target recognition in supervised robotic surgery."
    )
    res = C.classify_span(snippet, "Shared", paper)
    assert res.taxonomy_class == C.STITCHED
    assert res.n_evaluated == 2
    assert res.n_exact == 2


def test_drifted_minor_edits_only(paper):
    # One word dropped ("industrial") and a digit style change; still near-verbatim.
    snippet = (
        "A pair of CMOS cameras is used for online visual feedback data acquisition "
        "with 640 x 480 resolution and 30 frames per second."
    )
    res = C.classify_span(snippet, "R", paper)
    assert res.taxonomy_class == C.DRIFTED
    assert res.min_ratio >= C.THRESHOLD_PRIMARY
    assert res.n_exact == 0


def test_untraceable_partial_one_real_one_invented(paper):
    snippet = (
        "The system was evaluated on a tissue phantom in a bench setup. "
        "The authors report that regulatory clearance was obtained in 2019 for "
        "fully autonomous operation in human patients."
    )
    res = C.classify_span(snippet, "Proof of concept only", paper)
    assert res.taxonomy_class == C.UNTRACEABLE_PARTIAL
    assert 0 < res.n_exact < res.n_evaluated


def test_untraceable_no_basis_model_authored_rationale(paper):
    """The local-arm pattern: a justification written by the model, not a quote."""
    snippet = "The robot uses sensors and cameras to monitor the fetoscope tip."
    res = C.classify_span(snippet, "R", paper)
    assert res.taxonomy_class == C.UNTRACEABLE_NO_BASIS
    assert res.n_exact == 0
    assert res.min_ratio < C.THRESHOLD_PRIMARY


def test_absence_declared_vs_missing_snippet(paper):
    assert C.classify_span("", "NOT_FOUND", paper).taxonomy_class == C.ABSENCE_DECLARED
    assert C.classify_span(None, "NR", paper).taxonomy_class == C.ABSENCE_DECLARED
    assert C.classify_span("   ", "Shared", paper).taxonomy_class == C.MISSING_SNIPPET


def test_unclassifiable_short(paper):
    assert C.classify_span("H.", "H", paper).taxonomy_class == C.UNCLASSIFIABLE_SHORT


# ── normalization ────────────────────────────────────────────────────────


def test_normalization_transforms():
    assert normalize("&lt;1mm") == "<1mm"                      # T1
    assert normalize("ﬁne") == "fine"                          # T2
    assert normalize("a­b") == "ab"                       # T3
    assert normalize("“q” ‘r’") == '"q" \'r\''  # T4
    assert normalize("a—b") == "a-b"                      # T5
    assert normalize("a…b") == "a...b"                    # T6
    assert normalize("ori-\nfice") == "orifice"                # T7
    assert normalize("1990-\n1995") == "1990- 1995"            # T7 guard: no digit merge
    assert normalize("ABC") == "abc"                           # T8
    assert normalize("  a \n\t b  ") == "a b"                  # T9


def test_ellipsis_is_preserved_not_deleted():
    """T6 folds the spelling but must never remove the bridge marker."""
    assert "..." in normalize("A…B")


# ── segmentation ─────────────────────────────────────────────────────────


def test_segmenter_keeps_scientific_abbreviations_intact():
    text = "As shown in Fig. 2 (c), the error was 1.04 mm vs. 2 mm. The next sentence."
    segs = sentences(text)
    assert len(segs) == 2
    assert "Fig. 2 (c)" in segs[0] and "1.04 mm" in segs[0]


def test_ellipsis_pre_split_produces_two_units():
    segs = sentences("First real passage here. ... Second real passage here.")
    assert len(segs) == 2
    assert not any("..." in s for s in segs)


def test_short_fragments_counted_not_dropped(paper):
    snippet = "2 (c). The system was evaluated on a tissue phantom in a bench setup."
    res = C.classify_span(snippet, "x", paper)
    assert res.n_sentences > res.n_evaluated  # fragment counted, excluded from test
    assert res.taxonomy_class == C.STITCHED


# ── threshold sensitivity ────────────────────────────────────────────────


def test_reclassification_at_band_thresholds_is_derivable(paper):
    snippet = (
        "A pair of CMOS cameras is used for online visual feedback data acquisition "
        "with 640 x 480 resolution and 30 frames per second."
    )
    res = C.classify_span(snippet, "R", paper)
    for t in C.THRESHOLD_BAND:
        assert res.classes_at(t) in C.TAXONOMY_CLASSES
    assert res.classes_at(0.0) == C.DRIFTED
    assert res.classes_at(1.01) == C.UNTRACEABLE_NO_BASIS


def test_anchored_is_threshold_independent(paper):
    res = C.classify_span(
        "The system was evaluated on a tissue phantom in a bench setup.", "x", paper
    )
    assert res.taxonomy_class == C.ANCHORED
    assert all(res.classes_at(t) == C.ANCHORED for t in (0.0, 0.85, 0.95, 1.0))


# ── field classification ─────────────────────────────────────────────────


def test_field_classification_covers_all_twenty_codebook_fields():
    assert len(FIELD_CLASS) == 20
    assert all(v[0] in ("extractive", "interpretive") for v in FIELD_CLASS.values())
    assert all(v[1].strip() for v in FIELD_CLASS.values())


def test_field_class_lookup():
    assert field_class("clinical_readiness_assessment") == "interpretive"
    assert field_class("primary_outcome_value") == "extractive"
    assert field_class("no_such_field") == ""
