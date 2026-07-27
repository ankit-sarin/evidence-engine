"""grep_verify_fast() must agree with the real legacy function, verdict for verdict.

The fast version is only legitimate for restating the legacy figure if it is
verdict-identical. These fixtures cover the branches that could diverge: exact
substring, fuzzy accept just above threshold, fuzzy reject just below, empty
inputs, and a snippet longer than the paper.
"""

import pytest

from analysis.provenance.legacy import grep_verify_fast
from engine.agents.auditor import grep_verify

PAPER = """
The treatment planning was performed on the tablet PC by clicking on the target
position for the needle tip inside the US image. A pair of industrial CMOS
cameras is used for online visual feedback data acquisition with 640 x 480
resolution and 30 frames per second. The surgeon's decision and supervision
remains critical to reduce uncertainties of target recognition.
"""

CASES = [
    # exact substring
    "A pair of industrial CMOS cameras is used for online visual feedback data acquisition",
    # near-verbatim, one word dropped
    "A pair of CMOS cameras is used for online visual feedback data acquisition",
    # near-verbatim with punctuation and case drift
    "the surgeon's decision and supervision remains critical to reduce uncertainties",
    # paraphrase — should fail both
    "The robot autonomously monitors the surgical field using its onboard sensors.",
    # model-authored absence statement — should fail both
    "The paper does not report a comparison to human performance.",
    # ellipsis bridge — should fail both (no ellipsis handling in legacy)
    "The treatment planning was performed on the tablet PC... remains critical.",
    # snippet longer than the paper
    PAPER + PAPER,
    # degenerate inputs
    "",
    "   ",
]


@pytest.mark.parametrize("snippet", CASES)
def test_fast_matches_legacy(snippet):
    assert grep_verify_fast(snippet, PAPER) == grep_verify(snippet, PAPER)


def test_empty_paper():
    assert grep_verify_fast("anything at all here", "") == grep_verify("anything at all here", "")
