"""The review's identity, and everything derived from it (SPEC-AUTH-01).

One authority. A review is named once — by `review_id` — and both the
spec file and the data root are *derived* from that name:

    review_specs/<review_id>.yaml        the configuration
    data/<review_id>/                    the state

Before this module the direction ran the other way: `--review` chose the
data root, `--spec` chose the configuration, and nothing checked that the
two named the same review. A spec pointed at the wrong database was not
an error — it was a silently different run, and the database it wrote to
carried no record of which spec produced it.

`--spec` survives as an *override* for the one legitimate case (a spec
kept outside `review_specs/`), and `load_spec_for` refuses it if it
carries a different `review_id`. The check is deliberately here, in the
resolver, and not at each call site: it must run before any database is
opened, and a rule enforced in twenty-nine places is enforced in none.
"""

import re
from pathlib import Path

from engine.core.database import DATA_ROOT
from engine.core.review_spec import ReviewSpec, ReviewSpecError, load_review_spec

SPEC_ROOT = Path("review_specs")

# The `review_id` field's own pattern, restated for the *requested* id so a
# bad --review argument fails with a message about the argument rather than
# with a file-not-found about a path nobody meant to name. A test pins the
# two together.
REVIEW_ID_PATTERN = r"^[a-z][a-z0-9_]*$"
REVIEW_ID_MAX_LENGTH = 64


class ReviewIdMismatchError(ReviewSpecError):
    """Raised when a spec's review_id is not the review that was asked for."""


def _validate_requested_id(review_id: str) -> None:
    if not isinstance(review_id, str) or not review_id:
        raise ReviewSpecError("Review id must be a non-empty string.")
    if (
        not re.match(REVIEW_ID_PATTERN, review_id)
        or len(review_id) > REVIEW_ID_MAX_LENGTH
    ):
        raise ReviewSpecError(
            f"Invalid review id {review_id!r}. A review id is a lowercase slug: "
            f"it starts with a letter and contains only a-z, 0-9 and underscore, "
            f"max 64 characters (e.g. 'surgical_autonomy')."
        )


def spec_path_for(review_id: str) -> Path:
    """The Review Spec file for a review: `review_specs/<review_id>.yaml`."""
    _validate_requested_id(review_id)
    return SPEC_ROOT / f"{review_id}.yaml"


def data_root_for(review_id: str) -> Path:
    """The data root for a review: `data/<review_id>`.

    Returns the path only. Nothing here creates a directory or opens a
    database — `ReviewDatabase` still owns that, unchanged.
    """
    _validate_requested_id(review_id)
    return DATA_ROOT / review_id


def load_spec_for(review_id: str, override: str | Path | None = None) -> ReviewSpec:
    """Load the spec for `review_id`, optionally from an overriding path.

    Raises `ReviewIdMismatchError` if the spec that loads declares a
    different review, whether it came from the derived path or from an
    override. Call this *before* constructing a `ReviewDatabase`: the point
    of the check is that a mismatched run never reaches a database at all.
    """
    _validate_requested_id(review_id)
    path = Path(override) if override else spec_path_for(review_id)
    spec = load_review_spec(path)
    if spec.review_id != review_id:
        raise ReviewIdMismatchError(
            f"Review spec at {path} declares review_id {spec.review_id!r}, "
            f"but the run asked for {review_id!r}. Refusing before any database "
            f"is opened — running this spec against data/{review_id} would write "
            f"one review's results into another review's database. "
            f"Use --review {spec.review_id} to run the spec you passed, or pass "
            f"the spec for {review_id!r}."
        )
    return spec
