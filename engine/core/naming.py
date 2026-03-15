"""Naming convention for human review artifacts.

All human review touchpoints follow the pattern:
    {review}_{stage}_{direction}.{ext}

Where:
    review    = review name (e.g., "surgical_autonomy")
    stage     = one of REVIEW_STAGES keys
    direction = "queue" (HTML generated for review) or "decisions" (JSON exported by reviewer)
    ext       = file extension ("html", "json", "xlsx")
"""

from pathlib import Path

REVIEW_STAGES = {
    "abstract_adjudication": "Abstract Screening Adjudication",
    "ft_adjudication": "Full-Text Screening Adjudication",
    "pdf_acquisition": "PDF Acquisition",
    "pdf_quality": "PDF Quality Check",
    "extraction_audit": "Extraction Audit Review",
}

_VALID_DIRECTIONS = {"queue", "decisions"}


def review_artifact_filename(
    review_name: str, stage: str, direction: str, ext: str,
) -> str:
    """Return the canonical filename for a review artifact.

    Example: review_artifact_filename("surgical_autonomy", "ft_adjudication", "queue", "html")
             → "surgical_autonomy_ft_adjudication_queue.html"
    """
    if stage not in REVIEW_STAGES:
        raise ValueError(
            f"Unknown stage '{stage}'. Valid stages: {', '.join(sorted(REVIEW_STAGES))}"
        )
    if direction not in _VALID_DIRECTIONS:
        raise ValueError(
            f"Unknown direction '{direction}'. Must be one of: {', '.join(sorted(_VALID_DIRECTIONS))}"
        )
    return f"{review_name}_{stage}_{direction}.{ext}"


def review_artifact_path(
    data_dir: Path, review_name: str, stage: str, direction: str, ext: str,
) -> Path:
    """Return the full path for a review artifact.

    Example: review_artifact_path(Path("data/surgical_autonomy"), "surgical_autonomy",
                                  "ft_adjudication", "queue", "html")
             → Path("data/surgical_autonomy/surgical_autonomy_ft_adjudication_queue.html")
    """
    return data_dir / review_artifact_filename(review_name, stage, direction, ext)
