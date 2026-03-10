"""Rule-based categorization of flagged papers into FP pattern groups.

Categories and keyword rules are loaded from a per-review YAML config file.
If no config exists, all papers are categorized as 'ambiguous' (safe default).

Config location: data/{review_name}/adjudication_categories.yaml

Two matching modes per category (checked in order, first match wins):
  1. regex_patterns — raw regex (for power users / backward compat)
  2. title_keywords + abstract_keywords + exclude_if_also — simple keyword matching
"""

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_NAME = "adjudication_categories.yaml"
DATA_ROOT = Path("data")


# ── Config Loading ──────────────────────────────────────────────────


class CategoryConfig:
    """Parsed category configuration for a review."""

    def __init__(self, categories: list[dict]):
        """Each dict: name, description, regex_patterns, title_keywords,
        abstract_keywords, exclude_if_also."""
        self.categories = categories

    @classmethod
    def load(cls, config_path: Path | str) -> "CategoryConfig":
        """Load categories from a YAML config file."""
        import yaml

        config_path = Path(config_path)
        if not config_path.exists():
            logger.info("No category config at %s — all papers will be 'ambiguous'", config_path)
            return cls(categories=[])

        with open(config_path) as f:
            raw = yaml.safe_load(f)

        if not raw or "categories" not in raw:
            logger.warning("Category config at %s has no 'categories' key", config_path)
            return cls(categories=[])

        cats = []
        for name, spec in raw["categories"].items():
            cats.append({
                "name": name,
                "description": spec.get("description", ""),
                "regex_patterns": spec.get("regex_patterns", []),
                "title_keywords": [kw.lower() for kw in spec.get("title_keywords", [])],
                "abstract_keywords": [kw.lower() for kw in spec.get("abstract_keywords", [])],
                "exclude_if_also": [kw.lower() for kw in spec.get("exclude_if_also", [])],
            })

        logger.info("Loaded %d categories from %s", len(cats), config_path)
        return cls(categories=cats)

    @classmethod
    def empty(cls) -> "CategoryConfig":
        """Return an empty config — everything lands in 'ambiguous'."""
        return cls(categories=[])

    def get_descriptions(self) -> dict[str, str]:
        """Return {category_name: description} including 'ambiguous'."""
        descs = {c["name"]: c["description"] for c in self.categories}
        descs["ambiguous"] = "No clear FP pattern — needs careful human review"
        return descs


def config_path_for_review(review_name: str, data_root: Path | None = None) -> Path:
    """Return the expected config path for a review."""
    root = data_root or DATA_ROOT
    return root / review_name / DEFAULT_CONFIG_NAME


def load_config(review_name: str | None = None, config_path: Path | None = None,
                data_root: Path | None = None) -> CategoryConfig:
    """Load category config by review name or explicit path.

    Priority: config_path > review_name lookup > empty config.
    """
    if config_path:
        return CategoryConfig.load(config_path)
    if review_name:
        return CategoryConfig.load(config_path_for_review(review_name, data_root))
    return CategoryConfig.empty()


# ── Categorization Engine ───────────────────────────────────────────


def categorize_paper(title: str, abstract: str | None,
                     config: CategoryConfig | None = None) -> str:
    """Categorize a flagged paper based on title + abstract.

    If config is None or empty, returns 'ambiguous' for all papers.
    Returns the category name, or 'ambiguous' if no pattern matches.
    """
    if config is None or not config.categories:
        return "ambiguous"

    text = (title + " " + (abstract or "")).lower()

    for cat in config.categories:
        if _matches_category(text, title.lower(), (abstract or "").lower(), cat):
            return cat["name"]

    return "ambiguous"


def _matches_category(text: str, title_lower: str, abstract_lower: str,
                      cat: dict) -> bool:
    """Check if text matches a category's rules."""
    # Mode 1: regex patterns (takes precedence if present)
    if cat["regex_patterns"]:
        for pattern in cat["regex_patterns"]:
            if re.search(pattern, text):
                return True
        return False

    # Mode 2: keyword matching
    matched = False

    for kw in cat["title_keywords"]:
        if kw in title_lower:
            matched = True
            break

    if not matched:
        for kw in cat["abstract_keywords"]:
            if kw in abstract_lower:
                matched = True
                break

    if not matched:
        return False

    # Check exclusion co-occurrences
    if cat["exclude_if_also"]:
        for excl in cat["exclude_if_also"]:
            if excl in text:
                return False

    return True


def get_category_descriptions(config: CategoryConfig | None = None) -> dict[str, str]:
    """Return {category_name: description} for all defined categories."""
    if config is None or not config.categories:
        return {"ambiguous": "No clear FP pattern — needs careful human review"}
    return config.get_descriptions()


# ── Config Generation ───────────────────────────────────────────────


def generate_starter_config(output_path: Path | str,
                            sample_titles: list[str] | None = None) -> Path:
    """Generate a starter adjudication_categories.yaml template.

    If sample_titles are provided, adds them as comments to help the user
    understand what kinds of papers need categorization. This is scaffolding
    for future diagnostic-sample-driven config generation.
    """
    import yaml

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    template = {
        "categories": {
            "cv_perception": {
                "description": "Computer vision, detection, tracking, segmentation without robot control",
                "title_keywords": ["detection", "segmentation", "tracking", "recognition",
                                   "classification", "image analysis"],
                "abstract_keywords": ["deep learning", "convolutional neural", "object detection",
                                      "semantic segmentation"],
                "exclude_if_also": ["autonomous execution", "robot control", "semi-autonomous"],
            },
            "review_editorial": {
                "description": "Reviews, surveys, editorials, commentaries",
                "title_keywords": ["review", "survey", "editorial", "commentary",
                                   "meta-analysis", "framework", "conceptual"],
                "abstract_keywords": ["systematic review", "scoping review", "literature review"],
                "exclude_if_also": [],
            },
            "hardware_sensing": {
                "description": "Sensor design, actuator, mechanism without autonomous behavior",
                "title_keywords": ["sensor", "actuator", "mechanism design", "haptic",
                                   "end-effector", "gripper"],
                "abstract_keywords": ["piezoelectric", "strain gauge", "force sensor",
                                      "tactile sensor"],
                "exclude_if_also": ["autonomous"],
            },
            "planning_only": {
                "description": "Surgical planning or navigation without autonomous execution",
                "title_keywords": ["surgical planning", "preoperative planning",
                                   "navigation system", "3d print", "patient-specific"],
                "abstract_keywords": ["treatment planning", "image-guided", "virtual reality",
                                      "simulation training"],
                "exclude_if_also": ["autonomous", "execution", "robot performs"],
            },
            "teleoperation_only": {
                "description": "Purely teleoperated / master-slave without autonomy",
                "title_keywords": ["teleoperated", "master-slave", "remote surgery"],
                "abstract_keywords": ["teleoperation", "da vinci"],
                "exclude_if_also": ["autonomous", "shared control", "semi-autonomous"],
            },
            "rehabilitation_prosthetics": {
                "description": "Rehabilitation, exoskeletons, prosthetics (non-surgical)",
                "title_keywords": ["exoskeleton", "prosthetic", "rehabilitation robot",
                                   "orthosis", "assistive device"],
                "abstract_keywords": ["gait training", "rehabilitation", "mobility aid"],
                "exclude_if_also": [],
            },
            "industrial_nonmedical": {
                "description": "Industrial, agricultural, or non-medical robotics",
                "title_keywords": ["industrial robot", "manufacturing", "agricultural",
                                   "autonomous vehicle", "drone", "warehouse"],
                "abstract_keywords": ["farming", "harvest", "unmanned"],
                "exclude_if_also": [],
            },
        }
    }

    header = (
        "# FP pattern categories for screening adjudication\n"
        "# Review-specific — edit categories and keywords as needed\n"
        "#\n"
        "# Two matching modes per category:\n"
        "#   1. regex_patterns: list of raw regex (advanced, checked against title+abstract)\n"
        "#   2. title_keywords + abstract_keywords + exclude_if_also: simple keyword matching\n"
        "#      A paper matches if any title/abstract keyword is found,\n"
        "#      UNLESS an exclude_if_also keyword also appears.\n"
        "#\n"
        "# First matching category wins (order matters).\n"
        "# Papers matching no category are labeled 'ambiguous'.\n"
    )

    if sample_titles:
        header += "#\n# Sample flagged titles from diagnostic pass:\n"
        for t in sample_titles[:10]:
            header += f"#   - {t}\n"

    header += "\n"

    with open(output_path, "w") as f:
        f.write(header)
        yaml.dump(template, f, default_flow_style=False, sort_keys=False, width=100)

    logger.info("Generated starter category config at %s", output_path)
    return output_path
