"""Parse-quality metrics and an absolute PASS/FAIL verdict.

Parse quality is a **structure** property, not a length property. p455 is the
worked example: 59,964 characters against a 12-page PDF — a character ratio of
1.00, exactly the right number of characters — while the text itself reads
``c\\n o\\n m\\n p u\\n t e\\n r``. Every size-, ratio- or truncation-based check
passes it. Only segmentation exposes it: 8,394 units for 59,964 chars, 7.1 chars
per unit, 98.8% of units under three tokens. A gate that measures length cannot
see this document is destroyed.

**Relationship to PARSE-01.** The metric definitions are the Phase 1 sweep's
(`analysis/eval/parse01/sweep.py`, `measure_paper`), reproduced here semantically
rather than imported: `analysis/` is the study lane and must not become an engine
dependency. `tests/test_parse_quality.py` pins the reproduction against the
committed `sweep.jsonl` rows for all nine flagged papers, exactly — so a drift in
either copy is a test failure, not a silent divergence. The segmenter itself IS
imported (`analysis.provenance.segment.sentences`), following the precedent in
`engine/elicitation/units.py` and `engine/elicitation/contracts.py`: one
segmenter, never a second implementation.

**Thresholds here are ABSOLUTE; Phase 2's were corpus-relative.** `flag.py` used
Tukey far-outlier fences (Q3 + 3×IQR) over the 190-paper corpus — an instrument
for asking "which of these documents are unusual", which needs the whole corpus in
hand and answers a different question every time the corpus changes. A gate runs
on one document at ingest, with no corpus, so it needs fixed thresholds. The two
are not interchangeable and the fence values are not the defaults below.

**The defaults are PROVISIONAL — set for calibration, not ruled on.** See the
PARSE-GATE-01 report's calibration section.

Pure: no I/O, no database, no model calls. `compute_metrics` and `assess` are
functions of their text argument alone.
"""

from __future__ import annotations

import re
import statistics as st
from dataclasses import dataclass, field
from typing import Any, Mapping

from analysis.provenance.segment import sentences

# ── Phase 1 patterns, character-for-character from sweep.py ──────────

RE_REFERENCES = re.compile(r"^#{1,6}\s*references\b", re.IGNORECASE | re.MULTILINE)
RE_GLYPH = re.compile(r"GLYPH<[^>]*>|GLYPH&lt;[^&]*&gt;")
RE_IMAGE = re.compile(r"<!--\s*image\s*-->")
RE_FORMULA = re.compile(r"<!--\s*formula-not-decoded\s*-->")

# A unit shorter than this many whitespace tokens is "short". Phase 1's `t < 3`.
SHORT_UNIT_TOKENS = 3

#: The 17 Phase 1 metric names. `paper_id` and `parsed_file` are deliberately
#: absent: they identify a file, and `compute_metrics` is a function of text.
PHASE1_METRIC_NAMES = frozenset({
    "chars", "lines", "nonblank_lines", "unique_nonblank_lines",
    "max_line_chars", "median_line_chars", "chars_per_line",
    "references_sections", "glyph_artifacts", "image_comments",
    "formula_comments", "replacement_chars", "nonascii_density_pct",
    "pysbd_units", "median_unit_tokens", "chars_per_unit",
    "short_unit_share_pct",
})

#: Added here, not present in Phase 1. `glyph_density_per_kchar` makes the glyph
#: count comparable across document lengths (p586's 542 artifacts in 69K chars is
#: a worse document than p699's 419 in 48K only once both are per-kchar).
#: `is_empty` is the raw input for the EMPTY_TEXT criterion, which cannot be
#: derived from the others: whitespace-only text has chars > 0 yet zero units.
ADDED_METRIC_NAMES = frozenset({"glyph_density_per_kchar", "is_empty"})

METRIC_NAMES = PHASE1_METRIC_NAMES | ADDED_METRIC_NAMES

# ── Criteria — a closed vocabulary ───────────────────────────────────

EMPTY_TEXT = "EMPTY_TEXT"
SHORT_UNIT_SHARE = "SHORT_UNIT_SHARE"
CHARS_PER_UNIT = "CHARS_PER_UNIT"
GLYPH_DENSITY = "GLYPH_DENSITY"
REPLACEMENT_CHARS = "REPLACEMENT_CHARS"

CRITERIA = (EMPTY_TEXT, SHORT_UNIT_SHARE, CHARS_PER_UNIT, GLYPH_DENSITY,
            REPLACEMENT_CHARS)


@dataclass(frozen=True)
class Thresholds:
    """Absolute gate thresholds. PROVISIONAL — for calibration, not final.

    Only these four properties are judged. Length, reference-section count,
    image/formula comment counts and non-ASCII density are computed and returned
    but never gate a document: p415 (a whole 728-page proceedings volume) and
    p498 (148,805 clean chars) are both long, and only one of them is broken.
    Length is the fit guard's question, and the fit guard is a separate
    instrument that must not be merged with this one.
    """

    short_unit_share_pct_max: float = 50.0
    chars_per_unit_min: float = 20.0
    glyph_density_per_kchar_max: float = 5.0
    replacement_chars_max: int = 0

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any] | None) -> "Thresholds":
        """Build from a `pdf_parsing.parse_quality` mapping; missing keys default.

        Accepts a plain mapping or anything exposing the fields as attributes, so
        it works against a dict today and against a Review Spec model once
        PARSE-GATE-02 adds the field. Unknown keys are ignored rather than
        rejected: a spec written for a later engine must not break an earlier one.
        """
        if mapping is None:
            return cls()
        if not isinstance(mapping, Mapping):
            mapping = {
                f: getattr(mapping, f)
                for f in cls.__dataclass_fields__
                if hasattr(mapping, f)
            }
        known = {k: v for k, v in mapping.items() if k in cls.__dataclass_fields__}
        return cls(**known)


@dataclass(frozen=True)
class Verdict:
    """`passed` plus the criteria that failed, each with its value and threshold."""

    passed: bool
    failures: tuple[tuple[str, Any, Any], ...] = field(default_factory=tuple)
    metrics: Mapping[str, Any] = field(default_factory=dict)

    @property
    def criteria(self) -> tuple[str, ...]:
        return tuple(c for c, _, _ in self.failures)

    def describe(self) -> str:
        if self.passed:
            return "PASS"
        return "FAIL: " + "; ".join(
            f"{c}={v!r} (limit {t!r})" for c, v, t in self.failures
        )


def compute_metrics(text: str) -> dict[str, Any]:
    """Phase 1's `measure_paper` metrics over `text`, plus the two additions.

    Definitions are Phase 1's exactly, including its rounding (1 dp for
    `chars_per_line`, `chars_per_unit` and `short_unit_share_pct`; 3 dp for
    `nonascii_density_pct`), because `tests/test_parse_quality.py` pins the
    output against `sweep.jsonl` to the digit.
    """
    raw = text
    lines = raw.splitlines()
    nonblank = [ln for ln in lines if ln.strip()]
    units = sentences(raw)
    unit_tokens = [len(u.split()) for u in units]
    nonascii = sum(1 for ch in raw if ord(ch) > 127)
    glyph = len(RE_GLYPH.findall(raw))

    return {
        "chars": len(raw),
        "lines": len(lines),
        "nonblank_lines": len(nonblank),
        "unique_nonblank_lines": len(set(nonblank)),
        "max_line_chars": max((len(ln) for ln in lines), default=0),
        "median_line_chars": (
            int(st.median([len(ln) for ln in nonblank])) if nonblank else 0
        ),
        "chars_per_line": round(len(raw) / len(nonblank), 1) if nonblank else 0.0,
        "references_sections": len(RE_REFERENCES.findall(raw)),
        "glyph_artifacts": glyph,
        "image_comments": len(RE_IMAGE.findall(raw)),
        "formula_comments": len(RE_FORMULA.findall(raw)),
        "replacement_chars": raw.count("�"),
        "nonascii_density_pct": (
            round(100.0 * nonascii / len(raw), 3) if raw else 0.0
        ),
        "pysbd_units": len(units),
        "median_unit_tokens": st.median(unit_tokens) if unit_tokens else 0,
        "chars_per_unit": round(len(raw) / len(units), 1) if units else 0.0,
        "short_unit_share_pct": (
            round(
                100.0 * sum(1 for t in unit_tokens if t < SHORT_UNIT_TOKENS)
                / len(unit_tokens), 1,
            )
            if unit_tokens else 0.0
        ),
        # ── additions ──
        "glyph_density_per_kchar": (
            round(1000.0 * glyph / len(raw), 3) if raw else 0.0
        ),
        "is_empty": not raw.strip(),
    }


def assess(text: str, thresholds: Thresholds | None = None) -> Verdict:
    """Judge `text` against `thresholds`. Pure; no I/O.

    Empty text short-circuits to a single `EMPTY_TEXT` failure rather than
    cascading: whitespace-only input would otherwise fail `CHARS_PER_UNIT` with
    a value of 0.0, sending a reader to look for a segmentation defect in a
    document that has no text at all.
    """
    th = thresholds or Thresholds()
    m = compute_metrics(text)

    if m["is_empty"]:
        return Verdict(False, ((EMPTY_TEXT, m["chars"], None),), m)

    failures: list[tuple[str, Any, Any]] = []
    if m["short_unit_share_pct"] > th.short_unit_share_pct_max:
        failures.append((SHORT_UNIT_SHARE, m["short_unit_share_pct"],
                         th.short_unit_share_pct_max))
    if m["chars_per_unit"] < th.chars_per_unit_min:
        failures.append((CHARS_PER_UNIT, m["chars_per_unit"],
                         th.chars_per_unit_min))
    if m["glyph_density_per_kchar"] > th.glyph_density_per_kchar_max:
        failures.append((GLYPH_DENSITY, m["glyph_density_per_kchar"],
                         th.glyph_density_per_kchar_max))
    if m["replacement_chars"] > th.replacement_chars_max:
        failures.append((REPLACEMENT_CHARS, m["replacement_chars"],
                         th.replacement_chars_max))

    return Verdict(not failures, tuple(failures), m)
