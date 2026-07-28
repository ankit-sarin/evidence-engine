"""Extraction completeness guard (INSTRUMENT-01).

SPANLOSS-01 root cause: 21 Run 6 extractions stored one span instead of ~20 and
nothing noticed, because the only write-boundary check was "did we get *any*
spans" (`engine/cloud/base.py`, `if not spans: raise`). A response carrying a
single bare span object passed that check, was salvaged into a valid-looking
one-span extraction, committed, and silently entered the census, the
disagreement CSV and the Pass 2 judge.

This module supplies the check that was missing: the field set the prompt asked
for, compared against the field set the arm actually produced.

Design notes:

  * **One expected-field source.** `expected_field_names()` derives the set from
    the ReviewSpec's extraction schema — the same object `build_extraction_prompt`
    iterates to decide which fields to ask for — and cross-checks it against the
    codebook, warning on divergence. Nothing here hardcodes a field count; a
    codebook that grows to 25 fields moves the guard with it.
  * **Arm-agnostic.** The same predicate governs the local extractor and both
    cloud arms, so a repair run cannot be complete on one arm's terms and
    incomplete on another's.
  * **Fail the call, never write partially.** An incomplete result raises before
    any INSERT. There is no "store what we got" path, because that is exactly
    what produced the 21 collapsed extractions.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# Bounded retry budget for a completeness failure. Identical across all arms so
# the repair run's behaviour does not depend on which arm is being repaired.
MAX_COMPLETENESS_ATTEMPTS = 3


class IncompleteExtractionError(RuntimeError):
    """An extraction produced fewer fields than the prompt asked for.

    Carries the diagnosis so callers can log it without re-deriving anything.
    """

    def __init__(
        self,
        paper_id: int,
        arm: str,
        missing: tuple[str, ...],
        n_stored: int,
        n_expected: int,
        salvage: str | None = None,
        attempt: int | None = None,
    ):
        self.paper_id = paper_id
        self.arm = arm
        self.missing = missing
        self.n_stored = n_stored
        self.n_expected = n_expected
        self.salvage = salvage
        self.attempt = attempt
        shown = ", ".join(missing[:8]) + ("…" if len(missing) > 8 else "")
        super().__init__(
            f"Paper {paper_id} ({arm}): incomplete extraction — "
            f"{n_stored}/{n_expected} fields"
            + (f", attempt {attempt}" if attempt else "")
            + (f", salvage={salvage}" if salvage else "")
            + f". Missing: {shown}"
        )


@dataclass(frozen=True)
class CompletenessResult:
    """Outcome of comparing produced fields against expected fields."""

    complete: bool
    n_produced: int
    n_expected: int
    missing: tuple[str, ...] = ()
    unexpected: tuple[str, ...] = ()
    duplicated: tuple[str, ...] = ()
    produced: tuple[str, ...] = field(default=(), repr=False)

    def summary(self) -> str:
        parts = [f"{self.n_produced}/{self.n_expected} fields"]
        if self.missing:
            parts.append(f"missing={len(self.missing)}")
        if self.unexpected:
            parts.append(f"unexpected={list(self.unexpected)}")
        if self.duplicated:
            parts.append(f"duplicated={list(self.duplicated)}")
        return ", ".join(parts)


def expected_field_names(
    spec, codebook_path: str | Path | None = None
) -> tuple[str, ...]:
    """The field set the extraction prompt asked for, in prompt order.

    Source of truth is `spec.extraction_schema`, traversed tier 1→4 exactly as
    `build_extraction_prompt` traverses it, so the guard cannot diverge from the
    request. When a codebook path is supplied its field list is compared and any
    divergence is logged — the codebook supplies the *content* of each field
    block, the spec decides *which* fields are asked for, and the two agreeing is
    an invariant worth surfacing rather than assuming.
    """
    names: list[str] = []
    for tier in (1, 2, 3, 4):
        names.extend(f.name for f in spec.extraction_schema.fields_by_tier(tier))

    if codebook_path is not None:
        cb_names = _codebook_field_names(codebook_path)
        if cb_names is not None:
            only_spec = [n for n in names if n not in cb_names]
            only_cb = [n for n in cb_names if n not in names]
            if only_spec or only_cb:
                logger.warning(
                    "Spec/codebook field divergence — spec-only=%s codebook-only=%s. "
                    "Guard follows the spec because that is what the prompt asks for.",
                    only_spec, only_cb,
                )
    return tuple(names)


def _codebook_field_names(codebook_path: str | Path) -> tuple[str, ...] | None:
    path = Path(codebook_path)
    if not path.exists():
        logger.debug("Codebook not found for cross-check: %s", path)
        return None
    try:
        import yaml

        data = yaml.safe_load(path.read_text()) or {}
        return tuple(f["name"] for f in data.get("fields", []) if "name" in f)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Could not read codebook for cross-check (%s): %s", path, exc)
        return None


def check_completeness(spans, expected: tuple[str, ...]) -> CompletenessResult:
    """Compare produced spans against the expected field set.

    `spans` may be dicts (cloud path) or objects with `.field_name` (local path).
    Completeness means every expected field is present at least once; extra or
    duplicated fields are reported but do not by themselves fail the check —
    they are shape problems, not loss, and failing on them would turn a
    recoverable oddity into a dropped paper.
    """
    produced: list[str] = []
    for s in spans or []:
        name = s.get("field_name") if isinstance(s, dict) else getattr(s, "field_name", None)
        if name:
            produced.append(name)

    seen: set[str] = set()
    dupes: list[str] = []
    for n in produced:
        if n in seen:
            dupes.append(n)
        seen.add(n)

    missing = tuple(n for n in expected if n not in seen)
    unexpected = tuple(sorted(n for n in seen if n not in set(expected)))
    return CompletenessResult(
        complete=not missing,
        n_produced=len(seen),
        n_expected=len(expected),
        missing=missing,
        unexpected=unexpected,
        duplicated=tuple(sorted(set(dupes))),
        produced=tuple(produced),
    )


def enforce_completeness(
    spans,
    expected: tuple[str, ...],
    *,
    paper_id: int,
    arm: str,
    salvage: str | None = None,
    attempt: int | None = None,
) -> CompletenessResult:
    """Raise `IncompleteExtractionError` unless every expected field is present.

    Call this immediately before any write. A salvaged single span reaches here
    exactly like any other result and is rejected on the same terms — the salvage
    branch may repair a *shape*, it may never certify a *result*.
    """
    result = check_completeness(spans, expected)
    if not result.complete:
        raise IncompleteExtractionError(
            paper_id=paper_id,
            arm=arm,
            missing=result.missing,
            n_stored=result.n_produced,
            n_expected=result.n_expected,
            salvage=salvage,
            attempt=attempt,
        )
    if result.unexpected or result.duplicated:
        logger.warning(
            "Paper %d (%s): extraction complete but irregular — %s",
            paper_id, arm, result.summary(),
        )
    return result
