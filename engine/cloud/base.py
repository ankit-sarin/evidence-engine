"""Shared logic for cloud extraction arms."""

import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from engine.agents.extractor import build_extraction_prompt
from engine.agents.models import ExtractionOutput
from engine.cloud.schema import init_cloud_tables
from engine.core.completeness import (
    MAX_COMPLETENESS_ATTEMPTS,
    IncompleteExtractionError,
    check_completeness,
    enforce_completeness,
    expected_field_names,
)
from engine.core.extraction_telemetry import record_call
from engine.core.review_spec import ReviewSpec, load_review_spec

logger = logging.getLogger(__name__)


class CloudExtractorBase:
    """Base class for cloud API extraction arms."""

    ARM: str = ""  # Override in subclasses

    def __init__(self, db_path: str, review_spec_path: str):
        self.db_path = db_path
        self.spec = load_review_spec(review_spec_path)
        self.schema_hash = self.spec.extraction_hash()
        self._review_dir = Path(db_path).parent

        # Field set the prompt asks for — the completeness guard's reference.
        # Derived once per run from the spec, cross-checked against the codebook.
        self.expected_fields = expected_field_names(
            self.spec, self._review_dir / "extraction_codebook.yaml"
        )
        # Set by parse_response_to_spans() when a salvage branch fires, so the
        # guard and the telemetry can both say which repair was attempted.
        self._last_salvage: str | None = None

        # Initialize cloud tables
        init_cloud_tables(db_path)

        # DB connection (read-write for storing results)
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA foreign_keys=ON")

    def close(self):
        self._conn.close()

    def get_pending_papers(self, arm: str) -> list[dict]:
        """Get extraction-eligible papers with no cloud extraction for this arm.

        Includes FT_ELIGIBLE (parsed, not yet locally extracted) so cloud arms
        can run concurrently with local extraction.
        """
        rows = self._conn.execute(
            """SELECT p.id AS paper_id, p.title, p.authors, p.year
               FROM papers p
               WHERE p.status IN ('FT_ELIGIBLE', 'EXTRACTED', 'AI_AUDIT_COMPLETE', 'HUMAN_AUDIT_COMPLETE')
               AND p.id NOT IN (
                   SELECT ce.paper_id FROM cloud_extractions ce WHERE ce.arm = ?
               )
               ORDER BY p.id""",
            (arm,),
        ).fetchall()
        return [dict(r) for r in rows]

    def load_parsed_text(self, paper_id: int) -> str:
        """Load the most recent parsed markdown for a paper."""
        parsed_dir = self._review_dir / "parsed_text"
        md_files = sorted(parsed_dir.glob(f"{paper_id}_v*.md"), reverse=True)
        if not md_files:
            raise FileNotFoundError(
                f"No parsed text found for paper {paper_id} in {parsed_dir}"
            )
        return md_files[0].read_text()

    def build_prompt(self, parsed_text: str) -> str:
        """Build the extraction prompt — identical to the local extractor."""
        return build_extraction_prompt(parsed_text, self.spec)

    def parse_response_to_spans(self, response_json: str | dict) -> list[dict]:
        """Parse a cloud model's JSON response into evidence span dicts.

        Validates against ExtractionOutput Pydantic model (same as local).
        Returns list of {field_name, value, source_snippet, confidence, tier}.

        Sets `self._last_salvage` when a shape-repair branch fires, so the caller
        can record it and the completeness guard can report it.
        """
        self._last_salvage = None
        if isinstance(response_json, str):
            # Strip markdown ```json ... ``` fences (Anthropic wraps output this way)
            stripped = response_json.strip()
            if stripped.startswith("```"):
                stripped = re.sub(r"^```(?:json)?\s*", "", stripped)
                stripped = re.sub(r"\s*```\s*$", "", stripped)
            try:
                response_json = json.loads(stripped)
            except json.JSONDecodeError:
                logger.warning("Failed to parse response JSON")
                return []

        # Handle {"fields": [...]}, {"extractions": [...]}, {"data": [...]}, and bare [...] formats
        if isinstance(response_json, list):
            response_json = {"fields": response_json}
        elif isinstance(response_json, dict):
            # If fields is empty but raw content exists, try to recover from raw
            if (
                isinstance(response_json.get("fields"), list)
                and len(response_json["fields"]) == 0
                and "raw" in response_json
            ):
                raw = response_json["raw"].strip()
                if raw.startswith("```"):
                    raw = re.sub(r"^```(?:json)?\s*", "", raw)
                    raw = re.sub(r"\s*```\s*$", "", raw)
                try:
                    recovered = json.loads(raw)
                    if isinstance(recovered, list):
                        response_json = {"fields": recovered}
                        logger.info("Recovered %d fields from raw content", len(recovered))
                    elif isinstance(recovered, dict):
                        response_json = recovered
                        # Fall through to alt-key search below
                    else:
                        logger.warning("Raw content parsed but unrecognized type: %s", type(recovered))
                except json.JSONDecodeError:
                    logger.warning("Raw content present but not valid JSON")

        if isinstance(response_json, dict) and (
            "fields" not in response_json
            or (isinstance(response_json.get("fields"), list) and len(response_json["fields"]) == 0)
        ):
            # Try alternate top-level keys that cloud models use
            for alt_key in ("extractions", "extracted_fields", "extracted_data",
                            "data", "extraction", "results", "entries",
                            "extraction_results", "data_extraction"):
                if alt_key in response_json and isinstance(response_json[alt_key], list):
                    response_json = {"fields": response_json[alt_key]}
                    break
            else:
                # Single span dict (has field_name key) — wrap in list.
                # SPANLOSS-01: this is the branch that turned 17 bare single-span
                # responses into valid-looking one-span extractions. It stays as a
                # parse aid, but it is now recorded and its output is subject to
                # the completeness guard like any other result.
                if "field_name" in response_json:
                    self._last_salvage = "single_span_dict"
                    logger.warning(
                        "SALVAGE single_span_dict: response was a bare span object, "
                        "not a wrapped list — wrapping for parse. This is NOT a "
                        "complete extraction and must pass the completeness guard."
                    )
                    response_json = {"fields": [response_json]}
                # Flat field dict: keys are field names, values are span dicts
                elif all(isinstance(v, dict) for v in response_json.values()):
                    self._last_salvage = "flat_field_dict"
                    logger.warning(
                        "SALVAGE flat_field_dict: response keyed fields at top level "
                        "instead of a wrapped list — reshaping for parse."
                    )
                    spans = [
                        {"field_name": k, **v}
                        for k, v in response_json.items()
                    ]
                    response_json = {"fields": spans}
                else:
                    logger.warning(
                        "Response JSON has no recognized key; keys found: %s",
                        list(response_json.keys()),
                    )
                    return []

        # Cloud models sometimes omit source_snippet (e.g. for synthesized values).
        # Patch nulls to empty string before Pydantic validation so spans aren't dropped.
        for span in response_json.get("fields", []):
            if isinstance(span, dict) and "source_snippet" not in span:
                span["source_snippet"] = ""

        # Null value → "NR" conversion: Sonnet returns null for absent fields,
        # but Pydantic requires value: str.  Convert to "NR" (engine convention
        # for absent values) and clear the snippet.
        # Also coerce non-string values (int, float) to str — Sonnet sometimes
        # returns bare numbers for numeric fields like sample_size.
        for span in response_json.get("fields", []):
            if isinstance(span, dict) and span.get("value") is None:
                logger.debug(
                    "Null value → NR: field '%s'", span.get("field_name"),
                )
                span["value"] = "NR"
                span["source_snippet"] = ""
            elif isinstance(span, dict) and not isinstance(span.get("value"), str):
                logger.debug(
                    "Non-string value → str: field '%s', value=%r",
                    span.get("field_name"), span.get("value"),
                )
                span["value"] = str(span["value"])

        try:
            output = ExtractionOutput.model_validate(response_json)
        except Exception as exc:
            logger.warning("Failed to validate response against ExtractionOutput: %s", exc)
            return []

        return [
            {
                "field_name": span.field_name,
                "value": span.value,
                "source_snippet": span.source_snippet,
                "confidence": span.confidence,
                "tier": span.tier,
            }
            for span in output.fields
        ]

    def store_result(
        self,
        paper_id: int,
        arm: str,
        model_string: str,
        extracted_data: dict | list,
        reasoning_trace: str,
        prompt_text: str,
        input_tokens: int,
        output_tokens: int,
        reasoning_tokens: int,
        cost_usd: float,
        spans: list[dict],
    ) -> int:
        """Store extraction result and spans atomically.

        Raises ValueError if spans is empty — this indicates a parse failure
        that must be investigated, not silently stored without span rows.
        """
        if not spans:
            raise ValueError(
                f"Paper {paper_id} ({arm}): extraction produced 0 spans — "
                f"refusing to store without evidence spans. "
                f"Check parse_response_to_spans() logs for details."
            )

        # INSTRUMENT-01: the write boundary now checks completeness, not merely
        # non-emptiness. SPANLOSS-01's 17 collapsed openai extractions each had
        # exactly one span and passed the check above; they do not pass this one.
        enforce_completeness(
            spans,
            self.expected_fields,
            paper_id=paper_id,
            arm=arm,
            salvage=self._last_salvage,
        )

        now = datetime.now(timezone.utc).isoformat()

        try:
            cur = self._conn.execute(
                """INSERT INTO cloud_extractions
                   (paper_id, arm, model_string, extracted_data, reasoning_trace,
                    prompt_text, input_tokens, output_tokens, reasoning_tokens,
                    cost_usd, extraction_schema_hash, extracted_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    paper_id, arm, model_string,
                    json.dumps(extracted_data),
                    reasoning_trace, prompt_text,
                    input_tokens, output_tokens, reasoning_tokens,
                    cost_usd, self.schema_hash, now,
                ),
            )
            ext_id = cur.lastrowid

            for span in spans:
                self._conn.execute(
                    """INSERT INTO cloud_evidence_spans
                       (cloud_extraction_id, field_name, value, source_snippet,
                        confidence, tier, notes)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        ext_id,
                        span["field_name"],
                        span.get("value"),
                        span.get("source_snippet"),
                        span.get("confidence"),
                        span.get("tier"),
                        span.get("notes"),
                    ),
                )

            self._conn.commit()
            return ext_id

        except Exception:
            self._conn.rollback()
            raise

    def extract_with_completeness(
        self,
        paper_id: int,
        parsed_text: str,
        max_attempts: int = MAX_COMPLETENESS_ATTEMPTS,
    ) -> dict:
        """Extract, and re-issue the identical request until the result is complete.

        The request is unchanged between attempts — same prompt, same
        `response_format`, same parameters — because the failure this guards
        against is stochastic response *shape*, not a prompt defect. Re-asking
        the same question is the correct remedy; changing the question would
        change the extraction contract mid-run.

        Every attempt writes a telemetry row before its result is accepted or
        rejected, so an exhausted paper leaves a full record of what each attempt
        returned. On exhaustion the error propagates: the caller must fail the
        paper, never store the partial result.
        """
        last_error: IncompleteExtractionError | None = None
        for attempt in range(1, max_attempts + 1):
            result = self.extract_paper(paper_id, parsed_text)
            spans = result.get("spans", [])
            check = check_completeness(spans, self.expected_fields)
            salvage = self._last_salvage

            record_call(
                self._review_dir,
                arm=self.ARM,
                paper_id=paper_id,
                attempt=attempt,
                outcome="stored" if check.complete else (
                    "incomplete_retry" if attempt < max_attempts else "incomplete_exhausted"
                ),
                model=getattr(self, "model_string", None),
                finish_reason=result.get("finish_reason"),
                raw_content=result.get("raw_content"),
                spans_parsed=len(spans),
                fields_expected=len(self.expected_fields),
                missing_fields=check.missing,
                salvage=salvage,
                input_tokens=result.get("input_tokens"),
                output_tokens=result.get("output_tokens"),
                reasoning_tokens=result.get("reasoning_tokens"),
            )

            if check.complete:
                if attempt > 1:
                    logger.info(
                        "Paper %d (%s): complete on attempt %d/%d",
                        paper_id, self.ARM, attempt, max_attempts,
                    )
                return result

            last_error = IncompleteExtractionError(
                paper_id=paper_id, arm=self.ARM, missing=check.missing,
                n_stored=check.n_produced, n_expected=check.n_expected,
                salvage=salvage, attempt=attempt,
            )
            logger.warning(
                "Paper %d (%s): INCOMPLETE attempt %d/%d — %s%s. Re-issuing identical request.",
                paper_id, self.ARM, attempt, max_attempts, check.summary(),
                f" (salvage={salvage})" if salvage else "",
            )

        logger.error(
            "Paper %d (%s): INCOMPLETE after %d attempts — failing the paper, "
            "NOT storing a partial extraction. %s",
            paper_id, self.ARM, max_attempts, last_error,
        )
        raise last_error

    def get_progress(self, arm: str) -> dict:
        """Return progress stats for the given arm."""
        total = self._conn.execute(
            "SELECT COUNT(*) FROM papers WHERE status IN ('FT_ELIGIBLE', 'EXTRACTED', 'AI_AUDIT_COMPLETE', 'HUMAN_AUDIT_COMPLETE')"
        ).fetchone()[0]

        completed = self._conn.execute(
            "SELECT COUNT(*) FROM cloud_extractions WHERE arm = ?",
            (arm,),
        ).fetchone()[0]

        total_cost = self._conn.execute(
            "SELECT COALESCE(SUM(cost_usd), 0) FROM cloud_extractions WHERE arm = ?",
            (arm,),
        ).fetchone()[0]

        return {
            "total_papers": total,
            "completed": completed,
            "remaining": total - completed,
            "total_cost_usd": round(total_cost, 4),
        }

    def run_distribution_check(self, stats: dict) -> dict:
        """Run post-extraction distribution monitor for this arm.

        Returns the monitor summary dict. Raises DistributionCollapseError
        if any categorical field has zero variance (COLLAPSED).
        """
        from engine.validators.distribution_monitor import run_post_extraction_check

        codebook_path = self._review_dir / "extraction_codebook.yaml"
        return run_post_extraction_check(
            db_path=Path(self.db_path),
            review_name=self._review_dir.name,
            arm=self.ARM,
            codebook_path=codebook_path,
            extracted_count=stats.get("extracted", 0),
            failed_count=stats.get("failed", 0),
        )
