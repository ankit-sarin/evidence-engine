"""A11 — what a single-span Pass-2 collapse becomes. (1) the write-boundary check refuses it;
(2) once the 3-attempt budget is exhausted, the refusal's record is planned as an `extracted`
paper with one claim and the rest in payload.incomplete_fields (R140). Pure functions; no DB, no model."""
import pathlib
from engine.core.review_spec import load_review_spec
from engine.core.completeness import (enforce_completeness, expected_field_names,
                                      IncompleteExtractionError, MAX_COMPLETENESS_ATTEMPTS)
from engine.core.extraction_events import legacy_record, plan_extraction_events, counts_toward_abort
from engine.core.paper_state import COMPLETED_PROCESSING_STATES
from engine.agents.models import ExtractionOutput
repo = pathlib.Path("/home/ankitsarin/projects/evidence-engine")
spec = load_review_spec(repo / "review_specs/surgical_autonomy.yaml")
expected = expected_field_names(spec, repo / "data/surgical_autonomy/extraction_codebook.yaml")
one = '{"fields":[{"field_name":"%s","value":"RCT","source_snippet":"a randomised trial","confidence":0.9,"tier":1}]}' % expected[0]
out = ExtractionOutput.model_validate_json(one)
print(f"expected fields: {len(expected)}; a one-span response validates against the Pass-2 schema model: {len(out.fields)} span")
print("an EMPTY array validates too:", len(ExtractionOutput.model_validate_json('{"fields":[]}').fields), "spans")
spans = [s.model_dump() for s in out.fields]
try:
    enforce_completeness(spans, expected, paper_id=1, arm="local")
except IncompleteExtractionError as e:
    print(f"write boundary: REFUSED IncompleteExtractionError n_stored={e.n_stored} n_expected={e.n_expected} missing={len(e.missing)}  (budget {MAX_COMPLETENESS_ATTEMPTS} attempts)")
rec = legacy_record(paper_id=1, arm="local_deepseek_r1_32b", run_id=1, extraction_uid="u", parsed_text=None,
                    model="deepseek-r1:32b", model_digest="0"*64, expected=expected, spans=spans,
                    attempts=MAX_COMPLETENESS_ATTEMPTS, presented_context_sha256="h")
plan = plan_extraction_events(rec, live={}, from_state="parsed")
pe = plan.paper_event
print(f"after exhaustion: paper event type={pe['event_type']} to_state={pe['to_state']} reason_code={pe['reason_code']}")
print(f"  field events written: {[(f['event_type'], f['field_name']) for f in plan.field_events]}")
print(f"  payload asserted={pe['payload']['asserted']} incomplete_fields={len(pe['payload']['incomplete_fields'])}")
print(f"  to_state in COMPLETED_PROCESSING_STATES (=> analysis_ready for an eligible paper): {pe['to_state'] in COMPLETED_PROCESSING_STATES}")
print(f"  counts toward the consecutive-failure abort: {counts_toward_abort(rec)}")
