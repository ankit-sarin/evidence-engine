"""L01-2: a record in which NO field carries a value (every field contract_unmet
or declined) is planned as `extracted`, does not count toward the
consecutive-failure abort, and leaves claim-bearing events that carry the reuse
key. Pure functions only: no connection, no model."""
from pathlib import Path
from engine.core import extraction_events as X
from engine.core.parsed_text import ParsedTextRef
from engine.core.effective import CLAIM_EVENT_TYPES
from engine.core.paper_state import COMPLETED_PROCESSING_STATES

ref = ParsedTextRef("uid-1", 7, Path("/nonexistent"), "x", 1, "a" * 64)
expected = tuple(f"f{i}" for i in range(20))
rec = X.elicited_record(
    paper_id=7, arm="arm_x", run_id=1, extraction_uid="u-1", parsed_text=ref,
    model="m", model_digest="d" * 64, expected=expected,
    states={n: "CONTRACT_UNMET" for n in expected}, spans=[],
    violations={n: ("INDEX_MALFORMED",) for n in expected},
    unmet_token="CONTRACT_UNMET", escape_token="NO_EVIDENCE_LOCATABLE",
    evidenced_token="EVIDENCED_VALUE", presented_context_sha256="h1",
    context_chain=("h1",))
plan = X.plan_extraction_events(rec, live={}, from_state="parsed")
pe = plan.paper_event
print("fields with a value        :", sum(1 for f in rec.fields if f.kind == X.VALUE), "of", len(expected))
print("paper event                :", pe["event_type"], "->", pe["to_state"], "reason_code =", pe["reason_code"])
print("payload counts             :", {k: pe["payload"][k] for k in ("asserted", "contract_unmet", "declined")})
print("to_state is 'completed'    :", pe["to_state"] in COMPLETED_PROCESSING_STATES, "(analysis_ready when eligible)")
print("counts_toward_abort(record):", X.counts_toward_abort(rec))
claimish = [e for e in plan.field_events if e["event_type"] in CLAIM_EVENT_TYPES]
print("claim-bearing events       :", len(claimish), "- each carries reuse_key:",
      all(e["payload"].get("reuse_key") for e in claimish),
      "(selection._live_reuse_keys then skips the paper)")
