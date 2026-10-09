"""L01-4: on the LEGACY path the guard exempts a value equal to the codebook's
escape token (no snippet owed), and legacy_record has no DECLINED branch, so the
token is planned as an `asserted` VALUE with no snippet - a positive claim event
stored with nothing behind it. Pure functions; no connection, no model."""
from pathlib import Path
from engine.core import citation_guard as G, extraction_events as X
from engine.core.parsed_text import ParsedTextRef

ESC = "NO_EVIDENCE_LOCATABLE"
spans = [{"field_name": "f1", "value": ESC, "source_snippet": "", "confidence": 0.9},
         {"field_name": "f2", "value": "Suturing", "source_snippet": "", "confidence": 0.9}]
res = G.check_citations(spans, escape_token=ESC, absence_sentinels=frozenset({"NR"}), mode=G.LEGACY)
print("guard (LEGACY): n_escape =", res.n_escape, " offenders =", res.offenders)
ref = ParsedTextRef("uid-1", 7, Path("/nonexistent"), "x", 1, "a" * 64)
rec = X.legacy_record(paper_id=7, arm="arm_x", run_id=1, extraction_uid="u-1", parsed_text=ref,
                      model="m", model_digest="d" * 64, expected=("f1", "f2"), spans=spans,
                      offenders=res.offenders, presented_context_sha256="h", context_chain=("h",))
plan = X.plan_extraction_events(rec, live={}, from_state="parsed")
for e in plan.field_events:
    print(f"{e['field_name']}: event_type={e['event_type']} value={e['value']!r} "
          f"source_snippet={e['source_snippet']!r}")
