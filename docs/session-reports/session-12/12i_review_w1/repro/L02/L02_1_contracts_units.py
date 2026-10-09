"""L02 reproducer: pure functions only (no DB, no model, no network)."""
import json
from engine.elicitation import contracts as K, terminal as T, materialize as M
from engine.elicitation.units import build_unit_map, UnitMap, COMMENT_RE
from engine.core.citation_guard import check_citations, STRICT

CB = {"escape_token": "NO_EVIDENCE_LOCATABLE", "contract_unmet_token": "CONTRACT_UNMET",
      "absence_sentinels": ["NR"], "canonical_absence_sentinel": "NR",
      "fields": [{"name": "j", "field_class": "judgment"},
                 {"name": "s", "field_class": "stated"}]}
um = UnitMap(paper_id=1, units=tuple(f"Unit number {i} has words." for i in range(1, 61)),
             source_stripped="", min_unit_tokens=3)

def chk(entries, expected=("j", "s")):
    return K.check_response(json.dumps({"fields": entries}), um, CB, expected)

print("== C: JUDGMENT step shapes")
r = chk([{"field_name": "j", "reasoning_steps": [
            {"step": "cited", "unit_indices": [7]},
            "a bare-string step with no basis at all"], "value": "V"},
         {"field_name": "s", "unit_indices": [1], "value": "x"}])
print(" bare-string step beside a dict step ->", r.records["j"].violations, "ok=", r.records["j"].ok,
      "n_steps=", len(r.records["j"].steps))
r = chk([{"field_name": "j", "unit_indices": [3], "reasoning_steps": [
            {"step": "uncited claim", "criteria_application": "false"}], "value": "V"},
         {"field_name": "s", "unit_indices": [1], "value": "x"}])
print(" criteria_application='false' (string) ->", r.records["j"].violations, "ok=", r.records["j"].ok)
r = chk([{"field_name": "j", "unit_indices": [3], "reasoning_steps": [
            {"criteria_application": True}], "value": "V"},
         {"field_name": "s", "unit_indices": [1], "value": "x"}])
print(" one step, no text, criteria only ->", r.records["j"].violations, "ok=", r.records["j"].ok,
      "step text=", repr(r.records["j"].steps[0].text))

print("== scalar unit_indices")
r = chk([{"field_name": "s", "unit_indices": 12, "value": "x"}], expected=("s",))
print(" unit_indices=12 (not a list) ->", r.records["s"].indices, r.records["s"].violations)

print("== unparseable / empty / truncated Pass-1 response -> terminal states")
for label, raw in (("empty content", ""), ("truncated", '{"fields": [{"field_name": "s", "unit_ind'),
                   ("prose", "I could not complete this.")):
    res = K.check_response(raw, um, CB, ("j", "s"))
    print(f" {label}: parse_path={res.parse_path} states={T.terminal_states(res, CB)}")

print("== source_snippet: which run is 'first'")
rec = K.FieldRecord("s", "stated", "x", False, indices=(47, 48, 12))
print(" cited order (47, 48, 12) -> stored:", repr(M.source_snippet(rec, um)))

print("== write-boundary: empty Pass-2 value on an EVIDENCED field")
res = check_citations([{"field_name": "s", "value": "", "source_snippet": "Unit number 1 has words."},
                       {"field_name": "j", "value": "NR", "source_snippet": "Unit number 7 has words."}],
                      escape_token="NO_EVIDENCE_LOCATABLE", absence_sentinels=frozenset({"NR"}),
                      mode=STRICT, citation_counts={"s": 1, "j": 1},
                      contract_unmet_token="CONTRACT_UNMET")
print(" ok=", res.ok, "offenders=", res.offenders)

print("== units: comment regex")
txt = "First real sentence here. <!-- image Second real sentence is lost. Third one too. <!-- formula-not-decoded --> Fourth sentence survives here."
m = build_unit_map(1, txt)
print(" units:", list(m.units))
print(" matches unterminated tail alone:", bool(COMMENT_RE.search("text <!-- image and no close")))
