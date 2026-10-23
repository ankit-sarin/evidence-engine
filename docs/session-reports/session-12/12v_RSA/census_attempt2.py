"""12v-RSA Q4 — every elicited Pass-1 attempt 2 on disk, from telemetry only. Read-only; no review.db opened.
usage, from the repository root: census_attempt2.py <label>=<extraction_calls.jsonl> ...   (stdout: JSON)
Writes nothing. Each telemetry row's `extra.attempts` carries, per attempt, every field's class and
violation codes; a field is absent from a response when its codes include FIELD_MISSING (the rule of
12r_U13_PA/shape_pa.py). Shapes, per attempt:
  stated_only — every INFERABLE and JUDGMENT field FIELD_MISSING, at least one STATED field present
  full        — no field FIELD_MISSING
  partial     — some fields FIELD_MISSING, not stated_only
  empty       — every field FIELD_MISSING
The feedback attempt 2 was sent names exactly attempt 1's failed fields (pipeline.elicit →
prompts.build_feedback_block skips `rec.ok` records), so its classes are those fields' classes.
`raw` says whether that attempt's response text is stored: `attempts[i].raw_content` (12r and later),
or `extra.pass1_raw_content` for the accepted attempt only; otherwise the shape is derived from codes."""
import collections, hashlib, json, sys

CLASSES = ("stated", "inferable", "judgment")


def shape(fields):
    missing = {n for n, f in fields.items() if "FIELD_MISSING" in f["violations"]}
    by = {c: {n for n, f in fields.items() if f["class"] == c} for c in CLASSES}
    present = {c: len(by[c] - missing) for c in CLASSES}
    if not missing:
        s = "full"
    elif len(missing) == len(fields):
        s = "empty"
    elif (by["inferable"] | by["judgment"]) <= missing and present["stated"] > 0:
        s = "stated_only"
    else:
        s = "partial"
    return s, present


def codes(att):
    c = collections.Counter(v for f in att["fields"].values() for v in f["violations"])
    return dict(sorted(c.items()))


cases, sources = [], {}
for arg in sys.argv[1:]:
    label, path = arg.split("=", 1)
    sources[label] = {"path": path, "sha256": hashlib.sha256(open(path, "rb").read()).hexdigest()}
    for line in open(path, encoding="utf-8"):
        r = json.loads(line); x = r.get("extra") or {}
        atts = x.get("attempts") or []
        if len(atts) < 2:
            continue
        a1, a2 = atts[0], atts[1]
        acc = x.get("accepted_attempt")
        s1, p1 = shape(a1["fields"]); s2, p2 = shape(a2["fields"])
        fb_classes = sorted({a1["fields"][n]["class"] for n in a1["failed_fields"]})
        def raw(a, i):
            if a.get("raw_content"):
                return "stored (attempts[].raw_content)"
            if acc == i and x.get("pass1_raw_content"):
                return "stored (pass1_raw_content, accepted attempt)"
            return "none — shape derived from codes"
        cases.append({
            "source": label, "paper_id": r["paper_id"], "row_attempt": r.get("attempt"),
            "a1_shape": s1, "a1_present_SIJ": [p1[c] for c in CLASSES], "a1_n_failed": a1["n_failed"],
            "a1_codes": codes(a1), "a1_raw": raw(a1, 1),
            "feedback_classes": fb_classes, "feedback_fields": len(a1["failed_fields"]),
            "feedback_chars": a2.get("feedback_chars"), "a2_request_hash": a2.get("request_hash"),
            "a2_shape": s2, "a2_present_SIJ": [p2[c] for c in CLASSES], "a2_n_failed": a2["n_failed"],
            "a2_codes": codes(a2), "a2_raw": raw(a2, 2),
            "accepted_attempt": acc, "row_outcome": r.get("outcome"),
            "terminal_states": dict(sorted(collections.Counter((x.get("terminal_states") or {}).values()).items()))
                               if isinstance(x.get("terminal_states"), dict) else x.get("terminal_states"),
        })


def tally(sel):
    t = collections.Counter(c["a2_shape"] for c in sel)
    return {"n": len(sel), "a2_shapes": dict(sorted(t.items())),
            "a2_stated_only": t["stated_only"],
            "a2_stated_only_with_raw": sum(1 for c in sel if c["a2_shape"] == "stated_only" and c["a2_raw"].startswith("stored")),
            "a2_stated_only_derived": sum(1 for c in sel if c["a2_shape"] == "stated_only" and not c["a2_raw"].startswith("stored")),
            "a2_n_failed_lt_a1": sum(1 for c in sel if c["a2_n_failed"] < c["a1_n_failed"]),
            "accepted_2": sum(1 for c in sel if c["accepted_attempt"] == 2)}


stated_fb = [c for c in cases if c["feedback_classes"] == ["stated"]]
other_fb = [c for c in cases if c["feedback_classes"] != ["stated"]]
totals = {"all": tally(cases), "after_stated_only_feedback": tally(stated_fb),
          "after_other_feedback": tally(other_fb),
          "per_source": {s: {"after_stated_only_feedback": tally([c for c in stated_fb if c["source"] == s]),
                             "after_other_feedback": tally([c for c in other_fb if c["source"] == s])}
                         for s in sources},
          "a1_stated_only_then_a2": [(c["source"], c["paper_id"], c["a2_shape"]) for c in cases if c["a1_shape"] == "stated_only"]}
print(json.dumps({"sources": sources, "totals": totals, "cases": cases}, indent=1, sort_keys=True))
