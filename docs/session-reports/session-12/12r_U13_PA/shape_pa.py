"""12r-U13-PA — every Pass-1 attempt on both sides: is it STATED-only (every INFERABLE and JUDGMENT field
FIELD_MISSING, from the recorded codes), and, for attempt 2, the classes of the fields attempt 1 failed
(the fields its feedback block named). Read-only.
usage: shape_pa.py <ra_12q_dir> <ra_12g_copy_dir>   (stdout: JSON)"""
import json, os, sys
import yaml
out = {}
for side, d in (("12r", sys.argv[1]), ("12g", sys.argv[2])):
    cls = {f["name"]: f["field_class"] for f in yaml.safe_load(open(os.path.join(d, "extraction_codebook.yaml")))["fields"]}
    for line in open(os.path.join(d, "telemetry", "extraction_calls.jsonl")):
        r = json.loads(line); e = r.get("extra") or {}
        atts = e.get("attempts") or []
        for a in atts:
            missing = {n for n, f in a["fields"].items() if "FIELD_MISSING" in f["violations"]}
            non_stated = {n for n, c in cls.items() if c != "stated"}
            rec = {"stated_only": bool(non_stated) and non_stated <= missing, "n_failed": a["n_failed"]}
            if a["attempt"] == 2:
                rec["attempt1_failed_classes"] = sorted({cls[n] for n in atts[0]["failed_fields"]})
            out.setdefault(side, {}).setdefault(str(r["paper_id"]), {})[str(a["attempt"])] = rec
summary = {}
for side, ps in out.items():
    s = {"attempt1_stated_only": [p for p, a in ps.items() if a["1"]["stated_only"]],
         "attempt2_stated_only": [p for p, a in ps.items() if a.get("2", {}).get("stated_only")],
         "attempt2_after_stated_only_feedback": [p for p, a in ps.items() if a.get("2", {}).get("attempt1_failed_classes") == ["stated"]]}
    summary[side] = s
print(json.dumps({"summary": summary, "attempts": out}, indent=1, sort_keys=True))
