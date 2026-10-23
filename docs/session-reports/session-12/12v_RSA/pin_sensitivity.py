"""12v-RSA Q3 — which pin value moves under each kind of change, shown by construction, in memory.
Each variant monkeypatches one engine name inside a fresh interpreter, then runs
12l_U04/pin_tuples.py unchanged (pure recompute: digests fixed, fetch_model_digest refused, no
model call, no file written). No engine file is edited. A variant that moves nothing is evidence
that the pin cannot see that change.
usage, from the repository root: pin_sensitivity.py <spec.yaml> <codebook.yaml>   (stdout: JSON)
Variants:
  baseline              — nothing patched
  feedback_header       — build_feedback_block's output with "re-emit EVERY field in the schema" reworded
                          (patched where pipeline imports it, the name elicit() and the sentinel call)
  feedback_requirement  — the STATED VALUE_WITHOUT_CITATION requirement line reworded (prompts._requirement)
  precheck_never        — extractor._has_invalid_snippet always False (INT-s12-1's precheck disabled)
  invalid_snippet_re    — constants.INVALID_SNIPPET_RE replaced by a pattern that matches nothing"""
import json, os, re, subprocess, sys

PIN = "docs/session-reports/session-12/12l_U04/pin_tuples.py"
PATCH = {
    "baseline": "",
    "feedback_header": (
        "import engine.elicitation.pipeline as PL\n"
        "_orig = PL.build_feedback_block\n"
        "PL.build_feedback_block = lambda *a, **k: _orig(*a, **k).replace("
        "'re-emit EVERY field in the schema', 're-emit EVERY field in the schema, all classes')\n"),
    "feedback_requirement": (
        "import engine.elicitation.prompts as P\n"
        "_orig = P._requirement\n"
        "P._requirement = lambda code, cls, esc, n: _orig(code, cls, esc, n) + (' (x)' if cls == 'stated' else '')\n"),
    "precheck_never": (
        "import engine.agents.extractor as EX\n"
        "EX._has_invalid_snippet = lambda *a, **k: False\n"),
    "invalid_snippet_re": (
        "import re, engine.core.constants as K\n"
        "K.INVALID_SNIPPET_RE = re.compile(r'(?!x)x')\n"
        "import engine.agents.extractor as EX\n"
        "if hasattr(EX, 'INVALID_SNIPPET_RE'): EX.INVALID_SNIPPET_RE = K.INVALID_SNIPPET_RE\n"),
}
spec, cb = sys.argv[1], sys.argv[2]
out = {}
for name, patch in PATCH.items():
    code = ("import sys, runpy\nsys.path.insert(0, '.')\n" + patch +
            f"sys.argv = ['pin_tuples.py', {spec!r}, {cb!r}]\nrunpy.run_path({PIN!r}, run_name='__main__')\n")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    if r.returncode:
        out[name] = {"error": r.stderr[-800:]}; continue
    d = json.loads(r.stdout)
    out[name] = {lab: {"pin": d[lab]["pin"], "prompt_hashes": d[lab]["prompt_hashes"]} for lab in ("elicited", "non_elicited")}
base = out["baseline"]
summary = {}
for name, v in out.items():
    if name == "baseline" or "error" in v:
        continue
    summary[name] = {lab: {"pin_moved": v[lab]["pin"] != base[lab]["pin"],
                           "stages_moved": sorted(s for s, h in v[lab]["prompt_hashes"].items()
                                                  if h != base[lab]["prompt_hashes"].get(s))}
                     for lab in ("elicited", "non_elicited")}
print(json.dumps({"baseline_pins": {lab: base[lab]["pin"] for lab in ("elicited", "non_elicited")},
                  "summary": summary, "variants": out}, indent=1, sort_keys=True))
