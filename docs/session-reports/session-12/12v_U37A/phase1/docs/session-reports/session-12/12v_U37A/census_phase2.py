"""12v-U37A step 2e — every test that could pin the feedback block's text, the attempt-2 user turn, or the
elicited pin value. Two passes, both read-only: (1) AST: test functions whose body names one of the
builders below; (2) grep: test lines carrying one of the block's distinctive strings or a pin/prompt-hash
literal. usage, from the repository root: census_phase2.py   (stdout: JSON)"""
import ast, glob, json, re
NAMES = {"build_feedback_block", "sentinel_feedback_block", "sentinel_pass1_result", "pass1_prompt",
         "sentinel_pass1_prompt", "_FEEDBACK_SENTINEL", "FEEDBACK_ECHO_CAP", "FEEDBACK_TRUNCATION_MARKER",
         "_requirement", "elicit", "run_pass1", "render_messages", "prompt_hash", "pin_tuple"}
STRINGS = ["re-emit EVERY field", "did not meet the contract", "full replacement response", "Fix these",
           "Emit exactly one entry per field", "Each one below shows", "8f0aeecd", "d3672ab6", "77ae99d8",
           "elicitation_pass1"]
out = {"ast": {}, "grep": {}}
for f in sorted(glob.glob("tests/**/*.py", recursive=True)):
    src = open(f, encoding="utf-8").read()
    try:
        tree = ast.parse(src)
    except SyntaxError:
        continue
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name.startswith("test"):
            used = {x.id for x in ast.walk(n) if isinstance(x, ast.Name)} | {x.attr for x in ast.walk(n) if isinstance(x, ast.Attribute)}
            hit = sorted(used & NAMES)
            if hit:
                out["ast"][f"{f}::{n.name}"] = hit
    for i, line in enumerate(src.split("\n"), 1):
        for s in STRINGS:
            if s in line:
                out["grep"].setdefault(f, []).append([i, s])
print(json.dumps(out, indent=1, sort_keys=True))
