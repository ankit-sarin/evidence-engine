"""12v-U38A step 2d — every existing test that touches the extractor's snippet precheck, its regex, the
snippet retry's exhaustion, or the "" replacement. Read-only. Two passes: (1) AST: test functions whose
body names one of NAMES; (2) grep: test lines carrying one of STRINGS.
usage, from the repository root: census_u38.py   (stdout: JSON)"""
import ast, glob, json
NAMES = {"_has_invalid_snippet", "INVALID_SNIPPET_RE", "_INVALID_SNIPPET_RE", "_validate_and_retry_snippets",
         "_retry_snippet", "SNIPPET_MAX_RETRIES", "retry_snippet_messages"}
STRINGS = ["_has_invalid_snippet", "INVALID_SNIPPET_RE", "_validate_and_retry_snippets", "_retry_snippet",
           "SNIPPET_MAX_RETRIES", 'source_snippet == ""', "extract_retry_snippet"]
out = {"ast": {}, "grep": {}}
for f in sorted(glob.glob("tests/**/*.py", recursive=True)):
    src = open(f, encoding="utf-8").read()
    try:
        tree = ast.parse(src)
    except SyntaxError:
        continue
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name.startswith("test"):
            used = {x.id for x in ast.walk(n) if isinstance(x, ast.Name)} | \
                   {x.attr for x in ast.walk(n) if isinstance(x, ast.Attribute)} | \
                   {x.value for x in ast.walk(n) if isinstance(x, ast.Constant) and isinstance(x.value, str)}
            hit = sorted(used & NAMES)
            if hit:
                out["ast"][f"{f}::{n.name}"] = hit
    for i, line in enumerate(src.split("\n"), 1):
        for s in STRINGS:
            if s in line:
                out["grep"].setdefault(f, []).append([i, s])
print(json.dumps(out, indent=1, sort_keys=True))
