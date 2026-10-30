"""12aa-U20C — AST census of U20 commit 2's seams and the tests coupled to them. Read-only: AST only, no scanned
module is imported, no database is opened. usage, from the repository root: census_u20c.py   (stdout)
Classes (brief 3a, A3): (ii) a test file that rebinds the chat client and can drive it under a registered run;
(iii) the input_fit key-set tests; (iv) class-membership and per-class close-clause tests; (v) whole-function
replacements of the seams; (vi) preflight's error result; (vii) handlers above ollama_chat catching Exception
on a run path (listed, classed, not changed: C56)."""
import ast, re
from pathlib import Path
T = sorted(Path("tests").rglob("*.py"))
RUNISH = re.compile(r"\b(open_run|run_ft_invocation|run_pipeline|run_extraction|_open_run_manifest|open_screening_manifest|active_run|rm\.activate|activate\()")
REBIND = re.compile(r"""setattr\(\s*(oc|ollama_client|client_mod|engine\.utils\.ollama_client)\s*,\s*["']_client["']|ollama_client\._client["']|ollama_client\._client\b|oc\._client\s*=""")
print("## (ii) test files that rebind the chat client and can drive it under a run")
for p in T:
    s = p.read_text(encoding="utf-8")
    if not REBIND.search(s) or not RUNISH.search(s): continue
    tree = ast.parse(s); fakes = []
    for n in ast.walk(tree):
        if isinstance(n, ast.ClassDef):
            names = {m.name for m in n.body if isinstance(m, ast.FunctionDef)}
            if "chat" in names: fakes.append(f"{n.name}(ps={'ps' in names})")
    print(f"{p}: fakes {fakes}; ps patches {len(re.findall(r'_client.ps', s))}; digest_fn {len(re.findall(r'digest_fn', s))}")
for label, pat in (("(iii) input_fit key sets", r'estimate_low.*done_reason|"done_reason"|input_fit '),
                   ("(iv) membership and close clauses", r"RUN_REFUSED_MEMBERS|run_fault_classes|ModelDigestChanged|ServerEnvChanged|closes_aborted"),
                   ("(v) whole-function replacements", r"""["'](_check_input_was_read|_check_input_fits|check_call_digest|server_ps)["']|ollama_client\.(_check_input_was_read|check_call_digest|server_ps)|run_manifest\.check_call_digest"""),
                   ("(vi) preflight error results", r'status == "error"|error_message|check_model\(')):
    print(f"\n## {label}")
    for p in T:
        for i, l in enumerate(p.read_text(encoding="utf-8").splitlines(), 1):
            if re.search(pat, l): print(f"{p}:{i}: {l.strip()[:120]}")
print("\n## (vii) handlers above ollama_chat catching Exception, engine/ and scripts/")
funcs = {}
for root in ("engine", "scripts"):
    for p in sorted(Path(root).rglob("*.py")):
        src = p.read_text(encoding="utf-8"); tree = ast.parse(src)
        for n in ast.walk(tree):
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                calls = {getattr(c.func, "id", getattr(c.func, "attr", "")) for c in ast.walk(n) if isinstance(c, ast.Call)}
                funcs.setdefault(n.name, []).append((p, n, src, calls))
reach = {"ollama_chat"}; grew = True
while grew:
    grew = False
    for name, defs in funcs.items():
        if name not in reach and any(d[3] & reach for d in defs): reach.add(name); grew = True
seen = 0
for name, defs in sorted(funcs.items()):
    for p, n, src, _ in defs:
        for t in ast.walk(n):
            if not isinstance(t, ast.Try): continue
            body = {getattr(c.func, "id", getattr(c.func, "attr", "")) for s in t.body for c in ast.walk(s) if isinstance(c, ast.Call)}
            hit = sorted(body & reach)
            if not hit: continue
            for h in t.handlers:
                ty = ast.get_source_segment(src, h.type) if h.type else "bare"
                if not re.search(r"\b(Exception|BaseException)\b|bare", ty or ""): continue
                text = "\n".join(ast.get_source_segment(src, b) or "" for b in h.body)
                cls = "asks is_run_fault" if "is_run_fault" in text else ("re-raises" if re.search(r"^\s*raise\b", text, re.M) else "swallows")
                seen += 1
                print(f"{p}:{h.lineno} [{name}] except {ty}: {cls}; try body reaches {hit[:4]}")
print(f"# handlers {seen}; functions reaching ollama_chat by name {len(reach)}")
