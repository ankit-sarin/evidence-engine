"""12y-U18A 3h: static import closure (AST, absolute imports only) from the Run 7 entry points;
reports any analysis.* module reached. Read-only."""
import ast, os
os.chdir(os.path.expanduser("~/projects/evidence-engine"))
roots = ["scripts/run_pipeline.py", "engine/agents/extractor.py", "engine/agents/ft_screener.py",
         "engine/agents/auditor.py", "engine/utils/ollama_preflight.py", "engine/parsers/pdf_parser.py"]
def mod_path(m):
    p = m.replace(".", "/")
    for c in (p + ".py", p + "/__init__.py"):
        if os.path.exists(c): return c
seen, stack = set(), list(roots)
while stack:
    f = stack.pop()
    if f in seen: continue
    seen.add(f)
    for n in ast.walk(ast.parse(open(f).read())):
        names = []
        if isinstance(n, ast.Import): names = [a.name for a in n.names]
        elif isinstance(n, ast.ImportFrom) and n.module and n.level == 0:
            names = [n.module] + [n.module + "." + a.name for a in n.names]
        for m in names:
            if m.split(".")[0] in ("engine", "analysis", "scripts"):
                p = mod_path(m)
                if p: stack.append(p)
print("roots:", roots); print("modules reached:", len(seen))
print("analysis modules reached:", sorted(s for s in seen if s.startswith("analysis/")))
for f in ["analysis/eval/run_qualgap01.py", "analysis/eval/qualgap01.py", "analysis/eval/run_capture01.py",
          "analysis/eval/elicit01/runner.py", "analysis/paper1/judge.py", "engine/utils/ollama_client.py",
          "engine/utils/ollama_preflight.py", "engine/core/run_manifest.py", "engine/core/effective_config.py"]:
    print(f, "reached:", f in seen)
