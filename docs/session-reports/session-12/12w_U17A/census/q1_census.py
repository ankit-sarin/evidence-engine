"""Q1 census (12w-U17A): AST over engine/ scripts/ analysis/ tests/.
(1) every class field, attribute, keyword, dict key, string constant or SQL column whose name
    matches family|vendor|lineage|base_model|architecture (word-ish match, case-insensitive);
(2) every pydantic field in review_spec.py whose name contains 'model' or names a model role;
(3) the schema columns of run_manifests / run_stage_configs / arms (from the live db, mode=ro)."""
import ast, pathlib, re, sqlite3, sys, json
ROOT = pathlib.Path.cwd()
PAT = re.compile(r"(famil|vendor|lineage|base_model|basemodel|architecture)", re.I)
hits = []
for top in ("engine", "scripts", "analysis", "tests"):
    for p in sorted((ROOT / top).rglob("*.py")):
        try:
            tree = ast.parse(p.read_text(encoding="utf-8"))
        except SyntaxError as e:
            hits.append((str(p.relative_to(ROOT)), 0, "SYNTAX", str(e))); continue
        for n in ast.walk(tree):
            name = None; kind = None
            if isinstance(n, ast.Name): name, kind = n.id, "name"
            elif isinstance(n, ast.Attribute): name, kind = n.attr, "attr"
            elif isinstance(n, ast.arg): name, kind = n.arg, "arg"
            elif isinstance(n, ast.keyword) and n.arg: name, kind = n.arg, "kwarg"
            elif isinstance(n, (ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef)): name, kind = n.name, "def"
            elif isinstance(n, ast.Constant) and isinstance(n.value, str): name, kind = n.value, "str"
            if name and PAT.search(name):
                hits.append((str(p.relative_to(ROOT)), getattr(n, "lineno", 0), kind, name[:160].replace("\n", " ")))
print(f"# (1) identifier/string hits for {PAT.pattern}: {len(hits)}")
for h in hits: print("\t".join(map(str, h)))
# (2) review_spec fields
src = (ROOT / "engine/core/review_spec.py").read_text(encoding="utf-8")
tree = ast.parse(src)
print("# (2) review_spec.py class fields (all, per class)")
for c in [n for n in tree.body if isinstance(n, ast.ClassDef)]:
    fields = [s.target.id for s in c.body if isinstance(s, ast.AnnAssign) and isinstance(s.target, ast.Name)]
    print(f"{c.name}({', '.join(ast.unparse(b) for b in c.bases)}): {fields}")
# (3) schema
c = sqlite3.connect(f"file:{ROOT/'data/surgical_autonomy/review.db'}?mode=ro", uri=True)
print("# (3) live schema columns (mode=ro)")
for t in ("run_manifests", "run_stage_configs", "arms"):
    print(t, [r[1] for r in c.execute(f"PRAGMA table_info({t})")])
