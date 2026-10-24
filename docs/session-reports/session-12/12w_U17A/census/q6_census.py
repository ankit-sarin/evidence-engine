"""Q6/Q8 census (12w-U17A). AST over tests/ (and engine/scripts/analysis for live-spec readers):
A. call sites constructing a spec model: ReviewSpec(...), ReviewSpec.model_validate(...), load_review_spec(...),
   and the model-declaring classes ScreeningModels / FTScreeningModels / ExtractionModels / AuditModels /
   PDFParsing / PDFQualityCheck / ArmSpec (direct construction or model_validate / model_copy).
B. string constants that look like spec YAML or dict fixtures carrying a model-declaring key.
C. files that name review_specs/surgical_autonomy.yaml (or the spec dir) or the live spec sha256 917c24de.
D. YAML files under tests/ that carry a model-declaring key.
Each hit is attributed to its enclosing test function (test id) when there is one."""
import ast, pathlib, re, collections
ROOT = pathlib.Path.cwd()
CLASSES = {"ReviewSpec","ScreeningModels","FTScreeningModels","ExtractionModels","AuditModels","PDFParsing","PDFQualityCheck","ArmSpec"}
KEYS = re.compile(r"(screening_models|ft_screening_models|extraction_models|auditor_model|\baudit:|pdf_parsing|pdf_quality_check|\barms:|\"arms\"|'arms')")
def enclosing(tree):
    par = {}
    for n in ast.walk(tree):
        for c in ast.iter_child_nodes(n): par[c] = n
    return par
def owner(n, par, rel):
    names = []
    while n in par:
        n = par[n]
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)): names.append(n.name)
    names.reverse()
    test = [x for x in names if x.startswith("test")]
    cls = [x for x in names if x.startswith("Test")]
    if test: return f"{rel}::" + "::".join(cls + test[:1])
    return f"{rel}::<module-or-helper:{'.'.join(names) or 'module'}>"
A, B, C = [], [], []
for top in ("tests", "engine", "scripts", "analysis"):
    for p in sorted((ROOT / top).rglob("*.py")):
        rel = str(p.relative_to(ROOT)); src = p.read_text(encoding="utf-8")
        tree = ast.parse(src); par = enclosing(tree)
        for n in ast.walk(tree):
            if top == "tests" and isinstance(n, ast.Call):
                f = n.func; nm = None
                if isinstance(f, ast.Name): nm = f.id
                elif isinstance(f, ast.Attribute):
                    if isinstance(f.value, ast.Name) and f.value.id in CLASSES: nm = f"{f.value.id}.{f.attr}"
                    elif f.attr in ("load_review_spec",): nm = f.attr
                if nm and (nm.split(".")[0] in CLASSES or nm == "load_review_spec"):
                    A.append((nm, owner(n, par, rel), n.lineno))
            if isinstance(n, ast.Constant) and isinstance(n.value, str):
                if top == "tests" and KEYS.search(n.value) and ("\n" in n.value or n.value in ("arms",)):
                    B.append((owner(n, par, rel), n.lineno, KEYS.search(n.value).group(1)))
                if "surgical_autonomy.yaml" in n.value or "917c24de" in n.value or n.value.rstrip("/") == "review_specs":
                    C.append((owner(n, par, rel), n.lineno, n.value[:90].replace("\n"," ")))
D = []
for p in sorted((ROOT / "tests").rglob("*.y*ml")):
    t = p.read_text(encoding="utf-8")
    if KEYS.search(t): D.append(str(p.relative_to(ROOT)))
print(f"# A. spec/model-declaration constructions in tests/: {len(A)} call sites")
by = collections.Counter(a[0] for a in A); print("#   by callee:", dict(by))
tests_A = sorted({a[1] for a in A}); files_A = sorted({a[1].split('::')[0] for a in A})
print(f"#   distinct owners (test ids or helpers): {len(tests_A)}; files: {len(files_A)}")
for a in A: print("A", a[0], a[1], a[2], sep="\t")
print(f"# B. string fixtures carrying a model-declaring key in tests/: {len(B)}")
for b in B: print("B", *b, sep="\t")
print(f"# C. readers of the live spec path or its sha256 (all trees): {len(C)}")
for c in C: print("C", *c, sep="\t")
print(f"# D. YAML files under tests/ with a model-declaring key: {len(D)}")
for d in D: print("D", d, sep="\t")
