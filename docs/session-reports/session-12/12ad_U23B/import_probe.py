"""12ad-U23B §2 — the import closure at HEAD (I4). Each case in a fresh interpreter, from the repository root; prints the engine.* modules in sys.modules after each import, in
sys.modules order (importlib re-inserts a package when it finishes, so this is not strictly start order). No server, no network, no service manager, no database opened: only imports run, and none of these modules opens a database at import. The opt-out is set as
12ad_U23A/fake_seam_probe.py set it. usage, from the repository root: import_probe.py"""
import json, os, subprocess, sys
ENV = dict(os.environ, EVIDENCE_ENGINE_NO_OLLAMA_RESTART="1")
CHILD = r"""
import importlib, json, sys
sys.path.insert(0, ".")
out = []
for m in sys.argv[1:]:
    before = [k for k in sys.modules if k == "engine" or k.startswith("engine.")]
    importlib.import_module(m)
    after = [k for k in sys.modules if k == "engine" or k.startswith("engine.")]
    out.append({"import": m, "new": [k for k in after if k not in before], "total": len(after), "file": sys.modules[m].__file__})
print(json.dumps(out))
"""
CASES = [
    ("the five modules of I4, in turn, one interpreter", ["engine.core.database", "engine.core.review_paths", "engine.core.run_manifest", "engine.core.parsed_text", "engine.tools.inventory"]),
    ("engine.core.database alone", ["engine.core.database"]),
    ("engine.core alone (what a leaf under it pulls in besides itself)", ["engine.core"]),
    ("engine.utils.background alone", ["engine.utils.background"]),
    ("engine.adjudication.categorizer alone", ["engine.adjudication.categorizer"]),
    ("engine.tools.inventory alone", ["engine.tools.inventory"]),
    ("engine.core.run_manifest alone", ["engine.core.run_manifest"]),
]
for label, mods in CASES:
    r = subprocess.run([sys.executable, "-c", CHILD, *mods], capture_output=True, text=True, env=ENV)
    print(f"\n== {label}: exit {r.returncode}")
    if r.returncode:
        print(r.stderr[-1500:]); continue
    for x in json.loads(r.stdout):
        print(f"-- import {x['import']}: {len(x['new'])} new, {x['total']} engine.* in sys.modules")
        print("   " + " ".join(x["new"]))
