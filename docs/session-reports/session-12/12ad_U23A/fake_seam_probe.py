"""12ad-U23A §5 — where HEAD's spec path, data root and git_state resolve after a change of working directory. No server, no network, no
service manager, no active run, and NO database opened: ReviewDatabase is never constructed (its path arithmetic is taken from
tests/_live_db_guard.py's mirror, which opens nothing). subprocess.run is wrapped so that only `git` may run. The live spec and codebook are
read as files; two scratch trees are written under argv[1]. usage, from the repository root: fake_seam_probe.py <scratch dir>"""
import hashlib, importlib.util, os, re, subprocess, sys
from pathlib import Path
os.environ["EVIDENCE_ENGINE_NO_OLLAMA_RESTART"] = "1"
START = Path.cwd(); sys.path.insert(0, str(START))
_run = subprocess.run
def only_git(argv, *a, **k):
    if argv[0] != "git": raise SystemExit(f"refused: {argv[0]}")
    return _run(argv, *a, **k)
subprocess.run = only_git
from engine.core import database as db_mod, review_paths as rp, run_manifest as rm, parsed_text as pt, codebook as cb
sys.path.insert(0, str(START / "tests"))
from _live_db_guard import resolved_review_db_path
RID = "surgical_autonomy"; SCR = Path(sys.argv[1]).resolve(); SCR.mkdir(parents=True, exist_ok=True)
LIVE_SPEC, LIVE_CB = (START / "review_specs" / f"{RID}.yaml"), (START / "data" / RID / "extraction_codebook.yaml")
def show(label):
    s, d = rp.spec_path_for(RID), rp.data_root_for(RID); g = rm.git_state()
    print(f"\n[{label}] cwd = {Path.cwd()}")
    print(f"  spec_path_for -> {s!s}  resolved {s.resolve()}; under REPO_ROOT {s.resolve().is_relative_to(rm.REPO_ROOT)}")
    print(f"  data_root_for -> {d!s}  resolved {d.resolve()}; under REPO_ROOT {d.resolve().is_relative_to(rm.REPO_ROOT)}")
    print(f"  ReviewDatabase's path arithmetic (the guard's mirror, no open) -> {resolved_review_db_path(RID, None, db_mod.DATA_ROOT)}")
    print(f"  git_state() REPO_ROOT {rm.REPO_ROOT}; commit {g.commit}; dirty {g.dirty}")
    print(f"  ACCEPTANCE ASSERTION (both roots under git_state's root): {s.resolve().is_relative_to(rm.REPO_ROOT) and d.resolve().is_relative_to(rm.REPO_ROOT)}")
print("-- constants as imported")
print(f"SPEC_ROOT {rp.SPEC_ROOT!r} absolute {rp.SPEC_ROOT.is_absolute()}; DATA_ROOT {db_mod.DATA_ROOT!r} absolute {db_mod.DATA_ROOT.is_absolute()}; review_paths.DATA_ROOT is database.DATA_ROOT {rp.DATA_ROOT is db_mod.DATA_ROOT}")
m021s = importlib.util.spec_from_file_location("m021", START / "engine/migrations/021_parsed_text_sha256.py"); m021 = importlib.util.module_from_spec(m021s); m021s.loader.exec_module(m021)
inv = importlib.util.spec_from_file_location("inv", START / "engine/tools/inventory.py"); invm = importlib.util.module_from_spec(inv); inv.loader.exec_module(invm)
print(f"the four REPO_ROOTs equal: run_manifest {rm.REPO_ROOT}, parsed_text {pt.REPO_ROOT}, 021 {m021.REPO_ROOT}, inventory {invm.REPO_ROOT}: {len({rm.REPO_ROOT, pt.REPO_ROOT, m021.REPO_ROOT, invm.REPO_ROOT}) == 1}")
show("A. the repository root (the launch R350 names)")
spec = rp.load_spec_for(RID); print(f"  load_spec_for loads; title {spec.title!r}; spec_hash {rm.spec_hash(spec)[:16]}…")
cbk = cb.load_codebook_for(RID); print(f"  load_codebook_for path {cbk.path}; sha256 {cbk.sha256[:16]}…")
live_hash, live_cb = rm.spec_hash(spec), cbk.sha256
e = SCR / "empty"; e.mkdir(exist_ok=True); os.chdir(e)
show("B. a directory with no review_specs/ (after import: the chdir is at run time)")
try: rp.load_spec_for(RID); print("  load_spec_for loaded")
except Exception as x: print(f"  load_spec_for raises {type(x).__name__}: {x}")
try: cb.load_codebook_for(RID); print("  load_codebook_for loaded")
except Exception as x: print(f"  load_codebook_for raises {type(x).__name__}: {str(x)[:160]}")
s2 = rp.load_spec_for(RID, override=LIVE_SPEC)
print(f"  --spec override (absolute live spec) loads, title {s2.title!r}, spec_hash equal to live {rm.spec_hash(s2) == live_hash}; data_root_for still {rp.data_root_for(RID).resolve()}")
print(f"  data_root= given: {resolved_review_db_path(RID, SCR / 'elsewhere', db_mod.DATA_ROOT)} (does not follow the cwd; a relative data_root would)")
print(f"  resolve_stored_path('data/x/parsed_text/1_v1.md') -> {pt.resolve_stored_path('data/x/parsed_text/1_v1.md')} (R100: REPO_ROOT, not the cwd)")
print(f"  canonical_path(abspath('data/x/a.md')) -> {pt.canonical_path(os.path.abspath('data/x/a.md'))} (outside the root: absolute)")
print(f"  directories created under the cwd by these calls: {sorted(p.name for p in e.iterdir())}")
c = SCR / "copy"; (c / "review_specs").mkdir(parents=True, exist_ok=True); (c / "data" / RID).mkdir(parents=True, exist_ok=True)
t = LIVE_SPEC.read_text(encoding="utf-8"); t2, n = re.subn(r'^title: .*$', 'title: "A DIFFERENT SPEC, FROM ANOTHER TREE"', t, count=1, flags=re.M); assert n == 1
(c / "review_specs" / f"{RID}.yaml").write_text(t2, encoding="utf-8")
(c / "data" / RID / "extraction_codebook.yaml").write_text(LIVE_CB.read_text(encoding="utf-8") + "\n# another tree\n", encoding="utf-8")
os.chdir(c)
show("C. another tree with its own review_specs/<id>.yaml and data/<id>/")
s3 = rp.load_spec_for(RID); c3 = cb.load_codebook_for(RID)
print(f"  load_spec_for loads with no error, title {s3.title!r}; spec_hash equal to the engine checkout's {rm.spec_hash(s3) == live_hash}")
print(f"  load_codebook_for path {c3.path} resolved {Path(c3.path).resolve()}; sha256 equal to the engine checkout's {c3.sha256 == live_cb}; identity check passed (its review: is {c3.review!r})")
g = rm.git_state(); print(f"  the manifest would record commit {g.commit} dirty {g.dirty} (from {rm.REPO_ROOT}), spec_hash and codebook_sha256 of the other tree, and no path")
os.chdir(START)
print("\n-- git_state on a directory that is not a repository (a copy without .git)")
g2 = rm.git_state(e); print(f"  commit {g2.commit!r}; dirty {g2.dirty}; tag {g2.tag}")
print("\n-- back at the repository root: same objects, resolved afresh")
print(f"  SPEC_ROOT {rp.SPEC_ROOT!r} resolved {rp.SPEC_ROOT.resolve()}")
