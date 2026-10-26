"""12x-AQA A4 driver: O0 (no change), O1, O2 each applied alone in the worktree, measured,
reverted (git checkout -- engine; clean status recorded)."""
import json, os, re, subprocess
H = os.path.expanduser("~"); S = f"{H}/scratch/12x-aqa"; WT = f"{S}/wt"; OUT = f"{S}/a4"
PY = f"{H}/projects/evidence-engine/.venv/bin/python"; EC = "engine/core/effective_config.py"
OLD = ('        return (build_audit_messages(span, field_type="text")\n'
       '                + build_audit_messages(span, field_type="categorical"))\n')
OPTS = {
 "O0": None,
 "O1": ('        from engine.core.codebook import VALID_FIELD_TYPES\n'
        '        return [m for t in VALID_FIELD_TYPES for m in build_audit_messages(span, field_type=t)]\n'),
 "O2": ('        return (build_audit_messages(span, field_type="<field_type>")\n'
        '                + build_audit_messages(span, field_type="categorical"))\n'),
}
env = dict(os.environ, PYTHONPATH=".")
def run(*a, **k): return subprocess.run(a, cwd=WT, capture_output=True, text=True, **k)
assert run("git", "status", "--porcelain").stdout == ""
summary = {}
for name, new in OPTS.items():
    f = os.path.join(WT, EC); src = open(f, encoding="utf-8").read()
    if new is not None:
        assert src.count(OLD) == 1; open(f, "w", encoding="utf-8").write(src.replace(OLD, new))
    diff = run("git", "diff").stdout; open(f"{OUT}/{name}.diff", "w").write(diff)
    stat = run("git", "diff", "--numstat").stdout.strip()
    m = run(PY, f"{S}/a4/measure.py", f"{OUT}/{name}_measure.json", WT, env=env)
    meas = json.loads(open(f"{OUT}/{name}_measure.json").read()) if m.returncode == 0 else {"error": m.stderr[-600:]}
    t = run(PY, "-m", "pytest", "tests/test_u39_runtime_equals_hash.py::test_audit_runtime_equals_hash", "-v", "-rfE",
            "-p", "no:cacheprovider", f"--basetemp={S}/bt/{name}", env=env, timeout=600)
    open(f"{OUT}/{name}_u39_audit.txt", "w").write(t.stdout + t.stderr)
    e = re.findall(r"^E\s+(.*)$", t.stdout, re.M)
    p = run(PY, "docs/session-reports/session-12/12l_U04/pin_tuples.py", "review_specs/surgical_autonomy.yaml",
            "data/surgical_autonomy/extraction_codebook.yaml", env=env)
    pins = json.loads(p.stdout) if p.returncode == 0 else None
    run("git", "checkout", "--", "engine")
    status = run("git", "status", "--porcelain").stdout
    summary[name] = {"numstat": stat, "measure": meas, "u39_audit_rc": t.returncode, "u39_first_E": e[:2],
                     "pins": None if pins is None else {"elicited": pins["elicited"]["pin"], "non_elicited": pins["non_elicited"]["pin"]},
                     "status_after_revert": status or "clean"}
    print(name, json.dumps(summary[name], indent=1), flush=True)
    assert status == ""
json.dump(summary, open(f"{OUT}/summary.json", "w"), indent=1)
