import os, re, subprocess, sys
H=os.path.expanduser("~"); S=f"{H}/scratch/12x-u39c"; WT=f"{S}/wt"; PY=f"{H}/projects/evidence-engine/.venv/bin/python"
M = {
 "M5": ("engine/agents/auditor.py",
        '    else:\n        verification_question = (\n            f"Does the extracted value accurately',
        '    elif field_type == "numeric":\n        verification_question = (\n            f"Does the snippet state this number exactly, with its unit?"\n        )\n'
        '    else:\n        verification_question = (\n            f"Does the extracted value accurately'),
 "M6": ("engine/core/effective_config.py",
        '                + build_audit_messages(span, field_type="categorical"))\n',
        '                + build_audit_messages(span, field_type="categorical")\n'
        '                + build_audit_messages(span, field_type="numeric"))\n'),
}
env = dict(os.environ, PYTHONPATH=".:tests")
run = lambda *a, **k: subprocess.run(a, cwd=WT, capture_output=True, text=True, **k)
assert run("git", "status", "--porcelain").stdout == ""
for tag, (path, old, new) in M.items():
    f = os.path.join(WT, path); src = open(f, encoding="utf-8").read()
    assert src.count(old) == 1, tag
    open(f, "w", encoding="utf-8").write(src.replace(old, new))
    diff = run("git", "diff").stdout
    r = run(PY, "-m", "pytest", "-p", "conftest", f"{S}/test_u39_runtime_equals_hash.py", "-v", "-rfE", "-p", "no:cacheprovider",
            "--rootdir=.", "-c", "pyproject.toml", f"--basetemp={S}/bt/{tag}", env=env, timeout=600)
    out = f"{S}/runs/2c_{tag}.txt"
    open(out, "w").write(diff + "\n==== pytest ====\n" + r.stdout + r.stderr)
    run("git", "checkout", "--", "engine"); st = run("git", "status", "--porcelain").stdout
    failed = re.findall(r"^FAILED \S*::(\S+)", r.stdout, re.M)
    E = [l for l in r.stdout.splitlines() if l.startswith("E   ")][:4]
    tail = r.stdout.strip().splitlines()[-1]
    print(f"{tag} {path}: rc {r.returncode}; failed {failed}; {tail}\n   " + "\n   ".join(E) + f"\n   revert: {'clean' if st == '' else st}")
    assert st == ""
