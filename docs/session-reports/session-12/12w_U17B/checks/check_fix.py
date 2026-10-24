import subprocess, hashlib, sys, pathlib
run=lambda *a: subprocess.run(a,capture_output=True,text=True,check=True).stdout
G=pathlib.Path.home()/"scratch/12w-u17b/gate"
ok=True
def chk(n,c):
    global ok; ok&=bool(c); print(f"{n}: {bool(c)}")
# the tree as the gate saw it (HEAD + working-tree diff + untracked), recomputed now
now=subprocess.run(["bash",str(G/"tree_record.sh")],capture_output=True,text=True,check=True).stdout
# The aggregate line hashes untracked files apart from `git diff HEAD`, so staging the new test file
# changes it while no byte changes; the comparison is HEAD and every changed file's sha256 (12w-U17B 3g).
drop=lambda s: [l for l in s.splitlines() if not l.startswith("tree_diff_sha256 ")]
chk("HEAD and every changed file's sha256 == gate launch record", drop(now)==drop((G/"tree_at_launch.txt").read_text()))
chk("gate rc 0", (G/"gate_rc.txt").read_text().strip()=="0")
EXPECT=sorted(["CLAUDE.md","docs/architecture/models.md","docs/inventory/entry_points.json","docs/inventory/entry_points.md",
 "engine/core/review_spec.py","engine/core/run_manifest.py","review_specs/surgical_autonomy.yaml",
 "tests/test_cloud_extraction.py","tests/test_exporters.py","tests/test_run_fault_predicate.py","tests/test_u17_cross_lineage.py"])
staged=sorted(run("git","diff","--cached","--name-only").split())
chk(f"staged set == the fix's {len(EXPECT)} files", staged==EXPECT)
chk("nothing unstaged or untracked", run("git","status","--porcelain","--untracked-files=normal").count("\n")==len(EXPECT) and all(l[1]==" " for l in run("git","status","--porcelain").splitlines()))
d=run("git","diff","--cached","-U0","--","review_specs/surgical_autonomy.yaml").splitlines()
body=[l for l in d if l[:1] in "+-" and not l.startswith(("+++","---"))]
chk(f"live spec: {len(body)} changed lines, all added, one top-level key model_lineage", body and all(l.startswith("+") for l in body)
    and [l for l in body if l[1:2] not in (" ","#","")]==["+model_lineage:"])
chk("pins byte-identical to 12v_close", subprocess.run(["cmp",str(pathlib.Path.home()/"scratch/12w-u17b/pin_tuples_u17.json"),"docs/session-reports/session-12/12v_close/pin_tuples_close.json"]).returncode==0)
chk("codebook unchanged", hashlib.sha256(open("data/surgical_autonomy/extraction_codebook.yaml","rb").read()).hexdigest().startswith("89dbfa91"))
chk("inventory in sync", subprocess.run([".venv/bin/python","-m","engine.tools.inventory","--check"],capture_output=True).returncode==0)
print("live spec sha256", hashlib.sha256(open("review_specs/surgical_autonomy.yaml","rb").read()).hexdigest())
print("RESULT:","PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
