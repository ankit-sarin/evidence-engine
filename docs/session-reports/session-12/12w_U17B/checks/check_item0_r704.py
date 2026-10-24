import subprocess, hashlib, sys, importlib.util
P="docs/plan/ENGINE_REFACTOR_PLAN.md"; R="docs/session-reports/session-12/12w_U17B/r704.txt"
spec=importlib.util.spec_from_file_location("rc","docs/session-reports/session-12/12q_close/render_cell.py")
rc=importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
run=lambda *a: subprocess.run(a,capture_output=True,text=True,check=True).stdout
ok=True
def chk(name,cond):
    global ok; ok&=bool(cond); print(f"{name}: {bool(cond)}")
add,rem,_=run("git","diff","--cached","--numstat","--",P).split()
r=open(R,encoding="utf-8").read(); rb=r.encode()
chk(f"numstat + {add} / - {rem}; minus == 0", rem=="0")
plan=open(P,encoding="utf-8").read(); n=plan.count("\n")
chk(f"plan lines {n} == 9834 + {add}", n==9834+int(add))
chk("r704 sha256 == architect", hashlib.sha256(rb).hexdigest()=="f4715865907f0738fce546679e975c00be446f3ffc56dfb4423b50157b932809" and len(rb)==3746 and r.count("\n")==48)
blk=plan[plan.index("**Block 102.**"):]
fence=blk[blk.index("```text\n")+8:blk.index("\n```\n")+1]
chk("fence == r704.txt", fence==r)
row=[l for l in blk.splitlines() if l.startswith("| 2026-10-24 | R704(a)")]
cell=row[0][len("| 2026-10-24 | "):row[0].rindex(" | PI, ")] if len(row)==1 else None
chk("cell == render_cell", cell==rc.render_cell(r,"R704"))
chk("last line ends at R704", plan.splitlines()[-1]=="The decision log in this file ends at R704.")
chk("'ends at R703.' line still present once", plan.count("The decision log in this file ends at R703.\n")==1)
chk("Block 102 count 1", plan.count("**Block 102.**")==1)
staged=sorted(run("git","diff","--cached","--name-only").split())
chk(f"staged set {staged}", staged==sorted([P,R]))
chk("worktree == index", run("git","diff","--name-only")=="")
pb=open(P,"rb").read(); print("plan sha256", hashlib.sha256(pb).hexdigest(), len(pb), "bytes")
print("RESULT:", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
