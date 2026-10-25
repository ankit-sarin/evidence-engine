import subprocess, hashlib, sys, importlib.util
P="docs/plan/ENGINE_REFACTOR_PLAN.md"; R="docs/session-reports/session-12/12w_V1A/r705.txt"
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
chk(f"plan lines {n} == 9895 + {add}", n==9895+int(add))
chk("r705 sha256 == architect", hashlib.sha256(rb).hexdigest()=="31cbe8c3c87c74aa08726bfae765d88c224162880916672168e3ce4fd73e287f" and len(rb)==3703 and r.count("\n")==47)
blk=plan[plan.index("**Block 103.**"):]
fence=blk[blk.index("```text\n")+8:blk.index("\n```\n")+1]
chk("fence == r705.txt", fence==r)
row=[l for l in blk.splitlines() if l.startswith("| 2026-10-25 | R705(a)")]
cell=row[0][len("| 2026-10-25 | "):row[0].rindex(" | PI, ")] if len(row)==1 else None
chk("cell == render_cell", cell==rc.render_cell(r,"R705"))
chk("last line ends at R705", plan.splitlines()[-1]=="The decision log in this file ends at R705.")
chk("'ends at R704.' line still present once", plan.count("The decision log in this file ends at R704.\n")==1)
chk("Block 103 count 1", plan.count("**Block 103.**")==1)
staged=sorted(run("git","diff","--cached","--name-only").split())
chk(f"staged set {staged}", staged==sorted([P,R]))
chk("worktree == index", run("git","diff","--name-only")=="")
pb=open(P,"rb").read(); print("plan sha256", hashlib.sha256(pb).hexdigest(), len(pb), "bytes")
print("RESULT:", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
