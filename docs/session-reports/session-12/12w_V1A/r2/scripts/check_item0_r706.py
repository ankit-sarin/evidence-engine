import subprocess, hashlib, sys, json, importlib.util
P="docs/plan/ENGINE_REFACTOR_PLAN.md"; D="docs/session-reports/session-12/12w_V1A/"; R=D+"r706.txt"
spec=importlib.util.spec_from_file_location("rc","docs/session-reports/session-12/12q_close/render_cell.py")
rc=importlib.util.module_from_spec(spec); spec.loader.exec_module(rc)
run=lambda *a: subprocess.run(a,capture_output=True,text=True,check=True).stdout
ok=True
def chk(name,cond):
    global ok; ok&=bool(cond); print(f"{name}: {bool(cond)}")
add,rem,_=run("git","diff","--cached","--numstat","--",P).split()
chk(f"numstat + {add} / - {rem}; minus == 1", rem=="1")
diff=run("git","diff","--cached","-U0","--",P).splitlines()
minus=[l[1:] for l in diff if l.startswith("-") and not l.startswith("---")]
plus=[l[1:] for l in diff if l.startswith("+") and not l.startswith("+++")]
chk("the one removed line is INT-v12-1's row", len(minus)==1 and minus[0].startswith("| INT-v12-1 | "))
newrow=[l for l in plus if l.startswith("| INT-v12-1 | ")]
chk("its replacement keeps the old row as a strict prefix (less the final ' |') and ends ' |'",
    len(newrow)==1 and newrow[0].startswith(minus[0][:-2]) and newrow[0].endswith(" |") and len(newrow[0])>len(minus[0]))
note=newrow[0][len(minus[0])-2:] if newrow else ""
chk("the note names MEASURED (12w-V1A Q2, `a0f2323`)", "MEASURED (12w-V1A Q2, `a0f2323`)" in note)
ev={v: (json.load(open(D+f"pins/pin_{v}.json")), json.load(open(D+f"captures/capture_{v}.json"))) for v in ("head","V1","V2","V3")}
ab=lambda h: f"`{h[:8]}…{h[-4:]}`"
want=[ab(ev["V1"][0]["elicited"]["pin"]), ab(ev["V2"][0]["elicited"]["pin"]), ab(ev["head"][0]["non_elicited"]["pin"]),
      ab(ev["V1"][1]["attempt_2"]["request_kwargs_sha256"]), ab(ev["head"][1]["attempt_2"]["request_kwargs_sha256"]),
      "test_the_elicited_requests_are_byte_identical", "test_elicitation_pass1_hashes_both_attempts"]
chk(f"note carries {want}", all(w in note for w in want))
r=open(R,encoding="utf-8").read(); rb=r.encode()
chk("r706 sha256 == architect", hashlib.sha256(rb).hexdigest()=="4a56d094fb8986c2b312611f4d63a34ae046acb8de06f445489e1eac414a22de" and len(rb)==4008 and r.count("\n")==49)
plan=open(P,encoding="utf-8").read()
blk=plan[plan.index("**Block 104.**"):]
chk("fence == r706.txt", blk[blk.index("```text\n")+8:blk.index("\n```\n")+1]==r)
row=[l for l in blk.splitlines() if l.startswith("| 2026-10-25 | R706(a)")]
cell=row[0][len("| 2026-10-25 | "):row[0].rindex(" | PI, ")] if len(row)==1 else None
chk("cell == render_cell", cell==rc.render_cell(r,"R706"))
chk("last line ends at R706", plan.splitlines()[-1]=="The decision log in this file ends at R706.")
chk("Block 104 count 1", plan.count("**Block 104.**")==1)
staged=sorted(run("git","diff","--cached","--name-only").split())
chk(f"staged set {staged}", staged==sorted([P,R]))
chk("worktree == index", run("git","diff","--name-only")=="")
print("RESULT:", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
