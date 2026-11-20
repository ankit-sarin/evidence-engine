"""12ai-CLNA-R1 R4 check, written for this resume (R794(i)). Read-only on the repository. Run from the repo root after `git add` of the
exact paths; exit 0 only if every assertion holds. usage: python3 -I -B check_r1.py <OUT> <copy map tsv>"""
import hashlib, json, subprocess, sys
from pathlib import Path
OUT = Path(sys.argv[1]); MAP = [l.split("\t") for l in Path(sys.argv[2]).read_text().splitlines() if l]
D = "docs/session-reports/session-12"; P = "docs/plan/ENGINE_REFACTOR_PLAN.md"; RF = f"{D}/12ai_open/r822.txt"
RES = f"{D}/12ai_CLNA/brief_12ai_CLNA_R1.md"
RULES = {"docs/plan/STANDING_RULES.md": "d56dbdaeafb3659527cb77e1e81744eb01dadbe871fd96944104bb9e01b7e3ff",
         "docs/plan/standing_rules_ids.txt": "a28da0611e1a4d62cbc8185ff89df22d2df7a2a37340da0eb164dec678e75257",
         "docs/plan/standing_rules_glosses.json": "7f7374b25f7efd2c1df97cfbe3fb9b828c7fec5bce4e948d751cec08a4fa72ed"}
run = lambda *a: subprocess.run(list(a), capture_output=True, text=True, check=True).stdout
raw = lambda s: subprocess.run(["git", "show", s], capture_output=True, check=True).stdout
sha = lambda b: hashlib.sha256(b).hexdigest()
fails = []
def ok(c, m): print(("PASS " if c else "FAIL ") + m); c or fails.append(m)
st = {tuple(l.split("\t")) for l in run("git", "diff", "--cached", "--name-status").splitlines()}
want = {("M", P), ("A", RF), ("A", RES)} | {("A", d) for d, s in MAP}
ok(st == want, f"staged set exactly the plan, r822.txt, the resume file and {len(MAP)} copies: {len(st)} files (missing {sorted(want - st)}; extra {sorted(st - want)})")
ok(not any("/clone/" in p or p.endswith("/clone") for _, p in st), "no staged path under a directory named clone")
ok(run("git", "diff", "--name-only") == "" and run("git", "ls-files", "--others", "--exclude-standard") == "", "nothing unstaged, nothing untracked")
bad = [d for d, s in MAP if sha(raw(f":{d}")) != sha(Path(s).read_bytes())]
ok(not bad, f"every copy's index blob sha256-equal to its source ({len(MAP)}) {bad}")
d = raw(f":{RF}"); ok((sha(d), len(d), d.count(b"\n")) == ("59c2435d76e71737b3f8876fdbccb91159affa2cd2c0779de0d105fb07ada347", 4777, 61), "r822.txt at the architect's hash, 4777 B, 61 lines")
br = raw(f":{RES}"); print(f"resume file: sha256 {sha(br)}, {len(br)} B, {br.count(10)} lines")
ok(br.startswith(b"CLAUDE CODE\n") and br.endswith(b" STOP-12AI-CLNA-R1\n") and not br.endswith(b"\n\n"), "resume file runs CLAUDE CODE through STOP-12AI-CLNA-R1, one trailing newline")
seg = br.decode().split("===== BEGIN r822.txt =====\n")[1].split("===== END r822.txt =====")[0]
ok(seg.encode() == d, "the resume's r822.txt section equal to the staged r822.txt")
num = {l.split("\t")[2]: l.split("\t")[:2] for l in run("git", "diff", "--cached", "--numstat").splitlines()}
ok(num[P][1] == "0", f"plan numstat: +{num[P][0]} / −{num[P][1]}")
r = subprocess.run(["python3", "-I", "docs/session-reports/tools/transcribe_ruling.py", RF, "220", str(OUT / "block220.json"), "--check-plan", P], capture_output=True, text=True)
ok(r.returncode == 0, "--check-plan: " + (r.stdout + r.stderr).strip())
m = json.load(open(OUT / "block220.json")); today = run("date", "-u", "+%F").strip()
ok(m["transcribed"] == today and m["tag"] == "R822" and m["prev_ends"] == "R821", f"block220.json transcribed {m['transcribed']} = {today}; tag R822; prev_ends R821")
old, new = raw(f"HEAD:{P}").decode(), raw(f":{P}").decode()
eo = [l for l in old.split("\n") if l.startswith("The decision log in this file ends at R")]
en = [l for l in new.split("\n") if l.startswith("The decision log in this file ends at R")]
ok(new.startswith(old) and en[:len(eo)] == eo and len(en) == len(eo) + 1, f"the plan at HEAD a byte prefix of the staged plan; every earlier ends-at line kept: {len(eo)} -> {len(en)}")
ok(new.endswith("\nThe decision log in this file ends at R822.\n") and en.count("The decision log in this file ends at R822.") == 1, "last line '…ends at R822.', once")
for f, h in RULES.items():
    ok(sha(raw(f":{f}")) == h == sha(raw(f"HEAD:{f}")) and f not in {x[1] for x in st}, f"{f} = {h[:8]}…; not staged")
ok(sha(raw(f":{D}/12ai_CLNA/removal_list.tsv")) == "799f82dc533f5ce31286595123f94f906672752698f522e02f194c83f262d1e7", "removal_list.tsv at the sha256 R0 printed")
print("RESULT:", "PASS" if not fails else f"FAIL ({len(fails)})"); sys.exit(0 if not fails else 1)
