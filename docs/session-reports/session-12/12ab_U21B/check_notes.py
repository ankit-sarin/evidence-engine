"""usage: check_notes.py <plan> <engine hash> <gate hash> <dir> <evidence names...>"""
import hashlib, os, subprocess, sys
P, E, G, D, *names = sys.argv[1:]
T = os.path.dirname(os.path.abspath(__file__))
run = lambda *a: subprocess.run(a, capture_output=True, text=True, check=True).stdout
ok = True
def chk(n, c):
    global ok; ok &= bool(c); print(f"{n}: {bool(c)}")
add, rem, _ = run("git", "diff", "--cached", "--numstat", "--", P).split()
print(f"numstat plan +{add} / -{rem}")
old = run("git", "show", f"HEAD:{P}").split("\n"); new = open(P, encoding="utf-8").read().split("\n")
print(f"lines {len(old) - 1} -> {len(new) - 1}")
chk("lines = old + added - removed", len(new) == len(old) + int(add) - int(rem))
d = [l for l in run("git", "diff", "--cached", "-U0", "--", P).split("\n") if l and l[0] in "+-" and l[:3] not in ("+++", "---")]
minus = [l[1:] for l in d if l[0] == "-"]; plus = [l[1:] for l in d if l[0] == "+"]
print("minus lines:", [m[:14] for m in minus]); print("plus lines:", [p[:14] for p in plus])
chk("each minus survives as the head of a plus (only appended to)", all(any(p.startswith(m[:-2]) and p.endswith(" |") for p in plus) for m in minus))
chk("the rows are B-F14 and INT-ab12-1", sorted(m.split(" |")[0] for m in minus) == ["| B-F14", "| INT-ab12-1"])
chk("full engine hash in both notes", sum(E in p for p in plus) == 2 and run("git", "rev-parse", E[:7]).strip() == E)
chk("gate commit hash resolves", run("git", "rev-parse", G[:7]).strip() == G and G in "".join(plus))
chk("blocks untouched", all(not m.startswith("| 2026") for m in minus))
exp = sorted([P] + [f"{D}/{n}" for n in names])
chk("staged set exactly plan + evidence", sorted(run("git", "diff", "--cached", "--name-only").split()) == exp)
for n in names:
    same = open(f"{T}/{n}", "rb").read() == open(f"{D}/{n}", "rb").read(); ok &= same; print(f"  {hashlib.sha256(open(f'{D}/{n}','rb').read()).hexdigest()}  {n}  equal={same}")
chk("worktree == index", run("git", "diff", "--name-only") == ""); chk("no untracked", run("git", "ls-files", "--others", "--exclude-standard") == "")
print("RESULT:", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
