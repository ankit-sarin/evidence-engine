"""12ag-GATEUNIT-B 4c — the dry run's check, in 12aa-SETUP's 2d form (its four lines kept first), plus the unit launch.
Read-only. usage, from the repository root:
  python3 -I check_dry_gub.py <evidence_dir> <name> <selection> <launcher stdout> <reference gate dir> <scratch root>
  python3 -I check_dry_gub.py --form-only <launch_line.txt> <name>      (4a: the launch-form assertion alone)
The limit (R809(b)): the launch line's LimitNOFILE equals the smaller of the two values its first line records, and those are
this shell's soft limit and the user manager's own hard limit as read now. Exit 0 only if every assertion holds."""
import os, re, subprocess, sys
from pathlib import Path
fails = []
def ok(c, m): print(("PASS " if c else "FAIL ") + m); c or fails.append(m)
def form(line, name):
    return line.startswith("systemd-run --user ") and f" --unit=gate-{name} " in f" {line} "
if sys.argv[1] == "--form-only":
    ll = Path(sys.argv[2]).read_text().split("\n")
    ok(form(ll[1], sys.argv[3]), f"launch form: the line begins 'systemd-run --user ' and names gate-{sys.argv[3]} — {ll[1][:90]}…")
    print("RESULT:", "PASS" if not fails else "FAIL"); sys.exit(0 if not fails else 1)
E, name, sel, out, ref, root = Path(sys.argv[1]), sys.argv[2], sys.argv[3], Path(sys.argv[4]).read_text(), Path(sys.argv[5]), Path(sys.argv[6])
ll = (E / "launch_line.txt").read_text().split("\n"); line = ll[1]
gl = (E / "gate_launch.txt").read_text().split("\n")
m = re.search(r" bash (\S+)/run_gate\.sh (.*)$", line)
ok(bool(m) and Path(m.group(1)) == E and "run_gate.sh " + m.group(2) == gl[1], f"launch line's inner == what ran: {bool(m) and 'run_gate.sh ' + m.group(2) == gl[1]}")
ok(sel in line, f"selection in the line: {sel}")
want = sorted(n.replace("durations_12ag_open.txt", f"durations_{name}.txt") for n in os.listdir(ref))
have = sorted(os.listdir(E))
ok(have == want and len(have) == 19, f"nineteen files: {have == want} {len(have)} (names against {ref.name}/, durations_<name>.txt)")
bt = Path(str(E) + ".basetemp")
ok(bt.is_dir() and str(bt.resolve()).startswith(str(root.resolve()) + "/"), f"basetemp under scratch: {bt}")
ok(form(line, name), f"launch form: begins 'systemd-run --user ' and names gate-{name}")
rec = re.search(r"soft limit (\d+) and the user manager's hard limit (\d+)", ll[0])
lim = re.search(r"--property=LimitNOFILE=(\d+) ", line)
mpid = subprocess.run(["systemctl", "show", f"user@{os.getuid()}.service", "-p", "MainPID", "--value"], capture_output=True, text=True).stdout.strip()
mhard = next(l.split()[4] for l in open(f"/proc/{mpid}/limits") if l.startswith("Max open files"))
ssoft = subprocess.run(["bash", "-c", "ulimit -Sn"], capture_output=True, text=True).stdout.strip()
ok(bool(rec and lim) and int(lim.group(1)) == min(int(rec.group(1)), int(rec.group(2))) and rec.group(1) == ssoft and rec.group(2) == mhard,
   f"limit: LimitNOFILE={lim.group(1) if lim else None} = min(recorded soft {rec.group(1) if rec else None}, recorded manager hard {rec.group(2) if rec else None}); now soft {ssoft}, manager hard {mhard}")
ok(f"--setenv=LANG={os.environ.get('LANG', '')} " in line and line.count("--setenv=") == 1, f"LANG: --setenv=LANG={os.environ.get('LANG', '')}, the only variable passed")
ok(not re.search(r"Memory", line), "no memory property in the line")
ok("the unit has no memory cap" in ll[0], "launch_line.txt's first line says the unit has no memory cap")
ok(f"gate-{name}.service" in out and ("Active:" in out or "could not be found" in out), f"printed status names gate-{name}.service")
ok(re.search(r"^Linger=yes$", out, re.M) is not None, "Linger=yes printed")
rc = (E / "gate_rc.txt").read_text().strip(); log = (E / "gate.log").read_text()
ok(rc == "0" and f"pytest exit code: {rc}" in log.splitlines()[-1], f"gate_rc.txt {rc!r} = gate.log's last line {log.splitlines()[-1]!r}, and 0")
t = subprocess.run(["tmux", "has-session", "-t", name], capture_output=True, text=True)
ok(t.returncode != 0, f"no tmux session {name}: has-session exit {t.returncode}")
print("RESULT:", "PASS" if not fails else f"FAIL ({len(fails)})"); sys.exit(0 if not fails else 1)
