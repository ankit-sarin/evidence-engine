"""L11-C1: `python -m pkg.mod --background` is relaunched as `python /abs/pkg/mod.py`.
tmux is never spawned (subprocess.run stubbed inside the child). The relaunch
command string is then executed directly (no tmux, no tee) to show what the
detached session would do."""
import json, os, subprocess, sys
REPO = "/home/ankitsarin/projects/evidence-engine"
env = dict(os.environ, PYTHONPATH=REPO); env.pop("TMUX", None)
p = subprocess.run([sys.executable, "-m", "fakepkg.runner", "--review", "synthetic", "--background"],
                   capture_output=True, text=True, env=env)
print("parent stdout tail:", p.stdout.strip().splitlines()[-4:])
cap = json.loads([l for l in p.stdout.splitlines() if l.startswith("{")][0])
tmux_cmd = cap["argv"][-1]
print("parent exit code as maybe_background set it:", cap["exit"])
print("tmux bash -c string:", tmux_cmd)
inner = tmux_cmd.split(" 2>&1 | tee ")[0]
r = subprocess.run(["bash", "-c", inner], capture_output=True, text=True, env=env)
print("relaunch rc:", r.returncode)
print("relaunch stderr last line:", r.stderr.strip().splitlines()[-1] if r.stderr.strip() else "")
print("relaunch stdout:", r.stdout.strip())
# and what the session's own status would be: the pipeline's, i.e. tee's
r2 = subprocess.run(["bash", "-c", inner + " 2>&1 | tee /dev/null; echo pipeline_rc=$?"],
                    capture_output=True, text=True, env=env)
print("with `| tee`:", r2.stdout.strip().splitlines()[-1])
