"""12r-U13-PROBE Phase 2 driver (R669): one experiment-lock hold across every call. Writes only under out/.
usage, from the evidence-engine repository root: run_phase2.py <render_dir> <stored_hashes.json>
1. Reproduction: papers 3 then 10, 12r's attempt-1 request (render/head_p<id>.txt, --expect-hash = 12r's
   stored attempt-1 hash), three calls each. After a paper's three calls its decision is written to
   out/decision_p<id>.json and logged: REPRODUCES when stated_only on >= 2 of 3, else NOT REPRODUCED.
2. Ablation, only for a paper that REPRODUCES, one call each: a = out/cond_a_p<id>.txt, b =
   out/cond_b_p<id>.txt (expected hash: the composed would-send hash, compose_p<id>.json), c =
   render/g90_p<id>.txt (expected hash: its render hash).
Stops (after writing the current call's record): NRestarts above its start value, an earlyoom kill
line since the start, or the clock past 06:40 UTC. A call that raises (a watchdog timeout, a refusal)
is recorded by probe_one and the driver continues; no call is retried."""
import json, os, subprocess, sys
from datetime import datetime, timezone
HERE = os.path.dirname(os.path.abspath(__file__)); OUT = os.path.join(HERE, "out")
sys.path.insert(0, HERE)
from probe import probe_one
from engine.utils.ollama_lock import hold_experiment_lock

render, stored = sys.argv[1], json.load(open(sys.argv[2]))
LOG = open(os.path.join(OUT, "phase2_log.txt"), "a")
def log(msg):
    line = f"{datetime.now(timezone.utc).isoformat()} {msg}"; print(line, flush=True); LOG.write(line + "\n"); LOG.flush()
def nrestarts():
    return int(subprocess.run(["systemctl", "show", "ollama", "-p", "NRestarts", "--value"],
                              capture_output=True, text=True).stdout.strip())
START = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
def earlyoom_kills():
    j = subprocess.run(["journalctl", "-u", "earlyoom", "--since", START, "--no-pager", "-q"],
                       capture_output=True, text=True).stdout
    return [l for l in j.splitlines() if ("sending SIG" in l and "when mem" not in l) or ("killing" in l.lower() and "avoid" not in l.lower())]
N0 = nrestarts()
LIMIT = datetime(2026, 10, 19, 6, 40, tzinfo=timezone.utc)


def call(prompt, label, expect):
    log(f"CALL {label} expect={expect}")
    try:
        rec = probe_one(prompt, label, expect)
        log(f"DONE {label} done_reason={rec['done_reason']} raw_chars={rec['raw_chars']} "
            f"present={rec['present_by_class']} stated_only={rec['stated_only']} parses={rec['json_parses']} "
            f"eval_count={rec['eval_count']} load_ns={rec['load_duration_ns']}")
    except Exception as exc:
        rec = {"label": label, "error": f"{type(exc).__name__}: {exc}", "stated_only": None}
        log(f"ERROR {label} {rec['error']}")
    stop = []
    if nrestarts() != N0: stop.append(f"NRestarts {N0} -> {nrestarts()}")
    if earlyoom_kills(): stop.append(f"earlyoom: {earlyoom_kills()}")
    if datetime.now(timezone.utc) > LIMIT: stop.append("clock past 06:40 UTC")
    if stop:
        log(f"STOP after {label}: {stop}"); raise SystemExit(2)
    return rec


with hold_experiment_lock():
    log(f"lock held; NRestarts start {N0}")
    decisions = {}
    for pid in ("3", "10"):
        recs = [call(os.path.join(render, f"head_p{pid}.txt"), f"repro_p{pid}_{k}", stored[pid]) for k in (1, 2, 3)]
        n = sum(1 for r in recs if r.get("stated_only"))
        decisions[pid] = "REPRODUCES" if n >= 2 else "NOT REPRODUCED"
        json.dump({"paper_id": int(pid), "stated_only_calls": n, "of": 3, "decision": decisions[pid],
                   "labels": [r["label"] for r in recs]}, open(os.path.join(OUT, f"decision_p{pid}.json"), "w"), indent=1)
        log(f"DECISION paper {pid}: {decisions[pid]} ({n} of 3 STATED-only)")
    for pid in ("3", "10"):
        if decisions[pid] != "REPRODUCES":
            log(f"no ablation calls for paper {pid} (NOT REPRODUCED)"); continue
        comp = json.load(open(os.path.join(OUT, f"compose_p{pid}.json")))
        g90h = json.load(open(os.path.join(render, f"g90_p{pid}.json")))["request_hash"]
        call(os.path.join(OUT, f"cond_a_p{pid}.txt"), f"abl_a_p{pid}", comp["a"]["would_send_request_hash"])
        call(os.path.join(OUT, f"cond_b_p{pid}.txt"), f"abl_b_p{pid}", comp["b"]["would_send_request_hash"])
        call(os.path.join(render, f"g90_p{pid}.txt"), f"abl_c_p{pid}", g90h)
    log("all calls done; releasing lock")
