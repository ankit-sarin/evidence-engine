"""12aa-U20P P2-P5 (R740(f)) — two loads of the small model, one with a request num_ctx and one without, each a
one-token request followed by /api/ps and an unload, under the experiment lock (12v_U37R/send.py's mechanism: `with
hold_experiment_lock():`), outside any run manifest, writing to no review. Plain HTTP; the engine client is not
imported; each body saved as sent and received. Any error, a foreign model or an unconfirmed unload ends the probe
with the lock released; nothing is retried. usage, from the repository root: probe_load.py <out_dir> <model> <N>"""
import json, os, sys, time, urllib.error, urllib.request
from datetime import datetime, timezone
sys.path.insert(0, os.getcwd())
from engine.utils.ollama_lock import hold_experiment_lock
HOST = "http://127.0.0.1:11434"; OUT, MODEL, N = sys.argv[1], sys.argv[2], int(sys.argv[3])
now = lambda: datetime.now(timezone.utc).isoformat()
log = {"model": MODEL, "N": N, "pid": os.getpid(), "events": []}
def ev(name, **k): log["events"].append({"at": now(), "event": name, **k}); print(now(), name, k, flush=True)
class Stop(Exception): pass
def ps(name=None):
    b = urllib.request.urlopen(f"{HOST}/api/ps", timeout=10).read()
    if name: open(os.path.join(OUT, name), "wb").write(b)
    d = json.loads(b); foreign = [m["name"] for m in d["models"] if m["name"] != MODEL]
    if foreign: raise Stop(f"foreign model resident: {foreign}")
    return d, b
def generate(name, body):
    raw = json.dumps(body).encode(); open(os.path.join(OUT, f"{name}.request.json"), "wb").write(raw)
    req = urllib.request.Request(f"{HOST}/api/generate", data=raw, headers={"Content-Type": "application/json"})
    t0 = time.monotonic()
    try:
        resp = urllib.request.urlopen(req, timeout=120).read(); status = 200
    except urllib.error.HTTPError as e:
        resp, status = e.read(), e.code
    open(os.path.join(OUT, f"{name}.response.json"), "wb").write(resp)
    ev(name, status=status, wall_s=round(time.monotonic() - t0, 3), bytes=len(resp))
    if status != 200: raise Stop(f"{name}: HTTP {status}: {resp[:300]!r}")
    return json.loads(resp)
def unload(name):
    generate(name, {"model": MODEL, "keep_alive": 0})
    reads = []
    for _ in range(31):
        d, b = ps(); reads.append({"at": now(), "models": len(d["models"])})
        if not d["models"]:
            open(os.path.join(OUT, f"{name}.ps_last.json"), "wb").write(b)
            json.dump(reads, open(os.path.join(OUT, f"{name}.ps_reads.json"), "w"))
            ev(name + "_confirmed", reads=len(reads)); return
        time.sleep(1)
    json.dump(reads, open(os.path.join(OUT, f"{name}.ps_reads.json"), "w"))
    raise Stop(f"{name}: /api/ps not empty after 30 s")
def fields(r): return {k: r.get(k) for k in ("done_reason", "prompt_eval_count", "eval_count", "done", "load_duration")}
rc = 0
with hold_experiment_lock():
    log["lock_acquired_at"] = now(); ev("lock_acquired")
    try:
        d, _ = ps("P0_ps.json")
        if d["models"]: raise Stop("a model is resident at the start")
        r = generate("P2", {"model": MODEL, "prompt": "ok", "stream": False, "options": {"num_predict": 1, "num_ctx": N}})
        ev("P2_fields", **fields(r)); ps("P3_ps.json"); ev("P3_read")
        unload("U1")
        r = generate("P4", {"model": MODEL, "prompt": "ok", "stream": False, "options": {"num_predict": 1}})
        ev("P4_fields", **fields(r)); ps("P5_ps.json"); ev("P5_read")
        unload("U2")
    except Stop as e:
        rc = 3; ev("STOP", why=str(e))
        try: generate("U_stop", {"model": MODEL, "keep_alive": 0})
        except Exception as e2: ev("unload_attempt_failed", why=str(e2))
    log["lock_released_at"] = now()
ev("lock_released"); json.dump(log, open(os.path.join(OUT, "probe_log.json"), "w"), indent=1); sys.exit(rc)
