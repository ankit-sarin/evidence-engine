"""12ab-U21P (R757(b), 12ab_U21A.md §7b) — the B-F14 measurement. One small model under the experiment lock (12aa_U20P/probe_load.py's
mechanism), outside any run manifest, writing to no review, its own library Client and not ollama_chat. L0 loads the model so every
variant starts from a loaded-and-idle server (§7b); V0 baseline; V1 client closed at about 10 s; V2 thread abandoned at about 10 s;
after V1 and V2 a follow-up at about 12 s on a fresh client. Every request body is written before its send and every response after,
by httpx event hooks. A stop condition ends the probe with the lock released; nothing is retried. No restart, no kill.
usage, from the repository root: probe_u21.py <out_dir> <model> <num_ctx> <num_predict>"""
import json, os, subprocess, sys, threading, time
from datetime import datetime, timezone
import httpx, ollama
sys.path.insert(0, os.getcwd())
from engine.utils.ollama_lock import ExperimentLockHeld, hold_experiment_lock
HOST = "http://127.0.0.1:11434"; OUT, MODEL, NCTX, NPRED = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
TIMEOUT = httpx.Timeout(connect=30.0, read=900.0, write=30.0, pool=30.0)   # engine/utils/ollama_client.py:56–61's values
CLOSE_AT, FOLLOW_AT, FLOOR = 10.0, 12.0, 30.0
CANDIDATES = ('Return a JSON object whose "values" array holds exactly 2000 ten-digit integers, and nothing else.',)   # one send as V0, R760(c)
FORMAT = {"properties": {"values": {"items": {"type": "integer"}, "maxItems": 2000, "minItems": 2000, "title": "Values", "type": "array"}}, "required": ["values"], "title": "ProbeArray", "type": "object"}
PROMPT = CANDIDATES[0]
now = lambda: datetime.now(timezone.utc).isoformat()
start = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
log = {"model": MODEL, "num_ctx": NCTX, "num_predict": NPRED, "pid": os.getpid(), "journal_since": start, "events": []}
def ev(name, **k): log["events"].append({"at": now(), "event": name, **k}); print(now(), name, k, flush=True)
class Stop(Exception): pass
def path(n): return os.path.join(OUT, n)
def client(name, read=None):
    """A fresh library Client whose hooks save the bodies as sent and as received."""
    def on_request(req): open(path(f"{name}.request.json"), "wb").write(req.content); ev(f"{name}_request_written", bytes=len(req.content))
    def on_response(resp):
        resp.read(); open(path(f"{name}.response.json"), "wb").write(resp.content); ev(f"{name}_response_saved", status=resp.status_code, bytes=len(resp.content))
    t = TIMEOUT if read is None else httpx.Timeout(connect=30.0, read=read, write=30.0, pool=30.0)
    return ollama.Client(host=HOST, timeout=t, event_hooks={"request": [on_request], "response": [on_response]})
def service():
    r = subprocess.run(["systemctl", "show", "ollama", "--property=NRestarts", "--property=ExecMainStartTimestamp", "--no-pager"], capture_output=True, text=True)
    return r.stdout.strip()
SERVICE0 = service()
def ps(name):
    b = httpx.get(f"{HOST}/api/ps", timeout=10).content; open(path(name), "wb").write(b); d = json.loads(b)
    foreign = [m["name"] for m in d["models"] if m["name"] != MODEL]
    if foreign: raise Stop(f"a foreign model is resident: {foreign}")
    s = service()
    if s != SERVICE0: raise Stop(f"a restart was observed: {s!r} against {SERVICE0!r}")
    ev(name, models=[m["name"] for m in d["models"]]); return d
def chat_kwargs(predict):
    return dict(model=MODEL, messages=[{"role": "user", "content": PROMPT}], stream=False, format=FORMAT, options={"temperature": 0, "num_ctx": NCTX, "num_predict": predict})
def fields(r): return {k: getattr(r, k, None) for k in ("done_reason", "prompt_eval_count", "eval_count", "total_duration", "load_duration", "prompt_eval_duration", "eval_duration")}
def posts():
    r = subprocess.run(["journalctl", "-u", "ollama", "--utc", "--since", start, "--no-pager"], capture_output=True, text=True)
    return sum(1 for l in r.stdout.splitlines() if '"/api/chat"' in l and "POST" in l)
def wait_handlers(n, bound, label):
    t0 = time.monotonic()
    while time.monotonic() - t0 < bound:
        c = posts()
        if c >= n: ev(f"{label}_handlers_ended", journal_chat_posts=c, waited_s=round(time.monotonic() - t0, 1)); return
        time.sleep(2)
    raise Stop(f"{label}: journal shows {posts()} chat requests ended, expected {n}, after {bound} s")
class Worker(threading.Thread):
    def __init__(self, name, cl): super().__init__(name=name, daemon=True); self.cl = cl; self.result = None; self.exc = None; self.t_end = None
    def run(self):
        try: self.result = self.cl.chat(**chat_kwargs(NPRED))
        except BaseException as e: self.exc = e
        self.t_end = time.monotonic(); ev(f"{self.name}_worker_returned", exception=(None if self.exc is None else f"{type(self.exc).__name__}: {self.exc}"[:300]), **({} if self.result is None else fields(self.result)))
def follow_up(name, bound):
    cl = client(name, read=bound); t0 = time.monotonic()
    try:
        r = cl.chat(**chat_kwargs(1))
    except Exception as e:
        raise Stop(f"{name}: no answer by the bound: {type(e).__name__}: {e}")
    wall = round(time.monotonic() - t0, 3); ev(f"{name}_returned", wall_s=wall, **fields(r)); cl._client.close(); return wall
def variant(name, act, n_after, d0):
    """Worker sends at t0; at CLOSE_AT the main thread acts; at FOLLOW_AT the follow-up goes out on a fresh client."""
    ps(f"{name}_ps_before.json"); cl = client(name); w = Worker(name, cl); t0 = time.monotonic(); w.start(); ev(f"{name}_sent")
    time.sleep(CLOSE_AT); act(cl); ev(f"{name}_action_done", at_s=round(time.monotonic() - t0, 3))
    time.sleep(max(0, FOLLOW_AT - (time.monotonic() - t0)))
    bound = d0 + 120.0; ev(f"{name}_follow_up_sent", at_s=round(time.monotonic() - t0, 3))
    wall = follow_up(f"{name}F", bound)
    w.join(max(0.0, bound - (time.monotonic() - t0)))
    if w.is_alive(): raise Stop(f"{name}: the worker is still blocked at D0 + 120 s")
    log[name] = {"worker_end_s": round(w.t_end - t0, 3), "follow_up_wall_s": wall, "worker_exception": None if w.exc is None else f"{type(w.exc).__name__}: {w.exc}"[:300], "worker_fields": None if w.result is None else fields(w.result)}
    wait_handlers(n_after, bound, name)
def unload(name):
    """As 12aa_U20P/probe_load.py: /api/generate with keep_alive 0, confirmed by /api/ps within 30 s."""
    raw = json.dumps({"model": MODEL, "keep_alive": 0}).encode(); open(path(f"{name}.request.json"), "wb").write(raw)
    resp = httpx.post(f"{HOST}/api/generate", content=raw, headers={"Content-Type": "application/json"}, timeout=120)
    open(path(f"{name}.response.json"), "wb").write(resp.content); ev(name, status=resp.status_code)
    for _ in range(31):
        if not httpx.get(f"{HOST}/api/ps", timeout=10).json()["models"]: ev(name + "_confirmed"); return True
        time.sleep(1)
    ev(name + "_NOT_confirmed_within_30_s"); return False
rc = 0
try:
    with hold_experiment_lock():
        log["lock_acquired_at"] = now(); ev("lock_acquired")
        try:
            d = ps("P0_ps.json")
            if d["models"]: raise Stop("a model is resident at the start")
            cl = client("L0"); t0 = time.monotonic(); r = cl.chat(**chat_kwargs(1)); ev("L0_loaded", wall_s=round(time.monotonic() - t0, 3), **fields(r)); cl._client.close()
            wait_handlers(1, 60, "L0"); ps("L0_ps.json")
            d0 = None; log["calibration"] = []; sends = 1
            for i, PROMPT in enumerate(CANDIDATES, 1):   # R758(c)(ii): one send each; the first that qualifies IS V0, not re-sent
                ps(f"C{i}_ps_before.json"); cl = client(f"C{i}"); t0 = time.monotonic()
                try: r = cl.chat(**chat_kwargs(NPRED))
                except Exception as e: raise Stop(f"C{i}: {type(e).__name__}: {e}")
                wall = round(time.monotonic() - t0, 3); cl._client.close(); sends += 1; wait_handlers(sends, 60, f"C{i}")
                good = wall >= FLOOR   # R759(b): a floor of 30 s, whatever the done_reason
                log["calibration"].append({"candidate": i, "wall_s": wall, "verdict": "qualifies, is V0" if good else "does not qualify", "prompt": PROMPT, "fields": fields(r)})
                ev(f"C{i}_returned", wall_s=wall, qualifies=good, **fields(r))
                if good: d0 = wall; log["V0"] = {"candidate": i, "D0_s": d0, "fields": fields(r)}; break
            if d0 is None: raise Stop(f"calibration: V0 ran under the floor of {FLOOR} s")
            variant("V1", lambda c: c._client.close(), sends + 2, d0)
            variant("V2", lambda c: None, sends + 4, d0)
            ps("P_after_ps.json")
        except Stop as e:
            rc = 3; ev("STOP", why=str(e))
        except Exception as e:
            rc = 3; ev("STOP", why=f"unexpected: {type(e).__name__}: {e}")
        if not unload("U1"): rc = rc or 4
        log["lock_released_at"] = now()
    ev("lock_released")
except ExperimentLockHeld as e:
    rc = 3; ev("STOP", why=f"the lock is held by another process: {e}")
json.dump(log, open(path("probe_log.json"), "w"), indent=1, default=str); sys.exit(rc)
