"""12ab-U21A §5 — HEAD's seams against a fake client, no server. Read-only on the tree: replaces ollama_client._client,
effective_ceiling and subprocess.run in this process only. The opt-out is set and subprocess.run raises if reached, so no
service manager is touched; no network call is made; no run is active, so nothing records. usage, from the repository root."""
import os, sys, threading, time
os.environ["EVIDENCE_ENGINE_NO_OLLAMA_RESTART"] = "1"
sys.path.insert(0, os.getcwd())
from engine.utils import ollama_client as oc
def boom(*a, **k): raise SystemExit("subprocess.run reached")
oc.subprocess.run = boom
oc.effective_ceiling = lambda model, options=None: 131072
class Fake:
    def __init__(self): self.lock = threading.Lock(); self.now = 0; self.peak = 0; self.entries = 0; self.release = threading.Event()
    def chat(self, **kw):
        with self.lock:
            self.entries += 1; self.now += 1; self.peak = max(self.peak, self.now)
        self.release.wait(30)
        with self.lock: self.now -= 1
        raise RuntimeError("released")
    def show(self, *a, **k): raise SystemExit("show reached")
f = Fake(); oc._client = f
t0 = time.monotonic()
try:
    oc.ollama_chat(model="m:8b", messages=[{"role": "user", "content": "x"}], wall_timeout=0.2, retry_delay=0, max_retries=2)
except BaseException as e:
    print(f"raised {type(e).__name__}: {e}")
print(f"elapsed {time.monotonic()-t0:.2f}s entries {f.entries} peak concurrent {f.peak} still in flight at the raise {f.now} threads {threading.active_count()}")
f.release.set()
for th in threading.enumerate():
    if th is not threading.current_thread(): th.join(5)
print(f"after release: in flight {f.now}; threads {threading.active_count()}")
