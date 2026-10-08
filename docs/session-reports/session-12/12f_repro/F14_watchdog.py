"""F14: ollama_chat's watchdog vs a blocked client call. Fake client, no Ollama, restart disabled."""
import os, sys, time, threading
os.environ["EVIDENCE_ENGINE_NO_OLLAMA_RESTART"] = "1"
import engine.utils.ollama_client as oc

def _no_subprocess(*a, **k): raise AssertionError("subprocess reached")
oc.subprocess.run = _no_subprocess

BLOCK = float(sys.argv[1]) if len(sys.argv) > 1 else 4.0
lock = threading.Lock(); active = 0; peak = 0; started = 0
class FakeInfo: modelinfo = {"fake.context_length": 8192}
class FakeClient:
    def show(self, model): return FakeInfo()
    def chat(self, **kw):
        global active, peak, started
        with lock:
            active += 1; started += 1; peak = max(peak, active)
        time.sleep(BLOCK)                      # a generation that outlives the watchdog
        with lock: active -= 1
        return None
oc._client = FakeClient()

t0 = time.monotonic()
try:
    oc.ollama_chat(model="fake", messages=[{"role": "user", "content": "x" * 100}],
                   wall_timeout=0.2, retry_delay=0.0, max_retries=2)
except TimeoutError as e:
    print("raised TimeoutError after %.2fs:" % (time.monotonic() - t0), str(e)[:70])
print("requests started:", started, "| still running when ollama_chat returned control:", active,
      "| peak simultaneous:", peak)
print("live worker threads:", sum(t.name.startswith("ThreadPoolExecutor") for t in threading.enumerate()))
print("main done at %.2fs; process should now exit (code 1 path = sys.exit)" % (time.monotonic() - t0))
sys.stdout.flush()
sys.exit(1)
