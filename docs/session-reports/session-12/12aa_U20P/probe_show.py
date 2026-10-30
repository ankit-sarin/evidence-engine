"""12aa-U20P P1 (R740(f), R741(b)) — POST /api/show for the five provider-ollama models the live spec names and the
small model; each request body and raw response saved as received, with sha256. Plain HTTP; the engine client is not
imported. Reads /api/ps before and after. usage: probe_show.py <out_dir> (stdout: one line per model)."""
import hashlib, json, os, sys, urllib.error, urllib.request
from datetime import datetime, timezone
HOST = "http://127.0.0.1:11434"   # engine/utils/ollama_client.py: _parse_host(os.getenv("OLLAMA_HOST")), OLLAMA_HOST unset
MODELS = ["qwen3:8b", "qwen3:32b", "gemma3:27b", "deepseek-r1:32b", "qwen2.5vl:7b", "llama3.2:3b"]
NOT_SENT = ["o4-mini-2025-04-16", "claude-sonnet-4-6"]
OUT = sys.argv[1]; now = lambda: datetime.now(timezone.utc).isoformat()
def get_ps(name):
    b = urllib.request.urlopen(f"{HOST}/api/ps", timeout=10).read()
    open(os.path.join(OUT, name), "wb").write(b); print(now(), name, b.decode()); return json.loads(b)
if get_ps("ps_before.json")["models"]: sys.exit("STOP: a model is resident before any show")
for m in MODELS:
    tag = m.replace(":", "_")
    body = json.dumps({"model": m}).encode()
    open(os.path.join(OUT, f"{tag}.request.json"), "wb").write(body)
    req = urllib.request.Request(f"{HOST}/api/show", data=body, headers={"Content-Type": "application/json"})
    try:
        raw = urllib.request.urlopen(req, timeout=30).read(); status = 200
    except urllib.error.HTTPError as e:
        raw, status = e.read(), e.code
    open(os.path.join(OUT, f"{tag}.response.json"), "wb").write(raw)
    print(now(), m, status, len(raw), hashlib.sha256(raw).hexdigest())
print("not sent (cloud, not installed):", NOT_SENT)
if get_ps("ps_after.json")["models"]: sys.exit("STOP: a model is resident after the shows")
