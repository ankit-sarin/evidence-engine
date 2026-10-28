"""12y-U18A 3f: 20 sequential GET /api/tags and 20 GET /api/version, from the engine's own client
(engine.utils.ollama_client._client._client, the httpx.Client inside ollama.Client) and from httpx.get
(fetch_model_digest's route). Read-only; no model call. Writes measure_3f.json beside itself."""
import json, os, statistics, sys, time
from datetime import datetime, timezone
import httpx
sys.path.insert(0, os.path.expanduser("~/projects/evidence-engine"))
import engine.utils.ollama_client as oc
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "measure_3f.json")
now = lambda: datetime.now(timezone.utc).isoformat()
res = {"started": now(), "engine_client_host": oc._client_host(), "digest_base_url": oc.OLLAMA_BASE_URL}
res["ps_before"] = httpx.get("http://127.0.0.1:11434/api/ps", timeout=10).json()
eng = oc._client._client
def run(label, fn):
    t, sizes, codes = [], [], []
    for _ in range(20):
        t0 = time.perf_counter(); r = fn(); t.append((time.perf_counter() - t0) * 1000)
        sizes.append(len(r.content)); codes.append(r.status_code)
    res[label] = {"n": 20, "ms": [round(x, 3) for x in t], "median_ms": round(statistics.median(t), 3),
                  "max_ms": round(max(t), 3), "min_ms": round(min(t), 3), "bytes": sorted(set(sizes)), "status": sorted(set(codes))}
run("engine_client_tags", lambda: eng.get("/api/tags"))
run("engine_client_version", lambda: eng.get("/api/version"))
run("httpx_get_tags", lambda: httpx.get(oc.OLLAMA_BASE_URL + "/api/tags", timeout=5))
run("httpx_get_version", lambda: httpx.get(oc.OLLAMA_BASE_URL + "/api/version", timeout=5))
res["ps_after"] = httpx.get("http://127.0.0.1:11434/api/ps", timeout=10).json()
res["ended"] = now()
json.dump(res, open(OUT, "w"), indent=1)
for k in ("engine_client_tags", "engine_client_version", "httpx_get_tags", "httpx_get_version"):
    v = res[k]; print(f"{k:24s} median {v['median_ms']:8.3f} ms  max {v['max_ms']:8.3f} ms  min {v['min_ms']:7.3f}  bytes {v['bytes']}  status {v['status']}")
print("ps before", res["ps_before"], "after", res["ps_after"], res["started"], res["ended"])
