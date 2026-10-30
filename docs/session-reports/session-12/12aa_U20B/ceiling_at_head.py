"""12aa-U20B 2b — the ceiling the code yields for each committed show body with no request num_ctx. No network: a fake
client returns each raw body through the library's own ShowResponse. usage, from the repository root: ceiling_at_head.py <show_dir>"""
import json, os, sys
from types import SimpleNamespace
sys.path.insert(0, os.getcwd())
import ollama
from engine.utils import ollama_client as oc
D = sys.argv[1]; MODELS = ["qwen3:8b", "qwen3:32b", "gemma3:27b", "deepseek-r1:32b", "qwen2.5vl:7b", "llama3.2:3b"]
class Fake:
    _client = SimpleNamespace(base_url="http://127.0.0.1:11434")
    def show(self, model): return ollama.ShowResponse(**json.load(open(os.path.join(D, model.replace(":", "_") + ".response.json"))))
oc._client = Fake(); oc.clear_ceiling_cache()
print("HEAD", os.popen("git rev-parse HEAD").read().strip())
print("SERVER_DEFAULT_CTX", oc.SERVER_DEFAULT_CTX, "server_context_length()", oc.server_context_length(), "is_local_service_host", oc.is_local_service_host(oc._client_host()))
print("model | n_ctx_train | SERVER_DEFAULT_CTX | server_context_length | effective_ceiling(no num_ctx) | differs from trained")
for m in MODELS:
    t = oc.n_ctx_train(m); c = oc.effective_ceiling(m)
    print(m, t, oc.SERVER_DEFAULT_CTX, oc.server_context_length(), c, c != t, sep=" | ")
