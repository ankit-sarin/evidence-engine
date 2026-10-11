"""L11-C3 / C4 / C5, every boundary stubbed: no systemctl, no Ollama, no network."""
import subprocess
from unittest.mock import patch, MagicMock
from engine.utils import ollama_preflight as pf

# C3: systemctl unreadable -> reported as three env vars "not set"
with patch("engine.utils.ollama_preflight.subprocess.run",
           side_effect=subprocess.TimeoutExpired("systemctl", 10)):
    try:
        pf.check_ollama_env()
    except RuntimeError as e:
        print("C3 message when systemctl timed out:\n" + str(e))

# C4: `timeout` never reaches the call
seen = {}
def fake_chat(**kw):
    seen.update(wall_timeout=kw.get("wall_timeout"), max_retries=kw.get("max_retries"))
    return MagicMock()
with patch("engine.utils.ollama_preflight.ollama_chat", fake_chat), \
     patch("engine.utils.ollama_preflight.ollama.ps", return_value={"models": []}):
    pf.preflight_check(["m:1b"], timeout=5)
    print("C4 timeout=5 ->", seen)
    pf.preflight_check(["m:1b"], timeout=600)
    print("C4 timeout=600 ->", seen)

# C5: per-model VRAM read by bare prefix on the family name
with patch("engine.utils.ollama_preflight.ollama.ps",
           return_value={"models": [{"name": "qwen3.5:27b", "size": 20 * 1024**3}]}):
    print("C5 _get_model_vram_gb('qwen3:8b') with only qwen3.5:27b loaded ->",
          pf._get_model_vram_gb("qwen3:8b"))
    print("C5 _model_name_matches('qwen3:32b','qwen3:8b') ->",
          pf._model_name_matches("qwen3:32b", "qwen3:8b"))
