"""A4 — where chat calls go vs where the manifest digest is read. NO network call:
constructing ollama.Client opens no connection; nothing is requested."""
import os, sys, inspect, subprocess
if len(sys.argv) == 1:
    for env in ({}, {"OLLAMA_HOST": "http://127.0.0.1:11435"}):
        e = {k: v for k, v in os.environ.items() if k != "OLLAMA_HOST"}; e.update(env)
        print(f"--- env OLLAMA_HOST={env.get('OLLAMA_HOST', '<unset>')}")
        print(subprocess.run([sys.executable, __file__, "child"], env=e, capture_output=True, text=True).stdout, end="")
else:
    from engine.utils import ollama_client as oc
    print("chat  (_client.chat) host        :", oc._client_host())
    print("digest (fetch_model_digest) host :",
          inspect.signature(oc.fetch_model_digest).parameters["base_url"].default)
    import ollama
    oc._client = ollama.Client(host="http://127.0.0.1:11435", timeout=oc._httpx_timeout)  # run_qualgap01.bind_runtime's line
    print("after an in-process rebind       : chat", oc._client_host(), "| digest",
          inspect.signature(oc.fetch_model_digest).parameters["base_url"].default)
