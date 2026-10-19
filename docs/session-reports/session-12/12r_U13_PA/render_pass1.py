"""12r-U13-PA R4 — render one paper's Pass-1 attempt-1 request with a given tree's own builders. No
model call: every socket connect raises before any import, so a builder that needed a live connection
would fail loudly instead of reaching the server (I3).
usage: render_pass1.py <tree_root> <review_dir> <parsed_text_file> <paper_id> <out_prompt.txt>
  tree_root   — the repository root whose `engine` package is imported (HEAD, or a `git archive` of
                90f4697 under ~/scratch/12r-u13-pa/)
  review_dir  — the review whose spec.yaml and extraction_codebook.yaml the run used
(stdout: JSON — request hash over {"model", "messages", **cfg.kwargs()}, as ollama_chat hashes it;
prompt and system-message sha256 and lengths; the field names asked for; n_units.)
The paper text is decoded as read_parsed_text decodes it (UTF-8, universal newlines)."""
import hashlib, io, json, os, socket, sys

def _refuse(*a, **k):
    raise RuntimeError("network refused by render_pass1.py")
socket.socket.connect = _refuse
socket.create_connection = _refuse

tree, review_dir, text_path, pid, out = sys.argv[1:6]
pid = int(pid)
sys.path.insert(0, os.path.abspath(tree))
os.chdir(os.path.abspath(tree))
from engine.core.completeness import expected_field_names
from engine.core.effective_config import stage_config
from engine.core.review_spec import load_review_spec
from engine.core.run_manifest import request_hash
from engine.core.codebook import load_codebook
from engine.elicitation import pipeline as PL
from engine.elicitation.units import build_unit_map
import engine
assert os.path.dirname(engine.__file__).startswith(os.path.abspath(tree)), engine.__file__

spec = load_review_spec(os.path.join(review_dir, "spec.yaml"))
cb_path = os.path.join(review_dir, "extraction_codebook.yaml")
codebook = load_codebook(cb_path).raw
names = expected_field_names(spec, cb_path)
text = io.TextIOWrapper(io.BytesIO(open(text_path, "rb").read()), encoding="utf-8").read()
um = build_unit_map(pid, text)
prompt = PL.pass1_prompt(um, codebook, names)
messages = PL.pass1_messages(prompt)
kw = stage_config("elicitation_pass1", spec).kwargs()
# ollama_chat hashes {"model", "messages", **kwargs}; its own named parameters (e.g. `stage`) are
# consumed, not sent. Read them from this tree's signature rather than listing them here.
import inspect
from engine.utils.ollama_client import ollama_chat
consumed = {n for n, p in inspect.signature(ollama_chat).parameters.items()
            if p.kind is not p.VAR_KEYWORD and n not in ("model", "messages")}
request = {"model": kw.pop("model"), "messages": messages,
           **{k: v for k, v in kw.items() if k not in consumed}}
open(out, "w").write(prompt)
sha = lambda s: hashlib.sha256(s.encode()).hexdigest()
print(json.dumps({"tree": os.path.abspath(tree), "engine_file": engine.__file__, "paper_id": pid,
                  "request_hash": request_hash(request), "request_keys": sorted(request),
                  "options": {k: v for k, v in request.items() if k not in ("messages",)},
                  "system_sha256": sha(messages[0]["content"]), "prompt_sha256": sha(prompt),
                  "prompt_chars": len(prompt), "n_units": um.n, "field_names": list(names)},
                 indent=1, default=str))
