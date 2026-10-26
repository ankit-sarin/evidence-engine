"""12x-U39A Q3 diagnostic (not a test, not the U39 file): the abstract_screen_verifier
lists, captured with the spec's field name `screening_models.verification`, to record both
sha256 values for the test stopped on HEAD. ollama_chat is replaced in-process; no server."""
import copy, hashlib, sys
sys.path.insert(0, ".")
from engine.core import effective_config as ec
from engine.core.review_paths import load_spec_for
from engine.agents import screener as S
class _Sent(BaseException):
    def __init__(self, m): self.messages = copy.deepcopy(m)
def _fake(**kw): raise _Sent(kw["messages"])
S.ollama_chat = _fake
spec = load_spec_for("surgical_autonomy")
try:
    S.screen_paper(dict(ec.SENTINEL_PAPER), spec, 1, model=spec.screening_models.verification, role="verifier")
    raise SystemExit("no call made")
except _Sent as s:
    runtime = s.messages
rendered = ec.render_messages("abstract_screen_verifier", spec)
h = lambda x: hashlib.sha256(ec.canonical_json(x).encode()).hexdigest()
print("runtime  sha256", h(runtime), "roles", [m["role"] for m in runtime])
print("rendered sha256", h(rendered), "roles", [m["role"] for m in rendered])
print("equal:", runtime == rendered)
