"""L12-1: parse_response_to_spans coercions; Anthropic ''->NULL vs null->sentinel. No client, no DB."""
import json, logging
from types import SimpleNamespace
from engine.cloud.base import CloudExtractorBase
from engine.cloud.anthropic_extractor import AnthropicExtractor

logging.basicConfig(level=logging.WARNING)
base = object.__new__(CloudExtractorBase)
base._codebook = SimpleNamespace(canonical_absence_sentinel="NR")
base._last_salvage = None

resp = {"fields": [
    {"field_name": "no_value_key", "source_snippet": "The trial enrolled 40 patients.", "confidence": 0.9, "tier": 1},
    {"field_name": "list_value", "value": ["RCT", "Cohort"], "source_snippet": "q", "confidence": 0.9, "tier": 1},
    {"field_name": "bool_value", "value": True, "source_snippet": "q", "confidence": 0.9, "tier": 1},
    {"field_name": "dict_value", "value": {"n": 40}, "source_snippet": "q", "confidence": 0.9, "tier": 1},
    {"field_name": "explicit_null", "value": None, "source_snippet": "q", "confidence": 0.9, "tier": 1},
]}
for s in base.parse_response_to_spans(json.loads(json.dumps(resp))):
    print(f"{s['field_name']:14s} value={s['value']!r:28s} snippet={s['source_snippet']!r}")

# Anthropic transport: '' -> None (NULL), while null -> 'NR'
ax = object.__new__(AnthropicExtractor)
ax._codebook = base._codebook; ax._last_salvage = None
ax.spec = None; ax.cost_input_per_m = 3.0; ax.cost_output_per_m = 15.0
ax.build_prompt = lambda text: "PROMPT"
body = {"fields": [
    {"field_name": "empty_string", "value": "", "source_snippet": "", "confidence": 0.5, "tier": 1},
    {"field_name": "json_null", "value": None, "source_snippet": "", "confidence": 0.5, "tier": 1}]}
fake = SimpleNamespace(stop_reason="end_turn",
    content=[SimpleNamespace(type="text", text=json.dumps(body))],
    usage=SimpleNamespace(input_tokens=1, output_tokens=1))
ax.send = lambda pid, prompt: fake           # stubbed: no client exists
out = ax.extract_paper(1, "text")
print("anthropic:", [(s["field_name"], s["value"], s.get("notes")) for s in out["spans"]])
