"""12w-U17A Q8: scratch copies of the live spec, each adding a family attribute in one candidate form.
Text insertion only, anchored on exact live lines, so every other byte is the live spec's. Writes
<form>_<reading>.yaml beside this script. Never under review_specs/ or data/."""
import pathlib, hashlib
HERE = pathlib.Path(__file__).parent
LIVE = pathlib.Path.home() / "projects/evidence-engine/review_specs/surgical_autonomy.yaml"
src = LIVE.read_text(encoding="utf-8")
assert hashlib.sha256(src.encode()).hexdigest() == "917c24de29afdf802b5ddea61c34627a9bb0a857de3f97395af36d44ca664e5f"
# family values per model under each reading (Q4 table): (i) vendor of the weights as served,
# (ii) lineage of the base model. Lowercase slugs.
READ = {
  "vendor":  {"qwen3:8b": "alibaba", "qwen3:32b": "alibaba", "gemma3:27b": "google",
              "deepseek-r1:32b": "deepseek", "qwen2.5vl:7b": "alibaba",
              "o4-mini-2025-04-16": "openai", "claude-sonnet-4-6": "anthropic"},
  "lineage": {"qwen3:8b": "qwen", "qwen3:32b": "qwen", "gemma3:27b": "gemma",
              "deepseek-r1:32b": "qwen", "qwen2.5vl:7b": "qwen",
              "o4-mini-2025-04-16": "openai", "claude-sonnet-4-6": "anthropic"},
}
def ins_after(text, anchor, new):
    assert text.count(anchor) == 1, anchor
    return text.replace(anchor, anchor + new, 1)
def per_role(f):
    t = src
    t = ins_after(t, '  primary: "qwen3:8b"\n  verification: "gemma3:27b"\n',
                  f'  primary_family: {f["qwen3:8b"]}\n  verification_family: {f["gemma3:27b"]}\n')
    t = ins_after(t, '  primary: "qwen3:32b"\n  verifier: "gemma3:27b"\n',
                  f'  primary_family: {f["qwen3:32b"]}\n  verifier_family: {f["gemma3:27b"]}\n')
    t = ins_after(t, '  extractor: "deepseek-r1:32b"\n', f'  extractor_family: {f["deepseek-r1:32b"]}\n')
    for m in ("deepseek-r1:32b", "o4-mini-2025-04-16", "claude-sonnet-4-6"):
        t = ins_after(t, f'    model: "{m}"\n', f'    family: {f[m]}\n')
    t = ins_after(t, '  ai_model: "qwen2.5vl:7b"\n', f'  ai_model_family: {f["qwen2.5vl:7b"]}\n')
    # audit and pdf_parsing are absent from the live spec (declared defaults); the form must add them
    t = ins_after(t, 'low_yield_threshold: 4\n',
                  f'\naudit:\n  family: {f["gemma3:27b"]}\n\npdf_parsing:\n  vision_model_family: {f["qwen2.5vl:7b"]}\n')
    return t
def model_map(f):
    lines = "".join(f'  "{m}": {v}\n' for m, v in f.items())
    return ins_after(src, 'low_yield_threshold: 4\n', "\n# family of every model this spec names (12w-U17A scratch form B-map)\nmodel_families:\n" + lines)
out = []
for form, fn in (("per_role", per_role), ("model_map", model_map)):
    for reading, f in READ.items():
        p = HERE / f"{form}_{reading}.yaml"; t = fn(f); p.write_text(t, encoding="utf-8")
        out.append(f"{hashlib.sha256(t.encode()).hexdigest()}  {p.name}  (+{t.count(chr(10)) - src.count(chr(10))} lines)")
print("\n".join(out))
