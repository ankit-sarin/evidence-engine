"""12ae-U25A — FAKE probe for U25 (INT-i12-31). Three scratch codebook texts derived from the live one by a single-line text edit each, loaded
through engine.core.codebook.load_codebook; the first downstream readers driven in-process; then the live file. No server, no database:
nothing here opens a database or reaches a model (build_extraction_prompt and prompt_hash render text only). The live codebook is read and
hashed, never written. Options A and B are sketched here as functions over the parsed document / the text, to show their effect on the live
file and on the three scratch texts; neither touches engine code.
usage, from the repository root: .venv/bin/python fake_probe_u25a.py <fixtures dir>   (writes the three texts there; stdout: the record)"""
import hashlib, json, re, sys, traceback
from pathlib import Path
sys.path.insert(0, str(Path.cwd()))
import yaml
from engine.core import codebook as cbm
from engine.core.review_paths import load_spec_for
from engine.core.effective_config import stage_config, prompt_hash
from engine.agents.extractor import build_extraction_prompt, _build_field_block

FX = Path(sys.argv[1]); FX.mkdir(parents=True, exist_ok=True)
LIVE = Path("data/surgical_autonomy/extraction_codebook.yaml")
live_bytes = LIVE.read_bytes(); live_sha = hashlib.sha256(live_bytes).hexdigest(); text = live_bytes.decode()
RECORDED_SEMANTIC = json.load(open("docs/session-reports/session-12/12ad_close/pin_tuples_close.json"))
def find(o, k):
    if isinstance(o, dict):
        for kk, v in o.items():
            if kk == k: yield v
            yield from find(v, k)
    elif isinstance(o, list):
        for v in o: yield from find(v, k)
rec_sem = sorted(set(find(RECORDED_SEMANTIC, "codebook_semantic_hash")))
print(f"live codebook {LIVE} sha256 {live_sha} {len(live_bytes)} B (before)")
print(f"recorded codebook_semantic_hash values in 12ad_close/pin_tuples_close.json: {rec_sem}")

def one_edit(old, new):
    assert text.count(old) >= 1, old
    return text.replace(old, new, 1)
first_value = re.search(r'(\n(\s*)- value: "([^"]+)")', text)
variants = {
    "value_yes": (one_edit(first_value.group(1), f'\n{first_value.group(2)}- value: Yes'),
                  f"the first valid_values entry `value: \"{first_value.group(3)}\"` → `value: Yes` (unquoted)"),
    "sentinel_no": (one_edit('\n  - "NA"\n', '\n  - NO\n'), 'absence_sentinels item `- "NA"` → `- NO` (unquoted)'),
    "sentinel_null": (one_edit('\n  - "NA"\n', '\n  - null\n'), 'absence_sentinels item `- "NA"` → `- null` (unquoted)'),
}
spec = load_spec_for("surgical_autonomy")

def try_(label, fn):
    try:
        r = fn(); print(f"    {label}: returned {r!r}"[:300]); return r
    except Exception as exc:
        tb = traceback.extract_tb(exc.__traceback__)[-1]
        print(f"    {label}: RAISED {type(exc).__name__}: {exc} — at {Path(tb.filename).relative_to(Path.cwd()) if tb.filename.startswith(str(Path.cwd())) else tb.filename}:{tb.lineno} `{tb.line}`")

for name, (t, desc) in variants.items():
    d = FX / name; d.mkdir(exist_ok=True); p = d / "extraction_codebook.yaml"; p.write_text(t)
    print(f"\n=== {name}: {desc}; written to {p} (sha256 {hashlib.sha256(t.encode()).hexdigest()})")
    cbm.clear_cache()
    cb = try_("load_codebook", lambda: cbm.load_codebook(p).semantic_hash)
    if cb is None: continue
    c = cbm.load_codebook(p)
    print(f"    semantic_hash equal to the live file's: {c.semantic_hash == cbm.load_codebook(LIVE).semantic_hash}")
    if name == "value_yes":
        fname = next(f["name"] for f in c.fields if f.get("valid_values"))
        try_(f"enum_values({fname!r})", lambda: c.enum_values(fname))
        try_("_build_field_block (both prompt paths render through it)", lambda: _build_field_block(c.field(fname)))
        try_("build_extraction_prompt (legacy path)", lambda: build_extraction_prompt("x", spec, p)[:40])
        try_("prompt_hash('extract_pass1', …) — the render open_run makes", lambda: prompt_hash("extract_pass1", spec, stage_config("extract_pass1", spec), codebook_path=p))
        try_("human_import._load_codebook_valid_values (.lower())", lambda: __import__("analysis.paper1.human_import", fromlist=["x"])._load_codebook_valid_values(p))
    else:
        try_("absence_sentinels (the loaded tuple)", lambda: c.absence_sentinels)
        tok = "NO" if name == "sentinel_no" else "null"
        try_(f"is_absence_sentinel({tok!r}) — the token as the codebook author wrote it", lambda: c.is_absence_sentinel(tok))
        coerced = "False" if name == "sentinel_no" else "None"
        try_(f"is_absence_sentinel({coerced!r}) — the coerced string", lambda: c.is_absence_sentinel(coerced))
        try_("build_extraction_prompt (legacy path) renders", lambda: "rendered" if build_extraction_prompt("x", spec, p) else None)

print("\n=== the live file")
cbm.clear_cache(); live = cbm.load_codebook(LIVE)
print(f"    load_codebook: OK; semantic_hash {live.semantic_hash}; equal to the recorded value: {[live.semantic_hash] == rec_sem}")
print(f"    sha256 {live.sha256} equal to the file's: {live.sha256 == live_sha}")

print("\n=== option A sketched: a type check over the parsed document, after safe_load, before the model")
def option_a(doc):
    bad = []
    for f in doc["fields"]:
        for v in f.get("valid_values") or []:
            if not isinstance(v.get("value"), str): bad.append(f"fields[{f['name']}].valid_values value {v.get('value')!r}")
    for i, s in enumerate(doc["absence_sentinels"]):
        if not isinstance(s, str): bad.append(f"absence_sentinels[{i}] {s!r}")
    return bad
print(f"    live: {option_a(yaml.safe_load(text)) or 'no finding'}")
for name, (t, _) in variants.items(): print(f"    {name}: {option_a(yaml.safe_load(t))}")

print("\n=== option B sketched: a SafeLoader without the bool and null implicit resolvers")
class NoBoolNull(yaml.SafeLoader): pass
NoBoolNull.yaml_implicit_resolvers = {k: [(tag, rx) for tag, rx in v if tag not in ("tag:yaml.org,2002:bool", "tag:yaml.org,2002:null")]
                                      for k, v in yaml.SafeLoader.yaml_implicit_resolvers.items()}
live_b = yaml.load(text, Loader=NoBoolNull)
print(f"    live: parsed document equal to safe_load's: {live_b == yaml.safe_load(text)}; semantic hash equal: {cbm.compute_semantic_hash(live_b) == live.semantic_hash}")
for name, (t, _) in variants.items():
    d = yaml.load(t, Loader=NoBoolNull)
    if name == "value_yes": print(f"    {name}: the edited value loads as {next(v['value'] for f in d['fields'] for v in (f.get('valid_values') or []))!r}")
    else: print(f"    {name}: absence_sentinels {d['absence_sentinels']}")

after = hashlib.sha256(LIVE.read_bytes()).hexdigest()
print(f"\nlive codebook sha256 {after} (after); unchanged: {after == live_sha}")
