"""12x-AQA A3 — audit's runtime messages per live codebook field vs the rendered variants.
The U39 audit harness (tests/test_u39_runtime_equals_hash.py: `_Sent`, `_fake`, the call to
`semantic_verify`), outside pytest: `auditor.ollama_chat` is replaced by a stand-in that
raises with the messages; nothing is sent. field_type per field is the runtime's own
expression, audit_events.py:166 `types = {f["name"]: f.get("type", "text") for f in
codebook.fields}`. The span carries the render's sentinel value and snippet ("v", "s") and
the field's real name (A3 as briefed). Columns: (i) byte-identical to one rendered variant;
(ii) identical after the field-type value is replaced by the matched variant's; (ii') the
same with the field name also replaced by the render's "f" (both are substituted values);
(iii) the first difference left after (ii)."""
import copy, hashlib, json, sys
sys.path.insert(0, ".")
from engine.core import effective_config as ec
from engine.core.codebook import load_codebook
from engine.core.review_paths import load_spec_for
from engine.agents import auditor as A
from engine.agents.models import EvidenceSpan
class _Sent(BaseException):
    def __init__(self, m): self.messages = copy.deepcopy(m)
def _fake(**kw): raise _Sent(kw["messages"])
A.ollama_chat = _fake
spec = load_spec_for("surgical_autonomy")
cb = load_codebook("data/surgical_autonomy/extraction_codebook.yaml")
cfg = ec.stage_config("audit", spec)
rendered = ec.render_messages("audit", spec)
variants = {"text": rendered[0:2], "categorical": rendered[2:4]}
assert all(v[0]["role"] == "system" for v in variants.values()) and len(rendered) == 4
types = {f["name"]: f.get("type", "text") for f in cb.fields}
canon = lambda x: ec.canonical_json(x)
def first_diff(a, b):
    sa, sb = canon(a), canon(b)
    i = next((k for k in range(min(len(sa), len(sb))) if sa[k] != sb[k]), min(len(sa), len(sb)))
    return f"@{i}: runtime …{sa[max(0,i-40):i+40]!r}… vs rendered …{sb[max(0,i-40):i+40]!r}…"
def sub(msgs, old, new):
    return [{**m, "content": m["content"].replace(old, new)} for m in msgs]
rows = []
for f in cb.fields:
    name, t = f["name"], types[f["name"]]
    span = EvidenceSpan(field_name=name, value="v", source_snippet="s", confidence=0.5, tier=1)
    try:
        A.semantic_verify(span, field_type=t, cfg=cfg); raise SystemExit("no call")
    except _Sent as s:
        sent = s.messages
    branch = "categorical" if t == "categorical" else "text"
    var = variants[branch]
    i = any(sent == v for v in variants.values())
    ft_fixed = sub(sent, f"Field type: {t}\n", f"Field type: {branch}\n")
    ii = ft_fixed == var
    both = sub(ft_fixed, name, "f")
    ii2 = both == var
    rows.append({"field": name, "type": t, "branch": branch, "i": i, "ii": ii, "ii_name_too": ii2,
                 "iii": "" if ii else first_diff(ft_fixed, var),
                 "runtime_sha256": hashlib.sha256(canon(sent).encode()).hexdigest()})
print("| field | codebook type | builder branch | (i) identical to a variant | (ii) after type value replaced | (ii') name also replaced | (iii) first difference after (ii) |")
print("|---|---|---|---|---|---|---|")
for r in rows:
    print(f"| {r['field']} | {r['type']} | {r['branch']} | {r['i']} | {r['ii']} | {r['ii_name_too']} | {r['iii'][:160]} |")
from collections import Counter
print()
for t in sorted(set(types.values())):
    rs = [r for r in rows if r["type"] == t]
    print(f"{t}: {len(rs)} fields; (i) {sum(r['i'] for r in rs)}; (ii) {sum(r['ii'] for r in rs)}; (ii') {sum(r['ii_name_too'] for r in rs)}")
print("total fields", len(rows), "; 'text' among codebook types:", "text" in set(types.values()))
json.dump(rows, open(sys.argv[1], "w"), indent=1)
