"""A10 — attribute the misses of A10_corpus_measure: a miss against the RAW text that is a hit
against the comment-STRIPPED text is caused by comment stripping; a miss against both has
another cause (the segmenter). Same 60 files; exact test only; file reads only."""
import pathlib
from engine.elicitation.units import build_unit_map
from engine.core.locator import normalize
d = pathlib.Path("/home/ankitsarin/projects/evidence-engine/data/surgical_autonomy/parsed_text")
u_c = u_o = p_c = p_o = 0; ex = []
for f in sorted(d.glob("*.md"))[:60]:
    raw = f.read_text(errors="replace"); um = build_unit_map(0, raw)
    nr, ns = normalize(raw), normalize(um.source_stripped)
    for i, u in enumerate(um.units):
        n = normalize(u)
        if n not in nr:
            if n in ns: u_c += 1
            else:
                u_o += 1
                if len(ex) < 3: ex.append((f.name, u[:160]))
        if i + 1 < um.n:
            n2 = normalize(f"{u} {um.units[i+1]}")
            if n2 not in nr:
                if n2 in ns: p_c += 1
                else: p_o += 1
print(f"single-unit misses: caused by comment stripping {u_c}; other cause {u_o}")
print(f"adjacent-pair misses: caused by comment stripping {p_c}; other cause {p_o}")
for e in ex: print("  other-cause example:", e)
