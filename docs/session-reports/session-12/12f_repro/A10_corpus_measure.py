"""A10 — how often does a unit (or an adjacent-unit pair) built from the comment-stripped
text fail the locator's EXACT test against the RAW parsed text? Reads parsed_text/*.md files
only (no database, no model). First 60 files by name; fuzzy re-test on up to 40 single-unit misses."""
import pathlib, sys
from engine.elicitation.units import build_unit_map
from engine.core.locator import normalize, locate
d = pathlib.Path("/home/ankitsarin/projects/evidence-engine/data/surgical_autonomy/parsed_text")
files = sorted(d.glob("*.md"))[:60]
tot_u = miss_u = tot_p = miss_p = 0; fuzzy_checked = fuzzy_miss = 0; files_hit = 0
for f in files:
    raw = f.read_text(errors="replace"); um = build_unit_map(0, raw); nt = normalize(raw); hit = False
    for i, u in enumerate(um.units):
        tot_u += 1
        if normalize(u) not in nt:
            miss_u += 1; hit = True
            if fuzzy_checked < 40 and len(u.split()) <= 60:
                fuzzy_checked += 1; fuzzy_miss += (not locate(raw, u).located)
        if i + 1 < um.n:
            tot_p += 1
            if normalize(f"{u} {um.units[i+1]}") not in nt: miss_p += 1; hit = True
    files_hit += hit
print(f"files measured: {len(files)}; files with >=1 miss: {files_hit}")
print(f"single units: {tot_u}; not EXACT-locatable in raw text: {miss_u} ({100*miss_u/tot_u:.2f}%)")
print(f"  of the first {fuzzy_checked} such units (<=60 words), not located even FUZZY: {fuzzy_miss}")
print(f"adjacent unit pairs: {tot_p}; joined pair not EXACT-locatable in raw text: {miss_p} ({100*miss_p/tot_p:.2f}%)")
