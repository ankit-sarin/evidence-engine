"""canonical_absence_sentinel reaches the extraction prompt but not the semantic hash."""
from _common import CODEBOOK, write, LIVE_SPEC
from engine.core.codebook import load_codebook, SEMANTIC_KEYS
from engine.core.review_spec import load_review_spec
from engine.agents.extractor import build_extraction_prompt
a = write("canon_a.yaml", CODEBOOK)
b = write("canon_b.yaml", CODEBOOK.replace('canonical_absence_sentinel: "NR"', 'canonical_absence_sentinel: "NOT_FOUND"'))
ca, cb = load_codebook(a), load_codebook(b)
spec = load_review_spec(LIVE_SPEC)
pa, pb = build_extraction_prompt("PAPER", spec, a), build_extraction_prompt("PAPER", spec, b)
print("SEMANTIC_KEYS:", SEMANTIC_KEYS)
print("canonical a/b:", ca.canonical_absence_sentinel, cb.canonical_absence_sentinel)
print("semantic_hash equal:", ca.semantic_hash == cb.semantic_hash)
print("byte sha256 equal:  ", ca.sha256 == cb.sha256)
print("legacy extraction prompt equal:", pa == pb)
print('prompt a says set to "NR":', 'set to "NR"' in pa, '| prompt b says set to "NOT_FOUND":', 'set to "NOT_FOUND"' in pb)
