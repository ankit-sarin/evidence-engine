"""12ai-U30B 2b census, before the edit (census_u30a.py's form: declared patterns over tests/, hit lines per file). Read-only.
usage, from the repo root: python3 -I -B census_u30b.py > census_u30b.txt
(A) reaches run_arm or the cloud main; (B) reaches send or store_result (or a transport's run/extract_paper); (C) asserts on the close
(end_status / end_reason / close_run / is_closed), the rollback (rollback / in_transaction), run_calls or the spans table; (D) pins a
KeyboardInterrupt or RunInterrupted on the cloud path (a file in A or B naming either); (E) stores a field outside the codebook through
the cloud store (a file in B naming an invented/unexpected/extra field). The class is the census's first reading, decided by the
selected runs (2f)."""
import re
from pathlib import Path
P = {"A": r"run_arm\(|run_cloud_extraction|cli\.main\(|rce\.main\(",
     "B": r"\.send\(|store_result\(|extract_with_completeness\(|\.extract_paper\(|OpenAIExtractor|AnthropicExtractor|CloudExtractorBase",
     "C": r"end_status|end_reason|close_run|is_closed|rollback|in_transaction|run_calls|cloud_evidence_spans",
     "K": r"KeyboardInterrupt|RunInterrupted",
     "U": r"invented|unexpected|extra_field|not_in_codebook|junk"}
CLASS = {"tests/test_failed_close_reason.py": "(c) its cloud interrupt test pins a 'failed' close, superseded by R805(d)(1) (E2)"}
out = {}
for f in sorted(Path("tests").rglob("*.py")):
    lines = f.read_text(encoding="utf-8", errors="replace").split("\n")
    hits = {t: [(i + 1, l.strip()[:150]) for i, l in enumerate(lines) if re.search(p, l)] for t, p in P.items()}
    if not (hits["A"] or hits["B"]):
        continue
    sets = {t: h for t, h in hits.items() if h}
    if "K" in sets: sets["D"] = sets.pop("K")
    if "U" in sets: sets["E"] = sets.pop("U")
    out[str(f)] = sets
for t in "ABCDE":
    print(f"== set {t}: {sum(1 for v in out.values() if t in v)} files")
print()
for f, sets in out.items():
    print(f"### {f}  sets {''.join(sorted(sets))}  class: {CLASS.get(f, '(n) unless the selected run is red')}")
    for t in sorted(sets):
        for n, l in sets[t][:8]:
            print(f"  {t} :{n}  {l}")
        if len(sets[t]) > 8:
            print(f"  {t} … {len(sets[t]) - 8} more")
