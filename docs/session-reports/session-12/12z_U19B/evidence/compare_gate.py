"""Gate id and durations comparison against the 12y_close reference, in 12z-OPEN's
id_and_durations_compare.txt form. usage: compare_gate.py <gate_dir> <durations_file> <new_ids_file|-> [ref_ids]"""
import re, sys
from pathlib import Path
G = Path(sys.argv[1]); DUR = G / sys.argv[2]
REF = Path(sys.argv[4] if len(sys.argv) > 4 else
           "docs/session-reports/session-12/12y_close/collected_ids_12y_close.txt")
REFDUR = Path("docs/session-reports/session-12/12y_close/durations_12y_u18c.txt")
new = set() if sys.argv[3] == "-" else set(l for l in Path(sys.argv[3]).read_text().splitlines() if l)
log = (G / "gate.log").read_text()
ids = []
for line in log.splitlines():
    m = re.match(r"^(tests/\S+?::.*?) (PASSED|FAILED|ERROR|XFAIL|XPASS|SKIPPED)(?: |$)", line)
    if m: ids.append(m.group(1))
gate, ref = set(ids), set(l for l in REF.read_text().splitlines() if l)
(G / "collected_ids.txt").write_text("\n".join(sorted(gate)) + "\n")
og, orf = sorted(gate - ref), sorted(ref - gate)
print(f"gate ids {len(gate)} ref {len(ref)}")
print(f"only-gate: {len(og)}; only-ref: {len(orf)}")
print(f"only-gate == the new ids: {set(og) == new} ({len(new)} new)")
for i in og: print("  only-gate", i)
for i in orf: print("  only-ref", i)
def dur(p):
    d = {}
    for l in Path(p).read_text().splitlines():
        m = re.match(r"^([0-9.]+)s (call|setup|teardown)\s+(\S.*)$", l)
        if m: d[(m.group(2), m.group(3))] = float(m.group(1))
    return d
a, b = dur(REFDUR), dur(DUR)
print(f"phases ref {len(a)} gate {len(b)}")
slow = sorted(((b[k] - a.get(k, 0.0), k) for k in b if b[k] - a.get(k, 0.0) > 10), reverse=True)
print(f"slower by >10 s: {len(slow)}")
for d, k in slow: print(f"  +{d:.2f}s {k[0]} {k[1]} (ref {a.get(k, 0.0):.2f}s, gate {b[k]:.2f}s)")
print("-- summary line"); print([l for l in log.splitlines() if re.search(r"(passed|failed).* in [0-9.]+s", l)][-1])
print("-- warnings")
w = log.split("warnings summary", 1)
print(w[1].split("-- Docs", 1)[0].strip()[:1500] if len(w) > 1 else "(none)")
