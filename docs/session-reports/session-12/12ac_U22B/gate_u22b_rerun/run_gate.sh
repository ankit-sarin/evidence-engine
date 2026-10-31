#!/bin/bash
# The standard gate, run inside tmux by launch_gate.sh (R746(e)(iv)). usage: run_gate.sh <evidence_dir> <session> <ref_ids|-> <ref_durations|-> [selection...]
# With no selection it is the nightly's standard selection, in the shared temp root; a selection passes --basetemp under
# the evidence directory's sibling (R703(j)). Writes the nineteen-file set of record (R738(d)) and writes gate_rc.txt
# last, so a wait may watch that file.
E=$1; SESSION=$2; REF=$3; REFD=$4; shift 4; cd "$(git -C "$(dirname "$(readlink -f "$0")")" rev-parse --show-toplevel 2>/dev/null || echo "$HOME/projects/evidence-engine")"
[ -d .venv ] || cd "$HOME/projects/evidence-engine"
{ date -u +%FT%T.%NZ; echo "run_gate.sh $E $SESSION $REF $REFD $*"; } > "$E/gate_launch.txt"; s=$(date +%s.%N)
if [ $# -eq 0 ]; then SEL=(tests/); BT=(); else SEL=("$@"); BT=(--basetemp="$E.basetemp"); fi
.venv/bin/python -m pytest "${SEL[@]}" -v --tb=short --durations=0 -m "not network and not ollama and not integration" "${BT[@]}" > "$E/gate.log" 2>&1; rc=$?
e=$(date +%s.%N); date -u +%FT%T.%NZ > "$E/gate_exit.txt"
echo "pytest exit code: $rc" >> "$E/gate.log"; echo "wall: $(echo "$e - $s" | bc) s" > "$E/gate_wall.txt"
grep -E '^[0-9]+\.[0-9]+s (call|setup|teardown) ' "$E/gate.log" > "$E/durations_$SESSION.txt"
T=/tmp/pytest-of-$USER; { date -u +%FT%T.%NZ; ls -la --time-style=full-iso "$T"; du -sb "$T"; } > "$E/temp_root_after.txt" 2>&1
bash "$E/host_snapshot.sh" > "$E/host_snapshot_after_gate.txt" 2>&1
sleep 6; bash "$E/ollama_reads.sh" > "$E/ollama_reads_after.txt" 2>&1
python3 - "$E" "$SESSION" "$REF" "$REFD" > "$E/id_and_durations_compare.txt" 2>&1 <<'PY'
import re, sys
from pathlib import Path
G, session, REF, REFD = Path(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]
log = (G / "gate.log").read_text(errors="replace")
ids = [m.group(1) for m in (re.match(r"^(tests/\S+?::.*?) (PASSED|FAILED|ERROR|XFAIL|XPASS|SKIPPED)(?: |$)", l) for l in log.splitlines()) if m]
(G / "collected_ids.txt").write_text("\n".join(sorted(set(ids))) + "\n")
gate = set(ids)
if REF == "-":
    print(f"gate ids {len(gate)}; no id reference named")
else:
    ref = set(l for l in Path(REF).read_text().splitlines() if l)
    og, orf = sorted(gate - ref), sorted(ref - gate)
    print(f"gate ids {len(gate)} ref {len(ref)}\nonly-gate: {len(og)}; only-ref: {len(orf)}")
    for i in og: print("  only-gate", i)
    for i in orf: print("  only-ref", i)
def dur(p):
    d = {}
    for l in Path(p).read_text().splitlines():
        m = re.match(r"^([0-9.]+)s (call|setup|teardown)\s+(\S.*)$", l)
        if m: d[(m.group(2), m.group(3))] = float(m.group(1))
    return d
b = dur(G / f"durations_{session}.txt")
if REFD == "-":
    print(f"phases gate {len(b)}; no durations reference named")
else:
    a = dur(REFD); slow = sorted(((b[k] - a.get(k, 0.0), k) for k in b if b[k] - a.get(k, 0.0) > 10), reverse=True)
    print(f"phases ref {len(a)} gate {len(b)}\nslower by >10 s: {len(slow)}")
    for d, k in slow: print(f"  +{d:.2f}s {k[0]} {k[1]} (ref {a.get(k, 0.0):.2f}s, gate {b[k]:.2f}s)")
print("-- summary line"); print(([l for l in log.splitlines() if re.search(r"(passed|failed).* in [0-9.]+s", l)] or ["(none)"])[-1])
print("-- warnings"); w = log.split("warnings summary", 1)
print(w[1].split("-- Docs", 1)[0].strip()[:1500] if len(w) > 1 else "(none)")
PY
echo "$rc" > "$E/gate_rc.txt"
