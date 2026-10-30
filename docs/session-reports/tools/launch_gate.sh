#!/bin/bash
# Launch the standard gate detached, writing the launch line and the before-gate evidence (R746(e)(iv), R738(c), R660(b)).
# usage: launch_gate.sh <evidence_dir> <session_name> <ref_ids|-> <ref_durations|-> [selection...]   (from the repository root)
# Wait on <evidence_dir>/gate_rc.txt, which run_gate.sh writes last.
set -e
E=$(readlink -f "$1"); SESSION=$2; REF=$3; REFD=$4; shift 4; HERE=$(dirname "$(readlink -f "$0")")
mkdir -p "$E"; [ ! -e "$E/gate_rc.txt" ] || { echo "REFUSED: $E already holds a gate"; exit 2; }
tmux has-session -t "$SESSION" 2>/dev/null && { echo "REFUSED: tmux session $SESSION exists"; exit 2; }
cp "$HERE/run_gate.sh" "$E/run_gate.sh"
cat > "$E/host_snapshot.sh" <<'SNAP'
#!/bin/bash
date -u; uptime; top -b -n1 | head -14; free -m; uname -r; python3 --version; dpkg-query -W -f='${Version}\n' libsqlite3-0; cat /proc/driver/nvidia/version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1
cat /proc/sys/kernel/random/boot_id; systemctl show ollama -p NRestarts -p ExecMainStartTimestamp
echo "earlyoom: $(systemctl is-active earlyoom)"; ps -o args= -C earlyoom; echo "vm.min_free_kbytes $(cat /proc/sys/vm/min_free_kbytes)"
for p in $(pgrep -x ollama); do echo "ollama pid $p oom_score_adj $(cat /proc/$p/oom_score_adj)"; done
grep MemAvailable /proc/meminfo
echo "-- concurrent jobs (ffmpeg / nvenc / surgical-cv / uvicorn / pytest), this script and its ancestors excluded (R680(c))"; anc=" $$ "; p=$$; while [ "$p" -gt 1 ]; do p=$(ps -o ppid= -p $p | tr -d " "); anc="$anc$p "; done; echo "excluded pids:$anc"; ps -eo pid=,lstart=,args= | grep -E "ffmpe[g]|nven[c]|surgical-c[v]|uvicor[n]|pytes[t]" | while read pid rest; do case "$anc" in *" $pid "*) ;; *) echo "$pid $rest" | cut -c1-250;; esac; done; exit 0
SNAP
cat > "$E/ollama_reads.sh" <<'READS'
#!/bin/bash
# Ollama version / tags / ps reads (R660(b)), outside any gate window.
for ep in version tags ps; do
  echo "$(date -u +%FT%T.%NZ) /api/$ep"
  curl -s --max-time 10 http://localhost:11434/api/$ep | python3 -c '
import json,sys
d=json.load(sys.stdin); ep=sys.argv[1]
if ep=="tags": d={m["name"]:m["digest"] for m in d.get("models",[]) if m["name"] in ("gemma3:27b","qwen3:32b","deepseek-r1:32b")}
print(json.dumps(d))' $ep
done
READS
bash "$E/ollama_reads.sh" > "$E/ollama_reads_before.txt" 2>&1
bash "$E/host_snapshot.sh" > "$E/host_snapshot_before_gate.txt" 2>&1
{ git rev-parse HEAD; git status --short; } > "$E/tree_at_launch.txt"
T=/tmp/pytest-of-$USER; { date -u +%FT%T.%NZ; ls -la --time-style=full-iso "$T"; du -sb "$T"; } > "$E/temp_root_before.txt" 2>&1
sleep 6   # the before-gate reads are at least five seconds before the launch (R660(b))
LINE="tmux new-session -d -s $SESSION \"bash $E/run_gate.sh $E $SESSION $REF $REFD $*\""
{ echo "$(date -u +%FT%T.%NZ) launched by the line below, written into the evidence directory at launch (R731(b), R738(c))"; echo "$LINE"; } > "$E/launch_line.txt"
eval "$LINE"
cat "$E/launch_line.txt"
