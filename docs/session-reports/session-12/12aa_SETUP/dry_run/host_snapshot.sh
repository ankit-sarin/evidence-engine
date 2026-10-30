#!/bin/bash
date -u; uptime; top -b -n1 | head -14; free -m; uname -r; python3 --version; dpkg-query -W -f='${Version}\n' libsqlite3-0; cat /proc/driver/nvidia/version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1
cat /proc/sys/kernel/random/boot_id; systemctl show ollama -p NRestarts -p ExecMainStartTimestamp
echo "earlyoom: $(systemctl is-active earlyoom)"; ps -o args= -C earlyoom; echo "vm.min_free_kbytes $(cat /proc/sys/vm/min_free_kbytes)"
for p in $(pgrep -x ollama); do echo "ollama pid $p oom_score_adj $(cat /proc/$p/oom_score_adj)"; done
grep MemAvailable /proc/meminfo
echo "-- concurrent jobs (ffmpeg / nvenc / surgical-cv / uvicorn / pytest), this script and its ancestors excluded (R680(c))"; anc=" $$ "; p=$$; while [ "$p" -gt 1 ]; do p=$(ps -o ppid= -p $p | tr -d " "); anc="$anc$p "; done; echo "excluded pids:$anc"; ps -eo pid=,lstart=,args= | grep -E "ffmpe[g]|nven[c]|surgical-c[v]|uvicor[n]|pytes[t]" | while read pid rest; do case "$anc" in *" $pid "*) ;; *) echo "$pid $rest" | cut -c1-250;; esac; done; exit 0
