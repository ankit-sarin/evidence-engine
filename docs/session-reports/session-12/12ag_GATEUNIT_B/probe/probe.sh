#!/bin/bash
# 12ag-GATEUNIT-B step 2 — the probe unit's command. Writes what the unit got, then waits so its status can be read.
# Environment: NAMES only, never a value, except LANG and PATH (R808(b)(3); the brief's step 2).
O=$1
{ echo "started $(date -u +%FT%T.%NZ)"
  echo "nofile soft $(ulimit -Sn) hard $(ulimit -Hn)"
  echo "cgroup $(cat /proc/self/cgroup)"
  echo "oom_score_adj $(cat /proc/self/oom_score_adj)"
  echo "nice $(nice)"
  echo "LANG=$LANG"
  echo "PATH=$PATH"
  echo "pwd $(pwd)"
  echo "env names: $(env | sed 's/=.*//' | sort | tr '\n' ' ')"
} > "$O"
sleep 15
echo "done $(date -u +%FT%T.%NZ)" >> "$O"
