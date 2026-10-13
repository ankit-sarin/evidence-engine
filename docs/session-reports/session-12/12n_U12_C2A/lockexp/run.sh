#!/bin/bash
# 12n-U12-C2A A2 — INT-g12-7 on a SCRATCH lock path. Every case runs under `timeout`; 124 = it blocked.
set -u
cd ~/projects/evidence-engine
export OLLAMA_EXPERIMENT_LOCK="$HOME/scratch/12n-c2a/lockexp/scratch.lock"
export PYTHONPATH=.
PY=.venv/bin/python; C="$HOME/scratch/12n-c2a/lockexp/child.py"; T=8
: > "$OLLAMA_EXPERIMENT_LOCK"
echo "start $(date -u +%Y-%m-%dT%H:%M:%SZ) lock=$OLLAMA_EXPERIMENT_LOCK"
echo "E0 control: no outer lock, blocking acquire";            timeout $T $PY $C blocking; echo "  exit=$?"
echo "E1 outer flock(1) wrapper: flock <lock> python (fd inherited)"; timeout $T flock "$OLLAMA_EXPERIMENT_LOCK" $PY $C blocking; echo "  exit=$?"
echo "E2 outer flock -o (fd closed before exec; flock(1) parent holds)"; timeout $T flock -o "$OLLAMA_EXPERIMENT_LOCK" $PY $C blocking; echo "  exit=$?"
echo "E3 inherited fd 9, the health job's pattern (exec 9>>lock; flock -n 9)"; ( exec 9>>"$OLLAMA_EXPERIMENT_LOCK"; flock -n 9 || exit 99; timeout $T $PY $C blocking; echo "  exit=$?" )
echo "E4 fd 9 held by the shell, closed for the child (9>&-)"; ( exec 9>>"$OLLAMA_EXPERIMENT_LOCK"; flock -n 9 || exit 99; timeout $T $PY $C blocking 9>&-; echo "  exit=$?" )
echo "E5 same as E1, non-blocking acquire";                   timeout $T flock "$OLLAMA_EXPERIMENT_LOCK" $PY $C nonblocking; echo "  exit=$?"
echo "E6 a Python holder, its subprocess child and its fork child"; timeout 40 $PY $C holder_spawns; echo "  exit=$?"
echo "end $(date -u +%Y-%m-%dT%H:%M:%SZ)"
stat -c '%y %n' "$HOME/.ollama_experiment.lock"
