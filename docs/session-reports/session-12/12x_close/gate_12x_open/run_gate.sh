#!/bin/bash
# 12x-OPEN standard gate (copied from 12w_U17B/gate_12w_u17b/run_gate.sh, paths changed only), the nightly's selection with --durations=0. Writes only under ~/scratch/12x-open/gate/.
O=$HOME/scratch/12x-open/gate; cd $HOME/projects/evidence-engine
date -u +%FT%T.%NZ > $O/gate_launch.txt; s=$(date +%s.%N)
.venv/bin/python -m pytest tests/ -v --tb=short --durations=0 -m "not network and not ollama and not integration" > $O/gate.log 2>&1; rc=$?
e=$(date +%s.%N); date -u +%FT%T.%NZ > $O/gate_exit.txt
echo "pytest exit code: $rc" >> $O/gate.log; echo "wall: $(echo "$e - $s" | bc) s" > $O/gate_wall.txt
grep -E '^[0-9]+\.[0-9]+s (call|setup|teardown) ' $O/gate.log > $O/durations_12x_open.txt
echo "$rc" > $O/gate_rc.txt
