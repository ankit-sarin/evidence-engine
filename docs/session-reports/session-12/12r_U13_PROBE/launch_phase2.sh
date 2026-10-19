#!/bin/bash
# 12r-U13-PROBE Phase 2 launch. Writes only under ~/scratch/12r-u13-probe/.
Q=$HOME/scratch/12r-u13-probe; cd $HOME/projects/evidence-engine
.venv/bin/python $Q/run_phase2.py $HOME/scratch/12r-u13-pa/render $Q/stored_hashes.json > $Q/phase2_stdout.txt 2>&1
echo $? > $Q/phase2_rc.txt
