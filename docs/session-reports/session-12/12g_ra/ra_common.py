"""Shared by the 12g Rehearsal A analysis scripts. Read-only on the retained copy.
Usage of every script, from the repository root:
    .venv/bin/python docs/session-reports/session-12/12g_ra/<script>.py <retained_dir>
Outputs are written beside the scripts."""
import json
import os
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
RUN_ID = 2
LABEL = "stress sample (N=12, selected for length and text pathologies); not a Run 7 estimate"
LIVE_IDS = [121, 415, 11, 498, 607, 748, 368, 455, 699, 431, 783, 604]  # throwaway id n -> LIVE_IDS[n-1]


def retained():
    return sys.argv[1]


def db():
    c = sqlite3.connect(f"file:{os.path.join(retained(), 'review.db')}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c


def telemetry():
    path = os.path.join(retained(), "telemetry", "extraction_calls.jsonl")
    return [json.loads(l) for l in open(path)]


def write(name, obj):
    with open(os.path.join(HERE, name), "w") as f:
        json.dump(obj, f, indent=1, sort_keys=True)
        f.write("\n")
