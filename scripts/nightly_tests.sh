#!/bin/bash
# Nightly Evidence Engine test suite
# Crontab: 0 9 * * * /bin/bash ~/projects/evidence-engine/scripts/nightly_tests.sh
#
# Runs the STANDARD GATE: the selection every unit gate runs, written once, below.
# It makes no model call and takes no experiment lock (R625(a)(1)): the suite's
# Ollama fence refuses any request to the server, and the live tiers (ollama,
# network, integration) are deselected here and opt-in everywhere
# (EVIDENCE_ENGINE_LIVE_TIERS, never set by this script).
#
# Writes, under logs/, with one date stamp:
#   nightly_test_<date>.log        pytest's output, then "pytest exit code: N"
#   nightly_durations_<date>.txt   the per-test durations, cut from the log
# The script exits with pytest's exit code.

set -euo pipefail

PROJECT_DIR="$HOME/projects/evidence-engine"
LOG_DIR="$PROJECT_DIR/logs"
STAMP="$(date +%Y%m%d)"
LOG_FILE="$LOG_DIR/nightly_test_$STAMP.log"
DURATIONS_FILE="$LOG_DIR/nightly_durations_$STAMP.txt"

# The standard gate's marker expression.
STANDARD_GATE="not network and not ollama and not integration"

mkdir -p "$LOG_DIR"

rc=0
{
    echo "========================================"
    echo "Evidence Engine Nightly Test Suite"
    echo "Run: $(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "========================================"
    echo ""

    cd "$PROJECT_DIR"
    source .venv/bin/activate
    # `|| rc=$?`: a failing suite must not end the script before its code is logged.
    python -m pytest tests/ -v --tb=short --durations=0 -m "$STANDARD_GATE" || rc=$?
    echo ""
    echo "pytest exit code: $rc"
} > "$LOG_FILE" 2>&1

# One line per test phase, "<seconds>s <call|setup|teardown> <node id>", as pytest printed it.
grep -E '^[0-9]+\.[0-9]+s (call|setup|teardown) ' "$LOG_FILE" > "$DURATIONS_FILE" || true

exit "$rc"
