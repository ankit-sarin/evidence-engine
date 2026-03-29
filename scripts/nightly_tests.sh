#!/bin/bash
# Nightly Evidence Engine test suite
# Crontab: 0 9 * * * /bin/bash ~/projects/evidence-engine/scripts/nightly_tests.sh

set -euo pipefail

PROJECT_DIR="$HOME/projects/evidence-engine"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/nightly_test_$(date +%Y%m%d).log"

mkdir -p "$LOG_DIR"

{
    echo "========================================"
    echo "Evidence Engine Nightly Test Suite"
    echo "Run: $(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "========================================"
    echo ""

    cd "$PROJECT_DIR"
    source .venv/bin/activate
    python -m pytest tests/ -v --tb=short
} > "$LOG_FILE" 2>&1

exit 0
