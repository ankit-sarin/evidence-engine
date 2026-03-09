#!/usr/bin/env bash
# Run fresh primary screening + verification on expanded search papers.
# Designed to run inside tmux (launched via --background flag).
set -euo pipefail
cd "$(dirname "$0")/.."

echo "=== Phase 2: Primary screening (qwen3:8b dual-pass) ==="
python scripts/screen_expanded.py --fresh --screen-only

echo ""
echo "=== Phase 3: Verification screening (qwen3:32b) ==="
python scripts/screen_expanded.py --verify-only

echo ""
echo "=== All phases complete ==="
