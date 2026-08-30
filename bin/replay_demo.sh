#!/usr/bin/env bash
#
# replay_demo.sh — the M3 money-shot. Runs the detector at --asof 45/30/15/0 and
# prints the heap-leak early-warning timeline: the P1 fires ~40+ min before the
# projected OOM, with the ETA shrinking as "now" advances toward the outage.
#
# Usage:
#   bin/replay_demo.sh [--source local|iceberg] [--insecure]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

SOURCE="local"
EXTRA=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --source) SOURCE="$2"; shift 2 ;;
    --insecure) EXTRA="--insecure"; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

PY="${PYTHON:-python3}"
[[ -x "$REPO_ROOT/.venv/bin/python" ]] && PY="$REPO_ROOT/.venv/bin/python"

echo "============================================================"
echo " Heap-leak early-warning replay  (source=$SOURCE)"
echo " Reading the lake as of 45 / 30 / 15 / 0 min before data end"
echo "============================================================"

for ASOF in 45 30 15 0; do
  echo
  echo "----- as of  T-${ASOF}m -----------------------------------"
  # Detect, write only to JSON during replay (don't spam the alerts table).
  "$PY" "$REPO_ROOT/bin/04_detect.py" --source "$SOURCE" --asof "$ASOF" \
      --alerts-to json $EXTRA 2>/dev/null \
    | grep -E 'heap_leak|-> [0-9]+ alert' \
    | sed 's/^/  /' || echo "  (no alerts)"
done

echo
echo "============================================================"
echo " Read the OOM ETA column top-to-bottom: it shrinks toward 0."
echo " That P1 fired while every dashboard still showed green."
echo "============================================================"
