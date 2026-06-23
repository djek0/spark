#!/bin/bash
#
# Helper script to switch between production and debug logging modes
# Usage: ./set-logging-mode.sh [production|debug]
#

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONF_DIR="$SCRIPT_DIR/conf"

MODE="${1:-production}"

case "$MODE" in
  production|prod|info)
    echo "Setting PRODUCTION logging mode (INFO level)..."
    cp "$CONF_DIR/log4j-replication-production.properties.template" "$CONF_DIR/log4j.properties"
    echo "[OK] Production mode activated"
    echo "  - Shows: Job completion, consensus results, Byzantine faults"
    echo "  - Hides: Task details, hash storage, batching wait states"
    ;;
    
  debug|dev|verbose)
    echo "Setting DEBUG logging mode (DEBUG level)..."
    cp "$CONF_DIR/log4j-replication-debug.properties.template" "$CONF_DIR/log4j.properties"
    echo "[OK] Debug mode activated"
    echo "  - Shows: All flow details, task registration, hash storage, batching states"
    echo "  - Use this for: Development, troubleshooting, understanding the flow"
    ;;
    
  *)
    echo "Error: Invalid mode '$MODE'"
    echo ""
    echo "Usage: $0 [production|debug]"
    echo ""
    echo "Modes:"
    echo "  production, prod, info  - INFO level logging (clean output)"
    echo "  debug, dev, verbose     - DEBUG level logging (detailed output)"
    echo ""
    exit 1
    ;;
esac

echo ""
echo "Current log4j.properties:"
head -n 25 "$CONF_DIR/log4j.properties" | grep -E "^#|^log4j\.(root|logger)" | tail -n 10
echo ""
echo "Ready to run examples!"
