#!/bin/bash
# Convenience wrapper for rdfstar-viz CLI tool
# This script makes it easier to call the visualization tool from anywhere

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WS_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Path to the installed CLI tool
RDFSTAR_VIZ="$WS_DIR/install/triplestar_viz/lib/triplestar_viz/rdfstar-viz"

# Check if the tool exists
if [ ! -f "$RDFSTAR_VIZ" ]; then
    echo "Error: rdfstar-viz not found at $RDFSTAR_VIZ"
    echo "Please build the package first:"
    echo "  cd $WS_DIR"
    echo "  colcon build --packages-select triplestar_viz"
    exit 1
fi

# Pass all arguments to the CLI tool
exec "$RDFSTAR_VIZ" "$@"