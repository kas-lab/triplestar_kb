#!/bin/bash

if [ $# -lt 1 ]; then
  echo "Usage: set_viz_query.sh <query_file> [update_rate]"
  echo "  query_file: Path to SPARQL query file"
  echo "  update_rate: Optional update rate in seconds (default: 1.0)"
  exit 1
fi

query_file="$1"
update_rate="${2:-1.0}"

if [ ! -f "$query_file" ]; then
  echo "Error: Query file '$query_file' not found"
  exit 1
fi

# Read query from file
query_content=$(cat "$query_file")

# Call the ROS service
response=$(ros2 service call /triplestar_core/set_viz_query triplestar_msgs/srv/SetVizQuery "{query: '$query_content', update_rate: $update_rate}")

echo "$response"
