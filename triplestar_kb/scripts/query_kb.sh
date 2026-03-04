#!/bin/bash

if [ $# -lt 1 ]; then
  echo "Usage: query_kb <filename>"
  exit 1
fi

# Call the ROS service and capture the response
response=$(ros2 service call /triplestar_kb/query triplestar_kb_msgs/srv/Query "{query: '$(< "$1")'}")

# Extract the JSON from the result field and pretty print it with jq
echo "$response" | sed -n "s/.*result='\([^']*\)'.*/\1/p" | jq .
