#!/bin/bash

if [ $# -lt 1 ]; then
  echo "Usage: call_service <query_service>"
  exit 1
fi

# Call the ROS service and capture the response
response=$(ros2 service call /triplestar_core/query_services/$1 triplestar_msgs/srv/SelectQuery)

# Extract the JSON from the result field and pretty print it with jq
echo "$response" | sed -n "s/.*result='\([^']*\)'.*/\1/p" | jq .
