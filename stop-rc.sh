#!/bin/bash

set -e
set -u
# Disables printing security sensitive data to the logs
set +x

PID_FILE="test.pid"

if [ -f "$PID_FILE" ]; then
  pid=$(cat "$PID_FILE")
  echo "Stopping $pid"
  kill -9 $pid
  rm "$PID_FILE"
fi
