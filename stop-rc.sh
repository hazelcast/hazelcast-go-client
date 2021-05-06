#!/bin/bash

#
# Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# TODO: merge this with start-rc.sh to rc.sh

set -e
set -u
# Disables printing security sensitive data to the logs
set +x

log_info () {
  local msg=$1
  local ts
  ts=$(date "$TIMESTAMP_FMT")
  echo "$ts INFO : $msg"
}

TIMESTAMP_FMT="+%Y-%m-%d %H:%M:%S"
PID_FILE="test.pid"

if [ -f "$PID_FILE" ]; then
  pid=$(cat "$PID_FILE")
  log_info "Stopping $pid ..."
  kill -9 $pid
  rm "$PID_FILE"
fi
