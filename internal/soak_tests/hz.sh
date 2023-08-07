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

# Starts Soak Tester
#
# Usage:
#   Start with default Hazelcast version : ./soak.sh start
#   Start with given Hazelcast version   : HZ_VERSION=4.3 ./soak.sh start
#   Stop                                 : ./soak.sh stop

# Exit immediately on error.
set -e
# Treat unset variables an parameters as an error.
set -u
# Disable printing trace.
set +x

log_info () {
  local msg=$1
  local ts
  ts=$(date "$TIMESTAMP_FMT")
  echo "$ts INFO : $msg"
}

log_fatal () {
  local msg=$1
  local ts
  ts=$(date "$TIMESTAMP_FMT")
  echo "$ts FATAL: $msg"
  exit 1
}

download () {
  local repo=$1
  local jar_path=$2
  local artifact=$3
  if [ -f "$jar_path" ]; then
      log_info "$jar_path already exists, skipping download."
  else
      log_info "Downloading: $jar_path ($artifact)"
      mvn -q dependency:get -DremoteRepositories=$repo -Dartifact=$artifact -Ddest="$jar_path"
      if [ $? -ne 0 ]; then
          log_fatal "Failed downloading $jar_path ($artifact) from $repo"
      fi
  fi
}

downloadTests () {
  local jar_path="hazelcast-${HAZELCAST_TEST_VERSION}-tests.jar"
  local artifact="com.hazelcast:hazelcast:${HAZELCAST_TEST_VERSION}:jar:tests"
  download "$SNAPSHOT_REPO" "$jar_path" "$artifact"
  classpath="$classpath:$jar_path"
}

downloadHazelcast () {
  local jar_path="hazelcast-${HZ_VERSION}.jar"
  local artifact="com.hazelcast:hazelcast:${HZ_VERSION}"
  download "${repo}" "$jar_path" "$artifact"
  classpath="$classpath:$jar_path"
}

downloadHazelcastEnterprise () {
  local jar_path="hazelcast-enterprise-${HAZELCAST_ENTERPRISE_VERSION}.jar"
  local artifact="com.hazelcast:hazelcast-enterprise:${HAZELCAST_ENTERPRISE_VERSION}"
  download "${enterprise_repo}" "$jar_path" "$artifact"
  classpath="$classpath:$jar_path"
}

downloadTestsEnterprise () {
  local jar_path="hazelcast-enterprise-${HAZELCAST_ENTERPRISE_VERSION}-tests.jar"
  local artifact="com.hazelcast:hazelcast-enterprise:${HAZELCAST_ENTERPRISE_VERSION}:jar:tests"
  download "${enterprise_repo}" "$jar_path" "$artifact"
  classpath="$classpath:$jar_path"
}

startSoak () {
  if [ -f "$PID_FILE" ]; then
    log_fatal "PID file $PID_FILE exists. Is there an another instance of soak tester running?"
  fi

  if [ "${HZ_VERSION}" = "*-SNAPSHOT" ]
  then
    repo=${SNAPSHOT_REPO}
    enterprise_repo=${ENTERPRISE_SNAPSHOT_REPO}
  else
    repo=${RELEASE_REPO}
    enterprise_repo=${ENTERPRISE_RELEASE_REPO}
  fi

  classpath=""
  java_opts="-Djava.net.preferIPv4Stack=true com.hazelcast.core.server.HazelcastMemberStarter"

  # Download Hazelcast Community jars
  downloadTests
  downloadHazelcast

  if [ "x${HAZELCAST_ENTERPRISE_KEY:-}" != "x" ]; then
      # Download Hazelcast Enterprise jars
      downloadHazelcastEnterprise
      downloadTestsEnterprise
      java_opts="-Dhazelcast.enterprise.license.key=${HAZELCAST_ENTERPRISE_KEY} $java_opts"
  fi

  java_opts="-cp ${classpath} $java_opts"

  log_info "Starting Hazelcast in the background..."
  log_info "Run '$0 stop' to stop the soak tester."
  java $java_opts &
  pid=$!
  echo "$pid" > "$PID_FILE"
}

stopTester () {
  if [ -f "$PID_FILE" ]; then
    pid=$(cat "$PID_FILE")
    log_info "Stopping $pid ..."
    kill -9 $pid
    rm "$PID_FILE"
  fi
}

help () {
  echo "Usage: $0 [start|stop]"
  exit 1
}

TIMESTAMP_FMT="+%Y-%m-%d %H:%M:%S"
PID_FILE="hz.pid"
HZ_VERSION="${HZ_VERSION:-4.2}"
HAZELCAST_TEST_VERSION=${HZ_VERSION}
HAZELCAST_ENTERPRISE_VERSION=${HZ_VERSION}
SNAPSHOT_REPO="https://oss.sonatype.org/content/repositories/snapshots"
RELEASE_REPO="http://repo1.maven.apache.org/maven2"
ENTERPRISE_RELEASE_REPO="https://repository.hazelcast.com/release/"
ENTERPRISE_SNAPSHOT_REPO="https://repository.hazelcast.com/snapshot/"

case "${1:-}" in
  start)
    startSoak
    ;;
  stop)
    stopTester
    ;;
  *)
    help
    ;;
esac
