#!/bin/bash

#
# Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

# Starts Hazelcast Remote Controller
#
# Usage:
#   Start with default Hazelcast version : ./rc.sh start
#   Start with given Hazelcast version   : HZ_VERSION=4.3 ./rc.sh start
#   Stop                                 : ./rc.sh stop

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
  printf "$ts FATAL: $msg"
  exit 1
}

download () {
  local repo=$1
  local jar_path=$2
  local artifact=$3
  # Set this to anything in the caller to ignore the download error
  local ignore_err=${4:-}
  local err
  local output
  if [ -f "$jar_path" ]; then
      log_info "$jar_path already exists, skipping download."
  else
      log_info "Downloading: $jar_path ($artifact) from: $repo"
      set +e
      output=$(mvn -q org.apache.maven.plugins:maven-dependency-plugin:2.10:get -DremoteRepositories=$repo -Dartifact=$artifact -Dtransitive=false -Ddest="$jar_path" 2>&1)
      err=$?
      set -e
      if [ $err -ne 0 ]; then
        if [ "x$ignore_err" == "x" ]; then
            log_fatal "Failed downloading $jar_path ($artifact) from: $repo\nOutput:\n$output"
        else
          log_info "Ignoring the download error for $jar_path ($artifact) from: $repo"
        fi
      fi
  fi
}

downloadRC () {
  local jar_path="hazelcast-remote-controller-${HAZELCAST_RC_VERSION}.jar"
  local artifact="com.hazelcast:hazelcast-remote-controller:${HAZELCAST_RC_VERSION}"
  download "$SNAPSHOT_REPO" "$jar_path" "$artifact"
  classpath="$classpath:$jar_path"
}

downloadTests () {
  local jar_path="hazelcast-${HAZELCAST_TEST_VERSION}-tests.jar"
  local artifact="com.hazelcast:hazelcast:${HAZELCAST_TEST_VERSION}:jar:tests"
  download "${enterprise_repo}" "$jar_path" "$artifact"
  classpath="$classpath:$jar_path"
}

downloadHazelcast () {
  local jar_path="hazelcast-${HZ_VERSION}.jar"
  local artifact="com.hazelcast:hazelcast:${HZ_VERSION}"
  download "${enterprise_repo}" "$jar_path" "$artifact"
  classpath="$classpath:$jar_path"
}

downloadSQL () {
  local jar_path="hazelcast-sql-${HZ_VERSION}.jar"
  local artifact="com.hazelcast:hazelcast-sql:${HZ_VERSION}"
  download "${enterprise_repo}" "$jar_path" "$artifact"
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
  download "${enterprise_repo}" "$jar_path" "$artifact" ignore
  classpath="$classpath:$jar_path"
}

startRC () {
  if [ -f "$PID_FILE" ]; then
    log_fatal "PID file $PID_FILE exists. Is there an another instance of Hazelcast Remote Controller running?"
  fi

  if [[ "${HZ_VERSION}" == *-SNAPSHOT ]]
  then
    repo=${SNAPSHOT_REPO}
    enterprise_repo=${ENTERPRISE_SNAPSHOT_REPO}
  else
    repo=${RELEASE_REPO}
    enterprise_repo=${ENTERPRISE_RELEASE_REPO}
  fi

  classpath=""
  java_opts="-Dhazelcast.phone.home.enabled=false com.hazelcast.remotecontroller.Main --use-simple-server"

  # Download Remote Controller
  downloadRC
  # Download Hazelcast Community jars
  downloadTests
  downloadHazelcast
  # Download the SQL jar
  downloadSQL

  if [ "x${HAZELCAST_ENTERPRISE_KEY:-}" != "x" ]; then
      # Download Hazelcast Enterprise jars
      downloadHazelcastEnterprise
      downloadTestsEnterprise
      java_opts="-Dhazelcast.enterprise.license.key=${HAZELCAST_ENTERPRISE_KEY} $java_opts"
  fi

  java_opts="-cp ${classpath} $java_opts"

  log_info "Starting Remote Controller in the background..."
  log_info "Run '$0 stop' to stop the controller."
  java $java_opts &
  pid=$!
  echo "$pid" > "$PID_FILE"
}

stopRC () {
  if [ -f "$PID_FILE" ]; then
    pid=$(cat "$PID_FILE")
    log_info "Stopping $pid ..."
    kill -9 $pid
    rm "$PID_FILE"
  fi
}

help () {
  echo "Usage: ./rc.sh [start|stop]"
  exit 1
}

TIMESTAMP_FMT="+%Y-%m-%d %H:%M:%S"
PID_FILE="test.pid"
HZ_VERSION="${HZ_VERSION:-5.3.1}"
HAZELCAST_TEST_VERSION=${HZ_VERSION}
HAZELCAST_ENTERPRISE_VERSION=${HZ_VERSION}
HAZELCAST_RC_VERSION="0.8-SNAPSHOT"
SNAPSHOT_REPO="https://oss.sonatype.org/content/repositories/snapshots"
RELEASE_REPO="http://repo1.maven.apache.org/maven2"
ENTERPRISE_RELEASE_REPO="https://repository.hazelcast.com/release/"
ENTERPRISE_SNAPSHOT_REPO="https://repository.hazelcast.com/snapshot/"

echo "Java version:"
java -version
which java
echo "JAVA_HOME: $JAVA_HOME"


case "${1:-}" in
  start)
    startRC
    ;;
  stop)
    stopRC
    ;;
  *)
    help
    ;;
esac
