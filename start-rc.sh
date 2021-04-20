#!/bin/bash

set -e
set -u
# Disables printing security sensitive data to the logs
set +x

PID_FILE="test.pid"

if [ -f "$PID_FILE" ]; then
  echo "PID file $PID_FILE exists. Is there an another instance of Hazelcast Remote Controller running?"
  exit 1
fi

HZ_VERSION="4.1"
HAZELCAST_TEST_VERSION=${HZ_VERSION}
HAZELCAST_ENTERPRISE_VERSION=${HZ_VERSION}
HAZELCAST_RC_VERSION="0.8-SNAPSHOT"
SNAPSHOT_REPO="https://oss.sonatype.org/content/repositories/snapshots"
RELEASE_REPO="http://repo1.maven.apache.org/maven2"
ENTERPRISE_RELEASE_REPO="https://repository.hazelcast.com/release/"
ENTERPRISE_SNAPSHOT_REPO="https://repository.hazelcast.com/snapshot/"

if [ "${HZ_VERSION}" = "*-SNAPSHOT" ]
then
	REPO=${SNAPSHOT_REPO}
	ENTERPRISE_REPO=${ENTERPRISE_SNAPSHOT_REPO}
else
	REPO=${RELEASE_REPO}
	ENTERPRISE_REPO=${ENTERPRISE_RELEASE_REPO}
fi

if [ -f "hazelcast-remote-controller-${HAZELCAST_RC_VERSION}.jar" ]; then
    echo "Remote controller already exist, not downloading from maven."
else
    echo "Downloading: remote-controller jar com.hazelcast:hazelcast-remote-controller:${HAZELCAST_RC_VERSION}"
    mvn -q dependency:get -DrepoUrl=${SNAPSHOT_REPO} -Dartifact=com.hazelcast:hazelcast-remote-controller:${HAZELCAST_RC_VERSION} -Ddest=hazelcast-remote-controller-${HAZELCAST_RC_VERSION}.jar
    if [ $? -ne 0 ]; then
        echo "Failed download remote-controller jar com.hazelcast:hazelcast-remote-controller:${HAZELCAST_RC_VERSION}"
        exit 1
    fi
fi

if [ -f "hazelcast-${HAZELCAST_TEST_VERSION}-tests.jar" ]; then
    echo "hazelcast-test.jar already exists, not downloading from maven."
else
    echo "Downloading: hazelcast test jar com.hazelcast:hazelcast:${HAZELCAST_TEST_VERSION}:jar:tests"
    mvn -q dependency:get -DrepoUrl=${SNAPSHOT_REPO} -Dartifact=com.hazelcast:hazelcast:${HAZELCAST_TEST_VERSION}:jar:tests -Ddest=hazelcast-${HAZELCAST_TEST_VERSION}-tests.jar
    if [ $? -ne 0 ]; then
        echo "Failed download hazelcast test jar com.hazelcast:hazelcast:${HAZELCAST_TEST_VERSION}:jar:tests"
        exit 1
    fi
fi

if [ -f "hazelcast-${HZ_VERSION}.jar" ]; then
echo "hazelcast.jar already exists, not downloading from maven."
else
    echo "Downloading: hazelcast jar com.hazelcast:hazelcast:${HZ_VERSION}"
    mvn -q dependency:get -DrepoUrl=${REPO} -Dartifact=com.hazelcast:hazelcast:${HZ_VERSION} -Ddest=hazelcast-${HZ_VERSION}.jar
    if [ $? -ne 0 ]; then
        echo "Failed downloading hazelcast jar com.hazelcast:hazelcast:${HZ_VERSION}"
        exit 1
    fi
fi

CLASSPATH="hazelcast-remote-controller-${HAZELCAST_RC_VERSION}.jar:hazelcast-${HZ_VERSION}.jar:hazelcast-${HAZELCAST_TEST_VERSION}-tests.jar"
echo "Starting Remote Controller..."

java -cp ${CLASSPATH} -Dhazelcast.phone.home.enabled=false com.hazelcast.remotecontroller.Main --use-simple-server &
pid=$!
echo "$pid" > "$PID_FILE"
