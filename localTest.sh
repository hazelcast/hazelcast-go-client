#!/bin/bash

gofmt -d . 2>&1 | read; [ $? == 1 ]

if [ "$?" = "1" ]; then
    echo "gofmt -d .  detected formatting problems"
    gofmt -d .
    exit 1
fi

set -xe

HZ_VERSION="3.9-SNAPSHOT"

HAZELCAST_TEST_VERSION=${HZ_VERSION}
HAZELCAST_VERSION=${HZ_VERSION}
HAZELCAST_ENTERPRISE_VERSION=${HZ_VERSION}
HAZELCAST_RC_VERSION="0.3-SNAPSHOT"

CLASSPATH="hazelcast-remote-controller-${HAZELCAST_RC_VERSION}.jar:hazelcast-${HAZELCAST_TEST_VERSION}-tests.jar"
CLASSPATH="hazelcast-enterprise-${HAZELCAST_ENTERPRISE_VERSION}.jar:"${CLASSPATH}
CLASSPATH="hazelcast-${HAZELCAST_VERSION}.jar:"${CLASSPATH}
echo "Starting Remote Controller ... oss ..."

go build

java -cp ${CLASSPATH} com.hazelcast.remotecontroller.Main&
serverPid=$!
echo ${serverPid}

sleep 10

# Run tests (JUnit plugin)
echo "mode: set" > coverage.out
for pkg in $(go list ./...);
do
    if [[ $pkg != *"vendor"* ]]; then
      echo "testing... $pkg"
      go test -v -coverprofile=tmp.out $pkg >> test.out
      if [ -f tmp.out ]; then
         cat tmp.out | grep -v "mode: set" >> coverage.out | echo
      fi
    fi
done
rm -f ./tmp.out

kill -9 ${serverPid}
