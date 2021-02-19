#!/bin/bash

set -ex

if [ "x${HAZELCAST_ENTERPRISE_KEY}x" == "xx" ]; then
  echo "Enterprise key was not set, quitting..."
  exit 1
fi

HZ_VERSION="3.12.12"

HAZELCAST_TEST_VERSION=${HZ_VERSION}
HAZELCAST_VERSION=${HZ_VERSION}
HAZELCAST_ENTERPRISE_VERSION=${HZ_VERSION}
HAZELCAST_RC_VERSION="0.4-SNAPSHOT"

CLASSPATH="hazelcast-remote-controller-${HAZELCAST_RC_VERSION}.jar:hazelcast-${HAZELCAST_TEST_VERSION}-tests.jar"
CLASSPATH="hazelcast-enterprise-${HAZELCAST_ENTERPRISE_VERSION}.jar:"${CLASSPATH}
CLASSPATH="hazelcast-${HAZELCAST_VERSION}.jar:"${CLASSPATH}
echo "Starting Remote Controller ... oss ..."

go build

# Run tests (JUnit plugin)
echo "mode: set" > coverage.out
for pkg in $(go list ./...);
do
    if [[ $pkg != *"vendor"* ]]; then
      echo "testing... $pkg"
      go test -v -race -coverprofile=tmp.out $pkg >> test.out
      if [ -f tmp.out ]; then
         cat tmp.out | grep -v "mode: set" >> coverage.out | echo
      fi
    fi
done
