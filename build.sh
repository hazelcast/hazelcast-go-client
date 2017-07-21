#!/bin/bash

# Outside tools include:
# gocov: go get github.com/axw/gocov/gocov
# gocov-xml: go get github.com/t-yuki/gocov-xml
# go2xunit: go get github.com/tebeka/go2xunit

set -x

# Set up environment
export PRJ=`git config --get remote.origin.url | sed 's/^https:\/\///' | sed 's/\.git$//'`

go get git.apache.org/thrift.git/lib/go/thrift

bash ./start-rc.sh

sleep 10

# Run tests (JUnit plugin)
echo "mode: set" > coverage.out
for pkg in $(go list $PRJ/...);
do
    if [[ $pkg != *"vendor"* ]]; then
      echo "testing... $pkg"
      go test -v -coverprofile=tmp.out $pkg >> test.out
      if [ -f tmp.out ]; then
         cat tmp.out | grep -v "mode: set" >> coverage.out
      fi
    fi
done
rm -f ./tmp.out
cat test.out | go2xunit -output tests.xml

# Generate coverage reports (Cobertura plugin)
gocov convert coverage.out | gocov-xml > cobertura-coverage.xml

# Run vet tools (Compiler warning plugin)
go vet $PRJ > vet.txt

## Run lint tools (Compiler warning plugin)
#golint $PRJ > lint.txt
