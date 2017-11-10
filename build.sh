#!/bin/bash

# Outside tools include:
# gocov: go get github.com/axw/gocov/gocov
# gocov-xml: go get github.com/t-yuki/gocov-xml
# go2xunit: go get github.com/tebeka/go2xunit


gofmt -d . 2>&1 | read; [ $? == 1 ]

if [ "$?" = "1" ]; then
    echo "gofmt -d .  detected formatting problems"
    gofmt -d .
    exit 1
fi

set -ex

# Set up environment
export PRJ=`git config --get remote.origin.url | sed 's/^https:\/\///' | sed 's/\.git$//'`

go get git.apache.org/thrift.git/lib/go/thrift
pushd $GOPATH/src/git.apache.org/thrift.git/
git fetch --tags --quiet
git checkout 0.10.0
popd

pushd src/$PRJ
go build
popd

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
         cat tmp.out | grep -v "mode: set" >> coverage.out | echo
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