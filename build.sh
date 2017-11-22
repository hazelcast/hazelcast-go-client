#!/bin/bash

# Outside tools include:
# gocov: go get github.com/axw/gocov/gocov
# go2xunit: go get github.com/tebeka/go2xunit
# gocover-cobertura: go get github.com/t-yuki/gocover-cobertura


gofmt -d . 2>&1 | read; [ $? == 1 ]

if [ "$?" = "1" ]; then
    echo "gofmt -d .  detected formatting problems"
    gofmt -d .
    exit 1
fi

set -ex

# Set up environment
export CLIENT_IMPORT_PATH="github.com/hazelcast/go-client"
export PACKAGE_LIST=$(go list $CLIENT_IMPORT_PATH/... | grep -vE ".*/tests|.*/protocol|.*/rc|.*/samples" | sed -e 'H;${x;s/\n/,/g;s/^,//;p;};d')
echo $PACKAGE_LIST

go get git.apache.org/thrift.git/lib/go/thrift
pushd $GOPATH/src/git.apache.org/thrift.git/
git fetch --tags --quiet
git checkout 0.10.0
popd

pushd $GOPATH/src/$CLIENT_IMPORT_PATH
go build
popd

bash ./start-rc.sh

sleep 10

# Run tests (JUnit plugin) with race detection
for pkg in $(go list $CLIENT_IMPORT_PATH/...);
do
    if [[ $pkg != *"vendor"* ]]; then
      echo "testing with race detection on ... $pkg"
      go test -race -v  $pkg
    fi
done


# Run tests (JUnit plugin)
echo "mode: set" > coverage.out
for pkg in $(go list $CLIENT_IMPORT_PATH/...);
do
    if [[ $pkg != *"vendor"* ]]; then
      echo "testing... $pkg"
      go test -v -coverprofile=tmp.out -coverpkg ${PACKAGE_LIST} $pkg >> test.out
      if [ -f tmp.out ]; then
         cat tmp.out | grep -v "mode: set" >> coverage.out | echo
      fi
    fi
done
rm -f ./tmp.out
cat test.out | go2xunit -output tests.xml

# Generate coverage reports (Cobertura plugin)
gocover-cobertura < coverage.out > cobertura-coverage.xml

# Run vet tools (Compiler warning plugin)
go vet $CLIENT_IMPORT_PATH > vet.txt

## Run lint tools (Compiler warning plugin)
#golint $PRJ > lint.txt