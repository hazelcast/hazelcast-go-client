#!/bin/bash

# Outside tools include:
# gocov: go get github.com/axw/gocov/gocov
# go2xunit: go get github.com/tebeka/go2xunit
# gocover-cobertura: go get github.com/t-yuki/gocover-cobertura
# goimports: go get golang.org/x/tools/cmd/goimports

# get goimports
go get golang.org/x/tools/cmd/goimports

goimports -d $(find . -type f -name '*.go' -not -path "./vendor/*") ./ 2>&1 | read; [ $? == 1 ]


if [ "$?" = "1" ]; then
    echo "goimports  detected formatting problems"
    # format the code so that the same error will not occur in local
    goimports -w ./
    exit 1
fi

set -ex

# Set up environment
export CLIENT_IMPORT_PATH="github.com/hazelcast/hazelcast-go-client"
export PACKAGE_LIST=$(go list $CLIENT_IMPORT_PATH/... | grep -vE ".*/tests|.*/compatibility|.*/rc|.*/samples" | sed -e 'H;${x;s/\n/,/g;s/^,//;p;};d')
echo $PACKAGE_LIST


if [ -d $GOPATH/src/github.com/apache/thrift/  ]; then
    echo "thrift already exists, not downloading."
else
    go get github.com/apache/thrift/lib/go/thrift
    pushd $GOPATH/src/github.com/apache/thrift
    git fetch --tags --quiet
    git checkout 0.10.0
    popd
fi

pushd $GOPATH/src/$CLIENT_IMPORT_PATH
go build
popd

bash ./start-rc.sh

sleep 10


# Run vet tools (Compiler warning plugin)
go vet $CLIENT_IMPORT_PATH > vet.txt

go get github.com/t-yuki/gocover-cobertura
go get github.com/tebeka/go2xunit


# Run tests (JUnit plugin)
echo "mode: atomic" > coverage.out

for pkg in $(go list $CLIENT_IMPORT_PATH/...);
do
    if [[ $pkg != *"vendor"* ]]; then
      echo "testing... $pkg"
      go test -race -covermode=atomic  -v -coverprofile=tmp.out ${pkg} | tee -a test.out
      if [ -f tmp.out ]; then
         cat tmp.out | grep -v "mode: atomic" >> coverage.out | echo
      fi
    fi
done

rm -f ./tmp.out

cat test.out | go2xunit -output tests.xml

# Generate coverage reports (Cobertura plugin)
gocover-cobertura < coverage.out > cobertura-coverage.xml

## Run lint tools (Compiler warning plugin)
#golint $PRJ > lint.txt
