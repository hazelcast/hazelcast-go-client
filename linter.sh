#!/usr/bin/env bash

if ! [ -d $GOPATH/src/github.com/alecthomas/gometalinter/  ]; then
    echo "downloading gometalinter"
    go get -u github.com/alecthomas/gometalinter
    gometalinter --install
    if [  "$?" != "0" ] ; then
        echo "error when installing gometalinter"
    exit 1
    fi
fi

gometalinter ./... 2>&1 > gometalinter-err.out



cat gometalinter-err.out | read ; [ $? == 1 ]

if [  "$?" = "1" ] ; then
    echo "gometalinter ./...  detected problems"
    cat gometalinter-err.out
    rm gometalinter-err.out
    exit 1
fi

echo "code format is ok!"
rm gometalinter-err.out
exit 0
