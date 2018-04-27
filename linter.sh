#!/usr/bin/env bash

gometalinter ./... 2>&1 | read; [ $? == 1 ]


if [ "$?" = "1" ]; then
    echo "gometalinter ./...  detected problems"
    gometalinter ./...
    exit 1
fi

echo "code format is ok!"

exit 0
