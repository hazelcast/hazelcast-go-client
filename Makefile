.PHONY: build test test-all test-all-race doc

PORT ?= 5050

build:
	go build ./hazelcast/...

test: test-all

test-all:
	go test -count=1  ./hazelcast/...

test-all-race:
	go test -count=1 -race  ./hazelcast/...

doc:
	godoc -http=localhost:$(PORT)
