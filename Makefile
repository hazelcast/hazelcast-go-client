.PHONY: build test test-all test-all-race doc

PORT ?= 5050
TEST_FLAGS ?=

build:
	go build ./...

test: test-all

test-all:
	go test $(TEST_FLAGS) -count=1 ./...

test-all-race:
	go test $(TEST_FLAGS) -count=5 -race ./...

doc:
	godoc -http=localhost:$(PORT)
