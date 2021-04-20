.PHONY: build test test-all test-all-race doc

PORT ?= 5050
TEST_FLAGS ?=
MEMBER_COUNT ?= 3

build:
	go build ./...

test: test-all

test-all:
	env MEMBER_COUNT=$(MEMBER_COUNT) go test $(TEST_FLAGS) -count=1 ./...

test-all-race:
	env MEMBER_COUNT=$(MEMBER_COUNT) go test $(TEST_FLAGS) -count=2 -race ./...

doc:
	godoc -http=localhost:$(PORT)
