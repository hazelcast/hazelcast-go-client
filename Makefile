.PHONY: build test test-all test-all-race doc

PORT ?= 5050
TEST_FLAGS ?=
MEMBER_COUNT ?= 3
COVERAGE_OUT ?= coverage.out

build:
	go build ./...

test: test-all

test-all:
	env MEMBER_COUNT=$(MEMBER_COUNT) go test $(TEST_FLAGS) -count 1 ./...

test-all-race:
	env MEMBER_COUNT=$(MEMBER_COUNT) go test $(TEST_FLAGS) -count 2 -race ./...

test-cover:
	bash ./coverage.sh

view-cover:
	go tool cover -func $(COVERAGE_OUT) | grep total:
	go tool cover -html $(COVERAGE_OUT) -o coverage.html

doc:
	godoc -http=localhost:$(PORT)
