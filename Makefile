.PHONY: benchmark build check doc test test-all test-all-race

PORT ?= 5050
TEST_FLAGS ?=
MEMBER_COUNT ?= 3
COVERAGE_OUT ?= coverage.out
TEST_FLAGS ?= "-count 1 -timeout 20m"

build:
	go build ./...

test: test-all

test-all:
	env MEMBER_COUNT=$(MEMBER_COUNT) go test $(TEST_FLAGS) ./...

test-all-race:
	env MEMBER_COUNT=$(MEMBER_COUNT) go test $(TEST_FLAGS) -race ./...

test-cover:
	bash ./coverage.sh

view-cover:
	go tool cover -func $(COVERAGE_OUT) | grep total:
	go tool cover -html $(COVERAGE_OUT) -o coverage.html

benchmark:
	env MEMBER_COUNT=$(MEMBER_COUNT) go test $(TEST_FLAGS) -bench . -benchmem ./benchmarks

doc:
	godoc -http=localhost:$(PORT)

check:
	# requires staticcheck: use go install honnef.co/go/tools/cmd/staticcheck@latest
	# requires fieldalignment
	# requires go vet