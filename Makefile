.PHONY: benchmark build check doc test test-all test-all-race

PORT ?= 5050
TEST_FLAGS ?=
MEMBER_COUNT ?= 3
COVERAGE_OUT ?= coverage.out
TEST_FLAGS ?= "-count 1 -timeout 20m"
PACKAGES = $(go list ./... | grep -v org-website)

build:
	go build $(PACKAGES)

test: test-all

test-all:
	env MEMBER_COUNT=$(MEMBER_COUNT) go test $(TEST_FLAGS) $(PACKAGES)

test-all-race:
	env MEMBER_COUNT=$(MEMBER_COUNT) go test $(TEST_FLAGS) -race $(PACKAGES)

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
	bash ./check.sh
