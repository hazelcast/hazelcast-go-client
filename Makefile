.PHONY: benchmark build check doc test test-all test-all-race

PORT ?= 5050
MEMBER_COUNT ?= 3
COVERAGE_OUT ?= coverage.out
TEST_FLAGS ?= -v -count 1 -p 1 -timeout 50m -tags=hazelcastinternal,hazelcastinternaltest
PACKAGES = $(go list ./... | grep -v org-website)

build:
	go build $(PACKAGES)

test: test-all

test-all:
	env MEMBER_COUNT=$(MEMBER_COUNT) go test $(TEST_FLAGS) $(PACKAGES) ./...

test-race:
	env MEMBER_COUNT=$(MEMBER_COUNT) go test $(TEST_FLAGS) -race $(PACKAGES)

test-cover:
	env TEST_FLAGS="$(TEST_FLAGS)" bash ./coverage.sh

view-cover:
	go tool cover -func $(COVERAGE_OUT) | grep total:
	go tool cover -html $(COVERAGE_OUT) -o coverage.html

benchmark:
	env MEMBER_COUNT=$(MEMBER_COUNT) go test $(TEST_FLAGS) -bench . -benchmem ./benchmarks

doc:
	godoc -http=localhost:$(PORT)

check:
	bash ./check.sh
