GO := $(shell which go)

.PHONY: test

test:
	$(GO) test --race -count=1 ./...
