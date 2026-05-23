.PHONY: build test test-cache test-model clean

GOCACHE := .gocache
BUILD_DIR := build
BINARY := $(BUILD_DIR)/cluster
PKG := ./...

build:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BINARY) .

# Build with containerd backend support.
build-containerd:
	@mkdir -p $(BUILD_DIR)
	go build -tags containerd -o $(BINARY) .

# Build custom Redis Docker image.
build-image:
	bash scripts/build_image.sh

test: test-cache test-model

test-cache:
	go test ./cache/...

test-model:
	go test ./model/...

test-verbose:
	go test -v $(PKG)

test-race:
	go test -race $(PKG)

clean:
	rm -rf $(BINARY) $(BUILD_DIR)
	rm -rf $(GOCACHE)
