.PHONY: build test test-cache test-model clean

GOCACHE := .gocache
BINARY := cluster
PKG := ./...

build:
	go build -o $(BINARY) .

# Build with containerd backend support.
build-containerd:
	go build -tags containerd -o $(BINARY) .

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
	rm -f $(BINARY)
	rm -rf $(GOCACHE)
