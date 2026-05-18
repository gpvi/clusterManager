.PHONY: build test clean

GOCACHE := .gocache
BINARY := cluster
PKG := ./...

build:
	go build -o $(BINARY) .

test:
	go test $(PKG)

clean:
	rm -f $(BINARY)
	rm -rf $(GOCACHE)
