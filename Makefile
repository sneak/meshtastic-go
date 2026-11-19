.PHONY: default build test lint clean

default: build

build: test
	./build.sh

test:
	go test -v ./...

lint:
	golangci-lint run

clean:
	rm -rf builds

dev:
	DEBUG=true go run ./cmd/meshtastic-go/ --port /dev/tty.usbserial-0001 stream
