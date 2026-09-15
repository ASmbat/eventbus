.PHONY: run test build lint

run:
	go run ./cmd/server

test:
	go test ./... -race -cover

build:
	go build -o bin/eventbus ./cmd/server

lint:
	go vet ./...
