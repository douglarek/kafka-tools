cross-build:
	docker run --rm -v $(shell pwd):/app -w /app ghcr.io/douglarek/centos7-golang:latest go build -o kafka-replay cmd/replay/main.go

.PHONY: cross-build
