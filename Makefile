GO        ?= go
GOBIN     := $(shell $(GO) env GOPATH)/bin
PKGS      := ./...
COVERPKGS := ./internal/store/...,./internal/delivery/...,./internal/dispatch/...

.PHONY: build test lint proto up down demo chaos loadtest cover

build:
	$(GO) build -o bin/ ./cmd/...

test:
	$(GO) vet $(PKGS)
	$(GO) test -race $(PKGS)

cover:
	$(GO) test -race -coverpkg=$(COVERPKGS) -coverprofile=coverage.out ./internal/... ./test/...
	$(GO) tool cover -func=coverage.out | tail -1

lint:
	$(GOBIN)/golangci-lint run

proto:
	PATH="$(GOBIN):$$PATH" buf lint
	PATH="$(GOBIN):$$PATH" buf generate

up:
	docker compose up -d --build --wait

down:
	docker compose down -v

demo:
	./scripts/demo.sh

chaos:
	$(GO) test -race -tags chaos -count=1 -v ./test/chaos/

loadtest: build
	./scripts/loadtest.sh
