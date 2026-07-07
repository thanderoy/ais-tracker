# ais-tracker developer task runner.
# Local planning docs live in plan/ (gitignored); WORKPLAN.md is the index.

GIT_SHA  := $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
LDFLAGS  := -s -w -X main.version=$(GIT_SHA)
COMPOSE  := docker compose --env-file .env -f deploy/docker-compose.yml

# Load a local .env (KEY=VALUE lines) into recipe environments when present.
-include .env
export

.DEFAULT_GOAL := build
.PHONY: build run test lint tidy migrate-up migrate-down sqlc compose-up compose-down

## build: compile all binaries into ./bin
build:
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/tracker ./cmd/tracker
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/migrate ./cmd/migrate

## run: run the tracker service
run:
	go run ./cmd/tracker

## test: run all tests with the race detector
test:
	go test -race ./...

## lint: run golangci-lint with the configured ruleset
lint:
	golangci-lint run

## tidy: sync go.mod and go.sum
tidy:
	go mod tidy

## migrate-up: apply all pending migrations
migrate-up:
	go run ./cmd/migrate up

## migrate-down: roll back the last migration
migrate-down:
	go run ./cmd/migrate down 1

## sqlc: regenerate typed queries from SQL
sqlc:
	sqlc generate

## compose-up: start the local Postgres stack
compose-up:
	$(COMPOSE) up -d

## compose-down: stop the local Postgres stack
compose-down:
	$(COMPOSE) down
