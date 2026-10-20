# ais-tracker developer task runner.
# Local planning docs live in plan/ (gitignored); WORKPLAN.md is the index.
# The Go service lives in api/ (its own module), the Qwik terminal in web/.

GIT_SHA  := $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
LDFLAGS  := -s -w -X main.version=$(GIT_SHA)
COMPOSE  := docker compose --env-file .env -f deploy/docker-compose.yml
API      := api
WEB      := web
UI_DIST  := $(API)/internal/webui/dist
GOBUILD  := cd $(API) && CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)"

# Load a local .env (KEY=VALUE lines) into recipe environments when present.
-include .env
export

.DEFAULT_GOAL := build
.PHONY: build ui ui-dev run test lint tidy migrate-up migrate-down sqlc compose-up compose-down

## build: build the terminal UI, then compile all binaries into ./bin
build: ui
	$(GOBUILD) -o ../bin/tracker ./cmd/tracker
	$(GOBUILD) -o ../bin/migrate ./cmd/migrate
	$(GOBUILD) -o ../bin/seed-ports ./cmd/seed-ports
	$(GOBUILD) -o ../bin/seed-eez ./cmd/seed-eez
	$(GOBUILD) -o ../bin/download-sanctions ./cmd/download-sanctions

## ui: build the Qwik terminal into api/internal/webui/dist for embedding
ui:
	find $(UI_DIST) -mindepth 1 ! -name .gitkeep -delete
	cd $(WEB) && npm ci && npm run build

## ui-dev: run the terminal with hot reload, proxying /api and /ws to the tracker on :8080
ui-dev:
	cd $(WEB) && npm run dev

## run: run the tracker service
run:
	cd $(API) && go run ./cmd/tracker

## test: run all tests with the race detector
test:
	cd $(API) && go test -race ./...

## lint: run golangci-lint with the configured ruleset
lint:
	cd $(API) && golangci-lint run

## tidy: sync go.mod and go.sum
tidy:
	cd $(API) && go mod tidy

## migrate-up: apply all pending migrations
migrate-up:
	cd $(API) && go run ./cmd/migrate up

## migrate-down: roll back the last migration
migrate-down:
	cd $(API) && go run ./cmd/migrate down 1

## sqlc: regenerate typed queries from SQL
sqlc:
	cd $(API) && sqlc generate

## compose-up: start the local Postgres stack
compose-up:
	$(COMPOSE) up -d

## compose-down: stop the local Postgres stack
compose-down:
	$(COMPOSE) down
