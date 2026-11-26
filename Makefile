.PHONY: help build test test-race test-cover lint fmt check tidy clean nats-up nats-down run watch

.DEFAULT_GOAL := help

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME ?= $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildTime=$(BUILD_TIME)"

NATS_URL ?= nats://localhost:4222
NATS_CONTAINER ?= gorai-pentair-nats
ROBOT_ID ?= home

##@ General

help: ## Print this help message
	@echo "gorai-pentair - Pentair IntelliFlo 3 pump component for gorai"
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Build

build: ## Build bin/gorai-pentair and bin/pentair-watch
	CGO_ENABLED=0 go build $(LDFLAGS) -o bin/gorai-pentair ./cmd/gorai-pentair
	CGO_ENABLED=0 go build -o bin/pentair-watch ./cmd/pentair-watch

clean: ## Remove build artifacts
	rm -rf bin/ coverage.out coverage.html

##@ Test

test: ## Run unit tests
	go test ./...

test-race: ## Run unit tests with the race detector
	go test -race ./...

test-cover: ## Coverage report
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

lint: ## go vet
	go vet ./...

fmt: ## go fmt
	go fmt ./...

check: fmt lint test ## fmt, vet, test

tidy: ## go mod tidy
	go mod tidy

##@ Host NATS bus

nats-up: ## Start a NATS server (JetStream) on this host in Docker, ports 4222/8222
	docker run -d --rm --name $(NATS_CONTAINER) -p 4222:4222 -p 8222:8222 nats:2.12 -js -m 8222

nats-down: ## Stop the Docker NATS server
	docker stop $(NATS_CONTAINER)

run: build ## Follow the pump and publish to $(NATS_URL) (needs PENTAIR_USERNAME / PENTAIR_PASSWORD)
	./bin/gorai-pentair --nats $(NATS_URL) --robot-id $(ROBOT_ID)

watch: build ## Print everything the pool pump publishes on $(NATS_URL)
	./bin/pentair-watch --nats $(NATS_URL)
