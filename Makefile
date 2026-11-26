GO ?= go
DOCKER_COMPOSE ?= docker compose
BIN_DIR ?= bin
METRICS_TOKEN ?= local-metrics-token

.DEFAULT_GOAL := help
.PHONY: help fmt fmt-check build test test-race vet check migrate docker-build up down clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

fmt: ## Format Go source
	$(GO) fmt ./...

fmt-check: ## Verify Go source is formatted
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		echo "Unformatted files:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

build: ## Build api, worker, migrate and apikey into $(BIN_DIR)/
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BIN_DIR)/api ./cmd/api
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BIN_DIR)/worker ./cmd/worker
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BIN_DIR)/migrate ./cmd/migrate
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BIN_DIR)/apikey ./cmd/apikey

test: ## Run unit tests (integration tests skip without TEST_DATABASE_URL)
	$(GO) test -count=1 ./...

test-race: ## Run unit tests with the race detector
	$(GO) test -race -count=1 ./...

vet: ## Run go vet
	$(GO) vet ./...

check: fmt-check vet test-race build ## Run all local quality gates

migrate: ## Apply migrations using DATABASE_URL
	$(GO) run ./cmd/migrate

docker-build: ## Build the container images used by Compose
	METRICS_TOKEN=$(METRICS_TOKEN) $(DOCKER_COMPOSE) build

up: ## Start the full local stack with Compose
	METRICS_TOKEN=$(METRICS_TOKEN) $(DOCKER_COMPOSE) up --build

down: ## Stop the local stack
	$(DOCKER_COMPOSE) down

clean: ## Remove locally built binaries
	rm -rf $(BIN_DIR)
