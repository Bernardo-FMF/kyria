# kyria — build & dev tasks. Run `make` or `make help` to list targets.

GO      ?= go
PKGS    := ./...
CMD     := ./cmd/kyria        # entrypoint (added in a later phase)
BINARY  := kyria
BIN_DIR := bin
COVER   := coverage.out
IMAGE   ?= kyria
TAG     ?= dev

.DEFAULT_GOAL := help

## ---- build & run ---------------------------------------------------------

.PHONY: build
build: ## Compile every package (fast type/compile check)
	$(GO) build $(PKGS)

.PHONY: build-bin
build-bin: ## Build the kyria binary into ./bin (needs ./cmd/kyria)
	$(GO) build -o $(BIN_DIR)/$(BINARY) $(CMD)

.PHONY: run
run: ## Run the server (needs ./cmd/kyria)
	$(GO) run $(CMD)

## ---- tests ---------------------------------------------------------------

.PHONY: test
test: ## Run all tests
	$(GO) test $(PKGS)

.PHONY: test-race
test-race: ## Run all tests under the race detector
	$(GO) test -race $(PKGS)

.PHONY: cover
cover: ## Run tests with coverage and open an HTML report
	$(GO) test -coverprofile=$(COVER) $(PKGS)
	$(GO) tool cover -html=$(COVER)

.PHONY: bench
bench: ## Run benchmarks (no unit tests)
	$(GO) test -run '^$$' -bench=. -benchmem $(PKGS)

## ---- quality -------------------------------------------------------------

.PHONY: check
check: fmt-check vet test-race ## Everything CI should run: format, vet, race tests

.PHONY: vet
vet: ## Run go vet
	$(GO) vet $(PKGS)

.PHONY: fmt
fmt: ## Format the code in place (gofmt -s)
	gofmt -s -w .

.PHONY: fmt-check
fmt-check: ## Fail if any file is not gofmt-formatted
	@out=$$(gofmt -s -l .); if [ -n "$$out" ]; then \
		echo "These files need gofmt:"; echo "$$out"; exit 1; \
	fi

.PHONY: tidy
tidy: ## Sync go.mod / go.sum
	$(GO) mod tidy

## ---- docker --------------------------------------------------------------

.PHONY: image
image: ## Build the container image (override with IMAGE= / TAG=)
	docker build -t $(IMAGE):$(TAG) .

.PHONY: image-size
image-size: ## Show the built image's size
	@docker image ls $(IMAGE):$(TAG) --format '{{.Repository}}:{{.Tag}}  {{.Size}}'

# Standalone, so -addr keeps its default ":6379". Binding the wildcard is what a published
# port needs — the routable-host rule only applies once -gossip-addr enables clustering.
.PHONY: docker-run
docker-run: ## Run a single standalone node in a container on :6379
	docker run --rm -p 6379:6379 -e KYRIA_LOG_LEVEL=debug $(IMAGE):$(TAG)

## ---- housekeeping --------------------------------------------------------

.PHONY: clean
clean: ## Remove build and coverage artifacts
	rm -rf $(BIN_DIR) $(COVER)

.PHONY: help
help: ## Show this help
	@echo "kyria — available targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'
