# kyria — build & dev tasks. Run `make` or `make help` to list targets.

GO      ?= go
PKGS    := ./...
CMD     := ./cmd/kyria        # entrypoint (added in a later phase)
BINARY  := kyria
BIN_DIR := bin
COVER   := coverage.out

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

## ---- housekeeping --------------------------------------------------------

.PHONY: clean
clean: ## Remove build and coverage artifacts
	rm -rf $(BIN_DIR) $(COVER)

.PHONY: help
help: ## Show this help
	@echo "kyria — available targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
