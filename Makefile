PROJECT_NAME := rink/v2
PKG          := github.com/luno/$(PROJECT_NAME)
PKG_LIST = $(shell go list ${PKG}/... | grep -v /vendor/)

.PHONY: vet test race check

vet: ## Lint the files
	go vet ${PKG_LIST}

test: ## Run unit tests with coverage
	go test -short -coverprofile=coverage.out ${PKG_LIST}
	@go tool cover -func=coverage.out

race: ## Run data race detector with coverage
	go test -race -short -coverprofile=coverage-race.out ${PKG_LIST}
	@go tool cover -func=coverage-race.out

check: vet test race  ## Run all validations

help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
