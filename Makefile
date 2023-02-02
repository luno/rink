.PHONY: vet test race

vet: ## Lint the files
	@$(MAKE) -C v2 vet

test: ## Run unittests
	@$(MAKE) -C v2 test

race: ## Run data race detector
	@$(MAKE) -C v2 race

help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
