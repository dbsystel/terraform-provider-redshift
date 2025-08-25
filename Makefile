TEST?=$$(go list ./... |grep -v 'vendor')
GOFMT_FILES?=$$(find . -name '*.go' |grep -v vendor)

default: help

.PHONY: build
build: fmt ## Build and install provider binary
	go install

.PHONY: test
test: fmt vet ## Run unit tests
	go test $(TESTARGS) ./redshift

.PHONY: testacc
testacc: fmt ## Run acceptance tests
	TF_ACC=1 go test $(TEST) -v $(TESTARGS) -count=1 -timeout 120m

.PHONY: vet
vet: ## Run go vet command
	@echo "go vet ."
	@go vet $$(go list ./... | grep -v vendor/) ; if [ $$? -eq 1 ]; then \
		echo ""; \
		echo "Vet found suspicious constructs. Please check the reported constructs"; \
		echo "and fix them if necessary before submitting the code for review."; \
		exit 1; \
	fi

.PHONY: fmt
fmt: ## Run gofmt command
	gofmt -w $(GOFMT_FILES)

.PHONY: doc
doc: ## Generate documentation files
	@go generate

.PHONY: help
help: ## Show this help message
	@grep -Eh '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
