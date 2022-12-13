
.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: build
build: fmt vet ## Build terraform-provider-nifi binary.
	go build -o terraform-provider-nifi main.go

.PHONY: clean-tf
clean-tf:
	rm -rf examples/new_flow/.terraform
	rm -f examples/new_flow/.terraform.lock.hcl

.PHONY: test-tf
test-tf: build clean-tf
	
	mkdir -p /tmp/.terraform.d/plugins/github.com/glympse/nifi/1.0.0/darwin_amd64 
	yes | mv terraform-provider-nifi /tmp/.terraform.d/plugins/github.com/glympse/nifi/1.0.0/darwin_amd64/
	terraform  -chdir=examples/new_flow init -plugin-dir="/tmp/.terraform.d/plugins" 