# Makefile for Cluster Processor Demo

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
BINARY_NAME=cluster-processor-demo
BINARY_UNIX=$(BINARY_NAME)_unix

# Build the demo
build:
	$(GOBUILD) -o $(BINARY_NAME) -v ./

# Build for Linux
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) -o $(BINARY_UNIX) -v ./

# Clean build artifacts
clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -f $(BINARY_UNIX)
	rm -f master.csv
	rm -f cluster_results.json

# Run the demo
run:
	$(GOBUILD) -o $(BINARY_NAME) -v ./
	./$(BINARY_NAME)

# Run with custom SSH settings
run-with-ssh:
	@echo "Set SSH_KEY_PATH and SSH_USERNAME environment variables if needed"
	@echo "Example: SSH_KEY_PATH=~/.ssh/my_key SSH_USERNAME=myuser make run"
	$(GOBUILD) -o $(BINARY_NAME) -v ./
	./$(BINARY_NAME)

# Initialize Go modules (if needed)
mod-init:
	$(GOMOD) init cluster-processor-demo

# Download dependencies
deps:
	$(GOMOD) download
	$(GOMOD) tidy

# Test (placeholder for future tests)
test:
	$(GOTEST) -v ./...

# Install dependencies
install-deps:
	$(GOGET) golang.org/x/crypto/ssh
	$(GOGET) github.com/pkg/sftp

# Help
help:
	@echo "Available targets:"
	@echo "  build         - Build the demo binary"
	@echo "  build-linux   - Build for Linux"
	@echo "  clean         - Clean build artifacts and generated files"
	@echo "  run           - Build and run the demo"
	@echo "  run-with-ssh  - Run with SSH environment variables"
	@echo "  mod-init      - Initialize Go modules"
	@echo "  deps          - Download and tidy dependencies"
	@echo "  test          - Run tests"
	@echo "  install-deps  - Install required dependencies"
	@echo "  help          - Show this help message"

.PHONY: build build-linux clean run run-with-ssh mod-init deps test install-deps help
