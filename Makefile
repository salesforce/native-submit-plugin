# Native Submit Plugin Makefile

.PHONY: all build plugin clean test vet fmt deps help

# Build variables
PLUGIN_NAME = native-submit-plugin
PLUGIN_OUTPUT = $(PLUGIN_NAME).so
BINARY_OUTPUT = $(PLUGIN_NAME)
MAIN_DIR = ./main
GO_VERSION = 1.23.1

# Default target
all: deps vet test both

# Build regular binary
build:
	@echo "Building binary..."
	go build -o $(BINARY_OUTPUT) $(MAIN_DIR)

# Build plugin shared object
plugin:
	@echo "Building plugin..."
	go build -buildmode=plugin -o $(PLUGIN_OUTPUT) $(MAIN_DIR)

# Build both binary and plugin
both: build plugin

# Run tests
test:
	@echo "Running tests..."
	go test -v ./...

# Run tests with coverage
test-cover:
	@echo "Running tests with coverage..."
	go test -cover ./...

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Vet code
vet:
	@echo "Vetting code..."
	go vet ./...

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	go mod download
	go mod tidy

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -f $(PLUGIN_OUTPUT) $(BINARY_OUTPUT)

# Install plugin to local directory
install-local: plugin
	@echo "Installing plugin locally..."
	mkdir -p ./plugins
	cp $(PLUGIN_OUTPUT) ./plugins/

# Docker build (if needed for containerized plugin usage)
docker-build:
	@echo "Building Docker image..."
	docker build -t $(PLUGIN_NAME):latest .

# Help
help:
	@echo "Available targets:"
	@echo "  all          - Download deps, vet, test, and build plugin"
	@echo "  build        - Build regular binary"
	@echo "  plugin       - Build plugin shared object"
	@echo "  both         - Build both binary and plugin"
	@echo "  test         - Run tests"
	@echo "  test-cover   - Run tests with coverage"
	@echo "  fmt          - Format code"
	@echo "  vet          - Vet code"
	@echo "  deps         - Download dependencies"
	@echo "  clean        - Clean build artifacts"
	@echo "  install-local- Install plugin to ./plugins/ directory"
	@echo "  docker-build - Build Docker image"
	@echo "  help         - Show this help" 