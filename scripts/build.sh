#!/bin/bash

# Build script for Native Submit Plugin
set -euo pipefail

# Configuration
PLUGIN_NAME="native-submit-plugin"
BUILD_DIR="./build"
PLUGIN_OUTPUT="$BUILD_DIR/$PLUGIN_NAME.so"
BINARY_OUTPUT="$BUILD_DIR/$PLUGIN_NAME"
MAIN_DIR="./main"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check Go version
check_go_version() {
    if ! command -v go &> /dev/null; then
        log_error "Go is not installed"
        exit 1
    fi
    
    GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
    log_info "Go version: $GO_VERSION"
    
    # Check minimum version (1.21)
    MIN_VERSION="1.21"
    if [ "$(printf '%s\n' "$MIN_VERSION" "$GO_VERSION" | sort -V | head -n1)" != "$MIN_VERSION" ]; then
        log_error "Go version $GO_VERSION is too old. Minimum required: $MIN_VERSION"
        exit 1
    fi
}

# Clean build artifacts
clean() {
    log_info "Cleaning build artifacts..."
    rm -rf "$BUILD_DIR"
    rm -f ./*.so ./*.bin
}

# Setup build directory
setup() {
    log_info "Setting up build directory..."
    mkdir -p "$BUILD_DIR"
}

# Download dependencies
deps() {
    log_info "Downloading dependencies..."
    go mod download
    go mod tidy
}

# Run tests
test() {
    log_info "Running tests..."
    go test -v ./... || {
        log_error "Tests failed"
        exit 1
    }
}

# Format code
fmt() {
    log_info "Formatting code..."
    go fmt ./...
}

# Vet code
vet() {
    log_info "Vetting code..."
    go vet ./... || {
        log_error "Go vet failed"
        exit 1
    }
}

# Build plugin
build_plugin() {
    log_info "Building plugin..."
    
    # Check if CGO is required for plugin build mode
    export CGO_ENABLED=1
    
    go build -buildmode=plugin -o "$PLUGIN_OUTPUT" "$MAIN_DIR" || {
        log_error "Plugin build failed"
        exit 1
    }
    
    log_info "Plugin built successfully: $PLUGIN_OUTPUT"
}

# Build binary
build_binary() {
    log_info "Building binary..."
    
    go build -o "$BINARY_OUTPUT" "$MAIN_DIR" || {
        log_error "Binary build failed"
        exit 1
    }
    
    log_info "Binary built successfully: $BINARY_OUTPUT"
}

# Validate plugin
validate_plugin() {
    if [ ! -f "$PLUGIN_OUTPUT" ]; then
        log_error "Plugin file not found: $PLUGIN_OUTPUT"
        exit 1
    fi
    
    # Check if it's a valid shared object
    if file "$PLUGIN_OUTPUT" | grep -q "shared object"; then
        log_info "Plugin validation passed"
    else
        log_error "Plugin validation failed - not a valid shared object"
        exit 1
    fi
}

# Show help
show_help() {
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Options:"
    echo "  all        Build both plugin and binary (default)"
    echo "  plugin     Build plugin only"
    echo "  binary     Build binary only"
    echo "  test       Run tests"
    echo "  clean      Clean build artifacts"
    echo "  check      Check dependencies and environment"
    echo "  help       Show this help message"
    echo ""
}

# Main execution
main() {
    local command="${1:-all}"
    
    case $command in
        "all")
            check_go_version
            clean
            setup
            deps
            fmt
            vet
            test
            build_plugin
            build_binary
            validate_plugin
            log_info "Build completed successfully!"
            ;;
        "plugin")
            check_go_version
            setup
            deps
            fmt
            vet
            build_plugin
            validate_plugin
            log_info "Plugin build completed!"
            ;;
        "binary")
            check_go_version
            setup
            deps
            fmt
            vet
            build_binary
            log_info "Binary build completed!"
            ;;
        "test")
            check_go_version
            deps
            test
            ;;
        "clean")
            clean
            log_info "Clean completed!"
            ;;
        "check")
            check_go_version
            deps
            vet
            log_info "Environment check passed!"
            ;;
        "help"|"-h"|"--help")
            show_help
            ;;
        *)
            log_error "Unknown command: $command"
            show_help
            exit 1
            ;;
    esac
}

# Run main function
main "$@" 