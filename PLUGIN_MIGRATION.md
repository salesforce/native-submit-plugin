# Go Plugin Migration Guide

This document outlines the changes made to convert the native-submit repository into a Go plugin that can be dynamically loaded at runtime.

## Summary of Changes

### 1. Plugin Interface and Exported Functions (`main/spark_submit.go`)

**Added:**
- `SparkSubmitPlugin` interface defining the plugin contract
- `PluginInfo` struct for plugin metadata
- `nativeSubmitPlugin` struct implementing the interface
- Exported functions: `New()`, `Submit()`, `GetInfo()`
- Exported variable: `SparkSubmitPluginInstance`

**Purpose:** Enables dynamic loading and provides a clean interface for plugin consumers.

### 2. Build System (`Makefile`)

**Added:**
- Targets for building both plugin (`.so`) and binary
- Testing, formatting, and vetting targets
- Clean and dependency management
- Local installation support

**Usage:**
```bash
make plugin        # Build plugin shared object
make build         # Build regular binary  
make both          # Build both
make test          # Run tests
make clean         # Clean artifacts
```

### 3. Plugin Configuration (`plugin.yaml`)

**Added:**
- Plugin metadata and configuration
- Interface definitions
- Build specifications
- Compatibility requirements

**Purpose:** Provides standardized plugin information for plugin managers.

### 4. Build Scripts (`scripts/build.sh`)

**Added:**
- Comprehensive build script with error handling
- Environment validation
- Multiple build modes (plugin, binary, all)
- Cross-platform support

**Features:**
- Go version checking
- Dependency management
- Plugin validation
- Colored output

### 5. Example Plugin Loader (`examples/plugin_loader.go`)

**Added:**
- Complete example showing how to load the plugin
- Sample Spark application creation
- Error handling and validation
- Plugin interface usage demonstration

**Purpose:** Demonstrates plugin integration for consumers.

### 6. CI/CD Pipeline (`.github/workflows/build.yml`)

**Added:**
- Multi-version Go testing (1.21, 1.22, 1.23)
- Cross-platform builds (Linux, macOS)
- Artifact uploads
- Release automation
- Coverage reporting

**Features:**
- Matrix builds across Go versions and platforms
- Plugin validation
- Automated releases

### 7. Updated Documentation (`README.md`)

**Enhanced:**
- Plugin-specific installation instructions
- Usage examples for both plugin and binary modes
- Interface documentation
- Build instructions

## Plugin Interface

The plugin exposes the following interface:

```go
type SparkSubmitPlugin interface {
    SubmitSparkApplication(ctx context.Context, app *v1beta2.SparkApplication) (bool, error)
    GetPluginInfo() PluginInfo
}

type PluginInfo struct {
    Name        string
    Version     string  
    Description string
}
```

## Exported Symbols

The plugin exports these symbols for dynamic loading:

- **`SparkSubmitPluginInstance`** - Pre-instantiated plugin variable
- **`New()`** - Factory function to create new plugin instances
- **`Submit()`** - Direct submission function wrapper
- **`GetInfo()`** - Returns plugin metadata

## Usage Patterns

### 1. As a Go Plugin

```go
// Load plugin
p, err := plugin.Open("./native-submit-plugin.so")
symPlugin, err := p.Lookup("SparkSubmitPluginInstance")
sparkPlugin := symPlugin.(SparkSubmitPlugin)

// Use plugin
success, err := sparkPlugin.SubmitSparkApplication(ctx, app)
```

### 2. As a Regular Binary

```go
// Direct function call
success, err := Submit(ctx, app)
```

## Build Requirements

### Prerequisites
- Go 1.21+ (for plugin support)
- CGO enabled (for plugin mode)
- GCC/libc6-dev (Linux) or Xcode tools (macOS)

### Build Commands

```bash
# Plugin build
export CGO_ENABLED=1
go build -buildmode=plugin -o native-submit-plugin.so ./main

# Binary build  
go build -o native-submit-plugin ./main
```

## Compatibility

- **Go Version:** ≥1.21 (required for plugin buildmode improvements)
- **Kubernetes:** ≥1.25
- **Spark Operator:** ≥1.1.0
- **Platforms:** Linux, macOS (plugin mode), Windows (binary mode only)

## Notes

1. **Plugin Mode Limitations:**
   - Requires CGO (not available in all environments)
   - Platform-specific shared objects
   - Cannot be used with `CGO_ENABLED=0`

2. **Binary Mode Benefits:**
   - No CGO dependency
   - Cross-platform compilation
   - Simpler deployment

3. **Recommended Usage:**
   - Use plugin mode for dynamic loading scenarios
   - Use binary mode for static compilation and simple deployments

## Testing

```bash
# Test plugin build
make plugin

# Test binary build  
make build

# Run all tests
make test

# Test plugin loading
go build -o loader ./examples/plugin_loader.go
./loader  # (requires Kubernetes cluster access)
```

## Migration Notes

- All existing functionality remains unchanged
- Original `RunAltSparkSubmit()` function is preserved
- Plugin additions are additive, no breaking changes
- Both plugin and binary modes supported simultaneously 