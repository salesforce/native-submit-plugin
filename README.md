# Native Submit Plugin for Spark Operator

A high-performance alternative to `spark-submit` for launching Spark applications via the Spark Operator in Kubernetes clusters. This plugin eliminates the JVM spin-up overhead associated with traditional `spark-submit` commands, providing faster application startup times.

## Features

- 🚀 Native implementation bypassing JVM overhead
- ⚡ Faster Spark application startup
- 🔧 Flexible configuration options
- 🔒 Secure execution environment
- 📊 Resource management and optimization
- 🔄 Support for various Spark application types (Java, Scala, Python, R)

## Prerequisites


- Kubernetes cluster
- Spark Operator installed in the cluster
- kubectl configured to access the cluster

## Installation

### Building as Go Plugin

1. Clone the repository:
   ```bash
   git clone https://github.com/your-org/native-submit-plugin.git
   cd native-submit-plugin
   ```

2. Build the plugin:
   ```bash
   # Build plugin shared object
   make plugin
   
   # Or use go directly
   go build -buildmode=plugin -o native-submit-plugin.so ./main
   ```

3. Install locally for testing:
   ```bash
   make install-local
   ```

### Building as Regular Binary

```bash
# Build regular binary
make build

# Or use go directly  
go build -o native-submit-plugin ./main
```

## Usage

### As a Go Plugin

The plugin can be loaded dynamically at runtime:

```go
import "plugin"

// Load the plugin
p, err := plugin.Open("./native-submit-plugin.so")
if err != nil {
    log.Fatal(err)
}

// Get the plugin instance
symPlugin, err := p.Lookup("SparkSubmitPluginInstance")
sparkPlugin := symPlugin.(SparkSubmitPlugin)

// Use the plugin
success, err := sparkPlugin.SubmitSparkApplication(ctx, sparkApp)
```

See `examples/plugin_loader.go` for a complete example.

### Plugin Interface

The plugin exposes the following interface:

```go
type SparkSubmitPlugin interface {
    SubmitSparkApplication(ctx context.Context, app *v1beta2.SparkApplication) (bool, error)
    GetPluginInfo() PluginInfo
}
```

### Exported Functions

- `New() SparkSubmitPlugin` - Creates a new plugin instance
- `Submit(ctx, app) (bool, error)` - Direct submission function
- `GetInfo() PluginInfo` - Returns plugin metadata
- `SparkSubmitPluginInstance` - Pre-instantiated plugin variable


## Architecture

The plugin consists of several components:

- `common/`: Shared utilities and constants
- `driver/`: Driver pod management
- `service/`: Core service implementation
- `configmap/`: Configuration management
- `main/`: Plugin entry point


### Building

```bash
# Build the plugin
go build -buildmode=plugin -o plugin.so ./main

# Run tests
go test -v ./...
```

### Testing

```bash
# Run unit tests
go test -v ./...

# Run tests with coverage
go test -cover ./...

# Run specific package tests
go test -v ./pkg/...
```
