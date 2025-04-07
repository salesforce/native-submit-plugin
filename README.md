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

1. Clone the repository:
   ```bash
   git clone https://github.com/your-org/native-submit-plugin.git
   cd native-submit-plugin
   ```

2. Build the plugin:
   ```bash
   go build -buildmode=plugin -o plugin.so ./main
   ```

3. Deploy the plugin to your cluster:
   ```bash
   kubectl apply -f deploy/
   ```

## Usage
native-submit will be  plugin to spark operator.


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
