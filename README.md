# Native Submit Plugin for Spark Operator

A high-performance alternative to `spark-submit` for launching Spark applications via the Spark Operator in Kubernetes clusters. This plugin eliminates the JVM spin-up overhead associated with traditional `spark-submit` commands, providing faster application startup times.

## Overview

This repository contains a native submit plugin that runs as a sidecar container alongside the [Spark Operator](https://github.com/kubeflow/spark-operator/) controller. The plugin provides a gRPC service that the Spark Operator can use to submit Spark applications without the overhead of JVM startup.

## Features

- 🚀 Native Go implementation bypassing JVM overhead
- ⚡ Faster Spark application startup
- 🔧 gRPC service for Spark Operator integration
- 🔒 Secure execution environment with non-root user
- 📊 Health checks and metrics endpoints
- 🔄 Support for various Spark application types (Java, Scala, Python, R)
- 🐳 Containerized deployment as sidecar

## Architecture

The plugin is designed to run as a sidecar container alongside the Spark Operator controller:

```
┌─────────────────────────────────────────────────────────────┐
│                    Spark Operator Pod                      │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────────┐  ┌─────────────────────────────┐  │
│  │ Spark Operator      │  │ Native Submit Plugin       │  │
│  │ Controller          │  │ (Sidecar Container)        │  │
│  │                     │  │                             │  │
│  │ - Watches CRDs      │  │ - gRPC Server (port 50051) │  │
│  │ - Manages lifecycle │  │ - Health checks (port 9090)│  │
│  │ - Calls gRPC        │  │ - Native submit logic      │  │
│  └─────────────────────┘  └─────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Components

- **gRPC Service**: Runs on port 50051, provides `RunAltSparkSubmit` method
- **Health Checks**: HTTP endpoints on port 9090 (`/healthz`, `/readyz`)
- **Native Logic**: Go implementation for Spark application submission
- **Security**: Runs as non-root user (UID: 185, GID: 185)

## Prerequisites

- Kubernetes cluster
- Spark Operator installed in the cluster
- kubectl configured to access the cluster
- Docker (for building the container image)

## Quick Start

### 1. Build the Container Image

```bash
# Build the Docker image
docker build -t native-submit:latest .

# Tag for your registry (example)
docker tag native-submit:latest your-registry/native-submit:latest
docker push your-registry/native-submit:latest
```

### 2. Deploy with Spark Operator

The plugin is deployed as a sidecar container with the Spark Operator. The Spark Operator controller is configured to use the gRPC service:

```yaml
# Example deployment configuration
containers:
- name: spark-operator-controller
  image: ghcr.io/kubeflow/spark-operator/controller:2.2.1
  args:
  - --submitter-type=grpc
  - --grpc-server-address=localhost:50051
  - --grpc-submit-timeout=10s
  # ... other args

- name: native-submit
  image: your-registry/native-submit:latest
  ports:
  - containerPort: 50051  # gRPC
  - containerPort: 9090   # Health checks
```

### 3. Test the Integration

```bash
# Check if the sidecar is running
kubectl get pods -n spark-operator

# Check logs
kubectl logs -n spark-operator deployment/spark-operator-controller -c native-submit

# Test health endpoint
kubectl port-forward -n spark-operator deployment/spark-operator-controller 9090:9090
curl http://localhost:9090/healthz
```

## API Reference

### gRPC Service

The plugin provides a gRPC service with the following method:

```protobuf
service SparkSubmitService {
  rpc RunAltSparkSubmit(RunAltSparkSubmitRequest) returns (RunAltSparkSubmitResponse);
}
```

#### Request
```protobuf
message RunAltSparkSubmitRequest {
  SparkApplication spark_application = 1;
  string submission_id = 2;
}
```

#### Response
```protobuf
message RunAltSparkSubmitResponse {
  bool success = 1;
  string error_message = 2;
}
```

### HTTP Health Endpoints

- **Health Check**: `GET /healthz` - Service health status
- **Readiness Check**: `GET /readyz` - Service readiness status

## Configuration

### Environment Variables

- `GRPC_PORT`: gRPC server port (default: 50051)
- `HEALTH_PORT`: Health check port (default: 9090)

### Spark Operator Integration

The Spark Operator controller must be configured with:

```yaml
args:
- --submitter-type=grpc
- --grpc-server-address=localhost:50051
- --grpc-submit-timeout=10s
```

## Building

```bash
# Build the binary
go build -o native-submit ./main

# Build Docker image
docker build -t native-submit:latest .

# Run tests
go test -v ./...
```

## Testing

```bash
# Run unit tests
go test -v ./...

# Run tests with coverage
go test -cover ./...

# Test gRPC service locally
go run test_grpc_client.go
```

## Deployment

### With Helm (Recommended)

The plugin is typically deployed as part of the Spark Operator Helm chart:

```bash
# Install Spark Operator with native submit plugin
helm install spark-operator spark-operator/spark-operator \
    --namespace spark-operator \
    --create-namespace \
    --set controller.image.tag=2.2.1 \
    --set controller.args.submitter-type=grpc \
    --set controller.args.grpc-server-address=localhost:50051 \
    --set controller.args.grpc-submit-timeout=10s \
    --set controller.sidecars.native-submit.enabled=true \
    --set controller.sidecars.native-submit.image=your-registry/native-submit:latest
```

### Manual Deployment

For manual deployment, update the Spark Operator deployment to include the sidecar container and configure the controller to use the gRPC service.

## Monitoring

### Health Checks

The service includes:
- **Liveness Probe**: `GET /healthz` on port 9090
- **Readiness Probe**: `GET /readyz` on port 9090
- **Docker Health Check**: Built into the container

### Logs

```bash
# View plugin logs
kubectl logs -n spark-operator deployment/spark-operator-controller -c native-submit

# View controller logs
kubectl logs -n spark-operator deployment/spark-operator-controller -c spark-operator-controller
```

## Security

The container runs with:
- Non-root user (UID: 185, GID: 185)
- Read-only root filesystem
- Dropped capabilities
- Security context with minimal privileges

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE.txt) file for details.
