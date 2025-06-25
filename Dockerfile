# Use the Spark image as the builder to match glibc and architecture
FROM spark:3.5.5 AS builder

# Install build tools and Go
USER root
RUN apt-get update && apt-get install -y wget build-essential gcc libc6-dev \
    && wget https://golang.org/dl/go1.24.1.linux-arm64.tar.gz \
    && tar -C /usr/local -xzf go1.24.1.linux-arm64.tar.gz \
    && rm go1.24.1.linux-arm64.tar.gz
ENV PATH="/usr/local/go/bin:${PATH}"
ENV CGO_ENABLED=1
ENV GOARCH=arm64

WORKDIR /app

# Copy go module files and vendor directory
COPY go.mod go.sum ./
COPY vendor ./vendor

# Copy the rest of the source code
COPY . .

# Build the plugin
RUN go build -mod=vendor -buildmode=plugin -o /app/native-submit-plugin.so ./main

# Final stage: use the Spark image again
FROM spark:3.5.5

# Copy the built plugin from the builder stage
COPY --from=builder /app/native-submit-plugin.so /usr/bin/plugins/native-submit-plugin.so 