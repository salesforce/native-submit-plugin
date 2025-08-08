FROM golang:1.23-alpine as builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o native-submit ./main

# Final stage
FROM alpine:3.18

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/native-submit .

# Create non-root user
RUN addgroup -g 185 spark && \
    adduser -D -s /bin/sh -u 185 -G spark spark

# Change ownership of the binary
RUN chown spark:spark /app/native-submit

# Switch to non-root user
USER spark

# Expose ports
EXPOSE 50051 9090

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:9090/healthz || exit 1

# Run the binary
ENTRYPOINT ["/app/native-submit"] 