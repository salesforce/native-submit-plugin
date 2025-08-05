package main

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// Request metrics
	grpcRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_requests_total",
			Help: "Total number of gRPC requests",
		},
		[]string{"method", "status"},
	)

	grpcRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_request_duration_seconds",
			Help:    "Duration of gRPC requests",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	// Spark application metrics
	sparkApplicationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_applications_total",
			Help: "Total number of Spark applications submitted",
		},
		[]string{"status", "type"},
	)

	sparkApplicationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "spark_application_duration_seconds",
			Help:    "Duration of Spark application submission",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
		},
		[]string{"type"},
	)

	// Active connections
	activeConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "grpc_active_connections",
			Help: "Number of active gRPC connections",
		},
	)
)

// UnaryInterceptor for metrics
func metricsUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		// Increment active connections
		activeConnections.Inc()
		defer activeConnections.Dec()

		// Call the handler
		resp, err := handler(ctx, req)

		// Record metrics
		duration := time.Since(start).Seconds()
		grpcRequestDuration.WithLabelValues(info.FullMethod).Observe(duration)

		// Determine status
		statusCode := codes.OK
		if err != nil {
			if st, ok := status.FromError(err); ok {
				statusCode = st.Code()
			} else {
				statusCode = codes.Unknown
			}
		}

		grpcRequestsTotal.WithLabelValues(info.FullMethod, statusCode.String()).Inc()

		return resp, err
	}
}

// RecordSparkApplicationMetrics records metrics for Spark application submissions
func RecordSparkApplicationMetrics(appType string, success bool, duration time.Duration) {
	status := "success"
	if !success {
		status = "failure"
	}

	sparkApplicationsTotal.WithLabelValues(status, appType).Inc()
	sparkApplicationDuration.WithLabelValues(appType).Observe(duration.Seconds())
}
