package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	pb "nativesubmit/proto/spark"

	"google.golang.org/grpc"
)

func main() {
	// Get server address from environment or use default
	serverAddr := os.Getenv("GRPC_SERVER")
	if serverAddr == "" {
		serverAddr = "localhost:50051"
	}

	// Connect to the gRPC server
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(5*time.Second))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()

	client := pb.NewSparkSubmitServiceClient(conn)

	// Minimal SparkApplication for testing
	app := &pb.SparkApplication{
		Metadata: &pb.ObjectMeta{
			Name:      "test-app",
			Namespace: "default",
		},
		Spec: &pb.SparkApplicationSpec{
			Type: pb.SparkApplicationType_SPARK_APPLICATION_TYPE_PYTHON,
			Mode: pb.DeployMode_DEPLOY_MODE_CLUSTER,
		},
	}

	req := &pb.RunAltSparkSubmitRequest{
		SparkApplication: app,
		SubmissionId:     "test-submission-id",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Printf("Calling gRPC server at %s...\n", serverAddr)
	resp, err := client.RunAltSparkSubmit(ctx, req)
	if err != nil {
		log.Fatalf("gRPC call failed: %v", err)
	}

	fmt.Printf("Success: %v\nError: %s\n", resp.GetSuccess(), resp.GetErrorMessage())
}
