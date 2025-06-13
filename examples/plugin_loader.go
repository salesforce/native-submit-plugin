package main

import (
	"context"
	"fmt"
	"log"
	"plugin"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PluginInfo represents plugin metadata
type PluginInfo struct {
	Name        string
	Version     string
	Description string
}

// SparkSubmitPlugin interface that the plugin must implement
type SparkSubmitPlugin interface {
	SubmitSparkApplication(ctx context.Context, app *v1beta2.SparkApplication) (bool, error)
	GetPluginInfo() PluginInfo
}

func main() {
	// Load the plugin
	pluginPath := "./native-submit-plugin.so"
	p, err := plugin.Open(pluginPath)
	if err != nil {
		log.Fatalf("Failed to load plugin: %v", err)
	}

	// Look up the exported symbol
	symPlugin, err := p.Lookup("SparkSubmitPluginInstance")
	if err != nil {
		log.Fatalf("Failed to lookup plugin instance: %v", err)
	}

	// Assert to the expected interface
	sparkPlugin, ok := symPlugin.(SparkSubmitPlugin)
	if !ok {
		log.Fatal("Plugin does not implement SparkSubmitPlugin interface")
	}

	// Get plugin info
	info := sparkPlugin.GetPluginInfo()
	fmt.Printf("Loaded plugin: %s v%s - %s\n", info.Name, info.Version, info.Description)

	// Create a sample Spark application
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-spark-app",
			Namespace: "default",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Type:                v1beta2.SparkApplicationTypeScala,
			Image:               StringPtr("spark:3.5.0"),
			MainClass:           StringPtr("org.apache.spark.examples.SparkPi"),
			MainApplicationFile: StringPtr("local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar"),
			SparkVersion:        "3.5.0",
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Image: StringPtr("spark:3.5.0"),
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Image: StringPtr("spark:3.5.0"),
				},
				Instances: Int32Ptr(2),
			},
		},
	}

	// Use the plugin to submit the application
	ctx := context.Background()
	success, err := sparkPlugin.SubmitSparkApplication(ctx, app)
	if err != nil {
		log.Printf("Failed to submit Spark application: %v", err)
		return
	}

	if success {
		fmt.Println("Successfully submitted Spark application using plugin")
	} else {
		fmt.Println("Failed to submit Spark application")
	}
}

// Helper functions
func StringPtr(s string) *string {
	return &s
}

func Int32Ptr(i int32) *int32 {
	return &i
}
