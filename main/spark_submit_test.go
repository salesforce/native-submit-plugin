package main

import (
	"context"
	"testing"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRunAltSparkSubmit(t *testing.T) {
	// Skip the integration test that requires real Kubernetes cluster
	t.Skip("Skipping integration test that requires real Kubernetes cluster connection")

	tests := []struct {
		name         string
		app          *v1beta2.SparkApplication
		submissionID string
		wantSuccess  bool
		wantErr      bool
	}{
		{
			name: "valid spark application with submission ID",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Labels: map[string]string{
								"version": "1.0",
							},
						},
					},
				},
			},
			submissionID: "test-submission-id",
			wantSuccess:  true,
			wantErr:      false,
		},
		{
			name:         "nil spark application",
			app:          nil,
			submissionID: "test-submission-id",
			wantSuccess:  false,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup fake client
			s := runtime.NewScheme()
			v1beta2.AddToScheme(s)
			scheme.AddToScheme(s)
			client := fake.NewClientBuilder().WithScheme(s).Build()
			_ = client // Use the fake client when we implement proper unit testing

			success, err := RunAltSparkSubmit(context.Background(), tt.app)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.wantSuccess, success)
		})
	}
}

// TestRunAltSparkSubmit_NilApp tests only the nil application case without K8s dependencies
func TestRunAltSparkSubmit_NilApp(t *testing.T) {
	success, err := RunAltSparkSubmit(context.Background(), nil)
	assert.Error(t, err)
	assert.False(t, success)
	assert.Contains(t, err.Error(), "spark application cannot be nil")
}

func TestGetServiceName(t *testing.T) {
	tests := []struct {
		name string
		app  *v1beta2.SparkApplication
		want string
	}{
		{
			name: "short service name",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-app",
				},
			},
			want: "test-app-driver-svc",
		},
		{
			name: "long service name",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name: "this-is-a-very-long-spark-application-name-that-exceeds-the-kubernetes-dns-label-name-max-length",
				},
			},
			want: "spark-", // The actual value will be dynamic due to timestamp and random hex
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getServiceName(tt.app)
			if tt.name == "long service name" {
				assert.Contains(t, got, tt.want)
				assert.True(t, len(got) <= 63)
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestRandomHex(t *testing.T) {
	tests := []struct {
		name string
		n    int
	}{
		{
			name: "10 bytes",
			n:    10,
		},
		{
			name: "20 bytes",
			n:    20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := randomHex(tt.n)
			assert.NoError(t, err)
			assert.Len(t, got, tt.n*2) // Each byte is represented by 2 hex characters
		})
	}
}

// TestPluginInterface tests the plugin interface implementation
func TestPluginInterface(t *testing.T) {
	plugin := &nativeSubmitPlugin{}

	// Test GetPluginInfo
	info := plugin.GetPluginInfo()
	assert.Equal(t, "native-submit-plugin", info.Name)
	assert.Equal(t, "1.0.0", info.Version)
	assert.Equal(t, "Native Kubernetes submission plugin for Spark applications", info.Description)
}

// TestExportedFunctions tests the exported plugin functions
func TestExportedFunctions(t *testing.T) {
	// Test New function
	plugin := New()
	assert.NotNil(t, plugin)
	assert.Implements(t, (*SparkSubmitPlugin)(nil), plugin)

	// Test GetInfo function
	info := GetInfo()
	assert.Equal(t, "native-submit-plugin", info.Name)
	assert.Equal(t, "1.0.0", info.Version)

	// Test SparkSubmitPluginInstance
	assert.NotNil(t, SparkSubmitPluginInstance)
	assert.Implements(t, (*SparkSubmitPlugin)(nil), SparkSubmitPluginInstance)
}

// TestSubmit_NilApp tests the exported Submit function with nil app
func TestSubmit_NilApp(t *testing.T) {
	success, err := Submit(context.Background(), nil)
	assert.Error(t, err)
	assert.False(t, success)
	assert.Contains(t, err.Error(), "spark application cannot be nil")
}
