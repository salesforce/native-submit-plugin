package configmap

import (
	"strings"
	"testing"

	v1beta2 "github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCreate(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1beta2.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name                string
		app                 *v1beta2.SparkApplication
		submissionID        string
		applicationID       string
		driverConfigMapName string
		serviceName         string
		wantErr             bool
	}{
		{
			name: "valid application",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					Mode: v1beta2.DeployModeCluster,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores: int32ptr(1),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores: int32ptr(1),
						},
					},
				},
			},
			submissionID:        "test-submission",
			applicationID:       "test-app",
			driverConfigMapName: "test-configmap",
			serviceName:         "test-service",
			wantErr:             false,
		},
		{
			name:                "nil application",
			app:                 nil,
			submissionID:        "test-submission",
			applicationID:       "test-app",
			driverConfigMapName: "test-configmap",
			serviceName:         "test-service",
			wantErr:             true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewClientBuilder().WithScheme(scheme).Build()
			err := Create(tt.app, tt.submissionID, tt.applicationID, client, tt.driverConfigMapName, tt.serviceName)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func int32ptr(i int32) *int32 {
	return &i
}

func stringptr(s string) *string {
	return &s
}

func TestBuildAltSubmissionCommandArgs(t *testing.T) {
	app := &v1beta2.SparkApplication{
		Spec: v1beta2.SparkApplicationSpec{
			Type: v1beta2.SparkApplicationTypeScala,
			Mode: v1beta2.DeployModeCluster,
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores: int32ptr(1),
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores: int32ptr(1),
				},
			},
		},
	}

	driverPodName := "test-driver"
	submissionID := "test-submission"
	applicationID := "test-app"
	serviceName := "test-service"

	result, err := buildAltSubmissionCommandArgs(app, driverPodName, submissionID, applicationID, serviceName)
	assert.NoError(t, err)
	assert.NotEmpty(t, result)
}

func TestPopulateComputeInfo(t *testing.T) {
	app := &v1beta2.SparkApplication{
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores: int32ptr(2),
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores: int32ptr(3),
				},
			},
		},
	}

	var sb strings.Builder
	sparkConfKeyValuePairs := make(map[string]string)

	result, err := populateComputeInfo(&sb, *app, sparkConfKeyValuePairs)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Contains(t, result.String(), "spark.driver.cores=2")
	assert.NotContains(t, result.String(), "spark.executor.cores=3") // This is handled in populateMemoryInfo
}

func TestPopulateMemoryInfo(t *testing.T) {
	tests := []struct {
		name string
		app  v1beta2.SparkApplication
		want []string
	}{
		{
			name: "with specified values",
			app: v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory:         stringptr("2g"),
							MemoryOverhead: stringptr("512m"),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory:         stringptr("4g"),
							MemoryOverhead: stringptr("1024m"),
							Cores:          int32ptr(3),
						},
					},
				},
			},
			want: []string{
				"spark.driver.memory=2g",
				"spark.driver.memoryOverhead=512m",
				"spark.executor.memory=4g",
				"spark.executor.memoryOverhead=1024m",
				"spark.executor.cores=3",
			},
		},
		{
			name: "with default values",
			app: v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Driver:   v1beta2.DriverSpec{},
					Executor: v1beta2.ExecutorSpec{},
				},
			},
			want: []string{
				"spark.driver.memory=1024m", // DriverDefaultMemory
				"spark.executor.memory=1g",  // ExecutorDefaultMemory
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sparkConfKeyValuePairs := make(map[string]string)
			result := populateMemoryInfo("", tt.app, sparkConfKeyValuePairs)
			for _, expected := range tt.want {
				assert.Contains(t, result, expected)
			}
		})
	}
}

func int64ptr(i int64) *int64 {
	return &i
}
