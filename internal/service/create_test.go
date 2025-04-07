package service

import (
	"nativesubmit/common"
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
		name                  string
		app                   *v1beta2.SparkApplication
		serviceSelectorLabels map[string]string
		createdApplicationId  string
		serviceName           string
		wantErr               bool
	}{
		{
			name: "valid application with default values",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					Mode: v1beta2.DeployModeCluster,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores: int32ptr(1),
						},
					},
				},
			},
			serviceSelectorLabels: map[string]string{
				"spark-role": "driver",
			},
			createdApplicationId: "test-app-id",
			serviceName:          "test-service",
			wantErr:              false,
		},
		{
			name: "valid application with custom ports",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					Mode: v1beta2.DeployModeCluster,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores: int32ptr(1),
						},
					},
					SparkConf: map[string]string{
						"spark.driver.port":              "7079",
						"spark.driver.blockManager.port": "7078",
					},
				},
			},
			serviceSelectorLabels: map[string]string{
				"spark-role": "driver",
			},
			createdApplicationId: "test-app-id",
			serviceName:          "test-service",
			wantErr:              false,
		},
		{
			name: "nil application",
			app:  nil,
			serviceSelectorLabels: map[string]string{
				"spark-role": "driver",
			},
			createdApplicationId: "test-app-id",
			serviceName:          "test-service",
			wantErr:              true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewClientBuilder().WithScheme(scheme).Build()
			err := Create(tt.app, tt.serviceSelectorLabels, client, tt.createdApplicationId, tt.serviceName)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetDriverPodBlockManagerPort(t *testing.T) {
	tests := []struct {
		name string
		app  *v1beta2.SparkApplication
		want int32
	}{
		{
			name: "default port",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{},
			},
			want: common.DefaultBlockManagerPort,
		},
		{
			name: "custom port",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					SparkConf: map[string]string{
						"spark.driver.blockManager.port": "7078",
					},
				},
			},
			want: 7078,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getDriverPodBlockManagerPort(tt.app)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetDriverNBlockManagerPort(t *testing.T) {
	tests := []struct {
		name        string
		app         *v1beta2.SparkApplication
		portConfig  string
		defaultPort int32
		want        int32
	}{
		{
			name: "default port",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{},
			},
			portConfig:  "spark.driver.port",
			defaultPort: 7077,
			want:        7077,
		},
		{
			name: "custom port",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					SparkConf: map[string]string{
						"spark.driver.port": "7079",
					},
				},
			},
			portConfig:  "spark.driver.port",
			defaultPort: 7077,
			want:        7079,
		},
		{
			name: "invalid port",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					SparkConf: map[string]string{
						"spark.driver.port": "invalid",
					},
				},
			},
			portConfig:  "spark.driver.port",
			defaultPort: 7077,
			want:        7077,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getDriverNBlockManagerPort(tt.app, tt.portConfig, tt.defaultPort)
			assert.Equal(t, tt.want, got)
		})
	}
}

func int32ptr(i int32) *int32 {
	return &i
}
