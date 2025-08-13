package main

import (
	"testing"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestNativeSubmit_LaunchSparkApplication(t *testing.T) {
	image := "gcr.io/spark-operator/spark-py:v3.1.1"
	mainClass := "org.apache.spark.examples.SparkPi"
	driverMemory := "1g"
	executorMemory := "1g"
	driverCores := int32(1)
	executorCores := int32(1)

	tests := []struct {
		name    string
		app     *v1beta2.SparkApplication
		wantErr bool
	}{
		{
			name: "valid spark application",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Type:      "Python",
					Mode:      "cluster",
					Image:     &image,
					MainClass: &mainClass,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: &driverMemory,
							Cores:  &driverCores,
							Labels: map[string]string{
								"version": "1.0",
							},
						},
					},
					Executor: v1beta2.ExecutorSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: &executorMemory,
							Cores:  &executorCores,
						},
					},
					SparkConf: map[string]string{
						"spark.kubernetes.container.image": image,
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "nil spark application",
			app:     nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := &NativeSubmit{}
			cl := fake.NewClientBuilder().Build()
			err := ns.LaunchSparkApplication(tt.app, cl)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNew(t *testing.T) {
	result := New()
	assert.NotNil(t, result)
	_, ok := result.(*NativeSubmit)
	assert.True(t, ok)
}
