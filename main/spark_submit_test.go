package main

import (
	"testing"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRunAltSparkSubmit(t *testing.T) {
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
			cl := fake.NewClientBuilder().Build()
			success, err := runAltSparkSubmit(tt.app, tt.submissionID, cl)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.wantSuccess, success)
		})
	}
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
