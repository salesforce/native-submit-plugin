package driver

import (
	"context"
	"nativesubmit/common"
	"testing"

	v1beta2 "github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCreate(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1beta2.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name                string
		app                 *v1beta2.SparkApplication
		serviceLabels       map[string]string
		driverConfigMapName string
		appSpecVolumes      []corev1.Volume
		appSpecVolumeMounts []corev1.VolumeMount
		wantErr             bool
	}{
		{
			name: "basic pod creation with default values",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark-app",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					Mode: v1beta2.DeployModeCluster,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  int32ptr(1),
							Memory: stringptr("1g"),
						},
					},
				},
			},
			serviceLabels: map[string]string{
				"spark-role": "driver",
			},
			driverConfigMapName: "test-config-map",
			wantErr:             false,
		},
		{
			name: "pod creation with custom security context",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark-app",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					Mode: v1beta2.DeployModeCluster,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores: int32ptr(1),
							SecurityContext: &corev1.SecurityContext{
								RunAsUser:  int64ptr(1000),
								RunAsGroup: int64ptr(1000),
							},
						},
					},
				},
			},
			serviceLabels: map[string]string{
				"spark-role": "driver",
			},
			driverConfigMapName: "test-config-map",
			wantErr:             false,
		},
		{
			name: "pod creation with custom volumes",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark-app",
					Namespace: "default",
				},
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
			serviceLabels: map[string]string{
				"spark-role": "driver",
			},
			driverConfigMapName: "test-config-map",
			appSpecVolumes: []corev1.Volume{
				{
					Name: "test-volume",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
			appSpecVolumeMounts: []corev1.VolumeMount{
				{
					Name:      "test-volume",
					MountPath: "/test",
				},
			},
			wantErr: false,
		},
		{
			name: "nil application should error",
			app:  nil,
			serviceLabels: map[string]string{
				"spark-role": "driver",
			},
			driverConfigMapName: "test-config-map",
			wantErr:             true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewClientBuilder().WithScheme(scheme).Build()
			err := Create(tt.app, tt.serviceLabels, tt.driverConfigMapName, client, tt.appSpecVolumeMounts, tt.appSpecVolumes)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				// Verify the pod was created
				if tt.app != nil {
					podName := common.GetDriverPodName(tt.app)
					pod := &corev1.Pod{}
					err := client.Get(context.TODO(), types.NamespacedName{Name: podName, Namespace: tt.app.Namespace}, pod)
					assert.NoError(t, err)
					assert.NotNil(t, pod)

					// Verify basic pod properties
					assert.Equal(t, podName, pod.Name)
					assert.Equal(t, tt.app.Namespace, pod.Namespace)
					assert.Equal(t, tt.serviceLabels, pod.Labels)

					// Verify pod spec
					assert.NotNil(t, pod.Spec)
					assert.Equal(t, corev1.RestartPolicyNever, pod.Spec.RestartPolicy)
					assert.NotNil(t, pod.Spec.SecurityContext)

					// Verify container spec
					assert.Len(t, pod.Spec.Containers, 1)
					container := pod.Spec.Containers[0]
					assert.NotNil(t, container)

					// Verify volumes
					assert.NotEmpty(t, pod.Spec.Volumes)

					// Verify volume mounts
					assert.NotEmpty(t, container.VolumeMounts)
				}
			}
		})
	}
}

func int32ptr(i int32) *int32 {
	return &i
}

func int64ptr(i int64) *int64 {
	return &i
}

func stringptr(s string) *string {
	return &s
}
