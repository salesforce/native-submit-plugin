package common

import (
	"testing"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestInt32Pointer(t *testing.T) {
	tests := []struct {
		name string
		val  int32
	}{
		{"positive value", 42},
		{"zero value", 0},
		{"negative value", -42},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Int32Pointer(tt.val)
			assert.NotNil(t, result)
			assert.Equal(t, tt.val, *result)
		})
	}
}

func TestBoolPointer(t *testing.T) {
	tests := []struct {
		name string
		val  bool
	}{
		{"true value", true},
		{"false value", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BoolPointer(tt.val)
			assert.NotNil(t, result)
			assert.Equal(t, tt.val, *result)
		})
	}
}

func TestStringPointer(t *testing.T) {
	tests := []struct {
		name string
		val  string
	}{
		{"non-empty string", "test"},
		{"empty string", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StringPointer(tt.val)
			assert.NotNil(t, result)
			assert.Equal(t, tt.val, *result)
		})
	}
}

func TestInt64Pointer(t *testing.T) {
	tests := []struct {
		name string
		val  int64
	}{
		{"positive value", 42},
		{"zero value", 0},
		{"negative value", -42},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Int64Pointer(tt.val)
			assert.NotNil(t, result)
			assert.Equal(t, tt.val, *result)
		})
	}
}

func TestLabelsForSpark(t *testing.T) {
	labels := LabelsForSpark()
	assert.NotNil(t, labels)
	assert.Equal(t, "3.0.1", labels["version"])
}

func TestGetAppNamespace(t *testing.T) {
	tests := []struct {
		name     string
		app      *v1beta2.SparkApplication
		expected string
	}{
		{
			name: "namespace from app",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test-ns",
				},
			},
			expected: "test-ns",
		},
		{
			name: "namespace from spark conf",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					SparkConf: map[string]string{
						SparkAppNamespaceKey: "spark-conf-ns",
					},
				},
			},
			expected: "spark-conf-ns",
		},
		{
			name:     "default namespace",
			app:      &v1beta2.SparkApplication{},
			expected: "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetAppNamespace(tt.app)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetDriverPodName(t *testing.T) {
	tests := []struct {
		name     string
		app      *v1beta2.SparkApplication
		expected string
	}{
		{
			name: "pod name from spec",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-app",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Driver: v1beta2.DriverSpec{
						PodName: StringPointer("custom-driver"),
					},
				},
			},
			expected: "custom-driver",
		},
		{
			name: "pod name from spark conf",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-app",
				},
				Spec: v1beta2.SparkApplicationSpec{
					SparkConf: map[string]string{
						SparkDriverPodNameKey: "spark-conf-driver",
					},
				},
			},
			expected: "spark-conf-driver",
		},
		{
			name: "default pod name",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-app",
				},
			},
			expected: "test-app-driver",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetDriverPodName(tt.app)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetDriverPort(t *testing.T) {
	tests := []struct {
		name     string
		conf     map[string]string
		expected int
	}{
		{
			name:     "default port",
			conf:     map[string]string{},
			expected: DefaultDriverPort,
		},
		{
			name: "custom port",
			conf: map[string]string{
				SparkDriverPort: "8080",
			},
			expected: 8080,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetDriverPort(tt.conf)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetOwnerReference(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-app",
			UID:  "test-uid",
		},
	}

	ref := GetOwnerReference(app)
	assert.NotNil(t, ref)
	assert.Equal(t, v1beta2.SchemeGroupVersion.String(), ref.APIVersion)
	assert.Equal(t, "SparkApplication", ref.Kind)
	assert.Equal(t, "test-app", ref.Name)
	assert.Equal(t, "test-uid", string(ref.UID))
	assert.False(t, *ref.Controller)
}

func TestGetMemoryOverheadFactor(t *testing.T) {
	tests := []struct {
		name     string
		app      *v1beta2.SparkApplication
		expected string
	}{
		{
			name: "java application",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeJava,
				},
			},
			expected: JavaScalaMemoryOverheadFactor,
		},
		{
			name: "scala application",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
				},
			},
			expected: JavaScalaMemoryOverheadFactor,
		},
		{
			name: "other language",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypePython,
				},
			},
			expected: OtherLanguageMemoryOverheadFactor,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetMemoryOverheadFactor(tt.app)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCheckSparkConf(t *testing.T) {
	tests := []struct {
		name     string
		conf     map[string]string
		key      string
		expected bool
	}{
		{
			name: "key exists",
			conf: map[string]string{
				"test.key": "value",
			},
			key:      "test.key",
			expected: true,
		},
		{
			name: "key does not exist",
			conf: map[string]string{
				"other.key": "value",
			},
			key:      "test.key",
			expected: false,
		},
		{
			name:     "empty conf",
			conf:     map[string]string{},
			key:      "test.key",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSparkConf(tt.conf, tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}
