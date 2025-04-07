package common

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Int32Pointer(a int32) *int32 {
	return &a
}
func LabelsForSpark() map[string]string {
	return map[string]string{"version": "3.0.1"}
}
func BoolPointer(a bool) *bool {
	return &a
}

func StringPointer(a string) *string {
	return &a
}

func GetAppNamespace(app *v1beta2.SparkApplication) string {
	namespace := "default"
	if app.Namespace != "" {
		namespace = app.Namespace
	} else {
		spakConfNamespace, namespaceExists := app.Spec.SparkConf[SparkAppNamespaceKey]
		if namespaceExists {
			namespace = spakConfNamespace
		}
	}
	return namespace
}

// Helper func to get driver pod name from Spark Application CRD instance
func GetDriverPodName(app *v1beta2.SparkApplication) string {
	name := app.Spec.Driver.PodName
	if name != nil && len(*name) > 0 {
		return *name
	}
	sparkConf := app.Spec.SparkConf
	if sparkConf[SparkDriverPodNameKey] != "" {
		return sparkConf[SparkDriverPodNameKey]
	}
	return fmt.Sprintf("%s-driver", app.Name)
}
func GetDriverPort(sparkConfKeyValuePairs map[string]string) int {
	//Checking if port information is passed in the spec, and using same
	// or using the default ones
	driverPortToBeUsed := DefaultDriverPort
	driverPort, valueExists := sparkConfKeyValuePairs[SparkDriverPort]
	if valueExists {
		driverPortSupplied, err := strconv.Atoi(driverPort)
		if err != nil {
			panic("Driver Port not parseable - hence failing the spark submit" + fmt.Sprint(err))
		} else {
			driverPortToBeUsed = driverPortSupplied
		}
	}
	return driverPortToBeUsed
}

// Helper func to get Owner references to be added to Spark Application resources - pod, service, configmap
func GetOwnerReference(app *v1beta2.SparkApplication) *metav1.OwnerReference {
	controller := false
	return &metav1.OwnerReference{
		APIVersion: v1beta2.SchemeGroupVersion.String(),
		Kind:       reflect.TypeOf(v1beta2.SparkApplication{}).Name(),
		Name:       app.Name,
		UID:        app.UID,
		Controller: &controller,
	}
}

func GetMemoryOverheadFactor(app *v1beta2.SparkApplication) string {
	var memoryOverheadFactor string
	if app.Spec.Type == v1beta2.SparkApplicationTypeJava || app.Spec.Type == v1beta2.SparkApplicationTypeScala {
		memoryOverheadFactor = JavaScalaMemoryOverheadFactor
	} else {

		memoryOverheadFactor = OtherLanguageMemoryOverheadFactor
	}
	return memoryOverheadFactor
}
func CheckSparkConf(sparkConf map[string]string, configKey string) bool {
	valueExists := false
	_, valueExists = sparkConf[configKey]
	return valueExists
}
func Int64Pointer(a int64) *int64 {
	return &a
}
