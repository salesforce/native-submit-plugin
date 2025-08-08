package common

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strconv"

	"github.com/google/uuid"
	"github.com/kubeflow/spark-operator/api/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/kubectl/pkg/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
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

	// Ensure UID is not empty
	uid := app.ObjectMeta.UID
	if uid == "" {
		// Generate a UID if not present
		uid = types.UID(uuid.New().String())
	}

	return &metav1.OwnerReference{
		APIVersion: v1beta2.SchemeGroupVersion.String(),
		Kind:       reflect.TypeOf(v1beta2.SparkApplication{}).Name(),
		Name:       app.Name,
		UID:        uid,
		Controller: &controller,
	}
}

func GetOwnerReferenceFromCluster(app *v1beta2.SparkApplication) *metav1.OwnerReference {
	// Register the SparkApplication scheme
	_ = v1beta2.AddToScheme(scheme.Scheme)
	dynamicClient := getKubeDynamicClientOrDie()

	sparkAppGVR := schema.GroupVersionResource{
		Group:    "sparkoperator.k8s.io",
		Version:  "v1beta2",
		Resource: "sparkapplications",
	}

	//var sparkApp unstructured.Unstructured
	sparkApp, err := dynamicClient.Resource(sparkAppGVR).Namespace(app.GetObjectMeta().GetNamespace()).Get(context.TODO(), app.GetObjectMeta().GetName(), metav1.GetOptions{})
	if err != nil {
		log.Fatalf("Error getting SparkApplication: %v", err)
	}

	// Convert unstructured to SparkApplication
	var sparkAppTyped v1beta2.SparkApplication
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(sparkApp.Object, &sparkAppTyped)
	if err != nil {
		log.Printf("Error converting to SparkApplication: %v", err)
		return nil
	}

	ownerRef := GetOwnerReference(&sparkAppTyped)
	fmt.Printf("Found SparkApplication in cluster - UID: %s, Name: %s, Kind: %s",
		ownerRef.UID, ownerRef.Name, ownerRef.Kind)
	return ownerRef
}

// Helper func to get Owner references specifically for Service resources
// This ensures proper garbage collection protection for services by pointing to the Driver Pod
func GetServiceOwnerReference(app *v1beta2.SparkApplication, driverPodUID string) *metav1.OwnerReference {
	controller := true
	blockOwnerDeletion := true

	// Get the driver pod name
	podName := GetDriverPodName(app)

	// Use the actual Pod UID for proper owner reference
	uid := types.UID(driverPodUID)
	if driverPodUID == "" {
		// Fallback to SparkApplication UID if Pod UID is not provided
		uid = app.ObjectMeta.UID
		if uid == "" {
			// Generate a UID if not present
			uid = types.UID(uuid.New().String())
		}
	}

	return &metav1.OwnerReference{
		APIVersion:         "v1", // Pods are v1 resources
		Kind:               "Pod",
		Name:               podName,
		UID:                uid,
		Controller:         &controller,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}
}

// Helper func to get Owner references specifically for ConfigMap resources
// This ensures proper garbage collection protection for ConfigMaps by pointing to the SparkApplication
func GetConfigMapOwnerReference(app *v1beta2.SparkApplication) *metav1.OwnerReference {
	controller := true
	blockOwnerDeletion := true

	// Ensure UID is not empty
	uid := app.ObjectMeta.UID
	if uid == "" {
		// Generate a UID if not present
		uid = types.UID(uuid.New().String())
	}

	return &metav1.OwnerReference{
		APIVersion:         v1beta2.SchemeGroupVersion.String(),
		Kind:               reflect.TypeOf(v1beta2.SparkApplication{}).Name(),
		Name:               app.Name,
		UID:                uid,
		Controller:         &controller,
		BlockOwnerDeletion: &blockOwnerDeletion,
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

func getKubeDynamicClientOrDie() *dynamic.DynamicClient {
	// Get the Kubernetes REST config (from kubeconfig or in-cluster)
	config, err := ctrl.GetConfig()
	if err != nil {
		panic("failed to get kube config: " + err.Error())
	}
	// Create the clientset
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error creating dynamicClient: %v", err)
	}
	return dynamicClient
}
