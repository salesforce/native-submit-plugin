package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"nativesubmit/common"
	"nativesubmit/internal/configmap"
	"nativesubmit/internal/driver"
	"nativesubmit/internal/service"
	"strconv"
	"strings"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"k8s.io/client-go/kubernetes"
)

const (
	True                               = "true"
	Spark                              = "spark"
	SparkWithDash                      = "spark-"
	SparkAppDriverServiceNameExtension = "-driver-svc"
	KubernetesDNSLabelNameMaxLength    = 63
	ServiceNameExtension               = "-svc"
	DotSeparator                       = "."
	Version                            = "version"
	SparkAppName                       = "spark-app-name"
	ConfigMapExtension                 = "-conf-map"
	SparkApplicationSelectorLabel      = "spark-app-selector"
	// LabelAnnotationPrefix is the prefix of every labels and annotations added by the controller.
	LabelAnnotationPrefix = "sparkoperator.k8s.io/"
	// SparkAppNameLabel is the name of the label for the SparkApplication object name.
	SparkAppNameLabel = LabelAnnotationPrefix + "app-name"
	SparkRoleLabel    = "spark-role"
	// SparkDriverRole is the value of the spark-role label for the driver.
	SparkDriverRole                = "driver"
	SparkAppSubmissionIDAnnotation = "sparkoperator.k8s.io/submission-id"
	SparkAppLauncherSOAnnotation   = "sparkoperator.k8s.io/launched-by-spark-operator"
)

// |      +-+--+----+    |    +-----v--+-+
// |      |         |    |    |          |
// |      |         |    |    |          |
// |      |   New   +---------> Submitted|
// |      |         |    |    |          |
// |      |         |    |    |          |
// |      |         |    |    |          |
// |      +---------+    |    +----^-----|
// Logic involved in moving "New" Spark Application to "Submitted" state is implemented in Golang with this function RunAltSparkSubmit as starting step
// 3 Resources are created in this logic per new Spark Application, in the order listed: ConfigMap for the Spark Application, Driver Pod, Driver Service

func runAltSparkSubmit(app *v1beta2.SparkApplication, submissionID string) (bool, error) {
	log.Printf("=== Starting Spark Application submission process ===")

	if app == nil {
		log.Printf("ERROR: Spark application is nil")
		return false, fmt.Errorf("spark application cannot be nil")
	}

	log.Printf("App name: %s, Namespace: %s, Submission ID: %s", app.Name, app.Namespace, submissionID)

	kubeClient := getKubeClientOrDie()

	appSpecVolumeMounts := app.Spec.Driver.VolumeMounts
	appSpecVolumes := app.Spec.Volumes
	log.Printf("App spec volume mounts: %d, volumes: %d", len(appSpecVolumeMounts), len(appSpecVolumes))

	// // Create Application ID with the convention followed in Scala/Java
	// uuidString := strings.ReplaceAll(uuid.New().String(), "-", "")
	// createdApplicationId := fmt.Sprintf("%s-%s", Spark, uuidString)
	// log.Printf("Generated Application ID: %s", createdApplicationId)

	// //Update Application CRD Instance with Spark Application ID
	// app.Status.SparkApplicationID = createdApplicationId

	//Create Spark Application ConfigMap Name with the convention followed in Scala/Java
	driverConfigMapName := fmt.Sprintf("%s%s", common.GetDriverPodName(app), ConfigMapExtension)
	log.Printf("Driver ConfigMap name: %s", driverConfigMapName)

	serviceName := getServiceName(app)
	log.Printf("Service name: %s", serviceName)

	// //Update Application CRD Instance with Submission ID
	app.Status.SubmissionID = submissionID

	//Create Service Labels by aggregating Spark Application Specification level, driver specification level and dynamic lables
	serviceLabels := map[string]string{
		SparkAppNameLabel:              app.Name,
		SparkAppName:                   app.Name,
		SparkApplicationSelectorLabel:  string(app.ObjectMeta.GetUID()),
		SparkRoleLabel:                 SparkDriverRole,
		SparkAppSubmissionIDAnnotation: submissionID,
		SparkAppLauncherSOAnnotation:   True,
	}
	log.Printf("Created base service labels: %v", getMapKeys(serviceLabels))

	if app.Spec.Driver.Labels != nil {
		if version, exists := app.Spec.Driver.Labels[Version]; exists {
			serviceLabels[Version] = version
		}
		for key, val := range app.Spec.Driver.Labels {
			serviceLabels[key] = val
		}
		log.Printf("Added driver labels: %d entries", len(app.Spec.Driver.Labels))
	}

	if app.Labels != nil {
		for key, val := range app.Labels {
			serviceLabels[key] = val
		}
		log.Printf("Added app labels: %d entries", len(app.Labels))
	}

	// labels passed in sparkConf
	sparkConfKeyValuePairs := app.Spec.SparkConf
	for sparkConfKey, sparkConfValue := range sparkConfKeyValuePairs {
		if strings.Contains(sparkConfKey, "spark.kubernetes.driver.label.") {
			lastDotIndex := strings.LastIndex(sparkConfKey, DotSeparator)
			labelKey := sparkConfKey[lastDotIndex+1:]
			serviceLabels[labelKey] = sparkConfValue
		}
	}
	log.Printf("Final service labels count: %d", len(serviceLabels))

	//Spark Application ConfigMap Creation
	log.Printf("=== Step 1: Creating ConfigMap ===")
	createErr := configmap.Create(app, submissionID, string(app.ObjectMeta.GetUID()), kubeClient, driverConfigMapName, serviceName)
	if createErr != nil {
		log.Printf("ERROR: ConfigMap creation failed: %v", createErr)
		return false, fmt.Errorf("error while creating configmap %s in namespace %s: %w", driverConfigMapName, app.Namespace, createErr)
	}
	log.Printf("ConfigMap creation completed successfully")

	//Spark Application Driver Pod Creation
	log.Printf("=== Step 2: Creating Driver Pod ===")
	driverPodUID, createPodErr := driver.Create(app, serviceLabels, driverConfigMapName, kubeClient, appSpecVolumeMounts, appSpecVolumes)
	if createPodErr != nil {
		log.Printf("ERROR: Driver pod creation failed: %v", createPodErr)
		return false, fmt.Errorf("error while creating driver pod %s in namespace %s: %w", common.GetDriverPodName(app), app.Namespace, createPodErr)
	}
	log.Printf("Driver pod creation completed successfully, Pod UID: %s", driverPodUID)

	//Spark Application Driver Pod's Service Creation
	log.Printf("=== Step 3: Creating Driver Service ===")
	createServiceErr := service.Create(app, serviceLabels, kubeClient, string(app.ObjectMeta.GetUID()), serviceName, driverPodUID)
	if createServiceErr != nil {
		log.Printf("ERROR: Driver service creation failed: %v", createServiceErr)
		return false, fmt.Errorf("error while creating driver service %s in namespace %s: %w", serviceName, app.Namespace, createServiceErr)
	}
	log.Printf("Driver service creation completed successfully")

	log.Printf("=== Spark Application submission process completed successfully ===")
	return true, nil
}

// Helper function to get map keys for logging
func getMapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// getServiceName Helper function to get Spark Application Driver Pod's Service Name
func getServiceName(app *v1beta2.SparkApplication) string {
	var sb strings.Builder
	sb.WriteString(common.GetDriverPodName(app))
	sb.WriteString(ServiceNameExtension)
	driverPodServiceName := sb.String()

	if len(driverPodServiceName) > KubernetesDNSLabelNameMaxLength {
		sb.Reset()
		sb.WriteString(SparkWithDash)
		sb.WriteString(strconv.Itoa(int(time.Now().Unix())))
		randomHexString, _ := randomHex(10)
		sb.WriteString(randomHexString)
		sb.WriteString(SparkAppDriverServiceNameExtension)
		driverPodServiceName = sb.String()
	}
	return driverPodServiceName
}

// Helper func to create random string of given length to create unique service name for the Spark Application Driver Pod
func randomHex(n int) (string, error) {
	bytes := make([]byte, n)
	return hex.EncodeToString(bytes), nil
}

func getKubeClientOrDie() *kubernetes.Clientset {
	// Get the Kubernetes REST config (from kubeconfig or in-cluster)
	cfg, err := ctrl.GetConfig()
	if err != nil {
		panic("failed to get kube config: " + err.Error())
	}

	// Create the Kubernetes clientset
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		panic("failed to create clientset: " + err.Error())
	}

	return clientset
}
