package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"nativesubmit/common"
	"nativesubmit/internal/configmap"
	"nativesubmit/internal/driver"
	"nativesubmit/internal/service"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/kubeflow/spark-operator/api/v1beta2"
	ctrlClient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
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

func RunAltSparkSubmit(ctx context.Context, app *v1beta2.SparkApplication) (bool, error) {

	// Check if the context is nil
	// Check if the Spark Application is nil
	if app == nil {
		return false, fmt.Errorf("spark application cannot be nil")
	}

	appSpecVolumeMounts := app.Spec.Driver.VolumeMounts
	appSpecVolumes := app.Spec.Volumes

	// Create Application ID with the convention followed in Scala/Java
	uuidString := strings.ReplaceAll(uuid.New().String(), "-", "")
	createdApplicationId := fmt.Sprintf("%s-%s", Spark, uuidString)

	//Update Application CRD Instance with Spark Application ID
	app.Status.SparkApplicationID = createdApplicationId

	//Create Spark Application ConfigMap Name with the convention followed in Scala/Java
	driverConfigMapName := fmt.Sprintf("%s%s", common.GetDriverPodName(app), ConfigMapExtension)
	serviceName := getServiceName(app)

	// Initialize service labels map
	serviceLabels := make(map[string]string)

	// Merge driver labels
	if app.Spec.Driver.Labels != nil {
		if version, exists := app.Spec.Driver.Labels[Version]; exists {
			serviceLabels[Version] = version
		}
		for key, val := range app.Spec.Driver.Labels {
			serviceLabels[key] = val
		}
	}

	// Merge application labels
	if app.Labels != nil {
		for key, val := range app.Labels {
			serviceLabels[key] = val
		}
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

	// Initialize Kubernetes client
	cfg, err := config.GetConfig()
	if err != nil {
		return false, fmt.Errorf("failed to get Kubernetes config: %w", err)
	}

	kubeClient, err := ctrlClient.New(cfg, ctrlClient.Options{})
	if err != nil {
		return false, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	// Create resources
	if err := configmap.Create(ctx, app, app.Status.SubmissionID, createdApplicationId, kubeClient, driverConfigMapName, serviceName); err != nil {
		return false, fmt.Errorf("error while creating configmap %s in namespace %s: %w", driverConfigMapName, app.Namespace, err)
	}

	if err := driver.Create(ctx, app, serviceLabels, driverConfigMapName, kubeClient, appSpecVolumeMounts, appSpecVolumes); err != nil {
		return false, fmt.Errorf("error while creating driver pod %s in namespace %s: %w", common.GetDriverPodName(app), app.Namespace, err)
	}

	if err := service.Create(ctx, app, serviceLabels, kubeClient, createdApplicationId, serviceName); err != nil {
		return false, fmt.Errorf("error while creating driver service %s in namespace %s: %w", serviceName, app.Namespace, err)
	}

	return true, nil
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
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

// main function for compilation purposes
func main() {
	fmt.Println("Native Submit Plugin for Spark Applications")
	fmt.Println("This plugin provides native Kubernetes submission capabilities for Spark applications")
}

// Plugin interface for external consumption
type SparkSubmitPlugin interface {
	SubmitSparkApplication(ctx context.Context, app *v1beta2.SparkApplication) (bool, error)
	GetPluginInfo() PluginInfo
}

type PluginInfo struct {
	Name        string
	Version     string
	Description string
}

// Plugin implementation
type nativeSubmitPlugin struct{}

// GetPluginInfo returns information about this plugin
func (p *nativeSubmitPlugin) GetPluginInfo() PluginInfo {
	return PluginInfo{
		Name:        "native-submit-plugin",
		Version:     "1.0.0",
		Description: "Native Kubernetes submission plugin for Spark applications",
	}
}

// SubmitSparkApplication is the main plugin entry point
func (p *nativeSubmitPlugin) SubmitSparkApplication(ctx context.Context, app *v1beta2.SparkApplication) (bool, error) {
	return RunAltSparkSubmit(ctx, app)
}

// Exported variable that the plugin loader can access
var SparkSubmitPluginInstance SparkSubmitPlugin = &nativeSubmitPlugin{}

// New creates a new instance of the plugin (exported function for plugin loading)
func New() SparkSubmitPlugin {
	return &nativeSubmitPlugin{}
}

// Submit is an exported wrapper function for compatibility
func Submit(ctx context.Context, app *v1beta2.SparkApplication) (bool, error) {
	return RunAltSparkSubmit(ctx, app)
}

// GetInfo returns plugin information (exported function)
func GetInfo() PluginInfo {
	return PluginInfo{
		Name:        "native-submit-plugin",
		Version:     "1.0.0",
		Description: "Native Kubernetes submission plugin for Spark applications",
	}
}
