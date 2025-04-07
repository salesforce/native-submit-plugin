package service

import (
	"context"
	"fmt"
	"nativesubmit/common"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/kubeflow/spark-operator/api/v1beta2"
	apiv1 "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/util/retry"
	ctrlClient "sigs.k8s.io/controller-runtime/pkg/client"
)

// Helper func to create Service for the Driver Pod of the Spark Application
func Create(app *v1beta2.SparkApplication, serviceSelectorLabels map[string]string, kubeClient ctrlClient.Client, createdApplicationId string, serviceName string) error {
	if app == nil {
		return fmt.Errorf("spark application cannot be nil")
	}

	//Service Schema populating with specific values/data
	var serviceObjectMetaData metav1.ObjectMeta

	serviceObjectMetaData.ResourceVersion = ""

	//Driver Pod Service name
	serviceObjectMetaData.Name = serviceName
	// Spark Application Namespace
	serviceObjectMetaData.Namespace = common.GetAppNamespace(app)
	// Service Schema Owner References
	serviceObjectMetaData.OwnerReferences = []metav1.OwnerReference{*common.GetOwnerReference(app)}
	//Service Schema label
	serviceLabels := map[string]string{SparkApplicationSelectorLabel: createdApplicationId}

	ipFamilyString, ipFamilyExists := app.Spec.SparkConf["spark.kubernetes.driver.service.ipFamilies"]
	var ipFamily apiv1.IPFamily
	if ipFamilyExists {
		ipFamily = apiv1.IPFamily(ipFamilyString)
	} else {
		//Default Value
		ipFamily = apiv1.IPFamily("IPv4")
	}
	var ipFamilies [1]apiv1.IPFamily
	ipFamilies[0] = ipFamily

	// labels passed in sparkConf
	sparkConfKeyValuePairs := app.Spec.SparkConf
	for sparkConfKey, sparkConfValue := range sparkConfKeyValuePairs {
		if strings.Contains(sparkConfKey, "spark.kubernetes.driver.service.label.") {
			lastDotIndex := strings.LastIndex(sparkConfKey, DotSeparator)
			labelKey := sparkConfKey[lastDotIndex+1:]
			labelValue := sparkConfValue
			serviceLabels[labelKey] = labelValue
		}
	}

	serviceObjectMetaData.Labels = serviceLabels
	//Service Schema Annotation
	if app.Spec.Driver.Annotations != nil {
		serviceObjectMetaData.Annotations = app.Spec.Driver.Annotations
	}
	//Service Schema Creation
	driverPodService := &apiv1.Service{
		ObjectMeta: serviceObjectMetaData,
		Spec: apiv1.ServiceSpec{
			ClusterIP: None,
			Ports: []apiv1.ServicePort{
				{
					Name:     DriverPortName,
					Port:     getDriverNBlockManagerPort(app, DriverPortProperty, common.DefaultDriverPort),
					Protocol: Protocol,
					TargetPort: intstr.IntOrString{
						IntVal: common.DefaultDriverPort,
					},
				},
				{
					Name:     BlockManagerPortName,
					Port:     getDriverPodBlockManagerPort(app),
					Protocol: Protocol,
					TargetPort: intstr.IntOrString{
						IntVal: common.DefaultBlockManagerPort,
					},
				},
				{
					Name:     UiPortName,
					Port:     UiPort,
					Protocol: Protocol,
					TargetPort: intstr.IntOrString{
						IntVal: UiPort,
					},
				},
			},
			Selector:        serviceSelectorLabels,
			SessionAffinity: None,
			Type:            ClusterIP,
			IPFamilies:      ipFamilies[:],
		},
	}
	//K8S API Server Call to create Service
	createServiceErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existingService := &apiv1.Service{}
		err := kubeClient.Get(context.TODO(), ctrlClient.ObjectKeyFromObject(driverPodService), existingService)

		if apiErrors.IsNotFound(err) {
			createErr := kubeClient.Create(context.TODO(), driverPodService)

			if createErr == nil {
				return createAndCheckDriverService(kubeClient, app, driverPodService, 5, serviceName)
			}
		}
		if err != nil {
			return err
		}

		//Copying over the data to existing service
		existingService.ObjectMeta = serviceObjectMetaData
		existingService.Spec = driverPodService.Spec
		updateErr := kubeClient.Update(context.TODO(), existingService)

		if updateErr != nil {
			return fmt.Errorf("error while updating driver service: %w", updateErr)
		}

		return updateErr
	})
	return createServiceErr
}

func createAndCheckDriverService(kubeClient ctrlClient.Client, app *v1beta2.SparkApplication, driverPodService *apiv1.Service, attemptCount int, serviceName string) error {
	const sleepDuration = 2000 * time.Millisecond
	temp := &apiv1.Service{}

	for iteration := 0; iteration < attemptCount; iteration++ {
		err := kubeClient.Get(context.TODO(), ctrlClient.ObjectKey{
			Namespace: driverPodService.Namespace,
			Name:      driverPodService.Name,
		}, temp)

		if apiErrors.IsNotFound(err) {
			time.Sleep(sleepDuration)
			glog.Info("Service does not exist, attempt #", iteration+2, " to create service for the app %s", app.Name)
			driverPodService.ResourceVersion = ""

			if dvrSvcErr := kubeClient.Create(context.TODO(), driverPodService); err != dvrSvcErr {
				if !apiErrors.IsAlreadyExists(dvrSvcErr) {
					return fmt.Errorf("Unable to create driver service : %w", dvrSvcErr)
				} else {
					glog.Info("Driver service already exists, ignoring attempt to create it")
				}
			}
		} else {
			glog.Info("Driver Service found in attempt", iteration+2, "for the app %s", app.Name)
			return nil
		}
	}

	return nil
}

func getDriverPodBlockManagerPort(app *v1beta2.SparkApplication) int32 {
	if common.CheckSparkConf(app.Spec.SparkConf, DriverBlockManagerPortProperty) {
		return getDriverNBlockManagerPort(app, DriverBlockManagerPortProperty, common.DefaultBlockManagerPort)
	}
	return common.DefaultBlockManagerPort
}

func getDriverNBlockManagerPort(app *v1beta2.SparkApplication, portConfig string, defaultPort int32) int32 {
	if common.CheckSparkConf(app.Spec.SparkConf, portConfig) {
		value, _ := app.Spec.SparkConf[portConfig]
		portVal, parseError := strconv.ParseInt(value, 10, 64)
		if parseError != nil {
			glog.Errorf("failed to parse %s in namespace %s: %v", portConfig, app.Namespace, parseError)
			return defaultPort
		}
		return int32(portVal)
	}
	return defaultPort
}
