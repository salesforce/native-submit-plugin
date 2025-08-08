package service

import (
	"context"
	"fmt"
	"log"
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
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

// Helper func to create Service for the Driver Pod of the Spark Application
func Create(app *v1beta2.SparkApplication, serviceSelectorLabels map[string]string, kubeClient *kubernetes.Clientset, createdApplicationId string, serviceName string, driverPodUID string) error {
	log.Printf("=== Starting Driver Service creation for app: %s, namespace: %s ===", app.Name, app.Namespace)
	log.Printf("Service name: %s, Application ID: %s, Driver Pod UID: %s", serviceName, createdApplicationId, driverPodUID)
	log.Printf("Service selector labels count: %d", len(serviceSelectorLabels))

	if app == nil {
		log.Printf("ERROR: Spark application is nil")
		return fmt.Errorf("spark application cannot be nil")
	}

	//Service Schema populating with specific values/data
	var serviceObjectMetaData metav1.ObjectMeta

	//Driver Pod Service name
	serviceObjectMetaData.Name = serviceName
	// Spark Application Namespace
	serviceObjectMetaData.Namespace = common.GetAppNamespace(app)
	// Service Schema Owner References - Use service-specific owner reference for proper garbage collection protection
	serviceObjectMetaData.OwnerReferences = []metav1.OwnerReference{*common.GetServiceOwnerReference(app, driverPodUID)}
	//Service Schema label
	serviceLabels := map[string]string{SparkApplicationSelectorLabel: createdApplicationId}
	log.Printf("Service object metadata - Name: %s, Namespace: %s", serviceObjectMetaData.Name, serviceObjectMetaData.Namespace)

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
	log.Printf("Service labels count after sparkConf processing: %d", len(serviceLabels))

	serviceObjectMetaData.Labels = serviceLabels
	annotations := map[string]string{
		"kubernetes.io/change-cause": "spark-driver-service-created-by-native-submit",
	}

	// Merge driver annotations if they exist
	if app.Spec.Driver.Annotations != nil {
		for key, value := range app.Spec.Driver.Annotations {
			annotations[key] = value
		}
		log.Printf("Using driver annotations for service")
	}

	serviceObjectMetaData.Annotations = annotations
	log.Printf("Service annotations set: %+v", annotations)

	// // Add finalizer to prevent garbage collection
	// serviceObjectMetaData.Finalizers = []string{"service.native-submit.io/finalizer"}
	// log.Printf("Service finalizers set: %+v", serviceObjectMetaData.Finalizers)

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
		},
	}
	log.Printf("Driver service object created with %d ports", len(driverPodService.Spec.Ports))
	log.Printf("Driver service object: %+v", driverPodService)

	//K8S API Server Call to create Service
	log.Printf("Attempting to create/update driver service...")
	createServiceErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existingService := &apiv1.Service{}
		_, err := kubeClient.CoreV1().Services(app.Namespace).Get(context.TODO(), driverPodService.Name, metav1.GetOptions{})
		if apiErrors.IsNotFound(err) {
			log.Printf("Service not found, creating new one...")
			_, createErr := kubeClient.CoreV1().Services(app.Namespace).Create(context.TODO(), driverPodService, metav1.CreateOptions{})
			if createErr == nil {
				log.Printf("Service created successfully, checking service availability...")
				return createAndCheckDriverService(kubeClient, app, driverPodService, 5, serviceName)
			}
			log.Printf("ERROR: Failed to create service: %v", createErr)
			return createErr
		}
		if err != nil {
			log.Printf("ERROR: Failed to get existing service: %v", err)
			return err
		}

		log.Printf("Service exists, updating...")
		//Copying over the data to existing service
		existingService.ObjectMeta = serviceObjectMetaData
		existingService.Spec = driverPodService.Spec
		_, updateErr := kubeClient.CoreV1().Services(app.Namespace).Update(context.TODO(), existingService, metav1.UpdateOptions{})

		if updateErr != nil {
			log.Printf("ERROR: Failed to update service: %v", updateErr)
			return fmt.Errorf("error while updating driver service: %w", updateErr)
		}
		log.Printf("Service updated successfully")
		return updateErr
	})

	if createServiceErr != nil {
		log.Printf("ERROR: Final service creation/update failed: %v", createServiceErr)
	} else {
		log.Printf("=== Successfully completed Driver Service creation ===")
	}

	return createServiceErr
}

func createAndCheckDriverService(kubeClient *kubernetes.Clientset, app *v1beta2.SparkApplication, driverPodService *apiv1.Service, attemptCount int, serviceName string) error {
	const sleepDuration = 2000 * time.Millisecond

	log.Printf("=== Starting createAndCheckDriverService for app: %s, service: %s, namespace: %s ===", app.Name, serviceName, app.Namespace)
	log.Printf("Service object details - Name: %s, Namespace: %s, ResourceVersion: %s", driverPodService.Name, driverPodService.Namespace, driverPodService.ResourceVersion)
	log.Printf("Service labels: %+v", driverPodService.Labels)
	log.Printf("Service selector: %+v", driverPodService.Spec.Selector)
	log.Printf("Service owner references: %+v", driverPodService.OwnerReferences)
	log.Printf("Attempt count: %d, Sleep duration: %v", attemptCount, sleepDuration)

	for iteration := 0; iteration < attemptCount; iteration++ {
		log.Printf("--- Iteration %d/%d ---", iteration+1, attemptCount)

		// Check if service exists
		log.Printf("Checking if service %s exists in namespace %s...", driverPodService.Name, driverPodService.Namespace)
		existingService, err := kubeClient.CoreV1().Services(driverPodService.Namespace).Get(context.TODO(), driverPodService.Name, metav1.GetOptions{})

		if apiErrors.IsNotFound(err) {
			log.Printf("Service %s NOT FOUND in namespace %s (iteration %d)", driverPodService.Name, driverPodService.Namespace, iteration+1)
			log.Printf("Sleeping for %v before attempting to create service...", sleepDuration)
			time.Sleep(sleepDuration)

			log.Printf("Attempting to create service %s in namespace %s (attempt #%d)", driverPodService.Name, driverPodService.Namespace, iteration+2)
			log.Printf("Clearing ResourceVersion before creation (was: %s)", driverPodService.ResourceVersion)
			driverPodService.ResourceVersion = ""

			createdService, dvrSvcErr := kubeClient.CoreV1().Services(app.Namespace).Create(context.TODO(), driverPodService, metav1.CreateOptions{})
			if dvrSvcErr != nil {
				if !apiErrors.IsAlreadyExists(dvrSvcErr) {
					log.Printf("ERROR: Failed to create service %s: %v", driverPodService.Name, dvrSvcErr)
					return fmt.Errorf("unable to create driver service : %w", dvrSvcErr)
				} else {
					log.Printf("INFO: Driver service %s already exists, ignoring attempt to create it", driverPodService.Name)
					// Try to get the existing service to verify it's accessible
					existingService, getErr := kubeClient.CoreV1().Services(app.Namespace).Get(context.TODO(), driverPodService.Name, metav1.GetOptions{})
					if getErr != nil {
						log.Printf("WARNING: Service exists but cannot be retrieved: %v", getErr)
					} else {
						log.Printf("SUCCESS: Retrieved existing service - Name: %s, UID: %s, ResourceVersion: %s",
							existingService.Name, existingService.UID, existingService.ResourceVersion)
					}
				}
			} else {
				log.Printf("SUCCESS: Service %s created successfully", driverPodService.Name)
				log.Printf("Created service details - Name: %s, UID: %s, ResourceVersion: %s, CreationTimestamp: %v",
					createdService.Name, createdService.UID, createdService.ResourceVersion, createdService.CreationTimestamp)
				log.Printf("Created service labels: %+v", createdService.Labels)
				log.Printf("Created service selector: %+v", createdService.Spec.Selector)
				log.Printf("Created service owner references: %+v", createdService.OwnerReferences)
			}
		} else if err != nil {
			log.Printf("ERROR: Failed to check if service %s exists: %v", driverPodService.Name, err)
			return fmt.Errorf("error checking service existence: %w", err)
		} else {
			log.Printf("SUCCESS: Driver Service %s found in attempt %d for app %s", driverPodService.Name, iteration+2, app.Name)
			log.Printf("Existing service details - Name: %s, UID: %s, ResourceVersion: %s, CreationTimestamp: %v",
				existingService.Name, existingService.UID, existingService.ResourceVersion, existingService.CreationTimestamp)
			log.Printf("Existing service labels: %+v", existingService.Labels)
			log.Printf("Existing service selector: %+v", existingService.Spec.Selector)
			log.Printf("Existing service owner references: %+v", existingService.OwnerReferences)

			// Check if service has proper owner references
			if len(existingService.OwnerReferences) == 0 {
				log.Printf("WARNING: Service %s has no owner references, which may cause garbage collection", existingService.Name)
			} else {
				log.Printf("Service %s has %d owner reference(s)", existingService.Name, len(existingService.OwnerReferences))
				for i, ownerRef := range existingService.OwnerReferences {
					log.Printf("  Owner reference %d: Kind=%s, Name=%s, UID=%s, Controller=%v",
						i+1, ownerRef.Kind, ownerRef.Name, ownerRef.UID, ownerRef.Controller)
				}
			}

			return nil
		}
	}

	log.Printf("WARNING: Service creation/verification failed after %d attempts for app %s", attemptCount, app.Name)
	log.Printf("=== Completed createAndCheckDriverService for app: %s ===", app.Name)
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
