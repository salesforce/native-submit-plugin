package configmap

import (
	"context"
	"fmt"
	"log"
	"nativesubmit/common"
	"os"
	"strings"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	apiv1 "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

const (
	kubernetesServiceHostEnvVar = "KUBERNETES_SERVICE_HOST"
	kubernetesServicePortEnvVar = "KUBERNETES_SERVICE_PORT"
)

// CreateConfigMapUtil Helper func to create Spark Application configmap
func createConfigMapUtil(configMapName string, app *v1beta2.SparkApplication, configMapData map[string]string, kubeClient *kubernetes.Clientset) error {
	log.Printf("=== Starting createConfigMapUtil ===")
	log.Printf("ConfigMap name: %s, Namespace: %s", configMapName, app.Namespace)

	// Test Kubernetes client connectivity
	_, err := kubeClient.CoreV1().Namespaces().Get(context.TODO(), app.Namespace, metav1.GetOptions{})
	if err != nil {
		log.Printf("ERROR: Cannot access namespace %s: %v", app.Namespace, err)
		return fmt.Errorf("cannot access namespace %s: %w", app.Namespace, err)
	}
	log.Printf("Kubernetes client connectivity verified - can access namespace: %s", app.Namespace)

	// Test ConfigMap permissions by listing ConfigMaps
	configMaps, listErr := kubeClient.CoreV1().ConfigMaps(app.Namespace).List(context.TODO(), metav1.ListOptions{})
	if listErr != nil {
		log.Printf("WARNING: Cannot list ConfigMaps in namespace %s: %v", app.Namespace, listErr)
	} else {
		log.Printf("ConfigMap permissions verified - found %d ConfigMaps in namespace %s", len(configMaps.Items), app.Namespace)
	}

	// Check if ConfigMap data is too large (Kubernetes limit is 1MB)
	totalSize := 0
	for key, value := range configMapData {
		totalSize += len(key) + len(value)
	}
	log.Printf("ConfigMap data size: %d bytes", totalSize)

	// If total size exceeds 900KB (leaving some buffer), split the data
	if totalSize > 900*1024 {
		log.Printf("WARNING: ConfigMap data size exceeds 900KB, attempting to truncate largest entry")
		// Split the largest data entry if possible
		largestKey := ""
		largestValue := ""
		for key, value := range configMapData {
			if len(value) > len(largestValue) {
				largestKey = key
				largestValue = value
			}
		}

		// If the largest value is too big, truncate it
		if len(largestValue) > 500*1024 {
			log.Printf("Truncating largest entry: %s (size: %d bytes)", largestKey, len(largestValue))
			configMapData[largestKey] = largestValue[:500*1024] + "\n# ... (truncated due to size limit)"
		}
	}

	ownerRef := common.GetConfigMapOwnerReference(app)

	// Log ConfigMap creation details for debugging
	if ownerRef != nil {
		log.Printf("Creating ConfigMap %s in namespace %s with owner UID: %s, total data size: %d bytes",
			configMapName, app.Namespace, ownerRef.UID, totalSize)
	} else {
		log.Printf("Creating ConfigMap %s in namespace %s without owner reference, total data size: %d bytes",
			configMapName, app.Namespace, totalSize)
	}

	configMap := &apiv1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            configMapName,
			Namespace:       app.Namespace,
			OwnerReferences: []metav1.OwnerReference{*ownerRef},
		},
		Data: configMapData,
	}

	createConfigMapErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		log.Printf("Attempting to create/update ConfigMap...")
		cm, err := kubeClient.CoreV1().ConfigMaps(app.Namespace).Get(context.TODO(), configMapName, metav1.GetOptions{})
		if apiErrors.IsNotFound(err) {
			log.Printf("ConfigMap not found, creating new one...")
			createdCM, createErr := kubeClient.CoreV1().ConfigMaps(app.Namespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
			if createErr != nil {
				log.Printf("ERROR: Failed to create ConfigMap: %v", createErr)
				return createErr
			}
			log.Printf("Successfully created ConfigMap: %s with UID: %s", configMapName, createdCM.UID)

			// Verify the ConfigMap was actually created
			verifyCM, verifyErr := kubeClient.CoreV1().ConfigMaps(app.Namespace).Get(context.TODO(), configMapName, metav1.GetOptions{})
			if verifyErr != nil {
				log.Printf("WARNING: ConfigMap creation verification failed: %v", verifyErr)
			} else {
				log.Printf("ConfigMap verification successful - Name: %s, UID: %s, Data entries: %d",
					verifyCM.Name, verifyCM.UID, len(verifyCM.Data))
			}
			return nil
		}
		if err != nil {
			log.Printf("ERROR: Failed to get existing ConfigMap: %v", err)
			return err
		}
		log.Printf("ConfigMap exists, updating...")
		cm.Data = configMapData
		_, updateErr := kubeClient.CoreV1().ConfigMaps(app.Namespace).Update(context.TODO(), cm, metav1.UpdateOptions{})
		if updateErr != nil {
			log.Printf("ERROR: Failed to update ConfigMap: %v", updateErr)
			return updateErr
		}
		log.Printf("Successfully updated ConfigMap: %s", configMapName)
		return updateErr
	})

	if createConfigMapErr != nil {
		log.Printf("ERROR: Final ConfigMap creation/update failed: %v", createConfigMapErr)
	} else {
		log.Printf("=== Successfully completed createConfigMapUtil ===")
	}

	return createConfigMapErr
}
func AddEscapeCharacter(configMapArg string) string {
	configMapArg = strings.ReplaceAll(configMapArg, ":", "\\:")
	configMapArg = strings.ReplaceAll(configMapArg, "=", "\\=")
	return configMapArg
}
func getMasterURL() (string, error) {
	kubernetesServiceHost := os.Getenv(kubernetesServiceHostEnvVar)
	if kubernetesServiceHost == "" {
		kubernetesServiceHost = "localhost"
	}
	kubernetesServicePort := os.Getenv(kubernetesServicePortEnvVar)
	if kubernetesServicePort == "" {
		kubernetesServicePort = "443"
	}

	return fmt.Sprintf("k8s://https://%s:%s", kubernetesServiceHost, kubernetesServicePort), nil
}

// addLocalDirConfOptions excludes local dir volumes, update SparkApplication and returns local dir config options
func addLocalDirConfOptions(app *v1beta2.SparkApplication) ([]string, error) {
	var localDirConfOptions []string

	sparkLocalVolumes := map[string]apiv1.Volume{}
	var mutateVolumes []apiv1.Volume

	// Filter local dir volumes
	for _, volume := range app.Spec.Volumes {
		if strings.HasPrefix(volume.Name, SparkLocalDirVolumePrefix) {
			sparkLocalVolumes[volume.Name] = volume
		} else {
			mutateVolumes = append(mutateVolumes, volume)
		}
	}
	app.Spec.Volumes = mutateVolumes

	// Filter local dir volumeMounts and set mutate volume mounts to driver and executor
	if app.Spec.Driver.VolumeMounts != nil {
		driverMutateVolumeMounts, driverLocalDirConfConfOptions := filterMutateMountVolumes(app.Spec.Driver.VolumeMounts, SparkDriverVolumesPrefix, sparkLocalVolumes)
		app.Spec.Driver.VolumeMounts = driverMutateVolumeMounts
		localDirConfOptions = append(localDirConfOptions, driverLocalDirConfConfOptions...)
	}

	if app.Spec.Executor.VolumeMounts != nil {
		executorMutateVolumeMounts, executorLocalDirConfConfOptions := filterMutateMountVolumes(app.Spec.Executor.VolumeMounts, SparkExecutorVolumesPrefix, sparkLocalVolumes)
		app.Spec.Executor.VolumeMounts = executorMutateVolumeMounts
		localDirConfOptions = append(localDirConfOptions, executorLocalDirConfConfOptions...)
	}

	return localDirConfOptions, nil
}

func filterMutateMountVolumes(volumeMounts []apiv1.VolumeMount, prefix string, sparkLocalVolumes map[string]apiv1.Volume) ([]apiv1.VolumeMount, []string) {
	var mutateMountVolumes []apiv1.VolumeMount
	var localDirConfOptions []string
	for _, volumeMount := range volumeMounts {
		if volume, ok := sparkLocalVolumes[volumeMount.Name]; ok {
			options := buildLocalVolumeOptions(prefix, volume, volumeMount)
			localDirConfOptions = append(localDirConfOptions, options...)
		} else {
			mutateMountVolumes = append(mutateMountVolumes, volumeMount)
		}
	}

	return mutateMountVolumes, localDirConfOptions
}
func buildLocalVolumeOptions(prefix string, volume apiv1.Volume, volumeMount apiv1.VolumeMount) []string {
	VolumeMountPathTemplate := prefix + "%s.%s.mount.path=%s"
	VolumeMountOptionTemplate := prefix + "%s.%s.options.%s=%s"

	var options []string
	switch {
	case volume.HostPath != nil:
		options = append(options, fmt.Sprintf(VolumeMountPathTemplate, "hostPath", volume.Name, volumeMount.MountPath))
		options = append(options, fmt.Sprintf(VolumeMountOptionTemplate, "hostPath", volume.Name, "path", volume.HostPath.Path))
		if volume.HostPath.Type != nil {
			options = append(options, fmt.Sprintf(VolumeMountOptionTemplate, "hostPath", volume.Name, "type", *volume.HostPath.Type))
		}
	case volume.EmptyDir != nil:
		options = append(options, fmt.Sprintf(VolumeMountPathTemplate, "emptyDir", volume.Name, volumeMount.MountPath))
	case volume.PersistentVolumeClaim != nil:
		options = append(options, fmt.Sprintf(VolumeMountPathTemplate, "persistentVolumeClaim", volume.Name, volumeMount.MountPath))
		options = append(options, fmt.Sprintf(VolumeMountOptionTemplate, "persistentVolumeClaim", volume.Name, "claimName", volume.PersistentVolumeClaim.ClaimName))
	}

	return options
}
