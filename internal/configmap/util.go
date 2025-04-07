package configmap

import (
	"context"
	"fmt"
	"nativesubmit/common"
	"os"
	"strings"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	apiv1 "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrlClient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	kubernetesServiceHostEnvVar = "KUBERNETES_SERVICE_HOST"
	kubernetesServicePortEnvVar = "KUBERNETES_SERVICE_PORT"
)

// CreateConfigMapUtil Helper func to create Spark Application configmap
func createConfigMapUtil(configMapName string, app *v1beta2.SparkApplication, configMapData map[string]string, kubeClient ctrlClient.Client) error {
	configMap := &apiv1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            configMapName,
			Namespace:       app.Namespace,
			OwnerReferences: []metav1.OwnerReference{*common.GetOwnerReference(app)},
		},
		Data: configMapData,
	}

	createConfigMapErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existingConfigMap := &apiv1.ConfigMap{}
		err := kubeClient.Get(context.TODO(), ctrlClient.ObjectKeyFromObject(configMap), existingConfigMap)
		//cm, err := kubeClient.CoreV1().ConfigMaps(app.Namespace).Get(context.TODO(), configMapName, metav1.GetOptions{})
		if apiErrors.IsNotFound(err) {
			createErr := kubeClient.Create(context.TODO(), configMap)
			//_, createErr := kubeClient.CoreV1().ConfigMaps(app.Namespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
			return createErr
		}
		if err != nil {
			return err
		}
		existingConfigMap.Data = configMapData
		updateErr := kubeClient.Update(context.TODO(), existingConfigMap)
		//_, updateErr := kubeClient.CoreV1().ConfigMaps(app.Namespace).Update(context.TODO(), cm, metav1.UpdateOptions{})
		return updateErr
	})
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
