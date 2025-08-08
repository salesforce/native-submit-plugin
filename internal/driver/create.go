package driver

import (
	"context"
	"fmt"
	"log"
	"math"
	"nativesubmit/common"
	"os"
	"strconv"
	"strings"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	apiv1 "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

// Helper func to create Driver Pod of the Spark Application
func Create(app *v1beta2.SparkApplication, serviceLabels map[string]string, driverConfigMapName string, kubeClient *kubernetes.Clientset, appSpecVolumeMounts []apiv1.VolumeMount, appSpecVolumes []apiv1.Volume) (string, error) {
	log.Printf("=== Starting Driver Pod creation for app: %s, namespace: %s ===", app.Name, app.Namespace)
	log.Printf("Driver ConfigMap name: %s", driverConfigMapName)
	log.Printf("Service labels count: %d", len(serviceLabels))

	if app == nil {
		log.Printf("ERROR: Spark application is nil")
		return "", fmt.Errorf("spark application cannot be nil")
	}

	//Load template file, if one supplied
	var initialPod apiv1.Pod
	var err error
	driverPodtemplateFile, templateFileExists := app.Spec.SparkConf["spark.kubernetes.driver.podTemplateFile"]
	if templateFileExists {
		log.Printf("Pod template file specified: %s", driverPodtemplateFile)
		podTemplateDriverContainerName := app.Spec.SparkConf["spark.kubernetes.driver.podTemplateContainerName"]
		initialPod, err = loadPodFromTemplate(driverPodtemplateFile, podTemplateDriverContainerName, app.Spec.SparkConf)
		if err != nil {
			log.Printf("ERROR: Failed to load template file: %v", err)
			return "", fmt.Errorf("failed to load template file for the driver pod %s in namespace %s: %v", common.GetDriverPodName(app), app.Namespace, err)
		}
		log.Printf("Successfully loaded pod template")
	}

	//Driver pod spec instance
	var driverPodSpec apiv1.PodSpec
	if templateFileExists {
		driverPodSpec = initialPod.Spec
		log.Printf("Using pod spec from template")
	} else {
		log.Printf("Using default pod spec")
	}

	// Spark Application Driver Pod schema populating with specific values/data
	var podObjectMetadata metav1.ObjectMeta
	//Driver Pod Name
	podObjectMetadata.Name = common.GetDriverPodName(app)
	//Driver pod Namespace
	podObjectMetadata.Namespace = common.GetAppNamespace(app)
	//Driver Pod labels
	podObjectMetadata.Labels = serviceLabels
	//Driver pod annotations
	if app.Spec.Driver.Annotations != nil {
		podObjectMetadata.Annotations = app.Spec.Driver.Annotations
		log.Printf("Using driver annotations from spec")
	} else {
		log.Printf("No driver annotations in spec, checking sparkConf")
		annotations := make(map[string]string)
		for sparkConfKey, sparkConfValue := range app.Spec.SparkConf {
			if strings.Contains(sparkConfKey, "spark.kubernetes.driver.annotation.") {
				lastDotIndex := strings.LastIndex(sparkConfKey, DotSeparator)
				annotationKey := sparkConfKey[lastDotIndex+1:]
				annotations[annotationKey] = sparkConfValue
			}
		}
		if len(annotations) > 0 {
			podObjectMetadata.Annotations = annotations
			log.Printf("Added %d annotations from sparkConf", len(annotations))
		}
	}
	//Driver Pod Owner Reference
	podObjectMetadata.OwnerReferences = []metav1.OwnerReference{*common.GetOwnerReference(app)}
	log.Printf("Driver pod name: %s, namespace: %s", podObjectMetadata.Name, podObjectMetadata.Namespace)

	//Driver pod DNS policy
	driverPodSpec.DNSPolicy = SparkDriverDNSPolicy

	//Driver pod enable service link
	driverPodSpec.EnableServiceLinks = common.BoolPointer(true)

	//Priority wise, values mentioned in Spec i.e. app.Spec.NodeSelector or/and app.Spec.Driver.NodeSelector will be higher than
	// ones specified in sparkConf

	if app.Spec.NodeSelector != nil {
		driverPodSpec.NodeSelector = app.Spec.NodeSelector
	} else if app.Spec.Driver.NodeSelector != nil {
		driverPodSpec.NodeSelector = app.Spec.Driver.NodeSelector
	} else {
		nodeSelectorList := make(map[string]string)
		//Driver pod node selector
		for sparkConfKey, sparkConfValue := range app.Spec.SparkConf {
			if strings.Contains(sparkConfKey, SparkNodeSelectorPrefix) {
				lastDotIndex := strings.LastIndex(sparkConfKey, DotSeparator)
				driverPodNodeSelectorKey := sparkConfKey[lastDotIndex+1:]
				nodeSelectorList[driverPodNodeSelectorKey] = sparkConfValue
			}
		}
		for sparkConfKey, sparkConfValue := range app.Spec.SparkConf {
			if strings.Contains(sparkConfKey, SparkDriverPodNodeSelectorPrefix) {
				lastDotIndex := strings.LastIndex(sparkConfKey, DotSeparator)
				driverPodNodeSelectorKey := sparkConfKey[lastDotIndex+1:]
				nodeSelectorList[driverPodNodeSelectorKey] = sparkConfValue
			}
		}
		if len(nodeSelectorList) > 0 {
			driverPodSpec.NodeSelector = nodeSelectorList
		}
	}

	//Image pull Secrets
	var imagePullSecrets []string
	if app.Spec.ImagePullSecrets != nil {
		imagePullSecrets = app.Spec.ImagePullSecrets
	} else if common.CheckSparkConf(app.Spec.SparkConf, common.SparkImagePullSecretKey) {
		imagePullSecretList := app.Spec.SparkConf[common.SparkImagePullSecretKey]
		imagePullSecrets = strings.Split(imagePullSecretList, ",")
	}
	var imagePullSecretsList []apiv1.LocalObjectReference
	for _, secretName := range imagePullSecrets {
		var imagePullSecret apiv1.LocalObjectReference
		imagePullSecret.Name = secretName
		imagePullSecretsList = append(imagePullSecretsList, imagePullSecret)
	}
	driverPodSpec.ImagePullSecrets = imagePullSecretsList

	//RestartPolicy
	driverPodSpec.RestartPolicy = DriverPodRestartPolicyNever
	//Driver pod security context
	if app.Spec.Driver.SecurityContext != nil {
		if app.Spec.Driver.SecurityContext.RunAsUser != nil || app.Spec.Driver.SecurityContext.RunAsNonRoot != nil {
			var podSecurityContext apiv1.PodSecurityContext
			if app.Spec.Driver.SecurityContext.RunAsUser != nil {
				podSecurityContext.RunAsUser = app.Spec.Driver.SecurityContext.RunAsUser
				podSecurityContext.FSGroup = app.Spec.Driver.SecurityContext.RunAsUser
				podSecurityContext.SupplementalGroups = SupplementalGroups(*app.Spec.Driver.SecurityContext.RunAsUser)
			}
			if app.Spec.Driver.SecurityContext.RunAsNonRoot != nil {
				podSecurityContext.RunAsNonRoot = app.Spec.Driver.SecurityContext.RunAsNonRoot
			}

			driverPodSpec.SecurityContext = &podSecurityContext
		}
	} else {
		driverPodSpec.SecurityContext = &apiv1.PodSecurityContext{
			RunAsUser: common.Int64Pointer(DriverPodSecurityContextID),
			FSGroup:   common.Int64Pointer(DriverPodSecurityContextID),
			//Run as non-root
			RunAsNonRoot:       common.BoolPointer(true),
			SupplementalGroups: SupplementalGroups(DriverPodSecurityContextID),
		}
	}
	//Service Account
	log.Printf("Service account: %v", app.Spec.Driver.ServiceAccount)

	if app.Spec.Driver.ServiceAccount != nil {
		driverPodSpec.ServiceAccountName = *app.Spec.Driver.ServiceAccount
	} else if common.CheckSparkConf(app.Spec.SparkConf, "spark.kubernetes.authenticate.driver.serviceAccountName") {
		driverPodSpec.ServiceAccountName = app.Spec.SparkConf["spark.kubernetes.authenticate.driver.serviceAccountName"]
	}
	//Pod Scheduler Name
	driverPodSchedulerName, driverPodSchedulerValueExists := app.Spec.SparkConf["spark.kubernetes.driver.scheduler.name"]
	podSchedulerName, podSchedulerValueExists := app.Spec.SparkConf["spark.kubernetes.scheduler.name"]
	if driverPodSchedulerValueExists {
		driverPodSpec.SchedulerName = driverPodSchedulerName
	} else if podSchedulerValueExists {
		driverPodSpec.SchedulerName = podSchedulerName
	}

	//Termination grace period
	if app.Spec.Driver.TerminationGracePeriodSeconds != nil {
		driverPodSpec.TerminationGracePeriodSeconds = app.Spec.Driver.TerminationGracePeriodSeconds
	} else {
		driverPodSpec.TerminationGracePeriodSeconds = common.Int64Pointer(DefaultTerminationGracePeriodSeconds)
	}
	//Tolerations
	if app.Spec.Driver.Tolerations != nil {
		driverPodSpec.Tolerations = app.Spec.Driver.Tolerations
	} else {
		//Assigning default toleration
		var tolerations []apiv1.Toleration
		tolerations = []apiv1.Toleration{
			{
				Effect:            TolerationEffect,
				Key:               NodeNotReady,
				Operator:          Operator,
				TolerationSeconds: common.Int64Pointer(DefaultTolerationSeconds),
			},
			{
				Effect:            TolerationEffect,
				Key:               NodeNotReachable,
				Operator:          Operator,
				TolerationSeconds: common.Int64Pointer(DefaultTolerationSeconds),
			},
		}
		driverPodSpec.Tolerations = tolerations
	}

	//Pod Volumes setup
	var driverPodVolumes []apiv1.Volume

	// spark-conf-volume-driver addition
	sparkConfVolume := apiv1.Volume{
		Name: SparkConfVolumeDriver,
		VolumeSource: apiv1.VolumeSource{
			ConfigMap: &apiv1.ConfigMapVolumeSource{
				DefaultMode: Int32Pointer(420),
				Items: []apiv1.KeyToPath{
					{
						Key:  SparkEnvScriptFileName,
						Mode: Int32Pointer(420),
						Path: SparkEnvScriptFileName,
					},
					{
						Key:  SparkPropertiesFileName,
						Mode: Int32Pointer(420),
						Path: SparkPropertiesFileName,
					},
				},
				LocalObjectReference: apiv1.LocalObjectReference{Name: driverConfigMapName},
			},
		},
	}

	driverPodVolumes = append(driverPodVolumes, sparkConfVolume)

	driverPodContainerSpec, resolvedLocalDirs := CreateDriverPodContainerSpec(app)
	var containerSpecList []apiv1.Container
	localDirFeatureSetupError := handleLocalDirsFeatureStep(app, resolvedLocalDirs, &driverPodVolumes, &driverPodContainerSpec.VolumeMounts, &driverPodContainerSpec.Env, appSpecVolumeMounts, appSpecVolumes)
	if localDirFeatureSetupError != nil {
		return "", fmt.Errorf("failed to setup local directory for the driver pod %s in namespace %s: %v", common.GetDriverPodName(app), app.Namespace, localDirFeatureSetupError)
	}

	volumeExtension := "-volume"
	if app.Spec.Driver.Secrets != nil {
		for _, secret := range app.Spec.Driver.Secrets {
			driverPodVolumes, driverPodContainerSpec = addSecret(secret, volumeExtension, driverPodVolumes, driverPodContainerSpec)
		}
	}
	//Populating secrets passed in sparkConf
	for sparkConfKey, sparkConfValue := range app.Spec.SparkConf {
		if strings.Contains(sparkConfKey, common.SparkDriverSecretKeyPrefix) {
			lastDotIndex := strings.LastIndex(sparkConfKey, DotSeparator)
			driverPodSecretKey := sparkConfKey[lastDotIndex+1:]
			var secret v1beta2.SecretInfo
			secret.Name = driverPodSecretKey
			secret.Path = sparkConfValue
			driverPodVolumes, driverPodContainerSpec = addSecret(secret, volumeExtension, driverPodVolumes, driverPodContainerSpec)
		}
	}
	containerSpecList = append(containerSpecList, driverPodContainerSpec)

	containerSpecList = handleSideCars(app, containerSpecList, appSpecVolumes)

	if app.Spec.Driver.InitContainers != nil {
		driverPodSpec.InitContainers = app.Spec.Driver.InitContainers
	}

	driverPodSpec.Containers = containerSpecList

	// Setting up pod volumes
	driverPodSpec.Volumes = driverPodVolumes

	driverPod := &apiv1.Pod{
		ObjectMeta: podObjectMetadata,
		Spec:       driverPodSpec,
	}

	//Check existence of pod
	createPodErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existingDriverPod := &apiv1.Pod{}
		_, err := kubeClient.CoreV1().Pods(app.Namespace).Get(context.TODO(), podObjectMetadata.Name, metav1.GetOptions{})
		if apiErrors.IsNotFound(err) {
			_, createErr := kubeClient.CoreV1().Pods(app.Namespace).Create(context.TODO(), driverPod, metav1.CreateOptions{})

			if createErr != nil {
				return fmt.Errorf("error while creating driver pod: %w", createErr)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("error while retrieving driver pod: %w", err)
			//return err
		}
		existingDriverPod.ObjectMeta = podObjectMetadata
		existingDriverPod.Spec = driverPodSpec
		_, updateErr := kubeClient.CoreV1().Pods(app.Namespace).Update(context.TODO(), existingDriverPod, metav1.UpdateOptions{})
		if updateErr != nil {
			return fmt.Errorf("error while updating driver pod: %w", updateErr)
		}

		return updateErr
	})

	if createPodErr != nil {
		return "", fmt.Errorf("failed to create/update driver pod %s in namespace %s: %v", common.GetDriverPodName(app), app.Namespace, createPodErr)
	}

	// Get the created pod to retrieve its UID
	pod, err := kubeClient.CoreV1().Pods(app.Namespace).Get(context.TODO(), common.GetDriverPodName(app), metav1.GetOptions{})
	if err != nil {
		log.Printf("WARNING: Failed to retrieve created pod for UID: %v", err)
		return "", nil // Return empty UID but no error since pod was created successfully
	}

	log.Printf("=== Driver Pod creation successful for app: %s, namespace: %s, Pod UID: %s ===", app.Name, app.Namespace, pod.UID)
	return string(pod.UID), nil
}

func handleSideCars(app *v1beta2.SparkApplication, containerSpecList []apiv1.Container, appSpecVolumes []apiv1.Volume) []apiv1.Container {
	if app.Spec.Driver.Sidecars != nil {
		var sideCarVolumeMounts []apiv1.VolumeMount
		for _, configMap := range app.Spec.Driver.ConfigMaps {
			//Volume mount for the configmap
			volumeMount := apiv1.VolumeMount{
				Name:      configMap.Name + "-vol",
				MountPath: configMap.Path,
			}
			sideCarVolumeMounts = append(sideCarVolumeMounts, volumeMount)
		}

		for _, sideCarContainer := range app.Spec.Driver.Sidecars {
			//Handle volume mounts
			for _, volumeTobeMounted := range sideCarContainer.VolumeMounts {
				//Volume mount for the volumes
				if checkVolumeMountIsVolume(appSpecVolumes, volumeTobeMounted.Name) {
					volumeMount := apiv1.VolumeMount{
						Name:      volumeTobeMounted.Name,
						MountPath: volumeTobeMounted.MountPath,
						ReadOnly:  volumeTobeMounted.ReadOnly,
					}
					sideCarVolumeMounts = append(sideCarVolumeMounts, volumeMount)
				}
			}
			sideCarContainer.VolumeMounts = nil
			if len(sideCarVolumeMounts) > 0 {
				sideCarContainer.VolumeMounts = sideCarVolumeMounts
			}

			containerSpecList = append(containerSpecList, sideCarContainer)
		}
	}
	return containerSpecList
}

func checkVolumeMountIsVolume(appSpecVolumes []apiv1.Volume, volumeName string) bool {
	for _, appSpecVolume := range appSpecVolumes {
		if appSpecVolume.Name == volumeName {
			return true
		}
	}
	return false
}

// CreateDriverPodContainerSpec Helper func to create Driver Pod Driver contianer spec creation
func CreateDriverPodContainerSpec(app *v1beta2.SparkApplication) (apiv1.Container, []string) {
	var driverPodContainerSpec apiv1.Container
	mainClass := ""
	mainApplicationFile := ""
	var args []string

	if app.Spec.Arguments != nil {
		args = app.Spec.Arguments
	}

	if app.Spec.MainClass != nil {
		mainClass = *app.Spec.MainClass
	}

	if app.Spec.MainApplicationFile != nil {
		mainApplicationFile = *app.Spec.MainApplicationFile
	}

	//Driver Container default arguments
	driverPodContainerSpec.Args = append(driverPodContainerSpec.Args, SparkDriverArg, SparkDriverArgPropertiesFile, SparkDriverArgPropertyFilePath, SparkDriverArgClass, mainClass, mainApplicationFile)
	driverPodContainerSpec.Args = append(driverPodContainerSpec.Args, args...)

	var driverPodContainerEnvVars []apiv1.EnvVar

	//Spark User details
	var sparkUserDetails apiv1.EnvVar
	if os.Getenv(SparkUser) != "" {
		sparkUserDetails.Value = os.Getenv(SparkUser)
	} else {
		sparkUserDetails.Value = SparkUserId
	}
	sparkUserDetails.Name = SparkUser
	driverPodContainerEnvVars = append(driverPodContainerEnvVars, sparkUserDetails)

	//Spark Application ID
	var sparkAppDetails apiv1.EnvVar
	sparkAppDetails.Name = SparkApplicationID
	sparkAppDetails.Value = app.Status.SparkApplicationID
	driverPodContainerEnvVars = append(driverPodContainerEnvVars, sparkAppDetails)

	//Spark Driver Bind Address
	var driverPodContainerEnvVarBindAddress apiv1.EnvVar
	driverPodContainerEnvVarBindAddress.Name = SparkDriverBindAddress
	driverPodContainerEnvVarBindAddress.ValueFrom = &apiv1.EnvVarSource{
		FieldRef: &apiv1.ObjectFieldSelector{
			APIVersion: ApiVersionV1,
			FieldPath:  SparkDriverPodIP,
		},
	}
	driverPodContainerEnvVars = append(driverPodContainerEnvVars, driverPodContainerEnvVarBindAddress)

	// Add all the spark.kubernetes.driverEnv. prefixed sparkConf key value pairs
	var resolvedLocalDirs []string
	resolvedLocalDirs, driverPodContainerEnvVars = processSparkConfEnv(app, driverPodContainerEnvVars)
	sparkConfKeyValuePairs := app.Spec.SparkConf

	// Addition of the spark.kubernetes.kerberos.tokenSecret.itemKey
	// Handling https://spark.apache.org/docs/3.0.0-preview2/security.html#long-running-applications
	//spark.kubernetes.kerberos.tokenSecret.name spark.kubernetes.kerberos.tokenSecret.itemKey
	sparkConfValue, valueExists := sparkConfKeyValuePairs[KerberosTokenSecretItemKey]
	if valueExists {
		var driverPodContainerEnvVar apiv1.EnvVar
		driverPodContainerEnvVar.Name = KerberosHadoopSecretFilePathKey
		driverPodContainerEnvVar.Value = KerberosHadoopSecretFilePath + sparkConfValue
		driverPodContainerEnvVars = append(driverPodContainerEnvVars, driverPodContainerEnvVar)
	}

	//Spark Config directory
	var sparkConfigDir apiv1.EnvVar
	sparkConfigDir.Name = common.SparkConfDirEnvVar
	sparkConfigDir.Value = SparkConfVolumeDriverMountPath
	driverPodContainerEnvVars = append(driverPodContainerEnvVars, sparkConfigDir)
	//Assign the Driver Pod Container Environment variables to Container Spec
	driverPodContainerSpec.Env = driverPodContainerEnvVars

	//Assign Driver Pod container image from Spec or from sparkConf
	if app.Spec.Driver.Image != nil {
		driverPodContainerSpec.Image = *app.Spec.Driver.Image
	} else if app.Spec.Image != nil {
		driverPodContainerSpec.Image = *app.Spec.Image
	} else if common.CheckSparkConf(app.Spec.SparkConf, "spark.kubernetes.driver.container.image") {
		driverPodContainerSpec.Image = app.Spec.SparkConf["spark.kubernetes.driver.container.image"]
	} else if common.CheckSparkConf(app.Spec.SparkConf, "spark.kubernetes.container.image") {
		driverPodContainerSpec.Image = app.Spec.SparkConf["spark.kubernetes.container.image"]
	}

	if app.Spec.ImagePullPolicy != nil {
		driverPodContainerSpec.ImagePullPolicy = apiv1.PullPolicy(*app.Spec.ImagePullPolicy)
	} else if common.CheckSparkConf(app.Spec.SparkConf, "spark.kubernetes.container.image.pullPolicy") {
		pullPolicy := app.Spec.SparkConf["spark.kubernetes.container.image.pullPolicy"]
		driverPodContainerSpec.ImagePullPolicy = apiv1.PullPolicy(pullPolicy)
	} else {
		//Default value
		driverPodContainerSpec.ImagePullPolicy = ImagePullPolicyIfNotPresent
	}

	//Driver Pod Container Name
	driverPodContainerSpec.Name = common.SparkDriverContainerName

	//Driver pod contianer ports
	driverPodContainerSpec.Ports = []apiv1.ContainerPort{
		{
			ContainerPort: int32(common.GetDriverPort(sparkConfKeyValuePairs)),
			Name:          DriverPortName,
			Protocol:      Protocol,
		},
		{
			ContainerPort: int32(getBlockManagerPort(sparkConfKeyValuePairs)),
			Name:          BlockManagerPortName,
			Protocol:      Protocol,
		},
		{
			ContainerPort: UiPort,
			Name:          UiPortName,
			Protocol:      Protocol,
		},
	}

	//Driver pod container cpu and memory requests and limits populating
	driverPodContainerSpec.Resources = handleResources(app)

	//Security Context
	driverPodContainerSpec.SecurityContext = &apiv1.SecurityContext{
		Capabilities: &apiv1.Capabilities{
			Drop: []apiv1.Capability{All},
		},
		Privileged: common.BoolPointer(false),
	}
	//Driver pod termination path
	driverPodContainerSpec.TerminationMessagePath = DriverPodTerminationLogPath
	//Driver pod termination message policy
	driverPodContainerSpec.TerminationMessagePolicy = DriverPodTerminationMessagePolicy
	//Driver pod container volume mounts
	var volumeMounts []apiv1.VolumeMount

	//Volume mount for the configmap
	volumeMount := apiv1.VolumeMount{
		Name:      SparkConfVolumeDriver,
		MountPath: SparkConfVolumeDriverMountPath,
	}
	volumeMounts = append(volumeMounts, volumeMount)

	volumeMounts = handleKerberoCreds(app, volumeMounts)

	driverPodContainerSpec.VolumeMounts = volumeMounts

	return driverPodContainerSpec, resolvedLocalDirs
}

func checkMountingKubernetesCredentials(sparkConfKeyValuePairs map[string]string) bool {
	if sparkConfKeyValuePairs != nil {
		OAuthTokenConfFileExists := sparkConfKeyValuePairs[OAuthTokenConfFile] != ""
		ClientKeyFileExists := sparkConfKeyValuePairs[ClientKeyFile] != ""
		ClientCertFileExists := sparkConfKeyValuePairs[ClientCertFile] != ""
		CaCertFileExists := sparkConfKeyValuePairs[CaCertFile] != ""
		if OAuthTokenConfFileExists || ClientKeyFileExists || ClientCertFileExists || CaCertFileExists {
			return true
		}
	}
	return false
}

func incorporateMemoryOvehead(memoryNumber int, app *v1beta2.SparkApplication, memoryUnit string) string {
	//Memory Overhead or Memory OverheadFactor incorporating
	var memory string
	if app.Spec.Driver.MemoryOverhead != nil || common.CheckSparkConf(app.Spec.SparkConf, "spark.kubernetes.memoryOverhead") || common.CheckSparkConf(app.Spec.SparkConf, "spark.driver.memoryOverhead") {
		var memoryOverhead string
		if app.Spec.Driver.MemoryOverhead != nil {
			memoryOverhead = *app.Spec.Driver.MemoryOverhead
		} else if common.CheckSparkConf(app.Spec.SparkConf, "spark.driver.memoryOverhead") {
			memoryOverhead = app.Spec.SparkConf["spark.driver.memoryOverhead"]
		} else if common.CheckSparkConf(app.Spec.SparkConf, "spark.kubernetes.memoryOverhead") {
			memoryOverhead = app.Spec.SparkConf["spark.kubernetes.memoryOverhead"]
		}
		//both memory and memoryOverhead are in same Memory Unit(MiB or GiB)
		// Amount of memory to use for the driver process, i.e. where SparkContext is initialized, in the same format as JVM memory strings with a size unit suffix ("k", "m", "g" or "t") (e.g. 512m, 2g).
		//https://spark.apache.org/docs/latest/configuration.html
		memoryOverheadInMiB := processMemoryUnit(memoryOverhead)

		memoryWithOverhead := memoryNumber + memoryOverheadInMiB
		memory = fmt.Sprintf("%d%s", memoryWithOverhead, memoryUnit)
	} else {
		MemoryOveheadMinInMiB := 384.0
		var memoryOverheadFactor float64
		if app.Spec.MemoryOverheadFactor != nil {
			memoryOverheadFactor, _ = strconv.ParseFloat(*app.Spec.MemoryOverheadFactor, 64)
		} else if common.CheckSparkConf(app.Spec.SparkConf, "spark.driver.memoryOverheadFactor") {
			memOverheadFactorString := app.Spec.SparkConf["spark.driver.memoryOverheadFactor"]
			memoryOverheadFactor, _ = strconv.ParseFloat(memOverheadFactorString, 64)
		} else {
			memoryOverheadFactor, _ = strconv.ParseFloat(common.GetMemoryOverheadFactor(app), 64)
		}
		memoryWithOverhead := memoryOverheadFactor * float64(memoryNumber)
		memoryOverheadFactorInclusion := math.Max(MemoryOveheadMinInMiB, memoryWithOverhead)
		memoryWithOverheadFactor := float64(memoryNumber) + memoryOverheadFactorInclusion
		memory = fmt.Sprintf("%F%s", memoryWithOverheadFactor, memoryUnit)
	}
	return memory
}

func processMemoryUnit(memoryData string) int {

	memoryData = strings.TrimSpace(memoryData)
	memoryData = strings.ToUpper(memoryData)
	units := map[string]float64{
		"K": 0.0009765625,
		"M": 1,
		"G": 1024,
		"T": 1024 * 1024,
	}
	for unit, multiplier := range units {
		if strings.Contains(memoryData, unit) {
			memoryNumber := memoryData[:strings.Index(memoryData, unit)]
			memoryConverted, _ := strconv.Atoi(memoryNumber)
			return int(float64(memoryConverted) * multiplier)
		}
	}
	//handling default case
	memoryInMiB, _ := strconv.Atoi(memoryData)
	return memoryInMiB
}

func processSparkConfEnv(app *v1beta2.SparkApplication, driverPodContainerEnvVars []apiv1.EnvVar) ([]string, []apiv1.EnvVar) {
	var resolvedLocalDirs []string
	sparkConfKeyValuePairs := app.Spec.SparkConf
	var driverPodContainerEnvConfigVars apiv1.EnvVar
	for sparkConfKey, sparkConfValue := range sparkConfKeyValuePairs {
		if strings.Contains(sparkConfKey, SparkDriverEnvPrefix) {
			lastDotIndex := strings.LastIndex(sparkConfKey, DotSeparator)
			driverPodContainerEnvConfigVarKey := sparkConfKey[lastDotIndex+1:]
			if driverPodContainerEnvConfigVarKey == "SPARK_LOCAL_DIRS" {
				resolvedLocalDirs = strings.Split(sparkConfValue, ",")
			}
			driverPodContainerEnvConfigVars.Name = driverPodContainerEnvConfigVarKey
			driverPodContainerEnvConfigVars.Value = sparkConfValue
			driverPodContainerEnvVars = append(driverPodContainerEnvVars, driverPodContainerEnvConfigVars)
		}
	}

	// Add envVars of Driver portion to Driver Pod Spec
	sparkDriverConfKeyValuePairs := app.Spec.Driver.EnvVars
	if sparkDriverConfKeyValuePairs != nil {
		var driverPodContainerEnvVar apiv1.EnvVar
		for sparkDriverEnvKey, sparkDriverEnvValue := range sparkDriverConfKeyValuePairs {
			driverPodContainerEnvVar.Name = sparkDriverEnvKey
			driverPodContainerEnvVar.Value = sparkDriverEnvValue
			driverPodContainerEnvVars = append(driverPodContainerEnvVars, driverPodContainerEnvVar)
		}
	}
	//spark.kubernetes.driver.secretKeyRef.
	var driverPodContainerEnvVar apiv1.EnvVar
	for sparkConfKey, sparkConfValue := range app.Spec.SparkConf {
		if strings.Contains(sparkConfKey, common.SparkDriverSecretKeyRefKeyPrefix) {
			lastDotIndex := strings.LastIndex(sparkConfKey, DotSeparator)
			driverPodSecretKeyRef := sparkConfKey[lastDotIndex+1:]
			driverPodContainerEnvVar.Name = driverPodSecretKeyRef
			driverPodContainerEnvVar.Value = sparkConfValue
			driverPodContainerEnvVars = append(driverPodContainerEnvVars, driverPodContainerEnvVar)
		}
	}
	return resolvedLocalDirs, driverPodContainerEnvVars
}

func handleKerberoCreds(app *v1beta2.SparkApplication, volumeMounts []apiv1.VolumeMount) []apiv1.VolumeMount {
	sparkConfKeyValuePairs := app.Spec.SparkConf
	if sparkConfKeyValuePairs != nil {
		_, kerberosPatheExists := sparkConfKeyValuePairs[KerberosPath]
		_, kerberosConfigMapNameExists := sparkConfKeyValuePairs[KerberosConfigMapName]
		if kerberosPatheExists || kerberosConfigMapNameExists {
			kerberosConfigMapVolumeMount := apiv1.VolumeMount{
				Name:      KerberosFileVolume,
				MountPath: KerberosFileDirectoryPath + ForwardSlash + KerberosFileName,
				SubPath:   KerberosFileName,
			}
			volumeMounts = append(volumeMounts, kerberosConfigMapVolumeMount)
		}
	}

	if checkMountingKubernetesCredentials(app.Spec.SparkConf) {
		kubernetesCredentialsVolumeMount := apiv1.VolumeMount{
			Name:      KubernetesCredentials,
			MountPath: KubernetesCredentialsVolumeMountPath,
		}
		volumeMounts = append(volumeMounts, kubernetesCredentialsVolumeMount)
	}
	return volumeMounts
}

func handleResources(app *v1beta2.SparkApplication) apiv1.ResourceRequirements {
	var driverPodResourceRequirement apiv1.ResourceRequirements
	var memoryQuantity resource.Quantity
	var memoryInBytes string
	memoryValExists := false
	cpuValExists := false
	//Memory Request
	if app.Spec.Driver.Memory != nil {
		memoryData := *app.Spec.Driver.Memory
		// Identify memory unit and convert everything in MiB for uniformity
		memoryInMiB := processMemoryUnit(memoryData)
		memoryInBytes = incorporateMemoryOvehead(memoryInMiB, app, "Mi")
		memoryQuantity = resource.MustParse(memoryInBytes)
		memoryValExists = true
	} else if common.CheckSparkConf(app.Spec.SparkConf, SparkDriverMemory) {
		memoryQuantity = resource.MustParse(app.Spec.SparkConf[SparkDriverMemory])
		memoryValExists = true
	} else { //setting default value
		memoryQuantity = resource.MustParse("512Mi")
	}

	var cpuQuantity resource.Quantity
	if app.Spec.Driver.CoreLimit != nil || common.CheckSparkConf(app.Spec.SparkConf, common.SparkDriverCoreLimitKey) {
		if app.Spec.Driver.CoreLimit != nil {
			cpuQuantity = resource.MustParse(*app.Spec.Driver.CoreLimit)
		} else {
			cpuQuantity = resource.MustParse(app.Spec.SparkConf[common.SparkDriverCoreLimitKey])
		}

		driverPodResourceRequirement.Limits = apiv1.ResourceList{
			apiv1.ResourceCPU:    cpuQuantity,
			apiv1.ResourceMemory: memoryQuantity,
		}
	} else { //set memory Limit
		driverPodResourceRequirement.Limits = apiv1.ResourceList{
			apiv1.ResourceMemory: memoryQuantity,
		}
	}
	//Cores OR Cores Request - https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/issues/581
	//spark.kubernetes.driver.request.cores takes precedence over spark.driver.cores for specifying the driver pod cpu request if set.
	//Priority sequence is - if value supplied in app spec directly, if not, then if supplied in sparkConf or default
	if app.Spec.Driver.CoreRequest != nil {
		cpuQuantity = resource.MustParse(*app.Spec.Driver.CoreRequest)
		cpuValExists = true
	} else if common.CheckSparkConf(app.Spec.SparkConf, common.SparkDriverCoreRequestKey) {
		cpuQuantity = resource.MustParse(app.Spec.SparkConf[common.SparkDriverCoreRequestKey])
		cpuValExists = true
	} else if app.Spec.Driver.Cores != nil {
		cpuQuantity = resource.MustParse(fmt.Sprint(*app.Spec.Driver.Cores))
		cpuValExists = true
	} else if common.CheckSparkConf(app.Spec.SparkConf, SparkDriverCores) {
		cpuQuantity = resource.MustParse(app.Spec.SparkConf[SparkDriverCores])
		cpuValExists = true
	} else {
		//Setting default value as cores or coreLimit is not passed
		cpuValExists = true
		cpuQuantity = resource.MustParse("1")
	}

	if cpuValExists && memoryValExists {
		driverPodResourceRequirement.Requests = apiv1.ResourceList{
			apiv1.ResourceMemory: memoryQuantity,
			apiv1.ResourceCPU:    cpuQuantity,
		}

	} else if memoryValExists {
		driverPodResourceRequirement.Requests = apiv1.ResourceList{
			apiv1.ResourceMemory: memoryQuantity,
		}
	} else if cpuValExists {
		driverPodResourceRequirement.Requests = apiv1.ResourceList{
			apiv1.ResourceCPU: cpuQuantity,
		}
	}

	return driverPodResourceRequirement
}
