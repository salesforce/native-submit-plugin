package configmap

import (
	"fmt"
	"nativesubmit/common"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/magiconair/properties"
	ctrlClient "sigs.k8s.io/controller-runtime/pkg/client"
)

// Function to create Spark Application Configmap
// Spark Application ConfigMap is pre-requisite for Driver Pod Creation; this configmap is mounted on driver pod
// Spark Application ConfigMap acts as configuration repository for the Driver, executor pods
func Create(app *v1beta2.SparkApplication, submissionID string, createdApplicationId string, kubeClient ctrlClient.Client, driverConfigMapName string, serviceName string) error {
	if app == nil {
		return fmt.Errorf("spark application cannot be nil")
	}

	var errorSubmissionCommandArgs error

	//ConfigMap is created with Key, Value Pairs
	driverConfigMapData := make(map[string]string)
	//Followed the convention of Scala Implementation for this attribute, constant value is assigned
	driverConfigMapData[SparkEnvScriptFileName] = SparkEnvScriptFileCommand
	//Spark Application namespace can be passed in either application spec metadata or in sparkConf property
	driverConfigMapData[SparkAppNamespaceKey] = common.GetAppNamespace(app)

	// Utility function buildAltSubmissionCommandArgs to add other key, value configuration pairs
	driverConfigMapData[SparkPropertiesFileName], errorSubmissionCommandArgs = buildAltSubmissionCommandArgs(app, common.GetDriverPodName(app), submissionID, createdApplicationId, serviceName)
	if errorSubmissionCommandArgs != nil {
		return fmt.Errorf("failed to create submission command args for the driver configmap %s in namespace %s: %v", driverConfigMapName, app.Namespace, errorSubmissionCommandArgs)
	}
	//Create Spark Application ConfigMap
	createErr := createConfigMapUtil(driverConfigMapName, app, driverConfigMapData, kubeClient)
	if createErr != nil {
		return fmt.Errorf("failed to create/update driver configmap %s in namespace %s: %v", driverConfigMapName, app.Namespace, createErr)
	}
	return nil
}

// Helper func to create key/value pairs required for the Spark Application Configmap
// Majority of the code borrowed from Scala implementation
func buildAltSubmissionCommandArgs(app *v1beta2.SparkApplication, driverPodName string, submissionID string, createdApplicationId string, serviceName string) (string, error) {
	var sb strings.Builder
	sparkConfKeyValuePairs := app.Spec.SparkConf
	masterURL, err := getMasterURL()
	if err != nil {
		return sb.String(), err
	}
	masterURL = AddEscapeCharacter(masterURL)

	//Construct Service Name
	//<servicename>.<namespace>.svc
	serviceName = fmt.Sprintf("%s.%s.%s", serviceName, app.Namespace, ServiceShortForm)

	sb.WriteString(fmt.Sprintf("%s=%s", SparkDriverHost, serviceName))
	sb.WriteString(NewLineString)

	sb.WriteString(fmt.Sprintf("%s=%s", SparkAppId, createdApplicationId))
	sb.WriteString(NewLineString)

	sb.WriteString(fmt.Sprintf("%s=%s", SparkMaster, masterURL))
	sb.WriteString(NewLineString)

	sb.WriteString(fmt.Sprintf("%s=%s", SparkSubmitDeploymentMode, string(app.Spec.Mode)))
	sb.WriteString(NewLineString)

	sb.WriteString(fmt.Sprintf("%s=%s", SparkAppNamespaceKey, app.Namespace))
	sb.WriteString(NewLineString)

	sb.WriteString(fmt.Sprintf("%s=%s", SparkAppNameKey, app.Name))
	sb.WriteString(NewLineString)

	sb.WriteString(fmt.Sprintf("%s=%s", SparkDriverPodNameKey, driverPodName))
	sb.WriteString(NewLineString)

	sb.WriteString(populateArtifacts(sb.String(), *app))

	sb.WriteString(populateContainerImageDetails(sb.String(), *app))
	if app.Spec.PythonVersion != nil {
		sb.WriteString(fmt.Sprintf("%s=%s", SparkPythonVersion, *app.Spec.PythonVersion))
		sb.WriteString(NewLineString)
	}
	if app.Spec.MemoryOverheadFactor != nil {
		sb.WriteString(fmt.Sprintf("%s=%s", SparkMemoryOverheadFactor, *app.Spec.MemoryOverheadFactor))
		sb.WriteString(NewLineString)
	} else {

		sb.WriteString(fmt.Sprintf("%s=%s", SparkMemoryOverheadFactor, common.GetMemoryOverheadFactor(app)))
		sb.WriteString(NewLineString)
	}

	// Operator triggered spark-submit should never wait for App completion
	sb.WriteString(fmt.Sprintf("%s=false", SparkWaitAppCompletion))
	sb.WriteString(NewLineString)

	sb.WriteString(populateSparkConfProperties(sb.String(), sparkConfKeyValuePairs))

	// Add Hadoop configuration properties.
	for key, value := range app.Spec.HadoopConf {
		sb.WriteString(fmt.Sprintf("spark.hadoop.%s=%s", key, value))
		sb.WriteString(NewLineString)
	}
	if app.Spec.HadoopConf != nil || app.Spec.HadoopConfigMap != nil {
		// Adding Environment variable
		sb.WriteString(fmt.Sprintf("spark.hadoop.%s=%s", HadoopConfDir, HadoopConfDirPath))
		sb.WriteString(NewLineString)
	}

	// Add the driver and executor configuration options.
	// Note that when the controller submits the application, it expects that all dependencies are local
	// so init-container is not needed and therefore no init-container image needs to be specified.
	sb.WriteString(fmt.Sprintf("%s%s=%s", SparkDriverLabelKeyPrefix, SparkAppNameLabel, app.Name))
	sb.WriteString(NewLineString)
	//driverConfOptions = append(driverConfOptions,
	sb.WriteString(fmt.Sprintf("%s%s=%s", SparkDriverLabelKeyPrefix, LaunchedBySparkOperatorLabel, "true"))
	sb.WriteString(NewLineString)

	sb.WriteString(fmt.Sprintf("%s%s=%s", SparkDriverLabelKeyPrefix, SubmissionIDLabel, submissionID))
	sb.WriteString(NewLineString)

	if app.Spec.Driver.Image != nil {
		sb.WriteString(fmt.Sprintf("%s=%s", SparkDriverContainerImageKey, *app.Spec.Driver.Image))
		sb.WriteString(NewLineString)
	}

	sbPtr, err := populateComputeInfo(&sb, *app, sparkConfKeyValuePairs)
	if err != nil {
		return "driver cores should be an integer", err
	}
	sb = *sbPtr

	sb.WriteString(populateMemoryInfo(sb.String(), *app, sparkConfKeyValuePairs))

	if app.Spec.Driver.ServiceAccount != nil {
		sb.WriteString(fmt.Sprintf("%s=%s", SparkDriverServiceAccountName, *app.Spec.Driver.ServiceAccount))
		sb.WriteString(NewLineString)
	}

	if app.Spec.Driver.JavaOptions != nil {
		driverJavaOptionsList := AddEscapeCharacter(*app.Spec.Driver.JavaOptions)
		sb.WriteString(fmt.Sprintf("%s=%s", SparkDriverJavaOptions, driverJavaOptionsList))
		sb.WriteString(NewLineString)
	}

	if app.Spec.Driver.KubernetesMaster != nil {
		sb.WriteString(fmt.Sprintf("%s=%s", SparkDriverKubernetesMaster, *app.Spec.Driver.KubernetesMaster))
		sb.WriteString(NewLineString)
	}

	//Populate SparkApplication Labels to Driver
	driverLabels := make(map[string]string, len(app.Labels)+len(app.Spec.Driver.Labels))
	for key, value := range app.Labels {
		driverLabels[key] = value
	}
	for key, value := range app.Spec.Driver.Labels {
		driverLabels[key] = value
	}

	for key, value := range driverLabels {
		sb.WriteString(fmt.Sprintf("%s%s=%s", SparkDriverLabelKeyPrefix, key, value))
		sb.WriteString(NewLineString)
	}
	sb.WriteString(populateDriverAnnotations(sb.String(), *app))

	for key, value := range app.Spec.Driver.EnvSecretKeyRefs {
		sb.WriteString(fmt.Sprintf("%s%s=%s:%s", SparkDriverSecretKeyRefKeyPrefix, key, value.Name, value.Key))
		sb.WriteString(NewLineString)
	}

	for key, value := range app.Spec.Driver.ServiceAnnotations {
		sb.WriteString(fmt.Sprintf("%s%s=%s", SparkDriverServiceAnnotationKeyPrefix, key, value))
		sb.WriteString(NewLineString)
	}

	sb.WriteString(populateDriverSecrets(sb.String(), *app))

	for key, value := range app.Spec.Driver.EnvVars {
		sb.WriteString(fmt.Sprintf("%s%s=%s", SparkDriverEnvVarConfigKeyPrefix, key, value))
		sb.WriteString(NewLineString)

	}

	for key, value := range app.Spec.Driver.Env {
		sb.WriteString(fmt.Sprintf("%s%d=%s", SparkDriverEnvVarConfigKeyPrefix, key, value))
		sb.WriteString(NewLineString)
	}

	sb.WriteString(fmt.Sprintf("%s%s=%s", SparkExecutorLabelKeyPrefix, SparkAppNameLabel, app.Name))
	sb.WriteString(NewLineString)

	sb.WriteString(fmt.Sprintf("%s%s=%s", SparkExecutorLabelKeyPrefix, LaunchedBySparkOperatorLabel, "true"))
	sb.WriteString(NewLineString)

	sb.WriteString(fmt.Sprintf("%s%s=%s", SparkExecutorLabelKeyPrefix, SubmissionIDLabel, submissionID))
	sb.WriteString(NewLineString)

	if app.Spec.Executor.Instances != nil {
		sb.WriteString(fmt.Sprintf("spark.executor.instances=%d", *app.Spec.Executor.Instances))
		sb.WriteString(NewLineString)
	}

	if app.Spec.Executor.Image != nil {
		sb.WriteString(fmt.Sprintf("%s=%s", SparkExecutorContainerImageKey, *app.Spec.Executor.Image))
		sb.WriteString(NewLineString)
	}

	if app.Spec.Executor.ServiceAccount != nil {
		sb.WriteString(fmt.Sprintf("%s=%s", SparkExecutorAccountName, *app.Spec.Executor.ServiceAccount))
		sb.WriteString(NewLineString)
	}

	if app.Spec.Executor.DeleteOnTermination != nil {
		sb.WriteString(fmt.Sprintf("%s=%t", SparkExecutorDeleteOnTermination, *app.Spec.Executor.DeleteOnTermination))
		sb.WriteString(NewLineString)
	}

	//Populate SparkApplication Labels to Executors
	executorLabels := make(map[string]string, len(app.Labels)+len(app.Spec.Executor.Labels))
	for key, value := range app.Labels {
		executorLabels[key] = value
	}
	for key, value := range app.Spec.Executor.Labels {
		executorLabels[key] = value
	}
	for key, value := range executorLabels {
		sb.WriteString(fmt.Sprintf("%s%s=%s", SparkExecutorLabelKeyPrefix, key, value))
		sb.WriteString(NewLineString)
	}

	sb.WriteString(populateExecutorAnnotations(sb.String(), *app))

	for key, value := range app.Spec.Executor.EnvSecretKeyRefs {
		sb.WriteString(fmt.Sprintf("%s%s=%s:%s", SparkExecutorSecretKeyRefKeyPrefix, key, value.Name, value.Key))
		sb.WriteString(NewLineString)
	}

	if app.Spec.Executor.JavaOptions != nil {
		sb.WriteString(fmt.Sprintf("%s=%s", SparkExecutorJavaOptions, *app.Spec.Executor.JavaOptions))
		sb.WriteString(NewLineString)
	}

	sb.WriteString(populateExecutorSecrets(sb.String(), *app))

	for key, value := range app.Spec.Executor.EnvVars {
		sb.WriteString(fmt.Sprintf("%s%s=%s", SparkExecutorEnvVarConfigKeyPrefix, key, value))
		sb.WriteString(NewLineString)
	}

	sb.WriteString(populateDynamicAllocation(sb.String(), *app))
	for key, value := range app.Spec.NodeSelector {
		sb.WriteString(fmt.Sprintf("%s%s=%s", SparkNodeSelectorKeyPrefix, key, value))
		sb.WriteString(NewLineString)
	}
	for key, value := range app.Spec.Driver.NodeSelector {
		sb.WriteString(fmt.Sprintf("%s%s=%s", SparkDriverNodeSelectorKeyPrefix, key, value))
		sb.WriteString(NewLineString)
	}
	for key, value := range app.Spec.Executor.NodeSelector {
		sb.WriteString(fmt.Sprintf("%s%s=%s", SparkExecutorNodeSelectorKeyPrefix, key, value))
		sb.WriteString(NewLineString)
	}

	sb.WriteString(fmt.Sprintf("%s=%s", SubmitInDriver, True))
	sb.WriteString(NewLineString)

	sb.WriteString(fmt.Sprintf("%s=%v", SparkDriverBlockManagerPort, common.DefaultBlockManagerPort))
	sb.WriteString(NewLineString)

	sb.WriteString(fmt.Sprintf("%s=%v", common.SparkDriverPort, common.GetDriverPort(sparkConfKeyValuePairs)))
	sb.WriteString(NewLineString)
	sb.WriteString(populateAppSpecType(sb.String(), *app))
	sb.WriteString(NewLineString)
	sb.WriteString(fmt.Sprintf("%s=%v", SparkApplicationSubmitTime, time.Now().UnixMilli()))
	sb.WriteString(NewLineString)

	sparkUIProxyBase := path.Join(ForwardSlash, common.GetAppNamespace(app), app.Name)

	sb.WriteString(fmt.Sprintf("%s=%s", SparkUIProxyBase, sparkUIProxyBase))
	sb.WriteString(NewLineString)

	sb.WriteString(fmt.Sprintf("%s=%s", SparkUIProxyRedirectURI, ForwardSlash))
	sb.WriteString(NewLineString)

	sb.WriteString(populateProperties(sb.String()))

	sb.WriteString(populateMonitoringInfo(sb.String(), *app))

	// Volumes
	if app.Spec.Volumes != nil {
		options, err := addLocalDirConfOptions(app)
		if err != nil {
			return "error occcurred while building configmap", err
		}
		for _, option := range options {
			sb.WriteString(option)
			sb.WriteString(NewLineString)
		}
	}

	if app.Spec.MainApplicationFile != nil {
		// Add the main application file if it is present.
		sparkAppJar := AddEscapeCharacter(*app.Spec.MainApplicationFile)
		sb.WriteString(fmt.Sprintf("%s=%s", SparkJars, sparkAppJar))
		sb.WriteString(NewLineString)
	}
	// Add application arguments.
	for _, argument := range app.Spec.Arguments {
		sb.WriteString(argument)
		sb.WriteString(NewLineString)
	}

	return sb.String(), nil
}
func populateDriverAnnotations(args string, app v1beta2.SparkApplication) string {
	for key, value := range app.Spec.Driver.Annotations {
		if key == OpencensusPrometheusTarget {
			value = strings.Replace(value, "\n", "", -1)
			value = AddEscapeCharacter(value)
		}
		args = args + fmt.Sprintf("%s%s=%s", SparkDriverAnnotationKeyPrefix, key, value) + NewLineString
	}
	return args
}
func populateSparkConfProperties(args string, sparkConfKeyValuePairs map[string]string) string {
	// Priority wise: Spark Application Specification value, if not, then value in sparkConf, if not, then, defaults that get applied by Spark Environment of Driver pod
	// Add Spark configuration properties.
	for key, value := range sparkConfKeyValuePairs {
		// Configuration property for the driver pod name has already been set.
		if key != SparkDriverPodNameKey {
			//Adding escape character for the spark.executor.extraClassPath and spark.driver.extraClassPath
			if key == SparkDriverExtraClassPath || key == SparkExecutorExtraClassPath {
				value = AddEscapeCharacter(value)
			}
			args = args + fmt.Sprintf("%s=%s", key, value) + NewLineString
		}
	}
	return args
}
func populateDriverSecrets(args string, app v1beta2.SparkApplication) string {
	for _, s := range app.Spec.Driver.Secrets {
		args = args + fmt.Sprintf("%s%s=%s", SparkDriverSecretKeyPrefix, s.Name, s.Path) + NewLineString
		//secretConfOptions = append(secretConfOptions, conf)
		if s.Type == v1beta2.SecretTypeGCPServiceAccount {
			args = args + fmt.Sprintf(
				"%s%s=%s",
				SparkDriverEnvVarConfigKeyPrefix,
				GoogleApplicationCredentialsEnvVar,
				filepath.Join(s.Path, ServiceAccountJSONKeyFileName)) + NewLineString

		} else if s.Type == v1beta2.SecretTypeHadoopDelegationToken {
			args = args + fmt.Sprintf(
				"%s%s=%s",
				SparkDriverEnvVarConfigKeyPrefix,
				HadoopTokenFileLocationEnvVar,
				filepath.Join(s.Path, HadoopDelegationTokenFileName)) + NewLineString

		}
	}
	return args
}
func populateExecutorAnnotations(args string, app v1beta2.SparkApplication) string {
	for key, value := range app.Spec.Executor.Annotations {
		if key == OpencensusPrometheusTarget {
			value = strings.Replace(value, "\n", "", -1)
			value = AddEscapeCharacter(value)
		}
		args = args + fmt.Sprintf("%s%s=%s", SparkExecutorAnnotationKeyPrefix, key, value) + NewLineString
	}
	return args
}
func populateExecutorSecrets(args string, app v1beta2.SparkApplication) string {
	for _, s := range app.Spec.Executor.Secrets {
		args = args + fmt.Sprintf("%s%s=%s", SparkExecutorSecretKeyPrefix, s.Name, s.Path) + NewLineString
		if s.Type == v1beta2.SecretTypeGCPServiceAccount {
			args = args + fmt.Sprintf(
				"%s%s=%s",
				SparkExecutorEnvVarConfigKeyPrefix,
				GoogleApplicationCredentialsEnvVar,
				filepath.Join(s.Path, ServiceAccountJSONKeyFileName)) + NewLineString

		} else if s.Type == v1beta2.SecretTypeHadoopDelegationToken {
			args = args + fmt.Sprintf(
				"%s%s=%s",
				SparkExecutorEnvVarConfigKeyPrefix,
				HadoopTokenFileLocationEnvVar,
				filepath.Join(s.Path, HadoopDelegationTokenFileName)) + NewLineString
		}
	}
	return args
}
func populateDynamicAllocation(args string, app v1beta2.SparkApplication) string {
	if app.Spec.DynamicAllocation != nil {
		args = args + fmt.Sprintf("%s=true", SparkDynamicAllocationEnabled) + NewLineString
		// Turn on shuffle tracking if dynamic allocation is enabled.
		args = args + fmt.Sprintf("%s=true", SparkDynamicAllocationShuffleTrackingEnabled) + NewLineString
		dynamicAllocation := app.Spec.DynamicAllocation
		if dynamicAllocation.InitialExecutors != nil {
			args = args + fmt.Sprintf("%s=%d", SparkDynamicAllocationInitialExecutors, *dynamicAllocation.InitialExecutors) + NewLineString
		}
		if dynamicAllocation.MinExecutors != nil {
			args = args + fmt.Sprintf("%s=%d", SparkDynamicAllocationMinExecutors, *dynamicAllocation.MinExecutors) + NewLineString
		}
		if dynamicAllocation.MaxExecutors != nil {
			args = args + fmt.Sprintf("%s=%d", SparkDynamicAllocationMaxExecutors, *dynamicAllocation.MaxExecutors) + NewLineString
		}
		if dynamicAllocation.ShuffleTrackingTimeout != nil {
			args = args + fmt.Sprintf("%s=%d", SparkDynamicAllocationShuffleTrackingTimeout, *dynamicAllocation.ShuffleTrackingTimeout) + NewLineString
		}
	}
	return args
}
func populateAppSpecType(args string, app v1beta2.SparkApplication) string {
	appSpecType := app.Spec.Type
	if appSpecType == SparkAppTypeScala || appSpecType == SparkAppTypeJavaCamelCase {
		appSpecType = SparkAppTypeJava
	} else if appSpecType == SparkAppTypePythonWithP {
		appSpecType = SparkAppTypePython
	} else if appSpecType == SparkAppTypeRWithR {
		appSpecType = SparkAppTypeR
	}
	args = args + fmt.Sprintf("%s=%s", SparkApplicationType, appSpecType) + NewLineString
	return args
}
func populateProperties(args string) string {
	sparkDefaultConfFilePath := SparkDefaultsConfigFilePath + DefaultSparkConfFileName
	propertyPairs, propertyFileReadError := properties.LoadFile(sparkDefaultConfFilePath, properties.UTF8)
	if propertyFileReadError == nil {
		keysList := propertyPairs.Keys()
		for _, key := range keysList {
			value := propertyPairs.GetString(key, "")
			args = args + fmt.Sprintf("%s=%s", key, value) + NewLineString
		}
	}
	return args
}
func populateMonitoringInfo(args string, app v1beta2.SparkApplication) string {
	//Monitoring Section
	if app.Spec.Monitoring != nil {
		SparkMetricsNamespace := common.GetAppNamespace(&app) + DotSeparator + app.Name
		args = args + fmt.Sprintf("%s=%s", SparkMetricsNamespaceKey, SparkMetricsNamespace) + NewLineString

		// Spark Metric Properties file
		if app.Spec.Monitoring.MetricsPropertiesFile != nil {
			args = args + fmt.Sprintf("%s=%s", SparkMetricConfKey, *app.Spec.Monitoring.MetricsPropertiesFile) + NewLineString
		}
	}
	return args
}
func populateArtifacts(args string, app v1beta2.SparkApplication) string {
	if len(app.Spec.Deps.Jars) > 0 {
		modifiedJarList := make([]string, 0, len(app.Spec.Deps.Jars))
		for _, sparkjar := range app.Spec.Deps.Jars {
			sparkjar = AddEscapeCharacter(sparkjar)
			modifiedJarList = append(modifiedJarList, sparkjar)
		}
		args = args + SparkJars + EqualsSign + strings.Join(modifiedJarList, CommaSeparator) + NewLineString
	}
	if len(app.Spec.Deps.Files) > 0 {
		args = args + SparkFiles + EqualsSign + strings.Join(app.Spec.Deps.Files, CommaSeparator) + NewLineString
	}
	if len(app.Spec.Deps.PyFiles) > 0 {
		args = args + SparkPyFiles + EqualsSign + strings.Join(app.Spec.Deps.PyFiles, CommaSeparator) + NewLineString
	}
	if len(app.Spec.Deps.Packages) > 0 {
		args = args + SparkPackages + EqualsSign + strings.Join(app.Spec.Deps.Packages, CommaSeparator) + NewLineString
	}
	if len(app.Spec.Deps.ExcludePackages) > 0 {
		args = args + SparkExcludePackages + EqualsSign + strings.Join(app.Spec.Deps.ExcludePackages, CommaSeparator) + NewLineString
	}
	if len(app.Spec.Deps.Repositories) > 0 {
		args = args + SparkRepositories + EqualsSign + strings.Join(app.Spec.Deps.Repositories, CommaSeparator) + NewLineString
	}
	return args
}
func populateContainerImageDetails(args string, app v1beta2.SparkApplication) string {
	if app.Spec.Image != nil {
		sparkContainerImage := AddEscapeCharacter(*app.Spec.Image)
		args = args + fmt.Sprintf("%s=%s", SparkContainerImageKey, sparkContainerImage) + NewLineString
	}
	if app.Spec.ImagePullPolicy != nil {
		args = args + fmt.Sprintf("%s=%s", SparkContainerImagePullPolicyKey, *app.Spec.ImagePullPolicy) + NewLineString
	}
	if len(app.Spec.ImagePullSecrets) > 0 {
		secretNames := strings.Join(app.Spec.ImagePullSecrets, CommaSeparator)
		args = args + fmt.Sprintf("%s=%s", SparkImagePullSecretKey, secretNames) + NewLineString
	}
	return args
}
func populateComputeInfo(sb *strings.Builder, app v1beta2.SparkApplication, sparkConfKeyValuePairs map[string]string) (*strings.Builder, error) {
	if app.Spec.Driver.Cores != nil {
		driverCores := *app.Spec.Driver.Cores
		sb.WriteString(fmt.Sprintf("%s=%d", SparkDriverCores, driverCores))
		sb.WriteString(NewLineString)
	} else if common.CheckSparkConf(sparkConfKeyValuePairs, SparkDriverCores) {
		driverCores, err := strconv.ParseInt(sparkConfKeyValuePairs[SparkDriverCores], 10, 32)
		if err != nil {
			return sb, err
		}
		sb.WriteString(fmt.Sprintf("%s=%d", SparkDriverCores, driverCores))
		sb.WriteString(NewLineString)
	} else { // Driver default cores
		sb.WriteString(fmt.Sprintf("%s=%s", SparkDriverCores, DriverDefaultCores))
		sb.WriteString(NewLineString)
	}

	if app.Spec.Driver.CoreRequest != nil {
		sb.WriteString(fmt.Sprintf("%s=%s", SparkDriverCoreRequestKey, *app.Spec.Driver.CoreRequest))
		sb.WriteString(NewLineString)
	} else if common.CheckSparkConf(sparkConfKeyValuePairs, SparkDriverCoreRequestKey) {
		sb.WriteString(fmt.Sprintf("%s=%s", SparkDriverCoreRequestKey, sparkConfKeyValuePairs[SparkDriverCoreRequestKey]))
		sb.WriteString(NewLineString)
	}

	if app.Spec.Driver.CoreLimit != nil {
		sb.WriteString(fmt.Sprintf("%s=%s", SparkDriverCoreLimitKey, *app.Spec.Driver.CoreLimit))
		sb.WriteString(NewLineString)
	} else if common.CheckSparkConf(sparkConfKeyValuePairs, SparkDriverCoreLimitKey) {
		sb.WriteString(fmt.Sprintf("%s=%s", SparkDriverCoreLimitKey, sparkConfKeyValuePairs[SparkDriverCoreLimitKey]))
		sb.WriteString(NewLineString)
	}

	if app.Spec.Executor.CoreRequest != nil {
		sb.WriteString(fmt.Sprintf("%s=%s", SparkExecutorCoreRequestKey, *app.Spec.Executor.CoreRequest))
		sb.WriteString(NewLineString)
	} else if common.CheckSparkConf(sparkConfKeyValuePairs, SparkExecutorCoreRequestKey) {
		sb.WriteString(fmt.Sprintf("%s=%s", SparkExecutorCoreRequestKey, sparkConfKeyValuePairs[SparkExecutorCoreRequestKey]))
		sb.WriteString(NewLineString)
	}
	return sb, nil
}
func populateMemoryInfo(args string, app v1beta2.SparkApplication, sparkConfKeyValuePairs map[string]string) string {
	if app.Spec.Driver.Memory != nil {
		args = args + fmt.Sprintf("spark.driver.memory=%s", *app.Spec.Driver.Memory) + NewLineString
	} else { //Driver default memory
		args = args + fmt.Sprintf("spark.driver.memory=%s", DriverDefaultMemory) + NewLineString
	}

	if app.Spec.Driver.MemoryOverhead != nil {
		args = args + fmt.Sprintf("spark.driver.memoryOverhead=%v", *app.Spec.Driver.MemoryOverhead) + NewLineString
	} else if common.CheckSparkConf(app.Spec.SparkConf, "spark.driver.memoryOverhead") {
		memoryOverhead := app.Spec.SparkConf["spark.driver.memoryOverhead"]
		args = args + fmt.Sprintf("spark.driver.memoryOverhead=%v", memoryOverhead) + NewLineString
	} else if common.CheckSparkConf(app.Spec.SparkConf, "spark.kubernetes.memoryOverhead") {
		memoryOverhead := app.Spec.SparkConf["spark.kubernetes.memoryOverhead"]
		args = args + fmt.Sprintf("spark.driver.memoryOverhead=%v", memoryOverhead) + NewLineString
	}

	// Property "spark.executor.cores" does not allow float values.
	if app.Spec.Executor.Cores != nil {
		args = args + fmt.Sprintf("%s=%d", SparkExecutorCoreKey, *app.Spec.Executor.Cores) + NewLineString
	} else if common.CheckSparkConf(sparkConfKeyValuePairs, SparkExecutorCoreKey) {
		val, _ := strconv.ParseInt(sparkConfKeyValuePairs[SparkExecutorCoreKey], 10, 32)
		args = args + fmt.Sprintf("%s=%d", SparkExecutorCoreKey, val) + NewLineString
	}

	if app.Spec.Executor.CoreLimit != nil {
		args = args + fmt.Sprintf("%s=%s", SparkExecutorCoreLimitKey, *app.Spec.Executor.CoreLimit) + NewLineString
	} else if common.CheckSparkConf(sparkConfKeyValuePairs, SparkExecutorCoreLimitKey) {
		args = args + fmt.Sprintf("%s=%s", SparkExecutorCoreLimitKey, sparkConfKeyValuePairs[SparkExecutorCoreLimitKey]) + NewLineString
	}

	if app.Spec.Executor.Memory != nil {
		args = args + fmt.Sprintf("spark.executor.memory=%s", *app.Spec.Executor.Memory) + NewLineString
	} else { //Setting default 1g
		//Executor default memory
		args = args + fmt.Sprintf("spark.executor.memory=%s", ExecutorDefaultMemory) + NewLineString
	}

	if app.Spec.Executor.MemoryOverhead != nil {
		args = args + fmt.Sprintf("spark.executor.memoryOverhead=%v", *app.Spec.Executor.MemoryOverhead) + NewLineString
	} else if common.CheckSparkConf(app.Spec.SparkConf, "spark.executor.memoryOverhead") {
		memoryOverhead := app.Spec.SparkConf["spark.executor.memoryOverhead"]
		args = args + fmt.Sprintf("spark.executor.memoryOverhead=%v", memoryOverhead) + NewLineString
	} else if common.CheckSparkConf(app.Spec.SparkConf, "spark.kubernetes.memoryOverhead") {
		memoryOverhead := app.Spec.SparkConf["spark.kubernetes.memoryOverhead"]
		args = args + fmt.Sprintf("spark.executor.memoryOverhead=%v", memoryOverhead) + NewLineString
	}

	return args
}
