package configmap

const (
	ExecutorDefaultMemory          = "1g"
	DriverDefaultMemory            = "1024m"
	DriverDefaultCores             = "1"
	SparkDriverCores               = "spark.driver.cores"
	SparkFiles                     = "spark.files"
	SparkPyFiles                   = "spark.pyFiles"
	SparkPackages                  = "spark.packages"
	SparkExcludePackages           = "spark.excludePackages"
	SparkRepositories              = "spark.repositories"
	CommaSeparator                 = ","
	EqualsSign                     = "="
	SparkMetricConfKey             = "spark.metrics.conf"
	SparkMetricsNamespaceKey       = "spark.metrics.namespace"
	DefaultSparkConfFileName       = "spark-defaults.conf"
	SparkDriverArgPropertyFilePath = "/opt/spark/conf/spark.properties"
	SparkDefaultsConfigFilePath    = "/opt/spark/conf/"
	SparkApplicationType           = "spark.kubernetes.resource.type"
	SparkAppTypeR                  = "r"
	SparkAppTypeRWithR             = "R"
	SparkAppTypeJava               = "java"
	SparkAppTypeJavaCamelCase      = "Java"
	SparkAppTypeScala              = "Scala"
	SparkAppTypePython             = "python"
	SparkAppTypePythonWithP        = "Python"
	SparkDriverExtraClassPath      = "spark.driver.extraClassPath"
	SparkExecutorExtraClassPath    = "spark.executor.extraClassPath"
	OpencensusPrometheusTarget     = "opencensus.k8s-integration.com/prometheus-targets"
	SparkJars                      = "spark.jars"
	SparkUIProxyRedirectURI        = "spark.ui.proxyRedirectUri"
	SparkUIProxyBase               = "spark.ui.proxyBase"
	ForwardSlash                   = "/"
	SparkApplicationSubmitTime     = "spark.app.submitTime"
	SparkDriverBlockManagerPort    = "spark.driver.blockManager.port"
	True                           = "true"
	SubmitInDriver                 = "spark.kubernetes.submitInDriver"
	SparkPropertiesFileName        = "spark.properties"
	HadoopConfDir                  = "HADOOP_CONF_DIR"
	HadoopConfDirPath              = "/opt/hadoop/conf"
	SparkEnvScriptFileName         = "spark-env.sh"
	SparkEnvScriptFileCommand      = "export SPARK_LOCAL_IP=$(hostname -i)\n"
	SparkSubmitDeploymentMode      = "spark.submit.deployMode"
	ServiceShortForm               = "svc"
	SparkAppId                     = "spark.app.id"
	SparkMaster                    = "spark.master"
	NewLineString                  = "\n"
	SparkDriverHost                = "spark.driver.host"
	DotSeparator                   = "."
	// SparkConfDirEnvVar is the environment variable to add to the driver and executor Pods that point
	// to the directory where the Spark ConfigMap is mounted.
	SparkConfDirEnvVar = "SPARK_CONF_DIR"
	// LabelAnnotationPrefix is the prefix of every labels and annotations added by the controller.
	LabelAnnotationPrefix = "sparkoperator.k8s.io/"
	// SparkAppNameLabel is the name of the label for the SparkApplication object name.
	SparkAppNameLabel = LabelAnnotationPrefix + "app-name"
	// ScheduledSparkAppNameLabel is the name of the label for the ScheduledSparkApplication object name.
	ScheduledSparkAppNameLabel = LabelAnnotationPrefix + "scheduled-app-name"
	// LaunchedBySparkOperatorLabel is a label on Spark pods launched through the Spark Operator.
	LaunchedBySparkOperatorLabel = LabelAnnotationPrefix + "launched-by-spark-operator"
	// SparkApplicationSelectorLabel is the AppID set by the spark-distribution on the driver/executors Pods.
	SparkApplicationSelectorLabel = "spark-app-selector"
	// SparkRoleLabel is the driver/executor label set by the operator/spark-distribution on the driver/executors Pods.
	SparkRoleLabel = "spark-role"
	// SparkDriverRole is the value of the spark-role label for the driver.
	SparkDriverRole = "driver"
	// SparkExecutorRole is the value of the spark-role label for the executors.
	SparkExecutorRole = "executor"
	// SubmissionIDLabel is the label that records the submission ID of the current run of an application.
	SubmissionIDLabel = LabelAnnotationPrefix + "submission-id"
	// SparkAppNameKey is the configuration property for application name.
	SparkAppNameKey = "spark.app.name"
	// SparkAppNamespaceKey is the configuration property for application namespace.
	SparkAppNamespaceKey = "spark.kubernetes.namespace"
	// SparkContainerImageKey is the configuration property for specifying the unified container image.
	SparkContainerImageKey = "spark.kubernetes.container.image"
	// SparkImagePullSecretKey is the configuration property for specifying the comma-separated list of image-pull
	// secrets.
	SparkImagePullSecretKey = "spark.kubernetes.container.image.pullSecrets"
	// SparkContainerImagePullPolicyKey is the configuration property for specifying the container image pull policy.
	SparkContainerImagePullPolicyKey = "spark.kubernetes.container.image.pullPolicy"
	// SparkNodeSelectorKeyPrefix is the configuration property prefix for specifying node selector for the pods.
	SparkNodeSelectorKeyPrefix = "spark.kubernetes.node.selector."
	//SparkDriverNodeSelectorKeyPrefix is the configuration property prefix for specifying node selector for the driver pods.
	SparkDriverNodeSelectorKeyPrefix = "spark.kubernetes.driver.node.selector."
	//SparkExecutorNodeSelectorKeyPrefix is the configuration property prefix for specifying node selector for the driver pods.
	SparkExecutorNodeSelectorKeyPrefix = "spark.kubernetes.executor.node.selector."
	// SparkDriverContainerImageKey is the configuration property for specifying a custom driver container image.
	SparkDriverContainerImageKey = "spark.kubernetes.driver.container.image"
	// SparkExecutorContainerImageKey is the configuration property for specifying a custom executor container image.
	SparkExecutorContainerImageKey = "spark.kubernetes.executor.container.image"
	// SparkDriverCoreRequestKey is the configuration property for specifying the physical CPU request for the driver.
	SparkDriverCoreRequestKey = "spark.kubernetes.driver.request.cores"
	// SparkExecutorCoreRequestKey is the configuration property for specifying the physical CPU request for executors.
	SparkExecutorCoreRequestKey = "spark.kubernetes.executor.request.cores"
	SparkExecutorCoreKey        = "spark.executor.cores"
	// SparkDriverCoreLimitKey is the configuration property for specifying the hard CPU limit for the driver pod.
	SparkDriverCoreLimitKey = "spark.kubernetes.driver.limit.cores"
	// SparkExecutorCoreLimitKey is the configuration property for specifying the hard CPU limit for the executor pods.
	SparkExecutorCoreLimitKey = "spark.kubernetes.executor.limit.cores"
	// SparkDriverSecretKeyPrefix is the configuration property prefix for specifying secrets to be mounted into the
	// driver.
	SparkDriverSecretKeyPrefix = "spark.kubernetes.driver.secrets."
	// SparkExecutorSecretKeyPrefix is the configuration property prefix for specifying secrets to be mounted into the
	// executors.
	SparkExecutorSecretKeyPrefix = "spark.kubernetes.executor.secrets."
	// SparkDriverSecretKeyRefKeyPrefix is the configuration property prefix for specifying environment variables
	// from SecretKeyRefs for the driver.
	SparkDriverSecretKeyRefKeyPrefix = "spark.kubernetes.driver.secretKeyRef."
	// SparkExecutorSecretKeyRefKeyPrefix is the configuration property prefix for specifying environment variables
	// from SecretKeyRefs for the executors.
	SparkExecutorSecretKeyRefKeyPrefix = "spark.kubernetes.executor.secretKeyRef."
	// SparkDriverEnvVarConfigKeyPrefix is the Spark configuration prefix for setting environment variables
	// into the driver.
	SparkDriverEnvVarConfigKeyPrefix = "spark.kubernetes.driverEnv."
	// SparkExecutorEnvVarConfigKeyPrefix is the Spark configuration prefix for setting environment variables
	// into the executor.
	SparkExecutorEnvVarConfigKeyPrefix = "spark.executorEnv."
	// SparkDriverAnnotationKeyPrefix is the Spark configuration key prefix for annotations on the driver Pod.
	SparkDriverAnnotationKeyPrefix = "spark.kubernetes.driver.annotation."
	// SparkExecutorAnnotationKeyPrefix is the Spark configuration key prefix for annotations on the executor Pods.
	SparkExecutorAnnotationKeyPrefix = "spark.kubernetes.executor.annotation."
	// SparkDriverLabelKeyPrefix is the Spark configuration key prefix for labels on the driver Pod.
	SparkDriverLabelKeyPrefix = "spark.kubernetes.driver.label."
	// SparkExecutorLabelKeyPrefix is the Spark configuration key prefix for labels on the executor Pods.
	SparkExecutorLabelKeyPrefix = "spark.kubernetes.executor.label."
	// SparkDriverVolumesPrefix is the Spark volumes configuration for mounting a volume into the driver pod.
	SparkDriverVolumesPrefix = "spark.kubernetes.driver.volumes."
	// SparkExecutorVolumesPrefix is the Spark volumes configuration for mounting a volume into the driver pod.
	SparkExecutorVolumesPrefix = "spark.kubernetes.executor.volumes."
	// SparkDriverPodNameKey is the Spark configuration key for driver pod name.
	SparkDriverPodNameKey = "spark.kubernetes.driver.pod.name"
	// SparkDriverServiceAccountName is the Spark configuration key for specifying name of the Kubernetes service
	// account used by the driver pod.
	SparkDriverServiceAccountName = "spark.kubernetes.authenticate.driver.serviceAccountName"
	// account used by the executor pod.
	SparkExecutorAccountName = "spark.kubernetes.authenticate.executor.serviceAccountName"
	// SparkWaitAppCompletion is the Spark configuration key for specifying whether to wait for application to complete.
	SparkWaitAppCompletion = "spark.kubernetes.submission.waitAppCompletion"
	// SparkPythonVersion is the Spark configuration key for specifying python version used.
	SparkPythonVersion = "spark.kubernetes.pyspark.pythonVersion"
	// SparkMemoryOverheadFactor is the Spark configuration key for specifying memory overhead factor used for Non-JVM memory.
	SparkMemoryOverheadFactor = "spark.kubernetes.memoryOverheadFactor"
	// SparkDriverJavaOptions is the Spark configuration key for a string of extra JVM options to pass to driver.
	SparkDriverJavaOptions = "spark.driver.extraJavaOptions"
	// SparkExecutorJavaOptions is the Spark configuration key for a string of extra JVM options to pass to executors.
	SparkExecutorJavaOptions = "spark.executor.extraJavaOptions"
	// SparkExecutorDeleteOnTermination is the Spark configuration for specifying whether executor pods should be deleted in case of failure or normal termination
	SparkExecutorDeleteOnTermination = "spark.kubernetes.executor.deleteOnTermination"
	// SparkDriverKubernetesMaster is the Spark configuration key for specifying the Kubernetes master the driver use
	// to manage executor pods and other Kubernetes resources.
	SparkDriverKubernetesMaster = "spark.kubernetes.driver.master"
	// SparkDriverServiceAnnotationKeyPrefix is the key prefix of annotations to be added to the driver service.
	SparkDriverServiceAnnotationKeyPrefix = "spark.kubernetes.driver.service.annotation."
	// SparkDynamicAllocationEnabled is the Spark configuration key for specifying if dynamic
	// allocation is enabled or not.
	SparkDynamicAllocationEnabled = "spark.dynamicAllocation.enabled"
	// SparkDynamicAllocationShuffleTrackingEnabled is the Spark configuration key for
	// specifying if shuffle data tracking is enabled.
	SparkDynamicAllocationShuffleTrackingEnabled = "spark.dynamicAllocation.shuffleTracking.enabled"
	// SparkDynamicAllocationShuffleTrackingTimeout is the Spark configuration key for specifying
	// the shuffle tracking timeout in milliseconds if shuffle tracking is enabled.
	SparkDynamicAllocationShuffleTrackingTimeout = "spark.dynamicAllocation.shuffleTracking.timeout"
	// SparkDynamicAllocationInitialExecutors is the Spark configuration key for specifying
	// the initial number of executors to request if dynamic allocation is enabled.
	SparkDynamicAllocationInitialExecutors = "spark.dynamicAllocation.initialExecutors"
	// SparkDynamicAllocationMinExecutors is the Spark configuration key for specifying the
	// lower bound of the number of executors to request if dynamic allocation is enabled.
	SparkDynamicAllocationMinExecutors = "spark.dynamicAllocation.minExecutors"
	// SparkDynamicAllocationMaxExecutors is the Spark configuration key for specifying the
	// upper bound of the number of executors to request if dynamic allocation is enabled.
	SparkDynamicAllocationMaxExecutors = "spark.dynamicAllocation.maxExecutors"
	// GoogleApplicationCredentialsEnvVar is the environment variable used by the
	// Application Default Credentials mechanism. More details can be found at
	// https://developers.google.com/identity/protocols/application-default-credentials.
	GoogleApplicationCredentialsEnvVar = "GOOGLE_APPLICATION_CREDENTIALS"
	// ServiceAccountJSONKeyFileName is the assumed name of the service account
	// Json key file. This name is added to the service account secret mount path to
	// form the path to the Json key file referred to by GOOGLE_APPLICATION_CREDENTIALS.
	ServiceAccountJSONKeyFileName = "key.json"
	// HadoopTokenFileLocationEnvVar is the environment variable for specifying the location
	// where the file storing the Hadoop delegation token is located.
	HadoopTokenFileLocationEnvVar = "HADOOP_TOKEN_FILE_LOCATION"
	// HadoopDelegationTokenFileName is the assumed name of the file storing the Hadoop
	// delegation token. This name is added to the delegation token secret mount path to
	// form the path to the file referred to by HADOOP_TOKEN_FILE_LOCATION.
	HadoopDelegationTokenFileName = "hadoop.token"
	// SparkDriverContainerName is name of driver container in spark driver pod
	SparkDriverContainerName = "spark-kubernetes-driver"
	// SparkLocalDirVolumePrefix is the volume name prefix for "scratch" space directory
	SparkLocalDirVolumePrefix = "spark-local-dir-"
)
