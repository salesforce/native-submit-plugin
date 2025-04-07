package common

const (
	DefaultUiPort                     = 4040
	DefaultDriverPort                 = 7078
	DefaultBlockManagerPort           = 7079
	SparkDriverPort                   = "spark.driver.port"
	JavaScalaMemoryOverheadFactor     = "0.10"
	OtherLanguageMemoryOverheadFactor = "0.40"
	// SparkAppNamespaceKey is the configuration property for application namespace.
	SparkAppNamespaceKey = "spark.kubernetes.namespace"
	// SparkDriverPodNameKey is the Spark configuration key for driver pod name.
	SparkDriverPodNameKey = "spark.kubernetes.driver.pod.name"
	// SparkImagePullSecretKey is the configuration property for specifying the comma-separated list of image-pull
	// secrets.
	SparkImagePullSecretKey = "spark.kubernetes.container.image.pullSecrets"
	// SparkDriverSecretKeyPrefix is the configuration property prefix for specifying secrets to be mounted into the
	// driver.
	SparkDriverSecretKeyPrefix = "spark.kubernetes.driver.secrets."
	// SparkConfDirEnvVar is the environment variable to add to the driver and executor Pods that point
	// to the directory where the Spark ConfigMap is mounted.
	SparkConfDirEnvVar = "SPARK_CONF_DIR"
	// SparkDriverContainerName is name of driver container in spark driver pod
	SparkDriverContainerName = "spark-kubernetes-driver"
	// SparkDriverSecretKeyRefKeyPrefix is the configuration property prefix for specifying environment variables
	// from SecretKeyRefs for the driver.
	SparkDriverSecretKeyRefKeyPrefix = "spark.kubernetes.driver.secretKeyRef."
	// SparkDriverCoreLimitKey is the configuration property for specifying the hard CPU limit for the driver pod.
	SparkDriverCoreLimitKey = "spark.kubernetes.driver.limit.cores"
	// SparkDriverCoreRequestKey is the configuration property for specifying the physical CPU request for the driver.
	SparkDriverCoreRequestKey = "spark.kubernetes.driver.request.cores"
)
