package service

const (
	// SparkApplicationSelectorLabel is the AppID set by the spark-distribution on the driver/executors Pods.
	SparkApplicationSelectorLabel  = "spark-app-selector"
	DotSeparator                   = "."
	None                           = "None"
	DriverPortName                 = "driver-rpc-port"
	BlockManagerPortName           = "blockmanager"
	Protocol                       = "TCP"
	UiPortName                     = "spark-ui"
	UiPort                         = 4040
	DriverPortProperty             = "spark.driver.port"
	ClusterIP                      = "ClusterIP"
	DriverBlockManagerPortProperty = "spark.driver.blockManager.port"
	// SparkAppNameLabel is the name of the label for the SparkApplication object name.
	SparkAppNameLabel = LabelAnnotationPrefix + "app-name"
	// LabelAnnotationPrefix is the prefix of every labels and annotations added by the controller.
	LabelAnnotationPrefix = "sparkoperator.k8s.io/"
)
