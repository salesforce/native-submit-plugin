package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "nativesubmit/proto/spark"

	"github.com/google/uuid"
	"github.com/kubeflow/spark-operator/api/v1beta2"
	"google.golang.org/grpc"
	wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Helper: Convert proto SparkApplication to v1beta2.SparkApplication
func convertProtoToSparkApplication(protoApp *pb.SparkApplication) *v1beta2.SparkApplication {
	if protoApp == nil {
		return nil
	}

	// Convert SparkApplicationType enum to string
	var appType v1beta2.SparkApplicationType
	switch protoApp.GetSpec().GetType() {
	case pb.SparkApplicationType_SPARK_APPLICATION_TYPE_JAVA:
		appType = v1beta2.SparkApplicationTypeJava
	case pb.SparkApplicationType_SPARK_APPLICATION_TYPE_SCALA:
		appType = v1beta2.SparkApplicationTypeScala
	case pb.SparkApplicationType_SPARK_APPLICATION_TYPE_PYTHON:
		appType = v1beta2.SparkApplicationTypePython
	case pb.SparkApplicationType_SPARK_APPLICATION_TYPE_R:
		appType = v1beta2.SparkApplicationTypeR
	default:
		appType = v1beta2.SparkApplicationTypeJava // default
	}

	// Convert DeployMode enum to string
	var deployMode v1beta2.DeployMode
	switch protoApp.GetSpec().GetMode() {
	case pb.DeployMode_DEPLOY_MODE_CLUSTER:
		deployMode = v1beta2.DeployModeCluster
	case pb.DeployMode_DEPLOY_MODE_CLIENT:
		deployMode = v1beta2.DeployModeClient
	case pb.DeployMode_DEPLOY_MODE_IN_CLUSTER_CLIENT:
		deployMode = v1beta2.DeployModeInClusterClient
	default:
		deployMode = v1beta2.DeployModeCluster // default
	}

	// Helper function to convert wrapper to string pointer
	getStringPtr := func(wrapper *wrapperspb.StringValue) *string {
		if wrapper != nil && wrapper.GetValue() != "" {
			val := wrapper.GetValue()
			return &val
		}
		return nil
	}

	// Helper function to convert wrapper to int32 pointer
	getInt32Ptr := func(wrapper *wrapperspb.Int32Value) *int32 {
		if wrapper != nil {
			val := wrapper.GetValue()
			return &val
		}
		return nil
	}

	// Helper function to convert wrapper to int64 pointer
	getInt64Ptr := func(wrapper *wrapperspb.Int64Value) *int64 {
		if wrapper != nil {
			val := wrapper.GetValue()
			return &val
		}
		return nil
	}

	// Helper function to convert wrapper to bool pointer
	getBoolPtr := func(wrapper *wrapperspb.BoolValue) *bool {
		if wrapper != nil {
			val := wrapper.GetValue()
			return &val
		}
		return nil
	}

	// Ensure UID is not empty - generate one if needed
	uid := protoApp.GetMetadata().GetUid()
	if uid == "" {
		uid = uuid.New().String()
	}

	// Helper function to convert proto Volume to apiv1.Volume
	convertVolume := func(protoVol *pb.Volume) apiv1.Volume {
		if protoVol == nil {
			return apiv1.Volume{}
		}
		return apiv1.Volume{
			Name: protoVol.GetName(),
			// Note: This is a simplified conversion. You may need to handle different volume types
			// based on the protoVol.GetType() value
		}
	}

	// Helper function to convert proto VolumeMount to apiv1.VolumeMount
	convertVolumeMount := func(protoVolMount *pb.VolumeMount) apiv1.VolumeMount {
		if protoVolMount == nil {
			return apiv1.VolumeMount{}
		}
		return apiv1.VolumeMount{
			Name:      protoVolMount.GetName(),
			MountPath: protoVolMount.GetMountPath(),
			ReadOnly:  protoVolMount.GetReadOnly(),
		}
	}

	// Helper function to convert proto EnvVar to apiv1.EnvVar
	convertEnvVar := func(protoEnvVar *pb.EnvVar) apiv1.EnvVar {
		if protoEnvVar == nil {
			return apiv1.EnvVar{}
		}
		return apiv1.EnvVar{
			Name:  protoEnvVar.GetName(),
			Value: protoEnvVar.GetValue(),
			// Note: ValueFrom conversion would need additional logic for complex field references
		}
	}

	// Helper function to convert proto Container to apiv1.Container
	convertContainer := func(protoContainer *pb.Container) apiv1.Container {
		if protoContainer == nil {
			return apiv1.Container{}
		}

		container := apiv1.Container{
			Name:  protoContainer.GetName(),
			Image: protoContainer.GetImage(),
		}

		// Convert command and args
		if len(protoContainer.GetCommand()) > 0 {
			container.Command = protoContainer.GetCommand()
		}
		if len(protoContainer.GetArgs()) > 0 {
			container.Args = protoContainer.GetArgs()
		}

		// Convert env vars
		for _, envVar := range protoContainer.GetEnv() {
			container.Env = append(container.Env, convertEnvVar(envVar))
		}

		// Convert volume mounts
		for _, volMount := range protoContainer.GetVolumeMounts() {
			container.VolumeMounts = append(container.VolumeMounts, convertVolumeMount(volMount))
		}

		return container
	}

	// Helper function to convert proto SparkPodSpec to v1beta2.SparkPodSpec
	convertSparkPodSpec := func(protoPodSpec *pb.SparkPodSpec) v1beta2.SparkPodSpec {
		if protoPodSpec == nil {
			return v1beta2.SparkPodSpec{}
		}

		podSpec := v1beta2.SparkPodSpec{
			Cores:                 getInt32Ptr(protoPodSpec.GetCores()),
			Labels:                protoPodSpec.GetLabels(),
			Annotations:           protoPodSpec.GetAnnotations(),
			NodeSelector:          protoPodSpec.GetNodeSelector(),
			SchedulerName:         getStringPtr(protoPodSpec.GetSchedulerName()),
			ServiceAccount:        getStringPtr(protoPodSpec.GetServiceAccount()),
			HostNetwork:           getBoolPtr(protoPodSpec.GetHostNetwork()),
			ShareProcessNamespace: getBoolPtr(protoPodSpec.GetShareProcessNamespace()),
		}

		// Convert env vars
		for _, envVar := range protoPodSpec.GetEnv() {
			podSpec.Env = append(podSpec.Env, convertEnvVar(envVar))
		}

		// Convert volume mounts
		for _, volMount := range protoPodSpec.GetVolumeMounts() {
			podSpec.VolumeMounts = append(podSpec.VolumeMounts, convertVolumeMount(volMount))
		}

		// Convert containers
		for _, container := range protoPodSpec.GetSidecars() {
			podSpec.Sidecars = append(podSpec.Sidecars, convertContainer(container))
		}

		for _, container := range protoPodSpec.GetInitContainers() {
			podSpec.InitContainers = append(podSpec.InitContainers, convertContainer(container))
		}

		return podSpec
	}

	// Helper function to convert string to int32
	stringToInt32 := func(s string) int32 {
		if s == "" {
			return 0
		}
		// Simple conversion - in production you might want more robust parsing
		var result int32
		_, err := fmt.Sscanf(s, "%d", &result)
		if err != nil {
			return 0
		}
		return result
	}

	// Helper function to convert proto PrometheusSpec to v1beta2.PrometheusSpec
	convertPrometheusSpec := func(protoPrometheus *pb.PrometheusSpec) *v1beta2.PrometheusSpec {
		if protoPrometheus == nil {
			return nil
		}
		return &v1beta2.PrometheusSpec{
			JmxExporterJar: protoPrometheus.GetJmxExporterJar(),
			Port:           getInt32Ptr(protoPrometheus.GetPort()),
			PortName:       getStringPtr(protoPrometheus.GetPortName()),
			ConfigFile:     getStringPtr(protoPrometheus.GetConfigFile()),
			Configuration:  getStringPtr(protoPrometheus.GetConfiguration()),
		}
	}

	// Helper function to convert proto MonitoringSpec to v1beta2.MonitoringSpec
	convertMonitoringSpec := func(protoMonitoring *pb.MonitoringSpec) *v1beta2.MonitoringSpec {
		if protoMonitoring == nil {
			return nil
		}
		return &v1beta2.MonitoringSpec{
			ExposeDriverMetrics:   protoMonitoring.GetExposeDriverMetrics(),
			ExposeExecutorMetrics: protoMonitoring.GetExposeExecutorMetrics(),
			MetricsProperties:     getStringPtr(protoMonitoring.GetMetricsProperties()),
			MetricsPropertiesFile: getStringPtr(protoMonitoring.GetMetricsPropertiesFile()),
			Prometheus:            convertPrometheusSpec(protoMonitoring.GetPrometheus()),
		}
	}

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:        protoApp.GetMetadata().GetName(),
			Namespace:   protoApp.GetMetadata().GetNamespace(),
			UID:         types.UID(uid),
			Labels:      protoApp.GetMetadata().GetLabels(),
			Annotations: protoApp.GetMetadata().GetAnnotations(),
		},
		Spec: v1beta2.SparkApplicationSpec{
			Type:                 appType,
			Mode:                 deployMode,
			Image:                getStringPtr(protoApp.GetSpec().GetImage()),
			ImagePullPolicy:      getStringPtr(protoApp.GetSpec().GetImagePullPolicy()),
			ImagePullSecrets:     protoApp.GetSpec().GetImagePullSecrets(),
			SparkConf:            protoApp.GetSpec().GetSparkConf(),
			HadoopConf:           protoApp.GetSpec().GetHadoopConf(),
			SparkConfigMap:       getStringPtr(protoApp.GetSpec().GetSparkConfigMap()),
			HadoopConfigMap:      getStringPtr(protoApp.GetSpec().GetHadoopConfigMap()),
			Arguments:            protoApp.GetSpec().GetArguments(),
			MainClass:            getStringPtr(protoApp.GetSpec().GetMainClass()),
			MainApplicationFile:  getStringPtr(protoApp.GetSpec().GetMainApplicationFile()),
			ProxyUser:            getStringPtr(protoApp.GetSpec().GetProxyUser()),
			FailureRetries:       getInt32Ptr(protoApp.GetSpec().GetFailureRetries()),
			RetryInterval:        getInt64Ptr(protoApp.GetSpec().GetRetryInterval()),
			MemoryOverheadFactor: getStringPtr(protoApp.GetSpec().GetMemoryOverheadFactor()),
			BatchScheduler:       getStringPtr(protoApp.GetSpec().GetBatchScheduler()),
			TimeToLiveSeconds:    getInt64Ptr(protoApp.GetSpec().GetTimeToLiveSeconds()),
			PythonVersion:        getStringPtr(&wrapperspb.StringValue{Value: protoApp.GetSpec().GetPythonVersion()}),
			SparkVersion:         protoApp.GetSpec().GetSparkVersion(),
		},
	}

	// Handle Dependencies
	if deps := protoApp.GetSpec().GetDeps(); deps != nil {
		app.Spec.Deps = v1beta2.Dependencies{
			Jars:            deps.GetJars(),
			Files:           deps.GetFiles(),
			PyFiles:         deps.GetPyFiles(),
			Packages:        deps.GetPackages(),
			ExcludePackages: deps.GetExcludePackages(),
			Repositories:    deps.GetRepositories(),
			Archives:        deps.GetArchives(),
		}
	}

	// Handle DynamicAllocation
	if dynAlloc := protoApp.GetSpec().GetDynamicAllocation(); dynAlloc != nil {
		initialExecutors := dynAlloc.GetInitialExecutors()
		minExecutors := dynAlloc.GetMinExecutors()
		maxExecutors := dynAlloc.GetMaxExecutors()
		shuffleTimeout := dynAlloc.GetShuffleTrackingTimeout()

		app.Spec.DynamicAllocation = &v1beta2.DynamicAllocation{
			Enabled:                dynAlloc.GetEnabled(),
			InitialExecutors:       &initialExecutors,
			MinExecutors:           &minExecutors,
			MaxExecutors:           &maxExecutors,
			ShuffleTrackingTimeout: &shuffleTimeout,
		}
	}

	// Handle RestartPolicy
	if restartPolicy := protoApp.GetSpec().GetRestartPolicy(); restartPolicy != nil {
		app.Spec.RestartPolicy = v1beta2.RestartPolicy{
			Type: v1beta2.RestartPolicyType(restartPolicy.GetType()),
		}
	}

	// Handle MonitoringSpec
	if monitoring := protoApp.GetSpec().GetMonitoring(); monitoring != nil {
		app.Spec.Monitoring = convertMonitoringSpec(monitoring)
	}

	// Handle DriverSpec
	if driverSpec := protoApp.GetSpec().GetDriver(); driverSpec != nil {
		app.Spec.Driver = v1beta2.DriverSpec{
			SparkPodSpec: convertSparkPodSpec(driverSpec.GetSparkPodSpec()),
		}

		// Convert additional driver-specific fields
		if driverSpec.GetPodName() != nil {
			app.Spec.Driver.PodName = getStringPtr(driverSpec.GetPodName())
		}
		if driverSpec.GetCoreRequest() != nil {
			app.Spec.Driver.CoreRequest = getStringPtr(driverSpec.GetCoreRequest())
		}
		if driverSpec.GetJavaOptions() != nil {
			app.Spec.Driver.JavaOptions = getStringPtr(driverSpec.GetJavaOptions())
		}
		if driverSpec.GetKubernetesMaster() != nil {
			app.Spec.Driver.KubernetesMaster = getStringPtr(driverSpec.GetKubernetesMaster())
		}
		if driverSpec.GetPriorityClassName() != nil {
			app.Spec.Driver.PriorityClassName = getStringPtr(driverSpec.GetPriorityClassName())
		}

		// Convert service annotations and labels
		if len(driverSpec.GetServiceAnnotations()) > 0 {
			app.Spec.Driver.ServiceAnnotations = driverSpec.GetServiceAnnotations()
		}
		if len(driverSpec.GetServiceLabels()) > 0 {
			app.Spec.Driver.ServiceLabels = driverSpec.GetServiceLabels()
		}

		// Convert ports
		for _, port := range driverSpec.GetPorts() {
			app.Spec.Driver.Ports = append(app.Spec.Driver.Ports, v1beta2.Port{
				Name:          port.GetName(),
				Protocol:      port.GetProtocol(),
				ContainerPort: stringToInt32(port.GetContainerPort()),
			})
		}
	}

	// Handle ExecutorSpec
	if executorSpec := protoApp.GetSpec().GetExecutor(); executorSpec != nil {
		app.Spec.Executor = v1beta2.ExecutorSpec{
			SparkPodSpec: convertSparkPodSpec(executorSpec.GetSparkPodSpec()),
		}

		// Convert additional executor-specific fields
		if executorSpec.GetInstances() != nil {
			app.Spec.Executor.Instances = getInt32Ptr(executorSpec.GetInstances())
		}
		if executorSpec.GetCoreRequest() != nil {
			app.Spec.Executor.CoreRequest = getStringPtr(executorSpec.GetCoreRequest())
		}
		if executorSpec.GetJavaOptions() != nil {
			app.Spec.Executor.JavaOptions = getStringPtr(executorSpec.GetJavaOptions())
		}
		if executorSpec.GetDeleteOnTermination() != nil {
			app.Spec.Executor.DeleteOnTermination = getBoolPtr(executorSpec.GetDeleteOnTermination())
		}
		if executorSpec.GetPriorityClassName() != nil {
			app.Spec.Executor.PriorityClassName = getStringPtr(executorSpec.GetPriorityClassName())
		}

		// Convert ports
		for _, port := range executorSpec.GetPorts() {
			app.Spec.Executor.Ports = append(app.Spec.Executor.Ports, v1beta2.Port{
				Name:          port.GetName(),
				Protocol:      port.GetProtocol(),
				ContainerPort: stringToInt32(port.GetContainerPort()),
			})
		}
	}

	// Handle Volumes
	for _, volume := range protoApp.GetSpec().GetVolumes() {
		app.Spec.Volumes = append(app.Spec.Volumes, convertVolume(volume))
	}

	return app
}

type server struct {
	pb.UnimplementedSparkSubmitServiceServer
}

func (s *server) RunAltSparkSubmit(ctx context.Context, req *pb.RunAltSparkSubmitRequest) (*pb.RunAltSparkSubmitResponse, error) {
	start := time.Now()

	app := convertProtoToSparkApplication(req.GetSparkApplication())
	success, err := runAltSparkSubmit(app, req.GetSubmissionId())

	// Record metrics
	appType := "unknown"
	if app != nil && app.Spec.Type != "" {
		appType = string(app.Spec.Type)
	}
	RecordSparkApplicationMetrics(appType, success, time.Since(start))

	if err != nil {
		return &pb.RunAltSparkSubmitResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}
	return &pb.RunAltSparkSubmitResponse{
		Success:      success,
		ErrorMessage: "",
	}, nil
}

// HTTP health check handler
func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// readinessCheckHandler checks if the service is ready to serve requests
func readinessCheckHandler(w http.ResponseWriter, r *http.Request) {
	// Add any readiness checks here (e.g., database connectivity, dependencies)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ready"))
}

func main() {
	log.Println("Starting native-submit gRPC service...")

	// Get port from environment or use default
	port := os.Getenv("GRPC_PORT")
	if port == "" {
		port = "50051"
	}

	// Get health check port from environment or use default
	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "9090"
	}

	log.Printf("Using gRPC port: %s, Health port: %s", port, healthPort)

	// Create error channels for goroutines
	healthErr := make(chan error, 1)
	grpcErr := make(chan error, 1)

	// Start HTTP health check server
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", healthCheckHandler)
		mux.HandleFunc("/readyz", readinessCheckHandler)

		log.Printf("Health check server listening on :%s", healthPort)
		if err := http.ListenAndServe(":"+healthPort, mux); err != nil {
			log.Printf("Health check server error: %v", err)
			healthErr <- err
		}
	}()

	// Start gRPC server
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(metricsUnaryInterceptor()),
	)
	pb.RegisterSparkSubmitServiceServer(grpcServer, &server{})

	log.Printf("gRPC server listening on :%s", port)

	// Start gRPC server in goroutine
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server error: %v", err)
			grpcErr <- err
		}
	}()

	// Wait for interrupt signal or server errors
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Service started successfully. Waiting for signals or errors...")

	// Keep the main process running
	select {
	case <-quit:
		log.Println("Received shutdown signal, shutting down gRPC server...")
		grpcServer.GracefulStop()
		log.Println("Server stopped")
	case err := <-healthErr:
		log.Printf("Health server failed: %v", err)
		grpcServer.GracefulStop()
		log.Fatalf("Health server error: %v", err)
	case err := <-grpcErr:
		log.Printf("gRPC server failed: %v", err)
		log.Fatalf("gRPC server error: %v", err)
	}

	// This should never be reached, but just in case
	log.Println("Main process completed unexpectedly")
}
