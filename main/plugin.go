package main

import (
	"fmt"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type NativeSubmit struct{}

func (a *NativeSubmit) LaunchSparkApplication(app *v1beta2.SparkApplication, cl client.Client) error {
	if app == nil {
		return fmt.Errorf("spark application cannot be nil")
	}
	fmt.Println("Launching spark application")
	return runAltSparkSubmitWrapper(app, cl)
}

func New() interface{} {
	return &NativeSubmit{}
}

func main() {
	fmt.Println("Native Submit Plugin")
}
