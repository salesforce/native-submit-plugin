package driver

import (
	"fmt"
	"io"
	"log"
	"nativesubmit/common"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
)

type SupplementalGroup []int64

func SupplementalGroups(a int64) SupplementalGroup {
	s := SupplementalGroup{a}
	return s
}
func Int32Pointer(a int32) *int32 {
	return &a
}

func downloadFile(path string, targetDir string, sparkConf map[string]string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("Pod template file's download path is empty")
	}
	uri, err := url.Parse(path)
	if err != nil {
		return "", err
	}

	switch uri.Scheme {
	case "file", "local":
		return path, nil
	case "http", "https", "ftp":
		fname := filepath.Base(uri.Path)
		localFile, _ := doFetchFile(uri.String(), targetDir, fname, sparkConf)
		return localFile, nil
	default:
		fname := filepath.Base(uri.Path)
		localFile, err := doFetchFile(uri.String(), targetDir, fname, sparkConf)
		if err != nil {
			return "", err
		}
		return localFile, nil
	}
}
func doFetchFile(urlStr string, targetDir string, filename string, conf map[string]string) (string, error) {
	targetFile := filepath.Join(targetDir, filename)
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return "", err
	}

	switch scheme := parsedURL.Scheme; scheme {
	case "spark":
		return "", err
	case "http", "https", "ftp":
		fetchTimeoutDuration, _ := time.ParseDuration("5s")
		fetchTimeout, fetchTimeoutExists := conf["spark.files.fetchTimeout"]
		if fetchTimeoutExists {
			fetchTimeoutDuration, _ = time.ParseDuration(fetchTimeout)
		}
		client := http.Client{
			Timeout: fetchTimeoutDuration,
		}
		resp, err := client.Get(urlStr)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		return downloadFileFromURL(resp.Body, targetFile)
	case "file":
		sourceFile := parsedURL.Path
		return copyFile(sourceFile, targetFile, false)
	default:
		return "", err
	}
}
func copyFile(sourceFile string, destFile string, removeSourceFile bool) (string, error) {
	if err := os.Rename(sourceFile, destFile); err != nil {
		return "", err
	}
	if removeSourceFile {
		if err := os.Remove(sourceFile); err != nil {
			return "", err
		}
	}
	return destFile, nil
}
func downloadFileFromURL(reader io.Reader, destFile string) (string, error) {
	tempFile, err := os.CreateTemp(filepath.Dir(destFile), "fetchFileTemp")
	if err != nil {
		return "", err
	}
	defer tempFile.Close()
	defer os.Remove(tempFile.Name())

	if _, err := io.Copy(tempFile, reader); err != nil {
		return "", err
	}

	return copyFile(tempFile.Name(), destFile, true)
}
func createTempDir() string {
	dir, err := os.MkdirTemp("", "spark")
	if err != nil {
		log.Fatal(err)
	}
	return dir
}
func loadPodFromTemplate(templateFileName string, containerName string, conf map[string]string) (apiv1.Pod, error) {
	var file apiv1.Pod
	localFile, err := downloadFile(templateFileName, createTempDir(), conf)
	if err != nil {
		fmt.Errorf("Encountered exception while attempting to download the pod template file : %v", err)
		return file, err
	} else {
		data, err := os.ReadFile(localFile)
		if err != nil {
			return file, err
		}
		manifests := string(data)
		decode := scheme.Codecs.UniversalDeserializer().Decode
		obj, _, err := decode([]byte(manifests), nil, nil)
		if err != nil {
			return file, err
		}
		pod := obj.(*apiv1.Pod)
		newPod := selectSparkContainer(*pod, containerName)
		return newPod, nil
	}
}
func selectSparkContainer(pod apiv1.Pod, containerName string) apiv1.Pod {
	selectNamedContainer := func(containers []apiv1.Container, name string) (*apiv1.Container, []apiv1.Container, bool) {
		var rest []apiv1.Container
		for _, container := range containers {
			if container.Name == name {
				return &container, rest, true
			}
			rest = append(rest, container)
		}
		log.Printf("specified container %s not found on pod template, falling back to taking the first container", name)
		return nil, nil, false
	}

	containers := pod.Spec.Containers
	if containerName != "" {
		if selectedContainer, _, found := selectNamedContainer(containers, containerName); found {
			return apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{*selectedContainer},
				},
			}
		}
	}

	if len(containers) > 0 {
		var containerList []apiv1.Container
		containerList = append(containerList, containers[0])
		for _, container := range containers[1:] {
			containerList = append(containerList, container)
		}
		return apiv1.Pod{
			Spec: apiv1.PodSpec{
				Containers: containerList,
			},
		}

	}
	return apiv1.Pod{}
}
func getBlockManagerPort(sparkConfKeyValuePairs map[string]string) int {
	//BlockManager Port
	blockManagerPortToBeUsed := common.DefaultBlockManagerPort
	sparkBlockManagerPortSupplied, sparkBlockManagerPortSuppliedValueExists := sparkConfKeyValuePairs[SparkBlockManagerPort]
	sparkDriverBlockManagerPortSupplied, sparkDriverBlockManagerPortSuppliedValueExists := sparkConfKeyValuePairs[SparkDriverBlockManagerPort]
	if sparkDriverBlockManagerPortSuppliedValueExists {
		blockManagerPortFromConfig, err := strconv.Atoi(sparkDriverBlockManagerPortSupplied)
		if err != nil {
			panic("Block Manager Port not parseable to integer value - hence failing the spark submit" + fmt.Sprint(err))
		} else {
			blockManagerPortToBeUsed = blockManagerPortFromConfig
		}
	} else if sparkBlockManagerPortSuppliedValueExists {
		blockManagerPortFromConfig, err := strconv.Atoi(sparkBlockManagerPortSupplied)
		if err != nil {
			panic("Driver Block Manager Port not parseable to integer value - hence failing the spark submit" + fmt.Sprint(err))
		} else {
			blockManagerPortToBeUsed = blockManagerPortFromConfig
		}
	}
	return blockManagerPortToBeUsed
}

func addSecret(secret v1beta2.SecretInfo, volumeExtension string, driverPodVolumes []apiv1.Volume, driverPodContainerSpec apiv1.Container) ([]apiv1.Volume, apiv1.Container) {
	secretVolume := apiv1.Volume{
		Name: fmt.Sprintf("%s%s", secret.Name, volumeExtension),
		VolumeSource: apiv1.VolumeSource{
			Secret: &apiv1.SecretVolumeSource{
				SecretName: secret.Name,
			},
		},
	}
	driverPodVolumes = append(driverPodVolumes, secretVolume)

	//Volume mount for the local temp folder
	volumeMount := apiv1.VolumeMount{
		Name:      fmt.Sprintf("%s%s", secret.Name, volumeExtension),
		MountPath: secret.Path,
	}
	driverPodContainerSpec.VolumeMounts = append(driverPodContainerSpec.VolumeMounts, volumeMount)
	return driverPodVolumes, driverPodContainerSpec
}
