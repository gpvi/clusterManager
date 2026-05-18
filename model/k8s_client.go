package model

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func NewK8sClientset() (kubernetes.Interface, *rest.Config, error) {
	kubeConfigPath := KubeConfigPath
	if kubeConfigPath == "" {
		if v := os.Getenv("KUBECONFIG"); v != "" {
			kubeConfigPath = v
		} else {
			home, err := os.UserHomeDir()
			if err == nil {
				kubeConfigPath = filepath.Join(home, ".kube", "config")
			}
		}
	}

	var restCfg *rest.Config
	var err error

	if _, statErr := os.Stat(kubeConfigPath); kubeConfigPath != "" && statErr == nil {
		restCfg, err = clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	} else {
		restCfg, err = rest.InClusterConfig()
		if err != nil {
			return nil, nil, fmt.Errorf("neither kubeconfig (%s) found nor in-cluster config available: %w", kubeConfigPath, err)
		}
	}

	if err != nil {
		return nil, nil, fmt.Errorf("failed to build k8s config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create k8s clientset: %w", err)
	}

	return clientset, restCfg, nil
}
