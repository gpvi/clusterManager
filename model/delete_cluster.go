package model

import (
	"context"
	"fmt"
	"os"
	"redisClusterManager/utils"
)

func DeleteAllContainers(ctx context.Context, clusterName string) error {
	var err error
	err = InitConfig()
	if err != nil {
		return fmt.Errorf("init config fail: %v", err)
	}

	clientset, _, err := NewK8sClientset()
	if err != nil {
		return fmt.Errorf("create k8s clientset fail: %v", err)
	}

	nodeManager := NewK8sNodeManager(clientset, KubeNamespace)

	runtimeConfigPath := ClusterRuntimeConfigPath(clusterName)
	if utils.FileExists(runtimeConfigPath) {
		fmt.Printf("File %s already exists, deleting...\n", runtimeConfigPath)
		err := os.Remove(runtimeConfigPath)
		if err != nil {
			return fmt.Errorf("error deleting file: %v", err)
		}
	}

	containerFile := ClusterContainerInfoPath(clusterName)
	if utils.FileExists(containerFile) {
		fmt.Printf("File %s already exists, deleting...\n", containerFile)
		err := os.Remove(containerFile)
		if err != nil {
			return fmt.Errorf("error deleting file: %v", err)
		}
	}

	err = nodeManager.DeleteResources(ctx, clusterName)
	if err != nil {
		return fmt.Errorf("delete resources fail: %v", err)
	}

	clusterStateDir := ClusterStateDir(clusterName)
	if entries, readErr := os.ReadDir(clusterStateDir); readErr == nil && len(entries) == 0 {
		if err := os.Remove(clusterStateDir); err != nil {
			return fmt.Errorf("error deleting cluster state dir: %v", err)
		}
	}
	return nil
}
