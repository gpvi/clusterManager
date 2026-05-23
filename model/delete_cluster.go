package model

import (
	"context"
	"fmt"
	"os"

	"redisClusterManager/utils"
)

func DeleteAllContainers(ctx context.Context, cfg *RuntimeConfig, clusterName string) error {
	var err error

	nodeManager, err := NewNodeManager(cfg)
	if err != nil {
		return fmt.Errorf("create node manager fail: %v", err)
	}

	runtimeConfigPath := cfg.ClusterRuntimeConfigPath(clusterName)
	if utils.FileExists(runtimeConfigPath) {
		fmt.Printf("File %s already exists, deleting...\n", runtimeConfigPath)
		err := os.Remove(runtimeConfigPath)
		if err != nil {
			return fmt.Errorf("error deleting file: %v", err)
		}
	}

	err = nodeManager.DeleteResources(ctx, clusterName)
	if err != nil {
		return fmt.Errorf("delete resources fail: %v", err)
	}

	// Remove from SQLite if configured.
	if cfg.DBPath != "" {
		if store, serr := OpenStore(cfg.DBPath); serr == nil {
			defer store.Close()
			store.DeleteCluster(clusterName)
			store.LogOperation(clusterName, "delete", "removed all containers", true)
		}
	}

	clusterStateDir := cfg.ClusterStateDir(clusterName)
	if entries, readErr := os.ReadDir(clusterStateDir); readErr == nil && len(entries) == 0 {
		if err := os.Remove(clusterStateDir); err != nil {
			return fmt.Errorf("error deleting cluster state dir: %v", err)
		}
	}
	return nil
}
