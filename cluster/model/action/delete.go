package action

import (
	"context"
	"fmt"
	"os"

	"redisClusterManager/cluster/model"
	"redisClusterManager/cluster/model/pipeline"
	"redisClusterManager/cluster/model/store"
	"redisClusterManager/cluster/utils"
)

func DeleteAllContainers(ctx context.Context, cfg *model.RuntimeConfig, clusterName string) error {
	nodeManager, err := model.NewNodeManager(cfg)
	if err != nil {
		return fmt.Errorf("create node manager fail: %v", err)
	}

	var persister pipeline.Persister
	if cfg.DBPath != "" {
		if s, err := store.OpenStore(cfg.DBPath); err == nil {
			persister = s
		}
	}
	p := pipeline.New("delete-cluster-"+clusterName, persister)

	// Step 1: delete-resources — remove containers/pods
	p.Add(pipeline.Step{
		Name: "delete-resources",
		Do: func(ctx context.Context) error {
			// Remove runtime config file first.
			runtimeConfigPath := cfg.ClusterRuntimeConfigPath(clusterName)
			if utils.FileExists(runtimeConfigPath) {
				fmt.Printf("File %s exists, deleting...\n", runtimeConfigPath)
				if err := os.Remove(runtimeConfigPath); err != nil {
					return fmt.Errorf("delete config file: %w", err)
				}
			}
			return nodeManager.DeleteResources(ctx, clusterName)
		},
		Retry: 2,
	})

	// Step 2: persist-db — update SQLite records
	p.Add(pipeline.Step{
		Name: "persist-db",
		Do: func(ctx context.Context) error {
			if cfg.DBPath == "" {
				return nil
			}
			s, err := store.OpenStore(cfg.DBPath)
			if err != nil {
				return err
			}
			defer s.Close()
			s.DeleteCluster(clusterName)
			s.LogOperation(clusterName, "delete", "removed all containers", true)
			return nil
		},
	})

	// Step 3: cleanup-state — remove empty state directory
	p.Add(pipeline.Step{
		Name: "cleanup-state",
		Do: func(ctx context.Context) error {
			dir := cfg.ClusterStateDir(clusterName)
			entries, err := os.ReadDir(dir)
			if err != nil {
				return nil // already gone, not an error
			}
			if len(entries) == 0 {
				return os.Remove(dir)
			}
			return nil
		},
	})

	if err := p.Run(ctx); err != nil {
		return fmt.Errorf("delete cluster %s: %w", clusterName, err)
	}
	fmt.Println("Delete succeed!")
	return nil
}
