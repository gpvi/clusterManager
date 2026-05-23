package ops

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

	// ---- Step 1: delete-config ----
	p.Add(pipeline.Step{
		Name: "delete-config",
		Do: func(ctx context.Context) error {
			path := cfg.ClusterRuntimeConfigPath(clusterName)
			if utils.FileExists(path) {
				fmt.Printf("removing runtime config: %s\n", path)
				return os.Remove(path)
			}
			return nil
		},
	})

	// ---- Step 2: delete-resources ----
	p.Add(pipeline.Step{
		Name:  "delete-resources",
		Retry: 2,
		Do: func(ctx context.Context) error {
			return nodeManager.DeleteResources(ctx, clusterName)
		},
	})

	// ---- Step 3: persist-db ----
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
			if persister != nil {
				persister.DeletePipeline(p.Name)
			}
			return nil
		},
	})

	// ---- Step 4: cleanup-state ----
	p.Add(pipeline.Step{
		Name: "cleanup-state",
		Do: func(ctx context.Context) error {
			dir := cfg.ClusterStateDir(clusterName)
			entries, err := os.ReadDir(dir)
			if err != nil {
				return nil // already gone
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
