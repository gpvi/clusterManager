package action

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"redisClusterManager/cluster/model"
	"redisClusterManager/cluster/model/pipeline"
	"redisClusterManager/cluster/model/store"
	"redisClusterManager/cluster/utils"
)

func ScaleClusterAction(ctx context.Context, cfg *model.RuntimeConfig, additionalShards int, clusterName string) error {
	nodeManager, err := model.NewNodeManager(cfg)
	if err != nil {
		return fmt.Errorf("create node manager fail: %v", err)
	}

	runtimeConfigPath := cfg.ClusterRuntimeConfigPath(clusterName)
	var configFromFile RedisClusterConfig
	if err := utils.ReadFromYAMLFile(runtimeConfigPath, &configFromFile); err != nil {
		return fmt.Errorf("error reading from YAML file: %w", err)
	}
	nodesPerShard := configFromFile.EffectiveNodesPerShard()
	cfg.RedisContainerPort = configFromFile.Port
	fmt.Println("nodesPerShard:", nodesPerShard)

	clusterManager := model.NewClusterManager(nodesPerShard, nodeManager)
	if cfg.CacheInvalidator != nil {
		clusterManager.SetCacheInvalidator(cfg.CacheInvalidator)
	}

	savedMasterIDs := make([]string, len(clusterManager.MasterIDs))
	copy(savedMasterIDs, clusterManager.MasterIDs)

	var persister pipeline.Persister
	if cfg.DBPath != "" {
		if s, err := store.OpenStore(cfg.DBPath); err == nil {
			persister = s
		}
	}
	p := pipeline.New("scale-cluster-"+clusterName, persister)

	// Step 1: validate
	p.Add(pipeline.Step{
		Name: "validate",
		Do: func(ctx context.Context) error {
			if additionalShards <= 0 {
				return fmt.Errorf("additional shards must be greater than 0, got %d", additionalShards)
			}
			if err := nodeManager.ListPodsByCluster(ctx, clusterName); err != nil {
				return err
			}
			if nodeManager.GetNodeCount() == 0 {
				return fmt.Errorf("current pods num is 0: %w", model.ErrClusterNotFound)
			}
			return nil
		},
	})

	// Step 2: sync-state — restore cluster topology from running nodes
	p.Add(pipeline.Step{
		Name: "sync-state",
		Do: func(ctx context.Context) error {
			nodes := nodeManager.GetNodes()
			idx := 0
			for ; idx < nodeManager.GetNodeCount(); idx++ {
				if nodes[idx].ClusterName == clusterName {
					break
				}
			}
			if idx >= nodeManager.GetNodeCount() {
				return fmt.Errorf("cluster %s not found: %w", clusterName, model.ErrClusterNotFound)
			}
			if err := clusterManager.UpdateAfterMeet(ctx, nodes[idx], clusterName); err != nil {
				return fmt.Errorf("init meet info: %w", err)
			}
			if err := clusterManager.UpdateAfterSetNodeRole(ctx, nodes[idx], clusterName); err != nil {
				return fmt.Errorf("init set node role: %w", err)
			}
			if err := clusterManager.UpdateSlots(ctx, nodes[idx], clusterName); err != nil {
				return fmt.Errorf("init slots info: %w", err)
			}
			return nil
		},
	})

	// Step 3: add-shards + migrate
	p.Add(pipeline.Step{
		Name:  "add-shards",
		Retry: 1,
		Do: func(ctx context.Context) error {
			if err := clusterManager.AddShards(ctx, additionalShards, clusterName); err != nil {
				return err
			}
			fmt.Println("starting slot migration...")
			return clusterManager.MigratesSlotsToEmptyNode(ctx, clusterName)
		},
		Undo: func(ctx context.Context) error {
			// Restore master list to pre-scale state (best-effort rollback).
			clusterManager.MasterIDs = savedMasterIDs
			return nodeManager.DeleteResources(ctx, clusterName)
		},
	})

	// Step 4: persist-db
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
			totalShards := len(clusterManager.MasterIDs)
			s.UpsertCluster(store.ClusterRecord{
				Name: clusterName, Backend: cfg.Backend, Shards: totalShards,
				NodesPerShard: nodesPerShard, RedisPort: int(cfg.RedisContainerPort),
				Image: cfg.ImageName, Status: "ready",
			})
			for _, node := range nodeManager.GetNodes() {
				if node.ClusterName != clusterName {
					continue
				}
				nodeIdx, _ := strconv.Atoi(strings.TrimPrefix(node.Name, clusterName+"-redis-"))
				s.UpsertContainer(store.ContainerRecord{
					ClusterName: clusterName, Name: node.Name, ContainerID: node.ID,
					HostIP: node.HostIP, HostPort: int(node.HostPort),
					ContainerIP: node.ConIp, ContainerPort: int(node.ConPort),
					NodeIndex: nodeIdx, Status: "running",
					Hostname: node.Hostname,
				})
			}
			s.LogOperation(clusterName, "scale",
				fmt.Sprintf("added %d shard(s), total=%d", additionalShards, totalShards), true)
			return nil
		},
	})

	if err := p.Run(ctx); err != nil {
		return fmt.Errorf("scale cluster %s: %w", clusterName, err)
	}
	fmt.Println("Scale succeed!")
	return nil
}
