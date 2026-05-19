package model

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"redisClusterManager/utils"
)

func ScaleClusterAction(ctx context.Context, cfg *RuntimeConfig, additionalShards int, clusterName string) error {
	var err error

	nodeManager, err := NewNodeManager(cfg)
	if err != nil {
		return fmt.Errorf("create node manager fail: %v", err)
	}

	runtimeConfigPath := cfg.ClusterRuntimeConfigPath(clusterName)
	var configFromFile RedisClusterConfig
	err = utils.ReadFromYAMLFile(runtimeConfigPath, &configFromFile)
	if err != nil {
		return fmt.Errorf("error reading from YAML file: %s", err)
	}
	nodesPerShard := configFromFile.EffectiveNodesPerShard()
	cfg.RedisContainerPort = configFromFile.Port
	fmt.Println("nodesPerShard:")
	fmt.Println(nodesPerShard)

	clusterManager := NewClusterManager(nodesPerShard, nodeManager)

	err = nodeManager.ListPodsByCluster(ctx, clusterName)
	if err != nil {
		return err
	}

	if nodeManager.GetNodeCount() == 0 {
		return fmt.Errorf("current pods num is 0, please create cluster first: %w", ErrClusterNotFound)
	}

	nodes := nodeManager.GetNodes()
	ClusterNodeIndex := 0
	for ; ClusterNodeIndex < nodeManager.GetNodeCount(); ClusterNodeIndex++ {
		if nodes[ClusterNodeIndex].ClusterName == clusterName {
			break
		}
	}
	if ClusterNodeIndex >= nodeManager.GetNodeCount() {
		return fmt.Errorf("cluster with name %s not found: %w", clusterName, ErrClusterNotFound)
	}
	err = clusterManager.UpdateAfterMeet(ctx, nodes[ClusterNodeIndex], clusterName)
	if err != nil {
		return fmt.Errorf("init meet Info fail when add shards %v", err)
	}
	err = clusterManager.UpdateAfterSetNodeRole(ctx, nodes[ClusterNodeIndex], clusterName)
	if err != nil {
		return fmt.Errorf("init set node role info fail when add shard %v", err)
	}
	err = clusterManager.UpdateSlots(ctx, nodes[ClusterNodeIndex], clusterName)
	if err != nil {
		return fmt.Errorf("init slots info fail when add shard%v", err)
	}

	if additionalShards <= 0 {
		return fmt.Errorf("additional shards must be greater than 0, got %d", additionalShards)
	}
	err = clusterManager.AddShards(ctx, additionalShards, clusterName)
	if err != nil {
		return err
	}
	fmt.Println("starting slot migration...")
	err = clusterManager.MigratesSlotsToEmptyNode(ctx, clusterName)
	if err != nil {
		return err
	}

	// Persist updated state to SQLite.
	if cfg.DBPath != "" {
		if store, serr := OpenStore(cfg.DBPath); serr == nil {
			defer store.Close()
			totalShards := len(clusterManager.MasterIDs)
			store.UpsertCluster(ClusterRecord{
				Name:          clusterName,
				Backend:       cfg.Backend,
				Shards:        totalShards,
				NodesPerShard: nodesPerShard,
				RedisPort:     int(cfg.RedisContainerPort),
				Image:         cfg.ImageName,
				Status:        "ready",
			})
			for _, node := range nodeManager.GetNodes() {
				if node.ClusterName != clusterName {
					continue
				}
				nodeIdx, _ := strconv.Atoi(strings.TrimPrefix(node.Name, clusterName+"-redis-"))
				store.UpsertContainer(ContainerRecord{
					ClusterName: clusterName, Name: node.Name, ContainerID: node.ID,
					HostIP: node.HostIP, HostPort: int(node.HostPort),
					ContainerIP: node.ConIp, ContainerPort: int(node.ConPort),
					NodeIndex: nodeIdx, Status: "running",
				})
			}
			store.LogOperation(clusterName, "scale", fmt.Sprintf("added %d shard(s), total=%d", additionalShards, totalShards), true)
		}
	}

	return nil
}
