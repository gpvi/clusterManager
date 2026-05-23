package model

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"redisClusterManager/utils"
)

type RedisClusterConfig struct {
	NodesPerShard int    `yaml:"nodes_per_shard"`
	LegacyReplica int    `yaml:"replica,omitempty"`
	Port          uint16 `yaml:"port"`
}

func (c RedisClusterConfig) EffectiveNodesPerShard() int {
	if c.NodesPerShard != 0 {
		return c.NodesPerShard
	}
	return c.LegacyReplica
}

func CreateClusterAction(ctx context.Context, cfg *RuntimeConfig, shardCount int, nodesPerShard int, clusterName string) error {
	var err error

	nodeManager, err := NewNodeManager(cfg)
	if err != nil {
		return fmt.Errorf("create node manager fail: %v", err)
	}
	clusterManager := NewClusterManager(nodesPerShard, nodeManager)

	if shardCount <= 0 {
		return fmt.Errorf("shard count must be greater than 0")
	}
	if nodesPerShard < 2 {
		return fmt.Errorf("nodes per shard must be at least 2")
	}
	if clusterName == "" {
		return fmt.Errorf("cluster name must not be empty")
	}

	err = nodeManager.ListPodsByCluster(ctx, clusterName)
	if err != nil {
		return fmt.Errorf("list cluster pods fail: %v", err)
	}
	if nodeManager.HasCluster(clusterName) {
		return fmt.Errorf("cluster %s already exists with %d pod(s), please delete it before recreating: %w", clusterName, nodeManager.CountByCluster(clusterName), ErrClusterExists)
	}

	defer func() {
		if err == nil {
			return
		}
		if cleanupErr := nodeManager.DeleteResources(ctx, clusterName); cleanupErr != nil {
			fmt.Printf("rollback failed for cluster %s: %v\n", clusterName, cleanupErr)
			return
		}
		fmt.Printf("rolled back partially created cluster %s\n", clusterName)
	}()

	err = clusterManager.Bootstrap(ctx, shardCount, clusterName)
	if err != nil {
		return fmt.Errorf("bootstrap cluster fail: %v", err)
	}

	fmt.Println("Create succeed!")

	clusterStateDir := cfg.ClusterStateDir(clusterName)
	containerInfoPath := cfg.ClusterContainerInfoPath(clusterName)
	runtimeConfigPath := cfg.ClusterRuntimeConfigPath(clusterName)

	config := RedisClusterConfig{
		NodesPerShard: clusterManager.NodesPerShard,
		Port:          cfg.RedisContainerPort,
	}

	if err := nodeManager.SaveToJSON(containerInfoPath); err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Printf("Container information saved to %s\n", containerInfoPath)
	}

	if err := os.MkdirAll(clusterStateDir, 0755); err != nil {
		return fmt.Errorf("error creating state dir: %v", err)
	}

	if utils.FileExists(runtimeConfigPath) {
		fmt.Printf("File %s already exists, deleting...\n", runtimeConfigPath)
		err := os.Remove(runtimeConfigPath)
		if err != nil {
			return fmt.Errorf("error deleting file: %s", err)
		}
	}
	err = utils.WriteToYAMLFile(runtimeConfigPath, config)
	if err != nil {
		return fmt.Errorf("error writing to YAML file %s", err)
	}
	err = clusterManager.PrintClusterNodesInfo(ctx)
	if err != nil {
		return fmt.Errorf("print cluster nodes Info error :%v", err)
	}

	// Persist to SQLite if configured.
	if cfg.DBPath != "" {
		store, serr := OpenStore(cfg.DBPath)
		if serr == nil {
			defer store.Close()
			store.UpsertCluster(ClusterRecord{
				Name: clusterName, Backend: cfg.Backend, Shards: shardCount,
				NodesPerShard: nodesPerShard, RedisPort: int(cfg.RedisContainerPort),
				Image: cfg.ImageName, Status: "ready",
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
					Hostname: node.Hostname,
				})
			}
			store.LogOperation(clusterName, "create", fmt.Sprintf("shards=%d nodes_per_shard=%d", shardCount, nodesPerShard), true)
		}
	}

	return nil
}
