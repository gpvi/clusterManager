package action

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"redisClusterManager/cluster/model"
	"redisClusterManager/cluster/model/pipeline"
	"redisClusterManager/cluster/model/store"
	"redisClusterManager/cluster/utils"
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

func CreateClusterAction(ctx context.Context, cfg *model.RuntimeConfig, shardCount int, nodesPerShard int, clusterName string) error {
	nodeManager, err := model.NewNodeManager(cfg)
	if err != nil {
		return fmt.Errorf("create node manager fail: %v", err)
	}
	clusterManager := model.NewClusterManager(nodesPerShard, nodeManager)
	if cfg.CacheInvalidator != nil {
		clusterManager.SetCacheInvalidator(cfg.CacheInvalidator)
	}

	var persister pipeline.Persister
	if cfg.DBPath != "" {
		if s, err := store.OpenStore(cfg.DBPath); err == nil {
			persister = s
		}
	}
	p := pipeline.New("create-cluster-"+clusterName, persister)

	// Step 1: validate
	p.Add(pipeline.Step{
		Name: "validate",
		Do: func(ctx context.Context) error {
			if shardCount <= 0 {
				return fmt.Errorf("shard count must be greater than 0")
			}
			if nodesPerShard < 2 {
				return fmt.Errorf("nodes per shard must be at least 2")
			}
			if clusterName == "" {
				return fmt.Errorf("cluster name must not be empty")
			}
			if err := nodeManager.ListPodsByCluster(ctx, clusterName); err != nil {
				return fmt.Errorf("list cluster pods fail: %v", err)
			}
			if nodeManager.HasCluster(clusterName) {
				return fmt.Errorf("cluster %s already exists with %d pod(s): %w",
					clusterName, nodeManager.CountByCluster(clusterName), model.ErrClusterExists)
			}
			return nil
		},
	})

	// Step 2: create-pods
	p.Add(pipeline.Step{
		Name:    "create-pods",
		Retry:   1,
		Do: func(ctx context.Context) error {
			return clusterManager.Bootstrap(ctx, shardCount, clusterName)
		},
		Undo: func(ctx context.Context) error {
			return nodeManager.DeleteResources(ctx, clusterName)
		},
	})

	// Step 3: persist-config — save runtime YAML
	p.Add(pipeline.Step{
		Name: "persist-config",
		Do: func(ctx context.Context) error {
			dir := cfg.ClusterStateDir(clusterName)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("create state dir: %w", err)
			}
			runtimePath := cfg.ClusterRuntimeConfigPath(clusterName)
			if utils.FileExists(runtimePath) {
				os.Remove(runtimePath)
			}
			config := RedisClusterConfig{
				NodesPerShard: clusterManager.NodesPerShard,
				Port:          cfg.RedisContainerPort,
			}
			return utils.WriteToYAMLFile(runtimePath, config)
		},
		Undo: func(ctx context.Context) error {
			return os.Remove(cfg.ClusterRuntimeConfigPath(clusterName))
		},
	})

	// Step 4: persist-db — save to SQLite
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
			s.UpsertCluster(store.ClusterRecord{
				Name: clusterName, Backend: cfg.Backend, Shards: shardCount,
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
			s.LogOperation(clusterName, "create",
				fmt.Sprintf("shards=%d nodes_per_shard=%d", shardCount, nodesPerShard), true)
			return nil
		},
		Undo: func(ctx context.Context) error {
			if cfg.DBPath == "" {
				return nil
			}
			s, err := store.OpenStore(cfg.DBPath)
			if err != nil {
				return err
			}
			defer s.Close()
			s.DeleteCluster(clusterName)
			return nil
		},
	})

	// Step 5: print-info
	p.Add(pipeline.Step{
		Name: "print-info",
		Do: func(ctx context.Context) error {
			return clusterManager.PrintClusterNodesInfo(ctx)
		},
	})

	if err := p.Run(ctx); err != nil {
		return fmt.Errorf("create cluster %s: %w", clusterName, err)
	}
	fmt.Println("Create succeed!")
	return nil
}
