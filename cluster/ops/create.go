package ops

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"redisClusterManager/cluster/backend"
	"redisClusterManager/cluster/config"
	"redisClusterManager/cluster/data"
	"redisClusterManager/cluster/model"
	"redisClusterManager/cluster/pipeline"
	"redisClusterManager/cluster/data/store"
	"redisClusterManager/cluster/utils"
)

func CreateClusterAction(ctx context.Context, cfg *config.RuntimeConfig, shardCount int, nodesPerShard int, clusterName string) error {
	nodeManager, err := backend.NewNodeManager(cfg)
	if err != nil {
		return fmt.Errorf("create node manager fail: %v", err)
	}
	cm := model.NewClusterManager(nodesPerShard, nodeManager)
	if cfg.CacheInvalidator != nil {
		cm.SetCacheInvalidator(cfg.CacheInvalidator)
	}

	var persister pipeline.Persister
	if cfg.DBPath != "" {
		if s, err := store.OpenStore(cfg.DBPath); err == nil {
			persister = s
		}
	}
	p := pipeline.New("create-cluster-"+clusterName, persister)

	// ---- Step 1: validate ----
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

	// ---- Step 2: create-containers ----
	p.Add(pipeline.Step{
		Name:  "create-containers",
		Retry: 1,
		Do: func(ctx context.Context) error {
			sum := shardCount * nodesPerShard
			return cm.CreatePodsForCluster(ctx, clusterName, sum)
		},
		Undo: func(ctx context.Context) error {
			return nodeManager.DeleteResources(ctx, clusterName)
		},
	})

	// ---- Step 3: meet-nodes ----
	p.Add(pipeline.Step{
		Name: "meet-nodes",
		Do: func(ctx context.Context) error {
			idx := findClusterNode(nodeManager, clusterName)
			if idx < 0 {
				return fmt.Errorf("no node found for cluster %s", clusterName)
			}
			cli, err := nodeManager.GetNodes()[idx].CreateRedisClient()
			if err != nil {
				return fmt.Errorf("create redis client fail: %w", err)
			}
			defer cli.Close()
			return cm.MeetNodes(cli, ctx, clusterName)
		},
	})

	// ---- Step 4: update-topology ----
	p.Add(pipeline.Step{
		Name: "update-topology",
		Do: func(ctx context.Context) error {
			idx := findClusterNode(nodeManager, clusterName)
			if idx < 0 {
				return fmt.Errorf("no node found for cluster %s", clusterName)
			}
			return cm.UpdateAfterMeet(ctx, nodeManager.GetNodes()[idx], clusterName)
		},
	})

	// ---- Step 5: set-roles ----
	p.Add(pipeline.Step{
		Name: "set-roles",
		Do: func(ctx context.Context) error {
			return cm.SetAllNodeRole(ctx, clusterName)
		},
	})

	// ---- Step 6: alloc-slots ----
	p.Add(pipeline.Step{
		Name: "alloc-slots",
		Do: func(ctx context.Context) error {
			return cm.AllocateSlots(ctx, clusterName)
		},
	})

	// ---- Step 7: persist-config ----
	p.Add(pipeline.Step{
		Name: "persist-config",
		Do: func(ctx context.Context) error {
			dir := cfg.ClusterStateDir(clusterName)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return err
			}
			path := cfg.ClusterRuntimeConfigPath(clusterName)
			if utils.FileExists(path) {
				os.Remove(path)
			}
			return utils.WriteToYAMLFile(path, config.RedisClusterConfig{
				NodesPerShard: cm.NodesPerShard(),
				Port:          cfg.RedisContainerPort,
			})
		},
		Undo: func(ctx context.Context) error {
			os.Remove(cfg.ClusterRuntimeConfigPath(clusterName))
			return nil
		},
	})

	// ---- Step 8: persist-db ----
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
			s.UpsertCluster(data.ClusterRecord{
				Name: clusterName, Backend: cfg.Backend, Shards: shardCount,
				NodesPerShard: nodesPerShard, RedisPort: int(cfg.RedisContainerPort),
				Image: cfg.ImageName, Status: "ready",
			})
			for _, node := range nodeManager.GetNodes() {
				if node.ClusterName != clusterName {
					continue
				}
				nodeIdx, _ := strconv.Atoi(strings.TrimPrefix(node.Name, clusterName+"-redis-"))
				s.UpsertContainer(data.ContainerRecord{
					ClusterName: clusterName, Name: node.Name, ContainerID: node.ID,
					HostIP: node.HostIP, HostPort: int(node.HostPort),
					ContainerIP: node.ConIp, ContainerPort: int(node.ConPort),
					NodeIndex: nodeIdx, Status: "running", Hostname: node.Hostname,
				})
			}
			s.LogOperation(clusterName, "create",
				fmt.Sprintf("shards=%d nodes_per_shard=%d", shardCount, nodesPerShard), true)
			if persister != nil {
				persister.DeletePipeline(p.Name)
			}
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

	// ---- Step 9: print-info ----
	p.Add(pipeline.Step{
		Name: "print-info",
		Do: func(ctx context.Context) error {
			return cm.PrintClusterNodesInfo(ctx)
		},
	})

	if err := p.Run(ctx); err != nil {
		return fmt.Errorf("create cluster %s: %w", clusterName, err)
	}
	fmt.Println("Create succeed!")
	return nil
}

func findClusterNode(nm model.PodManager, clusterName string) int {
	for i, node := range nm.GetNodes() {
		if node.ClusterName == clusterName {
			return i
		}
	}
	return -1
}
