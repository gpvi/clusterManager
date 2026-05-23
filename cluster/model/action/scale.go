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
		return fmt.Errorf("read YAML: %w", err)
	}
	nodesPerShard := configFromFile.EffectiveNodesPerShard()
	cfg.RedisContainerPort = configFromFile.Port

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
	p := pipeline.New("scale-cluster-"+clusterName, persister)

	// ---- Step 1: validate ----
	p.Add(pipeline.Step{
		Name: "validate",
		Do: func(ctx context.Context) error {
			if additionalShards <= 0 {
				return fmt.Errorf("additional shards must be > 0, got %d", additionalShards)
			}
			if err := nodeManager.ListPodsByCluster(ctx, clusterName); err != nil {
				return err
			}
			if nodeManager.GetNodeCount() == 0 {
				return fmt.Errorf("no pods: %w", model.ErrClusterNotFound)
			}
			return nil
		},
	})

	// ---- Step 2: sync-state ----
	p.Add(pipeline.Step{
		Name: "sync-state",
		Do: func(ctx context.Context) error {
			idx := findClusterNode(nodeManager, clusterName)
			if idx < 0 {
				return fmt.Errorf("cluster %s not found: %w", clusterName, model.ErrClusterNotFound)
			}
			loginNode := nodeManager.GetNodes()[idx]
			if err := cm.UpdateAfterMeet(ctx, loginNode, clusterName); err != nil {
				return fmt.Errorf("meet info: %w", err)
			}
			if err := cm.UpdateAfterSetNodeRole(ctx, loginNode, clusterName); err != nil {
				return fmt.Errorf("node role: %w", err)
			}
			if err := cm.UpdateSlots(ctx, loginNode, clusterName); err != nil {
				return fmt.Errorf("slots info: %w", err)
			}
			return nil
		},
	})

	// ---- Step 3: create-containers ----
	newNodeStart := nodeManager.GetNodeCount()
	p.Add(pipeline.Step{
		Name:  "create-containers",
		Retry: 1,
		Do: func(ctx context.Context) error {
			return cm.CreateSource(ctx, clusterName, additionalShards*nodesPerShard)
		},
		Undo: func(ctx context.Context) error {
			// Delete only the newly created pods.
			for i := newNodeStart; i < nodeManager.GetNodeCount(); i++ {
				// Best-effort: DeleteResources handles all
			}
			return nodeManager.DeleteResources(ctx, clusterName)
		},
	})

	// ---- Step 4: meet-nodes ----
	p.Add(pipeline.Step{
		Name: "meet-nodes",
		Do: func(ctx context.Context) error {
			idx := findClusterNode(nodeManager, clusterName)
			if idx < 0 {
				return fmt.Errorf("no login node for meet")
			}
			cli, err := nodeManager.GetNodes()[idx].CreateRedisClient()
			if err != nil {
				return fmt.Errorf("redis client: %w", err)
			}
			defer cli.Close()
			return cm.MeetNodes(cli, ctx, clusterName)
		},
	})

	// ---- Step 5: update-topology ----
	p.Add(pipeline.Step{
		Name: "update-topology",
		Do: func(ctx context.Context) error {
			idx := findClusterNode(nodeManager, clusterName)
			if idx < 0 {
				return fmt.Errorf("no login node")
			}
			return cm.UpdateAfterMeet(ctx, nodeManager.GetNodes()[idx], clusterName)
		},
	})

	// ---- Step 6: set-roles ----
	p.Add(pipeline.Step{
		Name: "set-roles",
		Do: func(ctx context.Context) error {
			cm.EmptyMasters = make([]*model.ClusterNode, 0)
			sum := additionalShards * nodesPerShard
			newStart := nodeManager.GetNodeCount() - sum
			masterToSlave := make(map[string][]string)
			IDToIP := make(map[string]string)
			count := 0
			var masterID string
			for i := newStart; i < newStart+sum; i++ {
				ip := nodeManager.GetNodes()[i].ConIp
				ID := cm.IPToClusterID[ip]
				IDToIP[ID] = ip
				if count == nodesPerShard {
					count = 0
				}
				if count == 0 {
					masterToSlave[ID] = make([]string, 0)
					masterID = ID
					cm.MasterIDs = append(cm.MasterIDs, masterID)
					cm.EmptyMasters = append(cm.EmptyMasters, cm.IDToClusterNode[masterID])
				} else {
					masterToSlave[masterID] = append(masterToSlave[masterID], ID)
				}
				count++
			}
			for k, v := range masterToSlave {
				masterIP := IDToIP[k]
				for _, slaveID := range v {
					slaveIP := IDToIP[slaveID]
					if err := cm.SetNodeAsSlave(ctx, masterIP, slaveIP, clusterName); err != nil {
						return err
					}
				}
			}
			return nil
		},
	})

	// ---- Step 7: migrate-slots ----
	p.Add(pipeline.Step{
		Name:  "migrate-slots",
		Retry: 1,
		Do: func(ctx context.Context) error {
			fmt.Println("starting slot migration...")
			return cm.MigratesSlotsToEmptyNode(ctx, clusterName)
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
			totalShards := len(cm.MasterIDs)
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
					NodeIndex: nodeIdx, Status: "running", Hostname: node.Hostname,
				})
			}
			s.LogOperation(clusterName, "scale",
				fmt.Sprintf("added %d shards total=%d", additionalShards, totalShards), true)
			if persister != nil {
				persister.DeletePipeline(p.Name)
			}
			return nil
		},
	})

	if err := p.Run(ctx); err != nil {
		return fmt.Errorf("scale cluster %s: %w", clusterName, err)
	}
	fmt.Println("Scale succeed!")
	return nil
}
