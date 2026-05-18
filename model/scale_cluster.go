package model

import (
	"context"
	"fmt"

	"redisClusterManager/utils"
)

func ScaleClusterAction(ctx context.Context, cfg *RuntimeConfig, additionalShards int, clusterName string) error {
	var err error

	clientset, _, err := NewK8sClientset(cfg.KubeConfigPath)
	if err != nil {
		return fmt.Errorf("create k8s clientset fail: %v", err)
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

	nodeManager := NewK8sNodeManager(clientset, cfg.KubeNamespace, cfg)
	clusterManager := NewClusterManager(nodesPerShard, nodeManager)

	err = nodeManager.ListPodsByCluster(ctx, clusterName)
	if err != nil {
		return err
	}

	if nodeManager.Num == 0 {
		return fmt.Errorf("Current pods num is 0, please create cluster first.")
	}

	ClusterNodeIndex := 0
	for ; ClusterNodeIndex < nodeManager.Num; ClusterNodeIndex++ {
		if nodeManager.Nodes[ClusterNodeIndex].ClusterName == clusterName {
			break
		}
	}
	if ClusterNodeIndex >= nodeManager.Num {
		return fmt.Errorf("cluster with name %s not found", clusterName)
	}
	err = clusterManager.UpdateAfterMeet(ctx, nodeManager.Nodes[ClusterNodeIndex], clusterName)
	if err != nil {
		return fmt.Errorf("init meet Info fail when add shards %v", err)
	}
	err = clusterManager.UpdateAfterSetNodeRole(ctx, nodeManager.Nodes[ClusterNodeIndex], clusterName)
	if err != nil {
		return fmt.Errorf("init set node role info fail when add shard %v", err)
	}
	err = clusterManager.UpdateSlots(ctx, nodeManager.Nodes[ClusterNodeIndex], clusterName)
	if err != nil {
		return fmt.Errorf("init slots info fail when add shard%v", err)
	}

	err = clusterManager.AddShards(ctx, additionalShards, clusterName)
	if err != nil {
		return err
	}
	fmt.Println("开始迁移slots ...")
	err = clusterManager.MigratesSlotsToEmptyNode(ctx, clusterName)
	if err != nil {
		return err
	}
	return nil
}
