package model

import (
	"context"
	"fmt"
	"os"
	"redisStudy/utils"

	"github.com/go-redis/redis/v8"
)

var cliRedis *redis.Client

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

func CreateClusterAction(ctx context.Context, shardCount int, nodesPerShard int, clusterName string) error {
	var err error
	//创建container 和 cluster 对象
	err = InitConfig()
	if err != nil {
		return fmt.Errorf("init config fail: %v", err)
	}
	clusterManager := NewClusterManager(nodesPerShard)
	containersManager := clusterManager.containersManager

	if shardCount <= 0 {
		return fmt.Errorf("shard count must be greater than 0")
	}
	if nodesPerShard < 2 {
		return fmt.Errorf("nodes per shard must be at least 2")
	}
	if clusterName == "" {
		return fmt.Errorf("cluster name must not be empty")
	}

	// 获取当前集群相关容器信息
	err = containersManager.LoadClusterContainers(ctx)
	if err != nil {
		return fmt.Errorf("load cluster containers fail: %v", err)
	}
	if containersManager.HasCluster(clusterName) {
		return fmt.Errorf("cluster %s already exists with %d container(s), please delete it before recreating", clusterName, containersManager.CountByCluster(clusterName))
	}

	created := false
	defer func() {
		if err == nil || !created {
			return
		}
		if cleanupErr := DeleteAllContainers(ctx, clusterName); cleanupErr != nil {
			fmt.Printf("rollback failed for cluster %s: %v\n", clusterName, cleanupErr)
			return
		}
		fmt.Printf("rolled back partially created cluster %s\n", clusterName)
	}()

	// 创建节点（包括创建容器、meet）
	err = clusterManager.CreateCluster(shardCount, ctx, clusterName)
	if err != nil {
		return fmt.Errorf("create clusterNodes fail: %v", err)
	}
	created = true
	// 设置主从关系
	err = clusterManager.SetAllNodeRole(ctx, clusterName)
	if err != nil {
		return fmt.Errorf("set node type fail: %v", err)
	}

	err = clusterManager.AllocateSlots(ctx, clusterName)
	if err != nil {
		return fmt.Errorf("allocate slots fail:%v", err)
	}

	println("Create succeed!")

	clusterStateDir := ClusterStateDir(clusterName)
	containerInfoPath := ClusterContainerInfoPath(clusterName)
	runtimeConfigPath := ClusterRuntimeConfigPath(clusterName)

	config := RedisClusterConfig{
		NodesPerShard: clusterManager.NodesPerShard,
		Port:          RedisContainerPort,
	}
	// 将结构体数据写入 JSON 文件
	if err := clusterManager.containersManager.SaveToJSON(containerInfoPath); err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Printf("Container information saved to %s\n", containerInfoPath)
	}

	if err := os.MkdirAll(clusterStateDir, 0755); err != nil {
		return fmt.Errorf("error creating state dir: %v", err)
	}

	// 检查文件是否存在
	if utils.FileExists(runtimeConfigPath) {
		fmt.Printf("File %s already exists, deleting...\n", runtimeConfigPath)
		err := os.Remove(runtimeConfigPath) // 删除文件
		if err != nil {
			return fmt.Errorf("error deleting file: %s", err)
		}
	}
	err = utils.WriteToYAMLFile(runtimeConfigPath, config)
	if err != nil {
		return fmt.Errorf("error writing to JSON file %s", err)
	}
	err = clusterManager.PrintClusterNodesInfo(ctx)
	if err != nil {
		return fmt.Errorf("print cluster nodes Info error :%v", err)
	}

	return nil
}
