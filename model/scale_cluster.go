package model

import (
	"context"
	"fmt"
	"redisStudy/utils"
)

func ScaleClusterAction(ctx context.Context, masterNum int, clusterName string) error {
	var err error
	err = InitConfig()
	if err != nil {
		return err
	}
	// 读取相关配置
	var configFromFile RedisClusterConfig
	err = utils.ReadFromYAMLFile(ConfigSaveFileName, &configFromFile)
	if err != nil {
		return fmt.Errorf("Error reading from JSON file: %s", err)
	}
	replica := configFromFile.Replica
	RedisContainerPort = configFromFile.Port
	println("replica:")
	println(replica)
	// 读取配置结束

	//集群数据初始化开始
	clusterManager := NewClusterManager(replica)
	containersManager := clusterManager.containersManager

	// 容器数据初始化
	err = containersManager.UpdateAllContainersInfo(ctx)
	if err != nil {
		return err
	}

	if containersManager.Num == 0 {
		return fmt.Errorf("Current Containers num is 0,please create cluster first. ")
	}

	// 集群数据初始化/
	ClusterNodeIndex := 0
	for ; ClusterNodeIndex < clusterManager.containersManager.Num; ClusterNodeIndex++ {
		if containersManager.Nodes[ClusterNodeIndex].ClusterName == clusterName {
			break
		}
	}
	if ClusterNodeIndex == -1 {
		return fmt.Errorf("cluster with name %s not found", clusterName)
	}
	err = clusterManager.UpdateAfterMeet(ctx, containersManager.Nodes[ClusterNodeIndex], clusterName)
	if err != nil {
		return fmt.Errorf("init meet Info fail when add shaders %v", err)
	}
	err = clusterManager.UpdateAfterSetNodeRole(ctx, containersManager.Nodes[ClusterNodeIndex], clusterName)
	if err != nil {
		return fmt.Errorf("init set node role info  fail when add shader %v", err)
	}
	err = clusterManager.UpdateSlots(ctx, containersManager.Nodes[ClusterNodeIndex], clusterName)
	if err != nil {
		return fmt.Errorf("init slots info fail when add shader%v", err)
	}
	// 数据初始化结束
	// 扩容开始
	err = clusterManager.AddShaders(ctx, masterNum, clusterName)
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
