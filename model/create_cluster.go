package model

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"os"
	"redisStudy/utils"
)

var cliRedis *redis.Client

type RedisClusterConfig struct {
	Replica int    `yaml:"replica"` // 副本数量
	Port    uint16 `yaml:"port"`
}

func CreateClusterAction(ctx context.Context, shard int, replica int, clusterName string) error {
	var err error
	//创建container 和 cluster 对象
	err = InitConfig()
	if err != nil {
		return fmt.Errorf("init config fail: %v", err)
	}
	clusterManager := NewClusterManager(replica)
	containersManager := clusterManager.containersManager
	//获取初始化容器信息
	err = containersManager.GetCurContainersNum(ctx)
	// 判断创建操作是否合法
	if containersManager.Num != 0 {
		return fmt.Errorf("already exist containers，please operate after delete  exist containers")
	}

	// 创建节点（包括创建容器、meet）
	err = clusterManager.CreateClusterNodes(shard, ctx, clusterName)
	if err != nil {
		return fmt.Errorf("create clusterNodes fail: %v", err)
	}
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

	config := RedisClusterConfig{
		Replica: clusterManager.Replica,
		Port:    RedisContainerPort,
	}
	// 将结构体数据写入 JSON 文件
	// 检查文件是否存在
	if utils.FileExists(ConfigSaveFileName) {
		fmt.Printf("File %s already exists, deleting...\n", ConfigSaveFileName)
		err := os.Remove(ConfigSaveFileName) // 删除文件
		if err != nil {
			return fmt.Errorf("error deleting file: %s", err)
		}
	}
	err = utils.WriteToYAMLFile(ConfigSaveFileName, config)
	if err != nil {
		return fmt.Errorf("error writing to JSON file %s", err)
	}
	err = clusterManager.PrintClusterNodesInfo(ctx)
	if err != nil {
		return fmt.Errorf("print cluster nodes Info error :%v", err)
	}
	return nil
}
