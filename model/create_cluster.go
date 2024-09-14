package model

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
)

var cliRedis *redis.Client

func CreateClusterAction(ctx context.Context, shared int, replica int) (context.Context, error) {
	var err error
	// 创建container 和 cluster 对象

	ctx, clusterManager := NewClusterManager(ctx, replica)
	containersManager := clusterManager.containersManager
	//获取初始化容器信息
	//// 以下操作导致podmanctx 内容丢失

	ctx, err = containersManager.UpdateNewContainersInfo(ctx)
	// 判断创建操作是否合法
	if containersManager.Num != 0 {
		return ctx, fmt.Errorf("already exist containers，please operate after delete  before containers")
	}

	// 创建节点（包括创建容器、meet）
	ctx, err = clusterManager.CreateClusterNodes(shared, ctx)
	if err != nil {
		return ctx, fmt.Errorf("create clusterNodes fail: %v", err)
	}
	// 设置主从关系
	ctx, err = clusterManager.SetAllNodeType(ctx)
	if err != nil {
		return ctx, fmt.Errorf("set node type fail: %v", err)
	}

	ctx, err = clusterManager.AllocateSlots(ctx)
	if err != nil {
		return ctx, fmt.Errorf("allocate slots fail:%v", err)
	}

	err = clusterManager.PrintClusterNodesInfo(ctx)
	if err != nil {
		return ctx, fmt.Errorf("print cluster info fail: %v", err)
	}
	return ctx, err
}
