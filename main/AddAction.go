package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

func AddClusterNode(ctx context.Context) (ContainerInfo, error) {
	// 获取初始的容器信息
	if err := GetContainerInfo(ctx); err != nil {
		return ContainerInfo{}, err
	}

	// 创建一个新的容器
	nodeId := ContainerNum + 1
	if err := CreateContainer(ctx, nodeId); err != nil {
		return ContainerInfo{}, err
	}
	time.Sleep(1 * time.Second)

	// 获取更新后的容器信息
	if err := GetContainerInfo(ctx); err != nil {
		return ContainerInfo{}, err
	}

	// 创建 Redis 客户端并使节点互相发现
	cliRedis, ctx := CreateRedisClient(AllContainerInfoList[0].IP, AllContainerInfoList[0].Port)
	if err := MeetNodes(cliRedis, ctx); err != nil {
		return ContainerInfo{}, err
	}
	time.Sleep(3 * time.Second)

	// 获取更新后的集群节点信息
	if err := GetClusterNodesInfo(ctx); err != nil {
		return ContainerInfo{}, err
	}

	newContainerNode := AllContainerInfoList[len(AllContainerInfoList)-1]
	time.Sleep(2 * time.Second) // 给新节点一些时间来初始化

	return newContainerNode, nil
}

func AddShaderAndReplica(ctx context.Context, replica int) (string, error) {
	err := GetClusterNodesInfo(ctx)
	if len(AllContainerInfoList) == 0 {
		return "", fmt.Errorf("Empty Cluster  Please Create cluster first !")
	}

	if err != nil {
		panic(err)
	}
	// 创建主节点
	masterNode, err := AddClusterNode(ctx)
	mNode := &masterNode
	if mNode == nil {
		println("masterNode is nil, cannot set slaves")
	}
	if err != nil {
		println("error retrieving masterNode: %v", err)
	}

	var slaveIPs []string

	// 添加从节点
	for i := 0; i < (replica - 1); i++ {
		slaveNode, err := AddClusterNode(ctx)
		if err != nil {
			log.Printf(err.Error())
		}
		slaveIPs = append(slaveIPs, slaveNode.ConIp)
	}
	// 设置主从关系
	for _, slaveIP := range slaveIPs {
		err = SetNodeAsSlave(masterNode.ConIp, slaveIP)
		if err != nil {
			log.Printf(err.Error())
		}
	}
	return masterNode.Id, err
}

func AddAction(ctx context.Context, masterNum int, replica int) error {

	ctx, err := DataInit()

	if err != nil {
		println(err)
	}
	for i := 0; i < masterNum; i++ {
		_, err := AddShaderAndReplica(ctx, replica)
		if err != nil {
			return err
		}
	}
	time.Sleep(4 * time.Second)

	ctx, err = DataInit()
	if err != nil {
		println(err)
	}
	err = MigratesSlotsToEmptyNode(ctx)
	if err != nil {
		println(err)
	}
	return nil
}
