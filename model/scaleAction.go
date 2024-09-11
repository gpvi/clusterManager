package model

import (
	"context"
	"fmt"
	"log"
	"time"
)

func AddClusterNode(ctx context.Context) (*ContainerNode, error) {
	// 获取初始的容器信息

	containerInfo, err := GetContainersInfo(ctx)
	if err != nil {
		return &ContainerNode{}, err
	}

	// 创建一个新的容器
	nodeId := containerInfo.ContainerNum + 1
	if err := CreateContainer(ctx, nodeId); err != nil {
		return &ContainerNode{}, err
	}
	time.Sleep(1 * time.Second)

	// 获取更新后的容器信息
	containerInfo, err = GetContainersInfo(ctx)
	if err != nil {
		return &ContainerNode{}, err
	}

	// 创建 Redis 客户端并使节点互相发现
	cliRedis, ctx := CreateRedisClient(containerInfo.AllContainerInfoList[0].IP, containerInfo.AllContainerInfoList[0].Port)
	if err := MeetNodes(cliRedis, ctx, containerInfo); err != nil {
		return &ContainerNode{}, err
	}
	time.Sleep(3 * time.Second)

	// 获取更新后的集群节点信息
	if err := GetClusterNodesInfo(ctx); err != nil {
		return &ContainerNode{}, err
	}

	newContainerNode := containerInfo.AllContainerInfoList[len(containerInfo.AllContainerInfoList)-1]
	time.Sleep(2 * time.Second) // 给新节点一些时间来初始化

	return &newContainerNode, nil
}

func AddShaderAndReplica(ctx context.Context, replica int) (string, error) {
	containerInfo, err := GetContainersInfo(ctx)
	err = GetClusterNodesInfo(ctx)
	if err != nil {
		return "", fmt.Errorf(err.Error())
	}
	if len(containerInfo.AllContainerInfoList) == 0 {
		return "", fmt.Errorf("Empty Cluster  Please Create cluster first !")
	}

	if err != nil {
		panic(err)
	}
	// 创建主节点
	masterNode, err := AddClusterNode(ctx)
	mNode := &masterNode
	if mNode == nil {
		return "", fmt.Errorf("masterNode is nil, cannot set slaves")
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
		if slaveNode == nil {
			return "", fmt.Errorf("create slaveNode fail")
		}
		slaveIPs = append(slaveIPs, slaveNode.ConIp)
	}
	// 设置主从关系
	for _, slaveIP := range slaveIPs {
		err = SetNodeAsSlave(masterNode.ConIp, slaveIP)
		if err != nil {
			return "", fmt.Errorf(err.Error())
		}
	}
	return masterNode.Id, err
}

func AddAction(ctx context.Context, masterNum int, replica int) error {

	ctx, _, err := DataInit()

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

	ctx, _, err = DataInit()
	if err != nil {
		println(err)
	}
	err = MigratesSlotsToEmptyNode(ctx)
	if err != nil {
		println(err)
	}
	return nil
}
