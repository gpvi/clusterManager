package main

import (
	"context"
	"fmt"
	"github.com/containers/podman/v5/pkg/bindings/containers"
	"redisStudy/Data"
)

func GetContainerInfo(ctx context.Context) error {
	containerList, err := containers.List(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}
	ContainerNum = len(containerList)
	for _, container := range containerList {
		if _, exist := ContainerIdToContainerINfoMapping[container.ID]; exist {
			continue
		}
		inspect, err := containers.Inspect(ctx, container.ID, nil)
		if err != nil {
			fmt.Printf("failed to inspect container %s: %v\n", container.ID, err)
			continue
		}
		for _, network := range inspect.NetworkSettings.Networks {
			containerNode := ContainerInfo{
				Name:    container.Names[0],
				IP:      "127.0.0.1", // 这个可能是占位符，如果需要可以更新
				ConIp:   network.IPAddress,
				Port:    container.Ports[0].HostPort,
				ConPort: container.Ports[0].ContainerPort,
				Id:      container.ID,
			}
			println(containerNode.Id, containerNode.Name, containerNode.IP, containerNode.Port, containerNode.ConIp)
			AllContainerInfoList = append(AllContainerInfoList, containerNode)
			IPToContainerInfoMapping[network.IPAddress] = containerNode
			ContainerIdToContainerINfoMapping[container.ID] = containerNode
		}

	}
	return nil
}

func GetClusterNodesInfo(ctx context.Context) error {
	if len(AllContainerInfoList) == 0 {
		return fmt.Errorf("no container info found")
	}
	client, ctx := CreateRedisClient(AllContainerInfoList[0].IP, AllContainerInfoList[0].Port)
	// 执行 CLUSTER NODES 命令获取集群中的所有节点信息

	nodesInfo, err := client.ClusterNodes(ctx).Result()
	nodes, err := Data.ParseRedisClusterNodes(nodesInfo)
	if err != nil {
		println(err)
	}

	// 清空切片
	masterIDs = []string{}
	ClusterNodeList = []ClusterNodeInfo{}

	// 清空映射
	masterSet = make(map[string]bool)
	MasterToSlaveMapping = make(map[string][]string)
	ClusterIdClusterInfoMapping = make(map[string]ClusterNodeInfo)
	IPToClusterIDMapping = make(map[string]string)
	if len(nodes) == 0 {
		return fmt.Errorf("no cluster nodes found")
	}

	for _, node := range nodes {
		if node.NodeType == "master" {
			masterIDs = append(masterIDs, node.ID)
			masterSet[node.ID] = true

			MasterToSlaveMapping[node.ID] = make([]string, 0)
			slotsCount := 0
			if len(node.Slots) == 0 {
				EmptyMasterNodes = append(EmptyMasterNodes, node)
			} else {
				for _, slots := range node.Slots {
					if slots.Start == slots.End {
						slotsCount += 1
					}
					if slots.Start != slots.End {
						slotsCount += slots.End - slots.Start + 1
					}
				}
			}
			node.SlotsNum = slotsCount

		}
		if node.NodeType == "slave" {
			if _, exist := masterSet[node.MasterID]; exist {
				MasterToSlaveMapping[node.MasterID] = append(MasterToSlaveMapping[node.MasterID], node.ID)
			}
		}
		ClusterIdClusterInfoMapping[node.ID] = node
		IPToClusterIDMapping[node.IP] = node.ID

		ClusterNodeList = append(ClusterNodeList, node)
	}

	return nil
}
