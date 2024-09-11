package model

import (
	"context"
	"fmt"
)

var EmptyMasterNodes = make([]ClusterNode, 0)

//var ContainerIdToContainerINfoMapping = make(map[string]ContainerNode)

var ClusterIdClusterInfoMapping = make(map[string]ClusterNode)

var IPToClusterIDMapping = make(map[string]string)

var AlreadyMeetNode = make(map[string]bool)

//var ClusterIDList = make([]string, 0)

var MasterToSlaveMapping = make(map[string][]string)

var AlreadySetCluster = make(map[string]bool)

var ClusterNodeList = make([]ClusterNode, 0)

var masterIDs = make([]string, 0)

var masterSet = make(map[string]bool)

func GetClusterNodesInfo(ctx context.Context) error {
	containerInfo, err := GetContainersInfo(ctx)
	if err != nil {
		return err
	}
	if len(containerInfo.AllContainerInfoList) == 0 {
		return fmt.Errorf("no container info found")
	}
	//println(containerInfo.AllContainerInfoList[0].IP, containerInfo.AllContainerInfoList[0].Port)
	client, ctx := CreateRedisClient(containerInfo.AllContainerInfoList[0].IP, containerInfo.AllContainerInfoList[0].Port)
	// 执行 CLUSTER NODES 命令获取集群中的所有节点信息

	nodesInfo, err := client.ClusterNodes(ctx).Result()
	nodes, err := ParseRedisClusterNodes(nodesInfo)
	if err != nil {
		return err
	}

	// 清空切片
	masterIDs = []string{}
	ClusterNodeList = []ClusterNode{}

	// 清空映射
	masterSet = make(map[string]bool)
	MasterToSlaveMapping = make(map[string][]string)
	ClusterIdClusterInfoMapping = make(map[string]ClusterNode)
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
