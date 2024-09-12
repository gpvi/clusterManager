package model

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
)

// MigrateSlot 迁移 slot
func MigrateSlot(ctx context.Context, slot int, sourceNodeID, destNodeID string) error {
	containerInfo, err := GetContainersInfoFromPodman(ctx)
	err = GetClusterNodesInfo(ctx)
	sourceNode := ClusterIdClusterInfoMapping[sourceNodeID]
	destNode := ClusterIdClusterInfoMapping[destNodeID]
	desCli, _ := CreateRedisClient(containerInfo.IPToNode[destNode.IP].IP, containerInfo.IPToNode[destNode.IP].Port)
	sourceCli, _ := CreateRedisClient(containerInfo.IDToNode[sourceNode.IP].IP, containerInfo.IPToNode[sourceNode.IP].Port)

	// Step 1: 设置 slot 状态为迁移中 (MIGRATING)
	_, err = ExecuteClusterCommand(ctx, sourceCli, "CLUSTER", "SETSLOT", strconv.Itoa(slot), "MIGRATING", destNodeID)
	if err != nil {
		return fmt.Errorf("failed to set slot as MIGRATING: %v", err)
	}
	//fmt.Printf("Slot %d set to MIGRATING state\n", slot)

	// Step 2: 在目标节点上接收 slot (IMPORTING)
	_, err = ExecuteClusterCommand(ctx, desCli, "cluster", "SETSLOT", strconv.Itoa(slot), "IMPORTING", destNodeID)
	if err != nil {
		return fmt.Errorf("failed to set slot as IMPORTING: %v", err)
	}
	//fmt.Printf("Slot %d set to IMPORTING state on destination node\n", slot)

	// Step 3: 迁移 slot 中的数据

	cmdstring := sourceCli.ClusterGetKeysInSlot(ctx, slot, 1000)
	keys := cmdstring.Val()
	port := fmt.Sprintf("%v", destNode.Port)
	for _, key := range keys {
		_, err := desCli.Migrate(ctx, destNode.IP, port, key, 0, 5000).Result()
		if err != nil {
			log.Printf("Failed to migrate key %s: %v", key, err)
			continue
		}
		//fmt.Printf("Migrated key: %s from slot: %d\n", key, slot)
	}

	// Step 4: 在目标节点上设置 slot 归属
	_, err = ExecuteClusterCommand(ctx, desCli, "CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", destNodeID)
	if err != nil {
		return fmt.Errorf("failed to set slot %d to NODE %s on destination: %v", slot, destNodeID, err)
	}
	//fmt.Printf("Slot %d assigned to node %s\n", slot, destNodeID)
	// step 5 :在原节点 设置slot
	_, err = ExecuteClusterCommand(ctx, sourceCli, "CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", destNodeID)
	if err != nil {
		return fmt.Errorf("failed to set slot %d to NODE %s on destination: %v", slot, destNodeID, err)
	}

	return nil
}

func MigratesSlotsToEmptyNode(ctx context.Context) error {

	err := GetClusterNodesInfo(ctx)
	if len(EmptyMasterNodes) == 0 {
		return fmt.Errorf("no Empty master")
	}
	if err != nil {
		log.Fatalf("Failed to get cluster nodes info: %v", err)
	}

	newVolum := totalSlots / len(masterIDs)
	// empty master index
	index := 0
	for _, masterID := range masterIDs {
		masterNode := ClusterIdClusterInfoMapping[masterID]
		fromId := masterID
		if masterNode.SlotsNum == 0 {
			continue
		}
		for masterNode.SlotsNum > newVolum {
			for _, slot := range masterNode.Slots {
				start := slot.Start
				end := slot.End
				// 将 slots 迁移到空的节点
				for i := end; i >= start && index < len(EmptyMasterNodes); i-- {
					toId := EmptyMasterNodes[index].ID
					err := MigrateSlot(ctx, i, fromId, toId)

					if err != nil {
						log.Printf("Failed to migrate slot %d: %v", i, err)
					}
					EmptyMasterNodes[index].SlotsNum++
					masterNode.SlotsNum--
					if EmptyMasterNodes[index].SlotsNum == newVolum {
						index++
						toId = masterIDs[index]
					}
					if masterNode.SlotsNum == newVolum {
						break
					}
				}
				if masterNode.SlotsNum == newVolum {
					break
				}
			}
		}
	}

	err = PrintClusterNodesInfo(ctx)
	return err
}

func AddClusterNode(ctx context.Context) (*ContainerNode, error) {
	// 获取初始的容器信息

	containerInfo, err := GetContainersInfoFromPodman(ctx)
	if err != nil {
		return &ContainerNode{}, err
	}

	// 创建一个新的容器
	nodeId := containerInfo.Num + 1
	if err := CreateContainer(ctx, nodeId); err != nil {
		return &ContainerNode{}, err
	}
	time.Sleep(1 * time.Second)

	// 获取更新后的容器信息
	containerInfo, err = GetContainersInfoFromPodman(ctx)
	if err != nil {
		return &ContainerNode{}, err
	}

	// 创建 Redis 客户端并使节点互相发现
	cliRedis, ctx := CreateRedisClient(containerInfo.Nodes[0].IP, containerInfo.Nodes[0].Port)
	if err := MeetNodes(cliRedis, ctx, containerInfo); err != nil {
		return &ContainerNode{}, err
	}
	time.Sleep(3 * time.Second)

	// 获取更新后的集群节点信息
	if err := GetClusterNodesInfo(ctx); err != nil {
		return &ContainerNode{}, err
	}

	newContainerNode := containerInfo.Nodes[len(containerInfo.Nodes)-1]
	time.Sleep(2 * time.Second) // 给新节点一些时间来初始化

	return &newContainerNode, nil
}

func AddShaderAndReplica(ctx context.Context, replica int) (string, error) {
	containerInfo, err := GetContainersInfoFromPodman(ctx)
	err = GetClusterNodesInfo(ctx)
	if err != nil {
		return "", fmt.Errorf(err.Error())
	}
	if len(containerInfo.Nodes) == 0 {
		return "", fmt.Errorf("Empty Cluster  Please Create cluster first !")
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
	containers := NewContainers()
	ctx, _, err := DataInit(containers)

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

	ctx, _, err = DataInit(containers)
	if err != nil {
		println(err)
	}
	err = MigratesSlotsToEmptyNode(ctx)
	if err != nil {
		println(err)
	}
	return nil
}
