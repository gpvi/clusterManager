package model

import (
	"fmt"
	"log"
	"strconv"
	"time"
)

// MigrateSlot 迁移 slot
func MigrateSlot(containers *Containers, slot int, sourceNodeID, destNodeID string) error {
	var err error
	ctx := containers.ctxPodman
	sourceNode := ClusterIdClusterInfoMapping[sourceNodeID]
	destNode := ClusterIdClusterInfoMapping[destNodeID]
	desCli, _ := CreateRedisClient(containers.IPToNode[destNode.IP].IP, containers.IPToNode[destNode.IP].Port)
	if _, exist := MasterToSlaveMapping[sourceNode.ID]; !exist {
		println(sourceNodeID)
	}
	sourceCli, _ := CreateRedisClient(containers.IPToNode[sourceNode.IP].IP, containers.IPToNode[sourceNode.IP].Port)

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

func MigratesSlotsToEmptyNode(containers *Containers) error {
	var err error
	if err != nil {
		log.Fatalf("Failed to update containers: %v", err)
		return err
	}
	if len(EmptyMasterNodes) == 0 {
		return fmt.Errorf("no Empty master")
	}
	newVolum := totalSlots / len(masterIDs)
	// empty master index
	index := 0
	if err != nil {
		return fmt.Errorf("failed to get cluster nodes info: %v", err)
	}
	for _, masterID := range masterIDs {
		masterNode := ClusterIdClusterInfoMapping[masterID]
		fromId := masterID
		if masterNode.SlotsNum == 0 {
			continue
		}
		if masterNode.SlotsNum > newVolum {
			for _, slot := range masterNode.Slots {
				start := slot.Start
				end := slot.End
				// 将 slots 迁移到空的节点
				for i := end; i >= start && index < len(EmptyMasterNodes); i-- {

					toId := EmptyMasterNodes[index].ID
					err = MigrateSlot(containers, i, fromId, toId)
					if fromId == toId {
						break
					}
					//log.Println("迁移节点:", fromId, "slot:", i, "to:", toId)
					if err != nil {
						log.Printf("Failed to migrate slot %d: %v", i, err)
						return err
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

	err = PrintClusterNodesInfo(containers)
	if err != nil {
		return err
	}
	return err
}

func AddClusterNode(containers *Containers) (*ContainerNode, error) {
	// 获取初始的容器信息
	var err error
	ctx := containers.ctxPodman
	err = containers.UpdateContainers()
	if err != nil {
		return &ContainerNode{}, err
	}

	if err := containers.AddContainers(ctx, 1); err != nil {
		return &ContainerNode{}, err
	}

	// 获取更新后的容器信息
	err = containers.UpdateContainers()
	if err != nil {
		return &ContainerNode{}, err
	}

	// 创建 Redis 客户端并使节点互相发现
	cliRedis, ctx := CreateRedisClient(containers.Nodes[0].IP, containers.Nodes[0].Port)
	defer cliRedis.Close()

	if err := MeetNodes(cliRedis, ctx, containers); err != nil {
		return &ContainerNode{}, err
	}

	// 获取更新后的集群节点信息
	err = containers.UpdateContainers()
	if err != nil {
		return &ContainerNode{}, err
	}

	newContainerNode := containers.Nodes[len(containers.Nodes)-1]
	time.Sleep(1 * time.Second) // 给新节点一些时间来初始化

	return newContainerNode, nil
}

func AddShaderAndReplica(containers *Containers, replica int) (string, error) {
	var err error
	err = containers.UpdateContainers()
	if err != nil {
		return "", fmt.Errorf(err.Error())
	}
	err = GetClusterNodesInfo(containers)
	if err != nil {
		return "", fmt.Errorf(err.Error())
	}
	if len(containers.Nodes) == 0 {
		return "", fmt.Errorf("Empty Cluster  Please Create cluster first !")
	}

	// 创建主节点
	masterNode, err := AddClusterNode(containers)
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
		slaveNode, err := AddClusterNode(containers)
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
		time.Sleep(1 * time.Second)
		if err != nil {
			return "", fmt.Errorf(err.Error())
		}
	}
	return masterNode.Id, err
}

func AddAction(containers *Containers, masterNum int, replica int) error {
	containers, err := NewContainers()
	if err != nil {
		return err
	}

	_, err = DataInit(containers)

	if err != nil {
		println(err)
	}
	for i := 0; i < masterNum; i++ {
		_, err := AddShaderAndReplica(containers, replica)
		if err != nil {
			return err
		}
	}

	err = containers.UpdateContainers()
	if err != nil {
		return err
	}
	time.Sleep(6 * time.Second)
	err = GetClusterNodesInfo(containers)
	if err != nil {
		return err
	}

	err = MigratesSlotsToEmptyNode(containers)
	if err != nil {
		println(err)
		return err
	}
	for _, node := range EmptyMasterNodes {
		println(node.ID)
	}

	return nil
}
