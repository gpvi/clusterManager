package model

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
)

// MigrateSlot 迁移 slot
func MigrateSlot(ctx context.Context, containers *Containers, cluster *Cluster, slot int, sourceNodeID, destNodeID string) error {
	var err error
	sourceNode := cluster.IDToClusterNode[sourceNodeID]
	destNode := cluster.IDToClusterNode[destNodeID]
	desCli := CreateRedisClient(ctx, containers.IPToNode[destNode.IP].HostIP, containers.IPToNode[destNode.IP].HostPort)
	if _, exist := cluster.MasterToSlave[sourceNode.ID]; !exist {
		println(sourceNodeID)
	}
	sourceCli := CreateRedisClient(ctx, containers.IPToNode[sourceNode.IP].HostIP, containers.IPToNode[sourceNode.IP].HostPort)

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

func MigratesSlotsToEmptyNode(ctx context.Context, containers *Containers, cluster *Cluster) error {
	var err error
	if err != nil {
		log.Fatalf("Failed to update containers: %v", err)
		return err
	}
	if len(cluster.EmptyMasters) == 0 {
		return fmt.Errorf("no Empty master")
	}
	newVolum := totalSlots / len(cluster.MasterIDs)
	// empty master index
	index := 0
	if err != nil {
		return fmt.Errorf("failed to get cluster nodes info: %v", err)
	}
	for _, masterID := range cluster.MasterIDs {
		masterNode := cluster.IDToClusterNode[masterID]
		fromId := masterID
		if masterNode.SlotsNum == 0 {
			continue
		}
		if masterNode.SlotsNum > newVolum {
			for _, slot := range masterNode.Slots {
				start := slot.Start
				end := slot.End
				// 将 slots 迁移到空的节点
				for i := end; i >= start && index < len(cluster.EmptyMasters); i-- {

					toId := cluster.EmptyMasters[index].ID
					err = MigrateSlot(ctx, containers, cluster, i, fromId, toId)
					if fromId == toId {
						break
					}
					//log.Println("迁移节点:", fromId, "slot:", i, "to:", toId)
					if err != nil {
						log.Printf("Failed to migrate slot %d: %v", i, err)
						return err
					}
					cluster.EmptyMasters[index].SlotsNum++
					masterNode.SlotsNum--
					if cluster.EmptyMasters[index].SlotsNum == newVolum {
						index++
						toId = cluster.MasterIDs[index]
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

	err = PrintClusterNodesInfo(ctx, containers, cluster)
	if err != nil {
		return err
	}
	return err
}

func AddClusterNode(ctx context.Context, containers *Containers, cluster *Cluster) (*ContainerNode, error) {
	// 获取初始的容器信息
	var err error
	err = containers.UpdateNodesInfo(ctx)
	if err != nil {
		return &ContainerNode{}, err
	}

	if err := containers.AddContainers(ctx, 1); err != nil {
		return &ContainerNode{}, err
	}

	// 获取更新后的容器信息
	err = containers.UpdateNodesInfo(ctx)
	if err != nil {
		return &ContainerNode{}, err
	}

	// 创建 Redis 客户端并使节点互相发现
	cliRedis := CreateRedisClient(ctx, containers.Nodes[0].HostIP, containers.Nodes[0].HostPort)
	defer func() {
		err = cliRedis.Close()
		if err != nil {
			log.Printf("Redsi 关闭连接失败%v", err)
		}
	}()

	if err := MeetNodes(cliRedis, ctx, containers, cluster); err != nil {
		return &ContainerNode{}, err
	}

	// 获取更新后的集群节点信息
	err = containers.UpdateNodesInfo(ctx)
	if err != nil {
		return &ContainerNode{}, err
	}

	newContainerNode := containers.Nodes[len(containers.Nodes)-1]
	time.Sleep(1 * time.Second) // 给新节点一些时间来初始化

	return newContainerNode, nil
}

func AddShaderAndReplica(ctx context.Context, containers *Containers, cluster *Cluster, replica int) (string, error) {
	var err error
	err = containers.UpdateNodesInfo(ctx)
	if err != nil {
		return "", fmt.Errorf(err.Error())
	}
	ctx, err = cluster.UpdateClusterNodesInfo(ctx, containers, containers.Nodes[0])
	if err != nil {
		return "", fmt.Errorf(err.Error())
	}
	if len(containers.Nodes) == 0 {
		return "", fmt.Errorf("Empty Cluster  Please Create cluster first !")
	}

	// 创建主节点
	masterNode, err := AddClusterNode(ctx, containers, cluster)
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
		slaveNode, err := AddClusterNode(ctx, containers, cluster)
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
		ctx, err = SetNodeAsSlave(ctx, containers, cluster, masterNode.ConIp, slaveIP)
		time.Sleep(1 * time.Second)
		if err != nil {
			return "", fmt.Errorf(err.Error())
		}
	}
	return masterNode.ID, err
}

func AddAction(ctx context.Context, containers *Containers, cluster *Cluster, masterNum int, replica int) error {
	containers, err := NewContainers(ctx)
	if err != nil {
		return err
	}

	for i := 0; i < masterNum; i++ {
		_, err := AddShaderAndReplica(ctx, containers, cluster, replica)
		if err != nil {
			return err
		}
	}

	err = containers.UpdateNodesInfo(ctx)
	if err != nil {
		return err
	}
	time.Sleep(8 * time.Second)
	ctx, err = cluster.UpdateClusterNodesInfo(ctx, containers, containers.Nodes[0])
	if err != nil {
		return err
	}

	err = MigratesSlotsToEmptyNode(ctx, containers, cluster)
	if err != nil {
		println(err)
		return err
	}
	for _, node := range cluster.EmptyMasters {
		println(node.ID)
	}

	return nil
}
