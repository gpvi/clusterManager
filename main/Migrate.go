package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
)

type Cluster struct {
	ClusterIdClusterInfoMapping map[string]ClusterNodeInfo
}

func (c *Cluster) MigrateSlot(ctx context.Context, slot int, sourceNodeID, destNodeID string) error {
	sourceNode := c.ClusterIdClusterInfoMapping[sourceNodeID]
}

// MigrateSlot 迁移 slot
func MigrateSlot(ctx context.Context, slot int, sourceNodeID, destNodeID string) error {
	err := GetClusterNodesInfo(ctx)
	sourceNode := ClusterIdClusterInfoMapping[sourceNodeID]
	destNode := ClusterIdClusterInfoMapping[destNodeID]
	desCli, _ := CreateRedisClient(IPToContainerInfoMapping[destNode.IP].IP, IPToContainerInfoMapping[destNode.IP].Port)
	sourceCli, _ := CreateRedisClient(IPToContainerInfoMapping[sourceNode.IP].IP, IPToContainerInfoMapping[sourceNode.IP].Port)

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
