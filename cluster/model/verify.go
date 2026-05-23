package model

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

// verifyNodeTypeSet polls all nodes until the master/slave topology matches expectations.
func (c *ClusterManager) verifyNodeTypeSet(ctx context.Context, masterToSlave map[string][]string, clusterName string) (bool, error) {
	for _, node := range c.nodeManager.GetNodes() {
		if node.ClusterName != clusterName {
			continue
		}
		tryTimes := 10
		i := 0
		for i < tryTimes {
			err := c.UpdateAfterSetNodeRole(ctx, node, clusterName)
			if err != nil {
				i++
				fmt.Printf("try sync fail %d\n", i)
				time.Sleep(2 * time.Second)
				continue
			}
			i++
			if c.equalClusterNodeType(masterToSlave, c.MasterToSlave) {
				return true, nil
			}
			fmt.Printf("%v try %v /10 sync fail\n", node.Name, i)
			time.Sleep(3 * time.Second)
		}
		if i == tryTimes {
			return false, fmt.Errorf("failed to verify node type set")
		}
	}
	return true, nil
}

// equalClusterNodeType compares two master-to-slave maps for structural equality.
func (c *ClusterManager) equalClusterNodeType(m1 map[string][]string, m2 map[string][]string) bool {
	ok := true
	if len(m1) == len(m2) {
		for k, v := range m1 {
			l1 := len(v)
			v2, exists := m2[k]
			if !exists {
				ok = false
				break
			}
			l2 := len(v2)
			if l1 != l2 {
				ok = false
				break
			}
		}
	} else {
		ok = false
	}
	return ok
}

// waitForMeetSync waits until all expected nodes appear in the cluster.
func (c *ClusterManager) waitForMeetSync(client *redis.Client, ctx context.Context, expectedNodes int) error {
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		nodesInfo, err := client.ClusterNodes(ctx).Result()
		if err != nil {
			return fmt.Errorf("failed to get cluster nodes info: %v", err)
		}

		lines := strings.Split(nodesInfo, "\n")
		activeNodes := 0
		for _, line := range lines {
			fields := strings.Split(line, " ")
			if len(fields) < 8 {
				continue
			}
			if strings.Contains(fields[2], "fail") {
				continue
			}
			if strings.Contains(line, "connected") {
				activeNodes++
			}
		}
		if activeNodes == expectedNodes {
			return nil
		}
		time.Sleep(2 * time.Second)
		fmt.Printf("ClusterManager not fully synchronized, retrying... (%d/%d)\n", i+1, maxRetries)
	}

	return fmt.Errorf("cluster did not synchronize within the expected time")
}

// VerifyAllocateSlots verifies that all master nodes have the expected number of slots allocated.
func (c *ClusterManager) VerifyAllocateSlots(ctx context.Context, clusterName string) error {
	var err error
	for _, container := range c.nodeManager.GetNodes() {
		cluster := c
		tryTimes := 10
		for j := 0; j < tryTimes; j++ {
			err := cluster.UpdateSlots(ctx, container, clusterName)
			if err != nil {
				return err
			}
			if len(cluster.MasterIDs) == 0 {
				break
			}
			slotsPerMaster := TotalSlots / len(cluster.MasterIDs)
			remainder := TotalSlots % len(cluster.MasterIDs)
			ok := true
			for i := 0; i < len(cluster.MasterIDs); i++ {
				expectedSlots := slotsPerMaster
				if i == len(cluster.MasterIDs)-1 && remainder != 0 {
					expectedSlots = slotsPerMaster + remainder
				}
				masterNode := cluster.IDToClusterNode[cluster.MasterIDs[i]]
				if masterNode == nil || masterNode.SlotsNum != expectedSlots {
					ok = false
					break
				}
			}
			if ok {
				break
			}
			fmt.Printf("slot verification attempt %d/%d, retrying...\n", j+1, tryTimes)
			time.Sleep(2 * time.Second)
		}
	}
	return err
}
