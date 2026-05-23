package model

import (
	"context"
	"fmt"
	"log"
	"strings"

	"redisClusterManager/cluster/utils"
)

// GetClusterNodes retrieves and parses the cluster nodes info from a login node.
func (c *ClusterManager) GetClusterNodes(ctx context.Context, LoginNode *RuntimeNode, clusterName string) ([]ClusterNode, error) {
	if len(c.nodeManager.GetNodes()) == 0 {
		return nil, fmt.Errorf("no pods found: %w", ErrClusterNotFound)
	}
	client, err := CreateRedisClient(LoginNode.ClientConnAddr())
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis client: %w", err)
	}
	defer client.Close()

	nodesInfo, err := client.ClusterNodes(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster nodes: %w", err)
	}
	nodes, err := c.ParseRedisClusterNodes(ctx, nodesInfo, clusterName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cluster nodes: %w", err)
	}

	return nodes, nil
}

// ParseRedisClusterNodes parses CLUSTER NODES output and returns ClusterNode values.
func (c *ClusterManager) ParseRedisClusterNodes(ctx context.Context, data string, clusterName string) ([]ClusterNode, error) {
	lines := strings.Split(data, "\n")

	clusterFilter := func(ip string) bool {
		if c.nodeManager == nil {
			return false
		}
		node := c.nodeManager.GetNodeByHost(ip)
		return node != nil && node.ClusterName == clusterName
	}

	parsed, failedIDs := parseClusterNodeLines(lines, clusterFilter)
	for i := range parsed {
		parsed[i].ClusterName = clusterName
	}

	if len(failedIDs) > 0 && c.nodeManager != nil && len(c.nodeManager.GetNodes()) > 0 {
		clusterIndex := -1
		for i := 0; i < len(c.nodeManager.GetNodes()); i++ {
			if c.nodeManager.GetNodes()[i].ClusterName == clusterName {
				clusterIndex = i
				break
			}
		}
		if clusterIndex >= 0 {
			client, err := CreateRedisClient(c.nodeManager.GetNodes()[clusterIndex].ClientConnAddr())
			if err != nil {
				log.Printf("failed to create Redis client for forget: %v", err)
			} else {
				defer client.Close()
				for _, id := range failedIDs {
					if _, ferr := client.ClusterForget(ctx, id).Result(); ferr != nil {
						log.Printf("ClusterForget failed: %v", ferr)
					}
				}
			}
		}
	}

	return parsed, nil
}

// parseClusterNodeLines parses raw CLUSTER NODES output lines into ClusterNode objects.
func parseClusterNodeLines(lines []string, clusterFilter func(ip string) bool) ([]ClusterNode, []string) {
	var nodes []ClusterNode
	var failedIDs []string

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		fields := strings.Split(line, " ")
		if len(fields) < 8 {
			continue
		}
		if strings.Contains(fields[2], "fail") {
			failedIDs = append(failedIDs, fields[0])
			continue
		}

		ipPort := strings.Split(fields[1], "@")[0]
		ip, port := utils.ParseIPPort(ipPort)
		if ip == "" {
			continue
		}
		if clusterFilter != nil && !clusterFilter(ip) {
			continue
		}
		portUint16, err := utils.StringToUint16(port)
		if err != nil {
			continue
		}
		node := ClusterNode{
			ID:        fields[0],
			IP:        ip,
			Port:      portUint16,
			NodeType:  parseNodeType(fields[2]),
			MasterID:  fields[3],
			LinkState: fields[7],
		}
		if node.NodeType == Master && len(fields) > 8 {
			slots, err := ParseSlots(fields[8:])
			if err != nil {
				log.Printf("failed to parse slots for node %s: %v", node.ID, err)
				continue
			}
			node.Slots = slots
		}
		nodes = append(nodes, node)
	}
	return nodes, failedIDs
}

// parseNodeType determines the node type from the CLUSTER NODES flags field.
func parseNodeType(field string) string {
	if strings.Contains(field, "master") {
		return Master
	}
	return Slave
}
