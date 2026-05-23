package model

import (
	"context"
	"fmt"
	"log"
	"strings"

	"redisClusterManager/cluster/data"
)

// GetClusterNodes retrieves and parses the cluster nodes info from a login node.
func (c *ClusterManager) GetClusterNodes(ctx context.Context, LoginNode *data.RuntimeNode, clusterName string) ([]data.ClusterNode, error) {
	if len(c.nodeManager.GetNodes()) == 0 {
		return nil, fmt.Errorf("no pods found: %w", ErrClusterNotFound)
	}
	client, err := data.CreateRedisClient(LoginNode.ClientConnAddr())
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
func (c *ClusterManager) ParseRedisClusterNodes(ctx context.Context, nodeData string, clusterName string) ([]data.ClusterNode, error) {
	lines := strings.Split(nodeData, "\n")

	clusterFilter := func(ip string) bool {
		if c.nodeManager == nil {
			return false
		}
		node := c.nodeManager.GetNodeByHost(ip)
		return node != nil && node.ClusterName == clusterName
	}

	parsed, failedIDs := data.ParseClusterNodeLines(lines, clusterFilter)
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
			client, err := data.CreateRedisClient(c.nodeManager.GetNodes()[clusterIndex].ClientConnAddr())
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
