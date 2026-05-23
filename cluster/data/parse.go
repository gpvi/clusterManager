package data

import (
	"log"
	"strings"

	"redisClusterManager/cluster/utils"
)

// ParseClusterNodeLines parses raw CLUSTER NODES output lines into ClusterNode objects.
func ParseClusterNodeLines(lines []string, clusterFilter func(ip string) bool) ([]ClusterNode, []string) {
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
