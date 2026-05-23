package backend

import "redisClusterManager/cluster/data"

// baseNodeManager holds shared node-tracking state used by both PodmanNodeManager
// and ContainerdNodeManager.
type baseNodeManager struct {
	IPToNode   map[string]*data.RuntimeNode
	HostToNode map[string]*data.RuntimeNode
	IDToNode   map[string]*data.RuntimeNode
	Nodes      []*data.RuntimeNode
	Num        int
}

func (b *baseNodeManager) addNode(node *data.RuntimeNode) {
	b.Nodes = append(b.Nodes, node)
	b.Num++
	b.IPToNode[node.ConIp] = node
	b.IDToNode[node.ID] = node
	if node.Hostname != "" {
		b.HostToNode[node.Hostname] = node
	}
}

func (b *baseNodeManager) hasCluster(clusterName string) bool {
	for _, node := range b.Nodes {
		if node.ClusterName == clusterName {
			return true
		}
	}
	return false
}

func (b *baseNodeManager) getNodes() []*data.RuntimeNode { return b.Nodes }

func (b *baseNodeManager) getNodeByIP(ip string) *data.RuntimeNode { return b.IPToNode[ip] }

func (b *baseNodeManager) getNodeByHost(host string) *data.RuntimeNode {
	if b.HostToNode != nil {
		if node, ok := b.HostToNode[host]; ok {
			return node
		}
	}
	return b.IPToNode[host]
}

func (b *baseNodeManager) getNodeCount() int { return b.Num }

func (b *baseNodeManager) countByCluster(name string) int {
	count := 0
	for _, node := range b.Nodes {
		if node.ClusterName == name {
			count++
		}
	}
	return count
}

func (b *baseNodeManager) resetMaps() {
	b.IPToNode = make(map[string]*data.RuntimeNode)
	b.HostToNode = make(map[string]*data.RuntimeNode)
	b.IDToNode = make(map[string]*data.RuntimeNode)
	b.Nodes = nil
	b.Num = 0
}
