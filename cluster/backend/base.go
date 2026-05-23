package backend

import "redisClusterManager/cluster/model"

// baseNodeManager holds shared node-tracking state used by both PodmanNodeManager
// and ContainerdNodeManager.
type baseNodeManager struct {
	IPToNode   map[string]*model.RuntimeNode
	HostToNode map[string]*model.RuntimeNode
	IDToNode   map[string]*model.RuntimeNode
	Nodes      []*model.RuntimeNode
	Num        int
}

func (b *baseNodeManager) addNode(node *model.RuntimeNode) {
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

func (b *baseNodeManager) getNodes() []*model.RuntimeNode { return b.Nodes }

func (b *baseNodeManager) getNodeByIP(ip string) *model.RuntimeNode { return b.IPToNode[ip] }

func (b *baseNodeManager) getNodeByHost(host string) *model.RuntimeNode {
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
	b.IPToNode = make(map[string]*model.RuntimeNode)
	b.HostToNode = make(map[string]*model.RuntimeNode)
	b.IDToNode = make(map[string]*model.RuntimeNode)
	b.Nodes = nil
	b.Num = 0
}
