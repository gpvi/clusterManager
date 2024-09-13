package model

import (
	"context"
	"fmt"
	"reflect"
	"sort"
)

// ByIP implements sort.Interface for sorting ClusterNodeList by IP address.
type ByIP []ClusterNode

func (a ByIP) Len() int           { return len(a) }
func (a ByIP) Less(i, j int) bool { return a[i].IP < a[j].IP }
func (a ByIP) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type ClusterManager struct {
	EmptyMasters      []ClusterNode
	IDToClusterNode   map[string]ClusterNode
	IPToClusterID     map[string]string
	AlreadyMeetNode   map[string]bool
	MasterToSlave     map[string][]string
	AlreadySetCluster map[string]bool
	ClusterNodeList   []ClusterNode
	MasterIDs         []string
	MasterSet         map[string]bool
	containersManager *ContainersManager
}

func NewClusterManager() *ClusterManager {
	return &ClusterManager{
		EmptyMasters:      make([]ClusterNode, 0),
		IDToClusterNode:   make(map[string]ClusterNode),
		IPToClusterID:     make(map[string]string),
		AlreadyMeetNode:   make(map[string]bool),
		MasterToSlave:     make(map[string][]string),
		AlreadySetCluster: make(map[string]bool),
		ClusterNodeList:   make([]ClusterNode, 0),
		MasterIDs:         make([]string, 0),
		MasterSet:         make(map[string]bool),
	}
}

// sortClusterNodesByIP sorts the ClusterNodeList by IP address.
func (c *ClusterManager) sortClusterNodesByIP(nodes []ClusterNode) {
	sort.Sort(ByIP(nodes))
}

func (c *ClusterManager) GetClusterNodes(ctx context.Context, containersManager *ContainersManager, LoginNode *ContainerNode) ([]ClusterNode, error) {
	var err error
	if len(containersManager.Nodes) == 0 {
		return nil, fmt.Errorf("no containers found")
	}
	//println(containerInfo.AllContainerInfoList[0].IP, containerInfo.AllContainerInfoList[0].Port)
	client := CreateRedisClient(ctx, LoginNode.HostIP, LoginNode.HostPort)
	defer func() {
		err = client.Close()
		if err != nil {
			fmt.Printf("Error closing Redis client: %v", err)
		}
	}()

	// 执行 CLUSTER NODES 命令获取集群中的所有节点信息
	nodesInfo, err := client.ClusterNodes(ctx).Result()
	nodes, err := ParseRedisClusterNodes(nodesInfo)
	return nodes, nil
}

func (c *ClusterManager) UpdateClusterNodes(ctx context.Context, containers *ContainersManager, LoginNode *ContainerNode) (context.Context, error) {
	var err error
	nodes, err := c.GetClusterNodes(ctx, containers, LoginNode)
	if err != nil {
		return ctx, nil
	}
	if len(nodes) == 0 {
		fmt.Println("the num of cluster nodes is 0")
		return ctx, nil
	}
	c.resetClusterData()
	var masters []*ClusterNode
	var slaves []*ClusterNode
	for _, node := range nodes {
		if node.NodeType == "master" {
			masters = append(masters, &node)
		} else if node.NodeType == "slave" {
			slaves = append(slaves, &node)
		} else {
			return ctx, fmt.Errorf("nodetype erroe")
		}
		c.IDToClusterNode[node.ID] = node
		c.IPToClusterID[node.IP] = node.ID
		c.ClusterNodeList = append(c.ClusterNodeList, node)
	}
	// 设置master 和 slave 相关信息
	for _, master := range masters {
		c.processMasterNode(*master)
	}

	for _, slave := range slaves {
		err = c.processSlaveNode(*slave)
		if err != nil {
			return ctx, err
		}
	}

	// 排序确保唯一性
	c.sortClusterNodesByIP(c.ClusterNodeList)
	c.sortClusterNodesByIP(c.EmptyMasters)
	for _, v := range c.MasterToSlave {
		sort.Strings(v)
	}
	sort.Strings(c.MasterIDs)

	return ctx, nil
}

// 清空信息
func (c *ClusterManager) resetClusterData() {
	c.MasterIDs = []string{}
	c.ClusterNodeList = []ClusterNode{}
	c.MasterSet = make(map[string]bool)
	c.MasterToSlave = make(map[string][]string)
	c.IDToClusterNode = make(map[string]ClusterNode)
	c.IPToClusterID = make(map[string]string)
}

// 当节点为maser 节点进行操作
func (c *ClusterManager) processMasterNode(node ClusterNode) {
	c.MasterIDs = append(c.MasterIDs, node.ID)
	c.MasterSet[node.ID] = true
	c.MasterToSlave[node.ID] = make([]string, 0)

	// Calculate the number of slots this master node holds
	node.SlotsNum = c.calculateSlots(node.Slots)

	if node.SlotsNum == 0 {
		c.EmptyMasters = append(c.EmptyMasters, node)
	}

	c.IDToClusterNode[node.ID] = node
	c.IPToClusterID[node.IP] = node.ID
	c.ClusterNodeList = append(c.ClusterNodeList, node)
}

// 当节点为slave 节点进行操作
func (c *ClusterManager) processSlaveNode(node ClusterNode) error {
	if _, exists := c.MasterSet[node.MasterID]; !exists {
		return fmt.Errorf("master node %s not found for slave %s", node.MasterID, node.ID)
	}
	c.MasterToSlave[node.MasterID] = append(c.MasterToSlave[node.MasterID], node.ID)
	c.IDToClusterNode[node.ID] = node
	c.IPToClusterID[node.IP] = node.ID
	c.ClusterNodeList = append(c.ClusterNodeList, node)
	return nil
}

// 计算当前节点的
func (c *ClusterManager) calculateSlots(slots []SlotRange) int {
	totalSlots := 0
	for _, slot := range slots {
		totalSlots += slot.End - slot.Start + 1
	}
	return totalSlots
}
func (c *ClusterManager) Equals(c2 *ClusterManager) bool {
	if c == nil || c2 == nil {
		return c == c2 // 如果两个 ClusterManager 都是 nil，则相等；如果只有一个是 nil，则不相等
	}

	// 比较切片
	if !reflect.DeepEqual(c.EmptyMasters, c2.EmptyMasters) {
		return false
	}

	// 比较映射（map）
	if !reflect.DeepEqual(c.IDToClusterNode, c2.IDToClusterNode) {
		return false
	}
	if !reflect.DeepEqual(c.IPToClusterID, c2.IPToClusterID) {
		return false
	}
	if !reflect.DeepEqual(c.AlreadyMeetNode, c2.AlreadyMeetNode) {
		return false
	}
	if !reflect.DeepEqual(c.MasterToSlave, c2.MasterToSlave) {
		return false
	}
	if !reflect.DeepEqual(c.AlreadySetCluster, c2.AlreadySetCluster) {
		return false
	}

	// 比较 ClusterNodeList 切片
	if len(c.ClusterNodeList) != len(c2.ClusterNodeList) {
		return false
	}
	for i := range c.ClusterNodeList {
		if !c.ClusterNodeList[i].Equals(&c2.ClusterNodeList[i]) {
			return false
		}
	}

	// 比较字符串切片
	if !reflect.DeepEqual(c.MasterIDs, c2.MasterIDs) {
		return false
	}

	// 比较 MasterSet 映射
	if !reflect.DeepEqual(c.MasterSet, c2.MasterSet) {
		return false
	}

	return true
}
