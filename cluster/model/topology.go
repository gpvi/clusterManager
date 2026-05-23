package model

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"redisClusterManager/cluster/data"
	"redisClusterManager/cluster/utils"

	"github.com/go-redis/redis/v8"
)

// MeetNodes connects all nodes in the cluster using CLUSTER MEET.
func (c *ClusterManager) MeetNodes(client *redis.Client, ctx context.Context, clusterName string) error {
	nodes := c.nodeManager.GetNodes()
	for _, node := range nodes {
		if node.ClusterName != clusterName {
			continue
		}
		_, exist := c.AlreadyMeetNode[node.ConIp]
		if !exist {
			meetAddr := node.ClusterMeetAddr()
			host, portStr := utils.ParseIPPort(meetAddr)
			if host == "" {
				continue
			}
			port, err := utils.StringToUint16(portStr)
			if err != nil {
				continue
			}
			_, err = client.ClusterMeet(ctx, host, strconv.Itoa(int(port))).Result()
			if err != nil {
				return fmt.Errorf("could not meet node %v: %v", node.ConIp, err)
			}
		}
	}

	Clients := make([]*redis.Client, 0)
	defer func() {
		for _, cli := range Clients {
			if err := cli.Close(); err != nil {
				log.Printf("Error closing Redis client in MeetNodes: %v", err)
			}
		}
	}()

	for _, node := range c.nodeManager.GetNodes() {
		if node.ClusterName != clusterName {
			continue
		}
		cli, err := node.CreateRedisClient()
		if err != nil {
			return fmt.Errorf("failed to create Redis client for node %s: %v", node.ID, err)
		}
		Clients = append(Clients, cli)
		clusterNodeCount := 0
		for _, n := range c.nodeManager.GetNodes() {
			if n.ClusterName == clusterName {
				clusterNodeCount++
			}
		}
		err = c.waitForMeetSync(cli, ctx, clusterNodeCount)
		if err != nil {
			return fmt.Errorf("failed to wait for cluster sync: %v", err)
		}
	}
	return nil
}

// SetAllNodeRole assigns slave roles for all nodes in the cluster.
func (c *ClusterManager) SetAllNodeRole(ctx context.Context, clusterName string) error {
	var err error
	n := len(c.ClusterNodeList) / c.NodesPerShard
	expectedNodes := len(c.ClusterNodeList)
	var masterToSlave = make(map[string][]string)
	for i := 0; i < n; i++ {
		if c.ClusterNodeList[i].ClusterName != clusterName {
			continue
		}
		masterIP := c.ClusterNodeList[i].IP
		masterID := c.IPToClusterID[masterIP]
		masterToSlave[masterID] = make([]string, 0)
		c.MasterIDs = append(c.MasterIDs, masterID)

		slaveStart := n + (i * (c.NodesPerShard - 1))
		slaveEnd := slaveStart + c.NodesPerShard - 1

		for j := slaveStart; j < slaveEnd && j < expectedNodes; j++ {
			if c.ClusterNodeList[j].ClusterName != clusterName {
				continue
			}
			slaveIP := c.ClusterNodeList[j].IP
			slaveID := c.IPToClusterID[slaveIP]

			if c.AlreadySetCluster[masterID] && c.AlreadySetCluster[slaveID] {
				continue
			}
			masterToSlave[masterID] = append(masterToSlave[masterID], slaveID)
			err = c.SetNodeAsSlave(ctx, masterIP, slaveIP, clusterName)
			if err != nil {
				return err
			}
			c.AlreadySetCluster[masterID] = true
			c.AlreadySetCluster[slaveID] = true
		}

	}
	_, err = c.verifyNodeTypeSet(ctx, masterToSlave, clusterName)
	if err != nil {
		return fmt.Errorf("sync failed")
	}
	return err
}

// SetNodeAsSlave sets a node as replica of a master node.
func (c *ClusterManager) SetNodeAsSlave(ctx context.Context, masterIP string, slaveIP string, clusterName string) error {
	var err error
	slaveAddr := slaveIP
	slaveNode := c.nodeManager.GetNodeByHost(slaveAddr)
	if slaveNode == nil {
		return fmt.Errorf("slave node not found for IP %s", slaveAddr)
	}
	cli, err := data.CreateRedisClient(slaveNode.ClientConnAddr())
	if err != nil {
		return fmt.Errorf("failed to create Redis client: %w", err)
	}
	defer cli.Close()
	slaveAddrPort := fmt.Sprintf("%v:%d", slaveIP, slaveNode.HostPort)
	fmt.Printf("slave: %s\nid: %s\n", slaveAddrPort, c.IPToClusterID[slaveIP])

	masterID := c.IPToClusterID[masterIP]
	cmdMessage := cli.ClusterReplicate(ctx, masterID)
	if err := cmdMessage.Err(); err != nil {
		fmt.Printf("Error executing ClusterReplicate for slave %s %v\n", slaveAddrPort, err)
		return fmt.Errorf("failed to set node %s as replica of master %s: %v", slaveAddrPort, masterIP, err)
	}
	fmt.Printf("Node %s set as replica of master %s\n", slaveAddrPort, masterIP)
	if len(c.nodeManager.GetNodes()) == 0 {
		return fmt.Errorf("no nodes available: %w", ErrNoNodesAvailable)
	}
	err = c.UpdateAfterSetNodeRole(ctx, c.nodeManager.GetNodes()[0], clusterName)
	if err != nil {
		return fmt.Errorf("failed to get cluster nodes info: %v", err)
	}
	return nil
}

// AllocateSlots assigns hash slots to master nodes evenly.
func (c *ClusterManager) AllocateSlots(ctx context.Context, clusterName string) error {
	var err error
	if len(c.nodeManager.GetNodes()) == 0 {
		return fmt.Errorf("no nodes available: %w", ErrNoNodesAvailable)
	}
	err = c.UpdateSlots(ctx, c.nodeManager.GetNodes()[0], clusterName)
	if err != nil {
		return err
	}
	fmt.Println("start allocate...")
	numMasters := len(c.MasterIDs)
	if numMasters == 0 {
		fmt.Println("no current master node to allocate slots")
		return fmt.Errorf("no available master nodes for slot allocation: %w", ErrNoNodesAvailable)
	}
	slotsPerMaster := TotalSlots / numMasters
	for i := 0; i < numMasters; i++ {
		startPoint := i * slotsPerMaster
		endPoint := startPoint + slotsPerMaster - 1
		if i == numMasters-1 {
			endPoint = TotalSlots - 1
		}

		masterId := c.MasterIDs[i]
		masterNode, ok := c.IDToClusterNode[masterId]
		if !ok {
			return fmt.Errorf("master node not found for ID %s", masterId)
		}
		port := c.nodeManager.GetNodeByHost(masterNode.IP)
		if port == nil {
			return fmt.Errorf("runtime node not found for IP %s", masterNode.IP)
		}
		cliClusterMaster, err := data.CreateRedisClient(port.ClientConnAddr())
		if err != nil {
			return fmt.Errorf("failed to create Redis client: %w", err)
		}
		var slots = make([]int, 0)
		for j := startPoint; j <= endPoint; j++ {
			slots = append(slots, j)
		}
		addErr := cliClusterMaster.ClusterAddSlots(ctx, slots...).Err()
		cliClusterMaster.Close()
		if addErr != nil {
			return fmt.Errorf("failed to add slots to master %s: %w", masterId, addErr)
		}
	}
	err = c.VerifyAllocateSlots(ctx, clusterName)
	if err != nil {
		return fmt.Errorf("failed to verify slot allocation: %v", err)
	}
	return nil
}

// UpdateAfterMeet refreshes the cluster node map after a MEET operation.
func (c *ClusterManager) UpdateAfterMeet(ctx context.Context, LoginNode *data.RuntimeNode, clusterName string) error {
	var err error
	if c.nodeManager.GetNodeCount() == 0 {
		return nil
	}

	nodes, err := c.GetClusterNodes(ctx, LoginNode, clusterName)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if node.ClusterName != clusterName {
			continue
		}
		if _, exist := c.IDToClusterNode[node.ID]; exist {
			continue
		}
		c.AlreadyMeetNode[node.IP] = true
		c.IDToClusterNode[node.ID] = &node
		c.IPToClusterID[node.IP] = node.ID
		c.ClusterNodeList = append(c.ClusterNodeList, &node)
	}
	c.SortInfo()
	return nil
}

// UpdateAfterSetNodeRole refreshes the master/slave topology after role changes.
func (c *ClusterManager) UpdateAfterSetNodeRole(ctx context.Context, LoginNode *data.RuntimeNode, clusterName string) error {
	var err error
	c.MasterToSlave = make(map[string][]string)
	c.MasterIDs = make([]string, 0)
	c.MasterSet = make(map[string]bool)
	if c.nodeManager.GetNodeCount() == 0 {
		return nil
	}

	nodes, err := c.GetClusterNodes(ctx, LoginNode, clusterName)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		fmt.Println("the num of cluster nodes is 0")
		return nil
	}
	var masters []*data.ClusterNode
	var slaves []*data.ClusterNode

	for _, node := range nodes {
		if node.NodeType == "master" {
			masters = append(masters, &node)
		} else if node.NodeType == "slave" {
			slaves = append(slaves, &node)
		} else {
			return fmt.Errorf("node type error")
		}
	}

	for _, master := range masters {
		c.MasterIDs = append(c.MasterIDs, master.ID)
		c.MasterSet[master.ID] = true
		c.MasterToSlave[master.ID] = make([]string, 0)
	}

	for _, slave := range slaves {
		if _, exists := c.MasterSet[slave.MasterID]; !exists {
			return fmt.Errorf("master node %s not found for slave %s", slave.MasterID, slave.ID)
		}
		c.MasterToSlave[slave.MasterID] = append(c.MasterToSlave[slave.MasterID], slave.ID)
	}
	for _, v := range c.MasterToSlave {
		sort.Strings(v)
	}
	sort.Strings(c.MasterIDs)
	return nil
}

// UpdateSlots refreshes the slot allocation info for all master nodes.
func (c *ClusterManager) UpdateSlots(ctx context.Context, LoginNode *data.RuntimeNode, clusterName string) error {
	var err error
	if c.nodeManager.GetNodeCount() == 0 {
		return nil
	}

	c.EmptyMasters = make([]*data.ClusterNode, 0)

	nodes, err := c.GetClusterNodes(ctx, LoginNode, clusterName)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		fmt.Println("the num of cluster nodes is 0")
		return nil
	}
	for _, node := range nodes {
		if node.NodeType == data.Master {
			if _, ok := c.IDToClusterNode[node.ID]; ok {
				c.IDToClusterNode[node.ID].SlotsNum = c.calculateSlots(node.Slots)
				if len(node.Slots) == 0 {
					c.EmptyMasters = append(c.EmptyMasters, &node)
				}
			}
		}
	}
	return nil
}

// PrintClusterNodesInfo prints the current cluster nodes info.
func (c *ClusterManager) PrintClusterNodesInfo(ctx context.Context) error {
	if len(c.nodeManager.GetNodes()) == 0 {
		return fmt.Errorf("no nodes available: %w", ErrNoNodesAvailable)
	}
	time.Sleep(time.Duration(len(c.ClusterNodeList)/3) * time.Second)
	client, err := data.CreateRedisClient(c.nodeManager.GetNodes()[0].ClientConnAddr())
	if err != nil {
		return fmt.Errorf("failed to create Redis client: %w", err)
	}
	defer client.Close()
	nodesInfo, err := client.ClusterNodes(ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to get cluster nodes info: %w", err)
	}
	fmt.Println("\ncluster nodes lines:")
	lines := strings.Split(nodesInfo, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		fields := strings.Split(line, " ")
		if len(fields) < 8 {
			continue
		}
		if strings.Contains(fields[2], "fail") {
			continue
		}
		fmt.Println(line)
	}
	return nil
}

// calculateSlots returns the total number of slots in a slot range slice.
func (c *ClusterManager) calculateSlots(slots []data.SlotRange) int {
	totalSlots := 0
	for _, slot := range slots {
		totalSlots += slot.End - slot.Start + 1
	}
	return totalSlots
}
