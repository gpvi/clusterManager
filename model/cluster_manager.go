package model

import (
	"context"
	"fmt"
	"log"
	"redisClusterManager/utils"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

type ByIP []*ClusterNode

func (a ByIP) Len() int           { return len(a) }
func (a ByIP) Less(i, j int) bool  { return a[i].IP < a[j].IP }
func (a ByIP) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type ClusterManager struct {
	EmptyMasters      []*ClusterNode
	IDToClusterNode   map[string]*ClusterNode
	IPToClusterID     map[string]string
	AlreadyMeetNode   map[string]bool
	MasterToSlave     map[string][]string
	AlreadySetCluster map[string]bool
	ClusterNodeList   []*ClusterNode
	MasterIDs         []string
	MasterSet         map[string]bool
	nodeManager       *K8sNodeManager
	NodesPerShard     int
}

func NewClusterManager(nodesPerShard int, nodeManager *K8sNodeManager) *ClusterManager {
	clusterManager := ClusterManager{
		EmptyMasters:      make([]*ClusterNode, 0),
		IDToClusterNode:   make(map[string]*ClusterNode),
		IPToClusterID:     make(map[string]string),
		AlreadyMeetNode:   make(map[string]bool),
		MasterToSlave:     make(map[string][]string),
		AlreadySetCluster: make(map[string]bool),
		ClusterNodeList:   make([]*ClusterNode, 0),
		MasterIDs:         make([]string, 0),
		MasterSet:         make(map[string]bool),
		nodeManager:       nodeManager,
		NodesPerShard:     nodesPerShard,
	}

	return &clusterManager
}

func (c *ClusterManager) CreateSource(ctx context.Context, clusterName string, sum int) error {
	var err error
	err = c.nodeManager.CreatePods(ctx, sum, clusterName)
	if err != nil {
		return fmt.Errorf("create pods fail %v", err)
	}

	fmt.Println("pods info list follows:")

	for _, node := range c.nodeManager.Nodes {
		fmt.Println("clusterName:", node.ClusterName, "podName: ", node.Name, "podUID: ", node.ID, "HostIP: ", node.HostIP, "HostPort:", node.HostPort, "PodIP:", node.ConIp, "containerPort: ", node.ConPort)
	}
	return nil
}

// Bootstrap runs the complete cluster creation flow: pods -> ready -> MEET -> master/slave -> slots
func (c *ClusterManager) Bootstrap(ctx context.Context, shardCount int, clusterName string) error {
	var err error
	sum := shardCount * c.NodesPerShard
	err = c.CreateSource(ctx, clusterName, sum)
	if err != nil {
		return err
	}
	fmt.Printf("finish created, %d shards, %d nodes per shard\n", shardCount, c.NodesPerShard)

	meetNodeIndex := 0
	var cliRedis *redis.Client
	if len(c.nodeManager.Nodes) > 0 {
		for ; meetNodeIndex < len(c.nodeManager.Nodes); meetNodeIndex++ {
			if c.nodeManager.Nodes[meetNodeIndex].ClusterName == clusterName {
				break
			}
		}
		if meetNodeIndex >= len(c.nodeManager.Nodes) {
			return fmt.Errorf("no node found for cluster %s: %w", clusterName, ErrClusterNotFound)
		}
		cliRedis, err = c.nodeManager.Nodes[meetNodeIndex].CreateRedisClient()
		if err != nil {
			return fmt.Errorf("create redis client fail")
		}
		defer func() {
			if err := cliRedis.Close(); err != nil {
				fmt.Printf("Error closing Redis client: %v\n", err)
			}
		}()
	} else {
		fmt.Println("No pods available.")
	}

	fmt.Println("start Meet...")
	if cliRedis == nil {
		return fmt.Errorf("no cluster node found for meeting")
	}
	err = c.MeetNodes(cliRedis, ctx, clusterName)
	if err != nil {
		return fmt.Errorf("meet nodes fail %v", err)
	}
	err = c.UpdateAfterMeet(ctx, c.nodeManager.Nodes[meetNodeIndex], clusterName)
	if err != nil {
		return err
	}

	err = c.SetAllNodeRole(ctx, clusterName)
	if err != nil {
		return fmt.Errorf("set node type fail: %v", err)
	}

	err = c.AllocateSlots(ctx, clusterName)
	if err != nil {
		return fmt.Errorf("allocate slots fail: %v", err)
	}

	return nil
}

func (c *ClusterManager) GetContainerNum() int {
	return c.nodeManager.Num
}

func (c *ClusterManager) AddShards(ctx context.Context, shardCount int, clusterName string) error {
	var err error
	sum := shardCount * c.NodesPerShard
	err = c.CreateSource(ctx, clusterName, sum)
	if err != nil {
		return err
	}
	// Find a login node for meeting
	meetNodeIndex := 0
	for ; meetNodeIndex < c.nodeManager.Num-sum; meetNodeIndex++ {
		if c.nodeManager.Nodes[meetNodeIndex].ClusterName == clusterName {
			break
		}
	}
	if meetNodeIndex >= c.nodeManager.Num-sum {
		return fmt.Errorf("no existing node found for cluster %s", clusterName)
	}
	cliRedis, err := c.nodeManager.Nodes[meetNodeIndex].CreateRedisClient()
	if err != nil {
		return fmt.Errorf("create redis client fail: %w", err)
	}
	defer cliRedis.Close()
	err = c.MeetNodes(cliRedis, ctx, clusterName)
	if err != nil {
		return fmt.Errorf("meet nodes fail: %w", err)
	}
	err = c.UpdateAfterMeet(ctx, c.nodeManager.Nodes[meetNodeIndex], clusterName)
	if err != nil {
		return err
	}
	c.EmptyMasters = make([]*ClusterNode, 0)
	newNodeStartIndex := c.nodeManager.Num - sum
	var masterToSlave = make(map[string][]string)
	masterToSlave = c.MasterToSlave
	IDToIP := make(map[string]string)
	count := 0
	masterID := ""
	for i := newNodeStartIndex; i < newNodeStartIndex+sum; i++ {
		ip := c.nodeManager.Nodes[i].ConIp
		ID := c.IPToClusterID[ip]
		IDToIP[ID] = ip
		if count == c.NodesPerShard {
			count = 0
		}
		if count == 0 {
			masterToSlave[ID] = make([]string, 0)
			masterID = ID
			c.MasterIDs = append(c.MasterIDs, masterID)
			c.EmptyMasters = append(c.EmptyMasters, c.IDToClusterNode[masterID])
		} else {
			masterToSlave[masterID] = append(masterToSlave[masterID], ID)
		}
		count++
	}

	for k, v := range masterToSlave {
		masterID = k
		masterIP := IDToIP[masterID]

		if _, exist := IDToIP[masterID]; !exist {
			continue
		}
		if masterRuntime, ok := c.nodeManager.IPToNode[masterIP]; !ok || masterRuntime.ClusterName != clusterName {
			continue
		}

		for _, slaveID := range v {
			slaveIP := IDToIP[slaveID]
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

func (c *ClusterManager) UpdateAfterMeet(ctx context.Context, LoginNode *RuntimeNode, clusterName string) error {
	var err error
	if c.nodeManager.Num == 0 {
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

func (c *ClusterManager) SortInfo() {
	c.sortClusterNodesByIP(c.ClusterNodeList)
	c.sortClusterNodesByIP(c.EmptyMasters)
}

func (c *ClusterManager) UpdateAfterSetNodeRole(ctx context.Context, LoginNode *RuntimeNode, clusterName string) error {
	var err error
	c.MasterToSlave = make(map[string][]string)
	c.MasterIDs = make([]string, 0)
	c.MasterSet = make(map[string]bool)
	if c.nodeManager.Num == 0 {
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
	var masters []*ClusterNode
	var slaves []*ClusterNode

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

func (c *ClusterManager) UpdateSlots(ctx context.Context, LoginNode *RuntimeNode, clusterName string) error {
	var err error
	if c.nodeManager.Num == 0 {
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
	for _, node := range nodes {
		if node.NodeType == Master {
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

func (c *ClusterManager) MeetNodes(client *redis.Client, ctx context.Context, clusterName string) error {
	var err error
	nodes := c.nodeManager.Nodes
	for _, node := range nodes {
		if node.ClusterName != clusterName {
			continue
		}
		_, exist := c.AlreadyMeetNode[node.ConIp]
		if !exist {
			_, err = client.ClusterMeet(ctx, node.ConIp, strconv.Itoa(int(c.nodeManager.config.RedisContainerPort))).Result()
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

	for _, node := range c.nodeManager.Nodes {
		if node.ClusterName != clusterName {
			continue
		}
		cli, err := node.CreateRedisClient()
		if err != nil {
			return fmt.Errorf("failed to create Redis client for node %s: %v", node.ID, err)
		}
		Clients = append(Clients, cli)
		clusterNodeCount := 0
		for _, n := range c.nodeManager.Nodes {
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

func (c *ClusterManager) SetNodeAsSlave(ctx context.Context, masterIP string, slaveIP string, clusterName string) error {
	var err error
	slaveAddr := slaveIP
	slaveNode, ok := c.nodeManager.IPToNode[slaveAddr]
	if !ok {
		return fmt.Errorf("slave node not found for IP %s", slaveAddr)
	}
	cli, err := CreateRedisClient(ctx, "127.0.0.1", slaveNode.HostPort)
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
	if len(c.nodeManager.Nodes) == 0 {
		return fmt.Errorf("no nodes available: %w", ErrNoNodesAvailable)
	}
	err = c.UpdateAfterSetNodeRole(ctx, c.nodeManager.Nodes[0], clusterName)
	if err != nil {
		return fmt.Errorf("failed to get cluster nodes info: %v", err)
	}
	return nil
}

func (c *ClusterManager) AllocateSlots(ctx context.Context, clusterName string) error {
	var err error
	if len(c.nodeManager.Nodes) == 0 {
		return fmt.Errorf("no nodes available: %w", ErrNoNodesAvailable)
	}
	err = c.UpdateSlots(ctx, c.nodeManager.Nodes[0], clusterName)
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
		port, ok := c.nodeManager.IPToNode[masterNode.IP]
		if !ok {
			return fmt.Errorf("runtime node not found for IP %s", masterNode.IP)
		}
		port_val := port.HostPort
		cliClusterMaster, err := CreateRedisClient(ctx, "127.0.0.1", port_val)
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

func (c *ClusterManager) PrintClusterNodesInfo(ctx context.Context) error {
	if len(c.nodeManager.Nodes) == 0 {
		return fmt.Errorf("no nodes available: %w", ErrNoNodesAvailable)
	}
	time.Sleep(time.Duration(len(c.ClusterNodeList)/3) * time.Second)
	client, err := CreateRedisClient(ctx, c.nodeManager.Nodes[0].HostIP, c.nodeManager.Nodes[0].HostPort)
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

func (c *ClusterManager) MigrateSlot(ctx context.Context, slot int, sourceNodeID, destNodeID string) error {
	sourceNode := c.IDToClusterNode[sourceNodeID]
	destNode := c.IDToClusterNode[destNodeID]
	if destNode == nil {
		return fmt.Errorf("dest node not found for ID %s", destNodeID)
	}
	destRuntime, ok := c.nodeManager.IPToNode[destNode.IP]
	if !ok {
		return fmt.Errorf("runtime node not found for dest IP %s", destNode.IP)
	}
	desCli, err := CreateRedisClient(ctx, destRuntime.HostIP, destRuntime.HostPort)
	if err != nil {
		return fmt.Errorf("failed to create Redis client: %w", err)
	}
	defer desCli.Close()
	if _, exist := c.MasterToSlave[sourceNode.ID]; !exist {
		return fmt.Errorf("source node %s is not a master node", sourceNodeID)
	}
	sourceRuntime, ok := c.nodeManager.IPToNode[sourceNode.IP]
	if !ok {
		return fmt.Errorf("runtime node not found for source IP %s", sourceNode.IP)
	}
	sourceCli, err := CreateRedisClient(ctx, sourceRuntime.HostIP, sourceRuntime.HostPort)
	if err != nil {
		return fmt.Errorf("failed to create Redis client: %w", err)
	}
	defer sourceCli.Close()

	_, err = utils.ExecuteClusterCommand(ctx, desCli, "cluster", "SETSLOT", strconv.Itoa(slot), "IMPORTING", sourceNodeID)
	if err != nil {
		return fmt.Errorf("failed to set slot as IMPORTING: %v", err)
	}

	_, err = utils.ExecuteClusterCommand(ctx, sourceCli, "CLUSTER", "SETSLOT", strconv.Itoa(slot), "MIGRATING", destNodeID)
	if err != nil {
		return fmt.Errorf("failed to set slot as MIGRATING: %v", err)
	}

	cmdstring := sourceCli.ClusterGetKeysInSlot(ctx, slot, 1000)
	keys := cmdstring.Val()
	port := fmt.Sprintf("%v", destNode.Port)
	if len(keys) > 0 {
		chunkSize := len(keys) / 10
		if chunkSize == 0 {
			chunkSize = 1
		}

		for i := 0; i < len(keys); i += chunkSize {
			end := i + chunkSize
			if end > len(keys) {
				end = len(keys)
			}

			currentChunk := keys[i:end]

			migrateArgs := []interface{}{destNode.IP, port, "", 0, 5000 * time.Millisecond, "KEYS"}

			for _, key := range currentChunk {
				exists, err := sourceCli.Exists(ctx, key).Result()
				if err != nil {
					log.Printf("Error checking existence of key %s: %v", key, err)
					continue
				}
				if exists == 0 {
					log.Printf("Key %s does not exist, skipping migration.", key)
					continue
				}
				migrateArgs = append(migrateArgs, key)
			}

			if len(migrateArgs) > 6 {
				cmd := sourceCli.Do(ctx, append([]interface{}{"MIGRATE"}, migrateArgs...)...)
				if err := cmd.Err(); err != nil {
					log.Printf("Failed to migrate keys in chunk starting at index %d: %v", i, err)
					continue
				}
			} else {
				log.Printf("No valid keys to migrate in chunk starting at index %d.", i)
			}
		}
	}

	_, err = utils.ExecuteClusterCommand(ctx, desCli, "CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", destNodeID)
	if err != nil {
		return fmt.Errorf("failed to set slot %d to NODE %s on destination: %v", slot, destNodeID, err)
	}
	_, err = utils.ExecuteClusterCommand(ctx, sourceCli, "CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", destNodeID)
	if err != nil {
		return fmt.Errorf("failed to set slot %d to NODE %s on destination: %v", slot, destNodeID, err)
	}

	return nil
}

func (c *ClusterManager) MigratesSlotsToEmptyNode(ctx context.Context, clusterName string) error {
	var err error

	if len(c.EmptyMasters) == 0 {
		return fmt.Errorf("no Empty master")
	}
	if len(c.MasterIDs) == 0 {
		return fmt.Errorf("no master nodes for slot migration")
	}

	newV := TotalSlots / len(c.MasterIDs)
	index := 0
	for _, masterID := range c.MasterIDs {
		masterNode, ok := c.IDToClusterNode[masterID]
		if !ok {
			continue
		}
		fromId := masterID
		if masterNode.SlotsNum == 0 {
			continue
		}
		if masterNode.ClusterName != clusterName {
			continue
		}

		if masterNode.SlotsNum > newV {
			for _, slot := range masterNode.Slots {
				start := slot.Start
				end := slot.End
				var toId string
				for i := end; i >= start && index < len(c.EmptyMasters); i-- {
					toId = c.EmptyMasters[index].ID
					if fromId == toId {
						break
					}
					err = c.MigrateSlot(ctx, i, fromId, toId)
					if err != nil {
						log.Printf("Failed to migrate slot %d: %v", i, err)
						return err
					}
					c.EmptyMasters[index].SlotsNum++
					masterNode.SlotsNum--
					if c.EmptyMasters[index].SlotsNum == newV {
						index++
					}
					if masterNode.SlotsNum == newV {
						break
					}
				}
			}
		}
	}

	err = c.PrintClusterNodesInfo(ctx)
	if err != nil {
		return err
	}
	return err
}

func (c *ClusterManager) sortClusterNodesByIP(nodes []*ClusterNode) {
	sort.Sort(ByIP(nodes))
}

func (c *ClusterManager) GetClusterNodes(ctx context.Context, LoginNode *RuntimeNode, clusterName string) ([]ClusterNode, error) {
	if len(c.nodeManager.Nodes) == 0 {
		return nil, fmt.Errorf("no pods found: %w", ErrClusterNotFound)
	}
	client, err := CreateRedisClient(ctx, LoginNode.HostIP, LoginNode.HostPort)
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

func (c *ClusterManager) calculateSlots(slots []SlotRange) int {
	totalSlots := 0
	for _, slot := range slots {
		totalSlots += slot.End - slot.Start + 1
	}
	return totalSlots
}

func (c *ClusterManager) verifyNodeTypeSet(ctx context.Context, masterToSlave map[string][]string, clusterName string) (bool, error) {
	for _, node := range c.nodeManager.Nodes {
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

func (c *ClusterManager) VerifyAllocateSlots(ctx context.Context, clusterName string) error {
	var err error
	for _, container := range c.nodeManager.Nodes {
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

func (c *ClusterManager) ParseRedisClusterNodes(ctx context.Context, data string, clusterName string) ([]ClusterNode, error) {
	lines := strings.Split(data, "\n")

	clusterFilter := func(ip string) bool {
		if c.nodeManager == nil {
			return false
		}
		node := c.nodeManager.IPToNode[ip]
		return node != nil && node.ClusterName == clusterName
	}

	parsed, failedIDs := parseClusterNodeLines(lines, clusterFilter)
	for i := range parsed {
		parsed[i].ClusterName = clusterName
	}

	if len(failedIDs) > 0 && c.nodeManager != nil && len(c.nodeManager.Nodes) > 0 {
		clusterIndex := -1
		for i := 0; i < len(c.nodeManager.Nodes); i++ {
			if c.nodeManager.Nodes[i].ClusterName == clusterName {
				clusterIndex = i
				break
			}
		}
		if clusterIndex >= 0 {
			client, err := CreateRedisClient(ctx, c.nodeManager.Nodes[clusterIndex].HostIP, c.nodeManager.Nodes[clusterIndex].HostPort)
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
			ID:              fields[0],
			IP:              ip,
			Port:            portUint16,
			NodeType:        parseNodeType(fields[2]),
			MasterID:        fields[3],
			LinkState:       fields[7],
		}
		if node.NodeType == Master && len(fields) > 8 {
			slots, err := ParseSlots(fields[8:])
			if err != nil {
				return nil, nil
			}
			node.Slots = slots
		}
		nodes = append(nodes, node)
	}
	return nodes, failedIDs
}

func parseNodeType(field string) string {
	if strings.Contains(field, "master") {
		return Master
	}
	return Slave
}

