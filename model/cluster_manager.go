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

func (c *ClusterManager) CreateCluster(shardCount int, ctx context.Context, clusterName string) error {
	var err error
	sum := shardCount * c.NodesPerShard
	err = c.CreateSource(ctx, clusterName, sum)
	if err != nil {
		return err
	}
	fmt.Printf("finish created, %d shards, %d nodes per shard ", shardCount, c.NodesPerShard)
	meetNodeIndex := 0
	if len(c.nodeManager.Nodes) > 0 {
		for ; meetNodeIndex < len(c.nodeManager.Nodes); meetNodeIndex++ {
			if c.nodeManager.Nodes[meetNodeIndex].ClusterName == clusterName {
				break
			}
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
	err = c.MeetNodes(cliRedis, ctx, clusterName)
	if err != nil {
		return fmt.Errorf("meet nodes fail %v", err)
	}
	err = c.UpdateAfterMeet(ctx, c.nodeManager.Nodes[0], clusterName)
	if err != nil {
		return err
	}
	return err
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

func (c *ClusterManager) GetContainerNum() int {
	return c.nodeManager.Num
}

func (c *ClusterManager) AddShards(ctx context.Context, shardCount int, clusterName string) error {
	var err error
	sum := shardCount * c.NodesPerShard
	err = c.CreateCluster(shardCount, ctx, clusterName)
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
		if c.nodeManager.IPToNode[masterIP].ClusterName != clusterName {
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
		return fmt.Errorf("同步失败")
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
		c.AlreadyMeetNode[node.ID] = true
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
			return fmt.Errorf("nodetype erroe")
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
			c.IDToClusterNode[node.ID].SlotsNum = c.calculateSlots(node.Slots)
			if len(node.Slots) == 0 {
				c.EmptyMasters = append(c.EmptyMasters, &node)
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
		_, exist := c.AlreadyMeetNode[node.HostIP]
		if !exist {
			_, err = client.ClusterMeet(ctx, node.ConIp, strconv.Itoa(int(RedisContainerPort))).Result()
			if err != nil {
				return fmt.Errorf("could not meet node %v: %v", node.ConIp, err)
			}
		}
	}

	Clients := make([]*redis.Client, 0)
	defer func() {
		for _, cli := range Clients {
			err := cli.Close()
			if err != nil {
				return
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
		err = c.waitForMeetSync(cli, ctx, len(nodes))
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
		return fmt.Errorf("同步失败")
	}
	return err
}

func (c *ClusterManager) SetNodeAsSlave(ctx context.Context, masterIP string, slaveIP string, clusterName string) error {
	var err error
	slaveAddr := slaveIP
	cli := CreateRedisClient(ctx, "127.0.0.1", c.nodeManager.IPToNode[slaveAddr].HostPort)
	defer func() {
		err = cli.Close()
		fmt.Println(err)
	}()
	println("-----------------------------")
	slaveAddrPort := fmt.Sprintf("%v:%d", slaveIP, c.nodeManager.IPToNode[slaveAddr].HostPort)
	println("slave:", slaveAddrPort)
	println("id:", c.IPToClusterID[slaveIP])

	masterID := c.IPToClusterID[masterIP]
	cmdMessage := cli.ClusterReplicate(ctx, masterID)
	if err := cmdMessage.Err(); err != nil {
		fmt.Printf("Error executing ClusterReplicate for slave %s %v\n", slaveAddrPort, err)
		return fmt.Errorf("failed to set node %s as replica of master %s: %v", slaveAddrPort, masterIP, err)
	}
	fmt.Printf("Node %s set as replica of master %s\n", slaveAddrPort, masterIP)
	err = c.UpdateAfterSetNodeRole(ctx, c.nodeManager.Nodes[0], clusterName)
	if err != nil {
		return fmt.Errorf("failed to get cluster nodes info: %v", err)
	}
	return nil
}

func (c *ClusterManager) AddClusterNode(ctx context.Context, clusterName string) (*RuntimeNode, error) {
	var err error
	if err := c.nodeManager.CreatePods(ctx, 1, clusterName); err != nil {
		return &RuntimeNode{}, err
	}

	cliRedis, err := c.nodeManager.Nodes[0].CreateRedisClient()
	if err != nil {
		return &RuntimeNode{}, fmt.Errorf("create redis client fail")
	}
	defer func() {
		err = cliRedis.Close()
		if err != nil {
			log.Printf("Redsi %v", err)
		}
	}()
	err = c.MeetNodes(cliRedis, ctx, clusterName)
	if err != nil {
		return &RuntimeNode{}, err
	}

	newNode := c.nodeManager.Nodes[len(c.nodeManager.Nodes)-1]
	return newNode, nil
}

func (c *ClusterManager) AllocateSlots(ctx context.Context, clusterName string) error {
	var err error
	err = c.UpdateSlots(ctx, c.nodeManager.Nodes[0], clusterName)
	if err != nil {
		return err
	}
	println("start allocate...")
	numMasters := len(c.MasterIDs)
	if numMasters == 0 {
		println("no current master node to be allocate slots。")
		return fmt.Errorf("no available master nodes for slot allocation")
	}
	slotsPerMaster := TotalSlots / numMasters
	for i := 0; i < numMasters; i++ {
		startPoint := i * slotsPerMaster
		endPoint := startPoint + slotsPerMaster - 1
		if i == numMasters-1 {
			endPoint = TotalSlots - 1
		}

		masterId := c.MasterIDs[i]
		port := c.nodeManager.IPToNode[c.IDToClusterNode[masterId].IP].HostPort
		cliClusterMaster := CreateRedisClient(ctx, "127.0.0.1", port)
		err = cliClusterMaster.Close()
		if err != nil {
			return err
		}
		var slots = make([]int, 0)
		for j := startPoint; j <= endPoint; j++ {
			slots = append(slots, j)
		}
		cliClusterMaster.ClusterAddSlots(ctx, slots...)
	}
	err = c.VerifyAllocateSlots(ctx, c.nodeManager, clusterName)
	if err != nil {
		return fmt.Errorf("failed to verify slot allocation: %v", err)
	}
	return nil
}

func (c *ClusterManager) PrintClusterNodesInfo(ctx context.Context) error {
	time.Sleep(time.Duration(len(c.ClusterNodeList)/3) * time.Second)
	var err error
	client := CreateRedisClient(ctx, c.nodeManager.Nodes[0].HostIP, c.nodeManager.Nodes[0].HostPort)
	defer func() {
		err = client.Close()
		fmt.Println(err)
	}()
	nodesInfo, err := client.ClusterNodes(ctx).Result()
	if err != nil {
		println("failed to get cluster nodes info: %v", err)
	}
	println("\ncluster nodes lines:")
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
		println(line)
	}
	return nil
}

func (c *ClusterManager) MigrateSlot(ctx context.Context, slot int, sourceNodeID, destNodeID string) error {
	var err error
	sourceNode := c.IDToClusterNode[sourceNodeID]
	destNode := c.IDToClusterNode[destNodeID]
	desCli := CreateRedisClient(ctx, c.nodeManager.IPToNode[destNode.IP].HostIP, c.nodeManager.IPToNode[destNode.IP].HostPort)
	defer func() {
		err = desCli.Close()
		if err != nil {
			log.Printf("close cluster master fail: %v", err)
		}
	}()
	if _, exist := c.MasterToSlave[sourceNode.ID]; !exist {
		return fmt.Errorf("source node %s is not a master node", sourceNodeID)
	}
	sourceCli := CreateRedisClient(ctx, c.nodeManager.IPToNode[sourceNode.IP].HostIP, c.nodeManager.IPToNode[sourceNode.IP].HostPort)
	defer func() {
		err = sourceCli.Close()
		if err != nil {
			log.Printf("close cluster master fail: %v", err)
		}
	}()
	defer func() {
		err = desCli.Close()
		if err != nil {
			fmt.Printf("Redsi %v", err)
		}
	}()

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

	_, err = utils.ExecuteClusterCommand(ctx, desCli, "CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", sourceNodeID)
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

	newV := TotalSlots / len(c.MasterIDs)
	index := 0
	for _, masterID := range c.MasterIDs {
		masterNode := c.IDToClusterNode[masterID]
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
	var err error
	if len(c.nodeManager.Nodes) == 0 {
		return nil, fmt.Errorf("no pods found")
	}
	client := CreateRedisClient(ctx, LoginNode.HostIP, LoginNode.HostPort)
	defer func() {
		err = client.Close()
		if err != nil {
			fmt.Printf("Error closing Redis client: %v", err)
		}
	}()

	nodesInfo, err := client.ClusterNodes(ctx).Result()
	nodes, err := c.ParseRedisClusterNodes(ctx, nodesInfo, clusterName)

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
				println("try sync fail", i)
				i++
				time.Sleep(2 * time.Second)
				continue
			}
			i++
			ok := c.equalClusterNodeType(masterToSlave, c.MasterToSlave)
			if ok {
				break
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
			l2 := len(m2[k])
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
				client.ClusterForget(ctx, fields[0])
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

func (c *ClusterManager) VerifyAllocateSlots(ctx context.Context, nodeManager *K8sNodeManager, clusterName string) error {
	var err error
	for _, container := range nodeManager.Nodes {
		cluster := c
		tryTimes := 10
		for j := 0; j < tryTimes; j++ {
			err := cluster.UpdateSlots(ctx, container, clusterName)
			if err != nil {
				return err
			}
			Flag := false
			for i := 0; i < len(cluster.MasterIDs); i++ {
				if i == len(cluster.MasterIDs)-1 && TotalSlots%len(cluster.MasterIDs) != 0 {
					if cluster.IDToClusterNode[cluster.MasterIDs[i]].SlotsNum == TotalSlots%len(cluster.MasterIDs) {
						Flag = true
					}
				} else {
					if cluster.IDToClusterNode[cluster.MasterIDs[i]].SlotsNum == TotalSlots/len(cluster.MasterIDs) {
						Flag = true
					}
				}
				if !Flag {
					break
				}
			}
		}

	}
	return err
}

func (c *ClusterManager) ParseRedisClusterNodes(ctx context.Context, data string, clusterName string) ([]ClusterNode, error) {
	lines := strings.Split(data, "\n")
	var nodes []ClusterNode
	clusterIndex := 0
	for ; clusterIndex < len(c.nodeManager.Nodes); clusterIndex++ {
		if c.nodeManager.Nodes[clusterIndex].ClusterName == clusterName {
			break
		}
	}
	client := CreateRedisClient(ctx, c.nodeManager.Nodes[clusterIndex].HostIP, c.nodeManager.Nodes[clusterIndex].HostPort)
	defer func() {
		err := client.Close()
		if err != nil {
			log.Printf("failed to close redis client: %v", err)
		}
	}()
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		fields := strings.Split(line, " ")
		if len(fields) < 8 {
			continue
		}
		if strings.Contains(fields[2], "fail") {
			client.ClusterForget(ctx, fields[0])
			continue
		}

		ipPort := strings.Split(fields[1], "@")[0]
		ip, port := utils.ParseIPPort(ipPort)
		portUint16, err := utils.StringToUint16(port)
		if err != nil {
			log.Printf("Error parsing port: %v", err)
			continue
		}
		if c.nodeManager.IPToNode[ip].ClusterName != clusterName {
			continue
		}
		node := ClusterNode{
			ID:              fields[0],
			IP:              ip,
			Port:            portUint16,
			NodeType:        parseNodeType(fields[2]),
			MasterID:        fields[3],
			PingSent:        utils.ParseInt64(fields[4]),
			PongRecv:        utils.ParseInt64(fields[5]),
			ConfigEpoch:     utils.ParseInt64(fields[6]),
			LinkState:       fields[7],
			AdditionalFlags: parseAdditionalFlags(fields[2]),
			ClusterName:     clusterName,
		}
		if node.NodeType == Master && len(fields) > 8 {
			slots, err := ParseSlots(fields[8:])
			if err != nil {
				return nil, err
			}
			node.Slots = slots
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func parseNodeType(field string) string {
	if strings.Contains(field, "master") {
		return Master
	}
	return Slave
}

func parseAdditionalFlags(field string) []string {
	flags := strings.Split(field, ",")
	var additionalFlags []string
	for _, flag := range flags {
		if flag != "master" && flag != "slave" {
			additionalFlags = append(additionalFlags, flag)
		}
	}
	return additionalFlags
}
