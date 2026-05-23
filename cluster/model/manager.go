package model

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"redisClusterManager/cluster/data"
	"redisClusterManager/cluster/utils"

	"github.com/go-redis/redis/v8"
)

// Migration concurrency and thresholds.
const (
	migrationWorkers = 16
	smallKeyBatch    = 50
	chunkDivisor     = 10
	bigKeyThreshold  = 10 * 1024 * 1024 // 10 MB
)

// slotTask describes a single slot to migrate from one node to another.
type slotTask struct {
	slot          int
	fromID        string
	fromIP        string
	toID          string
	toIP          string
	destHostPort  uint16
	fromHostPort  uint16
	containerPort uint16
}

// keyEntry pairs a Redis key with its memory usage.
type keyEntry struct {
	key  string
	size int64
}

// group represents a set of slot migration tasks sharing the same source node.
type group struct {
	tasks       []slotTask
	fromIP      string
	fromHostPort uint16
}

// ByIP implements sort.Interface for ClusterNode slice sorted by IP.
type ByIP []*data.ClusterNode

func (a ByIP) Len() int           { return len(a) }
func (a ByIP) Less(i, j int) bool { return a[i].IP < a[j].IP }
func (a ByIP) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// ClusterManager manages Redis cluster operations.
type ClusterManager struct {
	EmptyMasters      []*data.ClusterNode
	IDToClusterNode   map[string]*data.ClusterNode
	IPToClusterID     map[string]string
	AlreadyMeetNode   map[string]bool
	MasterToSlave     map[string][]string
	AlreadySetCluster map[string]bool
	ClusterNodeList   []*data.ClusterNode
	MasterIDs         []string
	MasterSet         map[string]bool
	nodeManager       PodManager
	NodesPerShard     int
	cacheInvalidator  CacheInvalidator
}

// NewClusterManager creates a new ClusterManager.
func NewClusterManager(nodesPerShard int, nodeManager PodManager) *ClusterManager {
	return &ClusterManager{
		EmptyMasters:      make([]*data.ClusterNode, 0),
		IDToClusterNode:   make(map[string]*data.ClusterNode),
		IPToClusterID:     make(map[string]string),
		AlreadyMeetNode:   make(map[string]bool),
		MasterToSlave:     make(map[string][]string),
		AlreadySetCluster: make(map[string]bool),
		ClusterNodeList:   make([]*data.ClusterNode, 0),
		MasterIDs:         make([]string, 0),
		MasterSet:         make(map[string]bool),
		nodeManager:       nodeManager,
		NodesPerShard:     nodesPerShard,
	}
}

// SetCacheInvalidator registers a callback for cache invalidation after slot migration.
func (c *ClusterManager) SetCacheInvalidator(inv CacheInvalidator) {
	c.cacheInvalidator = inv
}

// ---------------------------------------------------------------------------
// Pod management helpers
// ---------------------------------------------------------------------------

// CreatePodsForCluster creates pods for the cluster.
func (c *ClusterManager) CreatePodsForCluster(ctx context.Context, clusterName string, sum int) error {
	if err := c.nodeManager.CreatePods(ctx, sum, clusterName); err != nil {
		return fmt.Errorf("create pods fail %v", err)
	}
	fmt.Println("pods info list follows:")
	for _, node := range c.nodeManager.GetNodes() {
		fmt.Println("clusterName:", node.ClusterName, "podName: ", node.Name,
			"podUID: ", node.ID, "HostIP: ", node.HostIP, "HostPort:", node.HostPort,
			"PodIP:", node.ConIp, "containerPort: ", node.ConPort)
	}
	return nil
}

func (c *ClusterManager) createPodsForCluster(ctx context.Context, shardCount int, clusterName string) error {
	sum := shardCount * c.NodesPerShard
	return c.CreatePodsForCluster(ctx, clusterName, sum)
}

func (c *ClusterManager) createPodsForNewShards(ctx context.Context, shardCount int, clusterName string) error {
	return c.createPodsForCluster(ctx, shardCount, clusterName)
}

func (c *ClusterManager) findFirstClusterNode(clusterName string) (*data.RuntimeNode, error) {
	if len(c.nodeManager.GetNodes()) == 0 {
		fmt.Println("No pods available.")
		return nil, fmt.Errorf("no cluster node found for meeting")
	}
	for _, node := range c.nodeManager.GetNodes() {
		if node.ClusterName == clusterName {
			return node, nil
		}
	}
	return nil, fmt.Errorf("no node found for cluster %s: %w", clusterName, ErrClusterNotFound)
}

func (c *ClusterManager) connectToClusterNode(ctx context.Context, clusterName string) (*redis.Client, error) {
	node, err := c.findFirstClusterNode(clusterName)
	if err != nil {
		return nil, err
	}
	cli, err := node.CreateRedisClient()
	if err != nil {
		return nil, fmt.Errorf("create redis client fail")
	}
	return cli, nil
}

func (c *ClusterManager) findExistingNode(clusterName string, shardCount int) (*data.RuntimeNode, error) {
	sum := shardCount * c.NodesPerShard
	limit := c.nodeManager.GetNodeCount() - sum
	for i := 0; i < limit; i++ {
		if c.nodeManager.GetNodes()[i].ClusterName == clusterName {
			return c.nodeManager.GetNodes()[i], nil
		}
	}
	return nil, fmt.Errorf("no existing node found for cluster %s", clusterName)
}

func (c *ClusterManager) connectToExistingNode(ctx context.Context, clusterName string, shardCount int) (*redis.Client, error) {
	node, err := c.findExistingNode(clusterName, shardCount)
	if err != nil {
		return nil, err
	}
	cli, err := node.CreateRedisClient()
	if err != nil {
		return nil, fmt.Errorf("create redis client fail: %w", err)
	}
	return cli, nil
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

// Bootstrap runs the complete cluster creation flow.
func (c *ClusterManager) Bootstrap(ctx context.Context, shardCount int, clusterName string) error {
	if err := c.createPodsForCluster(ctx, shardCount, clusterName); err != nil {
		return err
	}
	fmt.Printf("finish created, %d shards, %d nodes per shard\n", shardCount, c.NodesPerShard)
	cli, err := c.connectToClusterNode(ctx, clusterName)
	if err != nil {
		return err
	}
	defer cli.Close()
	return c.initCluster(ctx, cli, clusterName)
}

func (c *ClusterManager) initCluster(ctx context.Context, cli *redis.Client, clusterName string) error {
	fmt.Println("start Meet...")
	if err := c.MeetNodes(cli, ctx, clusterName); err != nil {
		return fmt.Errorf("meet nodes fail %v", err)
	}
	loginNode, err := c.findFirstClusterNode(clusterName)
	if err != nil {
		return err
	}
	if err := c.UpdateAfterMeet(ctx, loginNode, clusterName); err != nil {
		return err
	}
	if err := c.SetAllNodeRole(ctx, clusterName); err != nil {
		return fmt.Errorf("set node type fail: %v", err)
	}
	return c.AllocateSlots(ctx, clusterName)
}

// ---------------------------------------------------------------------------
// AddShards
// ---------------------------------------------------------------------------

// AddShards adds new shards to an existing cluster.
func (c *ClusterManager) AddShards(ctx context.Context, shardCount int, clusterName string) error {
	if err := c.createPodsForNewShards(ctx, shardCount, clusterName); err != nil {
		return err
	}
	cli, err := c.connectToExistingNode(ctx, clusterName, shardCount)
	if err != nil {
		return err
	}
	defer cli.Close()
	if err := c.meetAndSyncNewNodes(ctx, cli, clusterName, shardCount); err != nil {
		return err
	}
	c.EmptyMasters = make([]*data.ClusterNode, 0)
	return c.assignRolesToNewNodes(ctx, clusterName, shardCount)
}

func (c *ClusterManager) meetAndSyncNewNodes(ctx context.Context, cli *redis.Client, clusterName string, shardCount int) error {
	if err := c.MeetNodes(cli, ctx, clusterName); err != nil {
		return fmt.Errorf("meet nodes fail: %w", err)
	}
	loginNode, err := c.findExistingNode(clusterName, shardCount)
	if err != nil {
		return err
	}
	return c.UpdateAfterMeet(ctx, loginNode, clusterName)
}

func (c *ClusterManager) assignRolesToNewNodes(ctx context.Context, clusterName string, shardCount int) error {
	sum := shardCount * c.NodesPerShard
	newNodeStartIndex := c.nodeManager.GetNodeCount() - sum
	masterToSlave := c.MasterToSlave
	IDToIP := make(map[string]string)
	count := 0
	masterID := ""
	for i := newNodeStartIndex; i < newNodeStartIndex+sum; i++ {
		ip := c.nodeManager.GetNodes()[i].ConIp
		id := c.IPToClusterID[ip]
		IDToIP[id] = ip
		if count == c.NodesPerShard {
			count = 0
		}
		if count == 0 {
			masterToSlave[id] = make([]string, 0)
			masterID = id
			c.MasterIDs = append(c.MasterIDs, masterID)
			c.EmptyMasters = append(c.EmptyMasters, c.IDToClusterNode[masterID])
		} else {
			masterToSlave[masterID] = append(masterToSlave[masterID], id)
		}
		count++
	}
	return c.configureNewNodeReplication(ctx, masterToSlave, IDToIP, clusterName)
}

func (c *ClusterManager) configureNewNodeReplication(ctx context.Context, masterToSlave map[string][]string, IDToIP map[string]string, clusterName string) error {
	for masterID, slaves := range masterToSlave {
		masterIP, exists := IDToIP[masterID]
		if !exists {
			continue
		}
		masterRuntime := c.nodeManager.GetNodeByHost(masterIP)
		if masterRuntime == nil || masterRuntime.ClusterName != clusterName {
			continue
		}
		for _, slaveID := range slaves {
			slaveIP := IDToIP[slaveID]
			if err := c.SetNodeAsSlave(ctx, masterIP, slaveIP, clusterName); err != nil {
				return err
			}
			c.AlreadySetCluster[masterID] = true
			c.AlreadySetCluster[slaveID] = true
		}
	}
	_, err := c.verifyNodeTypeSet(ctx, masterToSlave, clusterName)
	if err != nil {
		return fmt.Errorf("sync failed")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Sort helpers
// ---------------------------------------------------------------------------

// SortInfo sorts cluster node lists by IP.
func (c *ClusterManager) SortInfo() {
	c.sortClusterNodesByIP(c.ClusterNodeList)
	c.sortClusterNodesByIP(c.EmptyMasters)
}

func (c *ClusterManager) sortClusterNodesByIP(nodes []*data.ClusterNode) {
	sort.Sort(ByIP(nodes))
}

// ---------------------------------------------------------------------------
// Cluster topology: Meet, role assignment, slot allocation
// ---------------------------------------------------------------------------

// MeetNodes connects all nodes in the cluster using CLUSTER MEET.
func (c *ClusterManager) MeetNodes(client *redis.Client, ctx context.Context, clusterName string) error {
	if err := c.sendMeetCommands(client, ctx, clusterName); err != nil {
		return err
	}
	return c.waitForAllMeetSync(ctx, clusterName)
}

func (c *ClusterManager) sendMeetCommands(client *redis.Client, ctx context.Context, clusterName string) error {
	for _, node := range c.nodeManager.GetNodes() {
		if node.ClusterName != clusterName || c.AlreadyMeetNode[node.ConIp] {
			continue
		}
		meetAddr := node.ClusterMeetAddr()
		host, portStr := utils.ParseIPPort(meetAddr)
		if host == "" {
			continue
		}
		port, err := utils.StringToUint16(portStr)
		if err != nil {
			continue
		}
		if _, err = client.ClusterMeet(ctx, host, strconv.Itoa(int(port))).Result(); err != nil {
			return fmt.Errorf("could not meet node %v: %v", node.ConIp, err)
		}
	}
	return nil
}

func (c *ClusterManager) waitForAllMeetSync(ctx context.Context, clusterName string) error {
	clients := make([]*redis.Client, 0)
	defer func() {
		for _, cli := range clients {
			if err := cli.Close(); err != nil {
				log.Printf("Error closing Redis client in MeetNodes: %v", err)
			}
		}
	}()
	clusterNodeCount := 0
	for _, n := range c.nodeManager.GetNodes() {
		if n.ClusterName == clusterName {
			clusterNodeCount++
		}
	}
	for _, node := range c.nodeManager.GetNodes() {
		if node.ClusterName != clusterName {
			continue
		}
		cli, err := node.CreateRedisClient()
		if err != nil {
			return fmt.Errorf("failed to create Redis client for node %s: %v", node.ID, err)
		}
		clients = append(clients, cli)
		if err := c.waitForMeetSync(cli, ctx, clusterNodeCount); err != nil {
			return fmt.Errorf("failed to wait for cluster sync: %v", err)
		}
	}
	return nil
}

// SetAllNodeRole assigns slave roles for all nodes in the cluster.
func (c *ClusterManager) SetAllNodeRole(ctx context.Context, clusterName string) error {
	masterToSlave := make(map[string][]string)
	if err := c.assignMastersAndSlaves(ctx, masterToSlave, clusterName); err != nil {
		return err
	}
	_, err := c.verifyNodeTypeSet(ctx, masterToSlave, clusterName)
	if err != nil {
		return fmt.Errorf("sync failed")
	}
	return nil
}

func (c *ClusterManager) assignMastersAndSlaves(ctx context.Context, masterToSlave map[string][]string, clusterName string) error {
	n := len(c.ClusterNodeList) / c.NodesPerShard
	expectedNodes := len(c.ClusterNodeList)
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
			if err := c.SetNodeAsSlave(ctx, masterIP, slaveIP, clusterName); err != nil {
				return err
			}
			c.AlreadySetCluster[masterID] = true
			c.AlreadySetCluster[slaveID] = true
		}
	}
	return nil
}

// SetNodeAsSlave sets a node as replica of a master node.
func (c *ClusterManager) SetNodeAsSlave(ctx context.Context, masterIP string, slaveIP string, clusterName string) error {
	cli, err := c.connectToSlaveNode(slaveIP)
	if err != nil {
		return err
	}
	defer cli.Close()
	fmt.Printf("slave: %s\nid: %s\n", slaveIP, c.IPToClusterID[slaveIP])

	masterID := c.IPToClusterID[masterIP]
	if err := cli.ClusterReplicate(ctx, masterID).Err(); err != nil {
		fmt.Printf("Error executing ClusterReplicate for slave %s %v\n", slaveIP, err)
		return fmt.Errorf("failed to set node %s as replica of master %s: %v", slaveIP, masterIP, err)
	}
	fmt.Printf("Node %s set as replica of master %s\n", slaveIP, masterIP)
	if len(c.nodeManager.GetNodes()) == 0 {
		return fmt.Errorf("no nodes available: %w", ErrNoNodesAvailable)
	}
	return c.UpdateAfterSetNodeRole(ctx, c.nodeManager.GetNodes()[0], clusterName)
}

func (c *ClusterManager) connectToSlaveNode(slaveIP string) (*redis.Client, error) {
	slaveNode := c.nodeManager.GetNodeByHost(slaveIP)
	if slaveNode == nil {
		return nil, fmt.Errorf("slave node not found for IP %s", slaveIP)
	}
	cli, err := data.CreateRedisClient(slaveNode.ClientConnAddr())
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis client: %w", err)
	}
	return cli, nil
}

// AllocateSlots assigns hash slots to master nodes evenly.
func (c *ClusterManager) AllocateSlots(ctx context.Context, clusterName string) error {
	if len(c.nodeManager.GetNodes()) == 0 {
		return fmt.Errorf("no nodes available: %w", ErrNoNodesAvailable)
	}
	if err := c.UpdateSlots(ctx, c.nodeManager.GetNodes()[0], clusterName); err != nil {
		return err
	}
	fmt.Println("start allocate...")
	if err := c.assignSlotsToMasters(ctx, clusterName); err != nil {
		return err
	}
	return c.VerifyAllocateSlots(ctx, clusterName)
}

func (c *ClusterManager) assignSlotsToMasters(ctx context.Context, clusterName string) error {
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
		masterID := c.MasterIDs[i]
		masterNode, ok := c.IDToClusterNode[masterID]
		if !ok {
			return fmt.Errorf("master node not found for ID %s", masterID)
		}
		port := c.nodeManager.GetNodeByHost(masterNode.IP)
		if port == nil {
			return fmt.Errorf("runtime node not found for IP %s", masterNode.IP)
		}
		cli, err := data.CreateRedisClient(port.ClientConnAddr())
		if err != nil {
			return fmt.Errorf("failed to create Redis client: %w", err)
		}
		var slots []int
		for j := startPoint; j <= endPoint; j++ {
			slots = append(slots, j)
		}
		if err := cli.ClusterAddSlots(ctx, slots...).Err(); err != nil {
			cli.Close()
			return fmt.Errorf("failed to add slots to master %s: %w", masterID, err)
		}
		cli.Close()
	}
	return nil
}

// ---------------------------------------------------------------------------
// Node info refresh
// ---------------------------------------------------------------------------

// UpdateAfterMeet refreshes the cluster node map after a MEET operation.
func (c *ClusterManager) UpdateAfterMeet(ctx context.Context, LoginNode *data.RuntimeNode, clusterName string) error {
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
	masters, slaves := c.classifyNodesByType(nodes)
	return c.buildMasterSlaveTopology(masters, slaves)
}

func (c *ClusterManager) classifyNodesByType(nodes []data.ClusterNode) (masters, slaves []data.ClusterNode) {
	for _, node := range nodes {
		if node.NodeType == "master" {
			masters = append(masters, node)
		} else if node.NodeType == "slave" {
			slaves = append(slaves, node)
		}
	}
	return
}

func (c *ClusterManager) buildMasterSlaveTopology(masters, slaves []data.ClusterNode) error {
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
		if node.NodeType != data.Master {
			continue
		}
		if _, ok := c.IDToClusterNode[node.ID]; !ok {
			continue
		}
		c.IDToClusterNode[node.ID].SlotsNum = c.calculateSlots(node.Slots)
		if len(node.Slots) == 0 {
			c.EmptyMasters = append(c.EmptyMasters, &node)
		}
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
	c.printClusterNodeLines(nodesInfo)
	return nil
}

func (c *ClusterManager) printClusterNodeLines(nodesInfo string) {
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
}

// ---------------------------------------------------------------------------
// Slot migration: planning, execution, progress, cache invalidation
// ---------------------------------------------------------------------------

// MigrateSlotsToEmptyNode migrates slots from existing masters to empty master nodes.
func (c *ClusterManager) MigrateSlotsToEmptyNode(ctx context.Context, clusterName string) error {
	if len(c.EmptyMasters) == 0 {
		return fmt.Errorf("no Empty master")
	}
	if len(c.MasterIDs) == 0 {
		return fmt.Errorf("no master nodes for slot migration")
	}
	tasks := c.collectSlotTasks(clusterName)
	if len(tasks) == 0 {
		return nil
	}
	groups := c.groupSlotTasks(tasks)
	fmt.Printf("slot migration: %d slots across %d source groups (%d workers each)\n",
		len(tasks), len(groups), migrationWorkers)
	c.executeSlotMigration(ctx, tasks, groups)
	if c.cacheInvalidator != nil {
		c.invalidateCacheAfterMigration(tasks)
	}
	return c.PrintClusterNodesInfo(ctx)
}

func (c *ClusterManager) collectSlotTasks(clusterName string) []slotTask {
	newV := TotalSlots / len(c.MasterIDs)
	var tasks []slotTask
	empIdx := 0
	for _, masterID := range c.MasterIDs {
		masterNode, ok := c.IDToClusterNode[masterID]
		if !ok || masterNode.SlotsNum == 0 || masterNode.ClusterName != clusterName || masterNode.SlotsNum <= newV {
			continue
		}
		empIdx = c.collectTasksFromMaster(masterNode, newV, empIdx, &tasks)
	}
	return tasks
}

func (c *ClusterManager) collectTasksFromMaster(masterNode *data.ClusterNode, newV, empIdx int, tasks *[]slotTask) int {
	for _, slot := range masterNode.Slots {
		for slotNum := slot.End; slotNum >= slot.Start && empIdx < len(c.EmptyMasters); slotNum-- {
			if masterNode.ID == c.EmptyMasters[empIdx].ID {
				break
			}
			*tasks = append(*tasks, c.buildSlotTask(slotNum, masterNode, empIdx))
			c.EmptyMasters[empIdx].SlotsNum++
			masterNode.SlotsNum--
			if c.EmptyMasters[empIdx].SlotsNum == newV {
				empIdx++
			}
			if masterNode.SlotsNum == newV {
				break
			}
		}
	}
	return empIdx
}

func (c *ClusterManager) buildSlotTask(slotNum int, masterNode *data.ClusterNode, empIdx int) slotTask {
	toIP := c.EmptyMasters[empIdx].IP
	toID := c.EmptyMasters[empIdx].ID
	destRuntime := c.nodeManager.GetNodeByHost(toIP)
	fromRuntime := c.nodeManager.GetNodeByHost(masterNode.IP)
	var destPort, fromPort, conPort uint16
	if destRuntime != nil {
		destPort = destRuntime.HostPort
		conPort = destRuntime.ConPort
	}
	if fromRuntime != nil {
		fromPort = fromRuntime.HostPort
	}
	return slotTask{
		slot:          slotNum,
		fromID:        masterNode.ID,
		fromIP:        masterNode.IP,
		toID:          toID,
		toIP:          toIP,
		destHostPort:  destPort,
		fromHostPort:  fromPort,
		containerPort: conPort,
	}
}

func (c *ClusterManager) groupSlotTasks(tasks []slotTask) map[string]*group {
	groups := make(map[string]*group)
	for i := range tasks {
		t := &tasks[i]
		key := t.fromIP
		if groups[key] == nil {
			groups[key] = &group{fromIP: t.fromIP, fromHostPort: t.fromHostPort}
		}
		groups[key].tasks = append(groups[key].tasks, *t)
	}
	return groups
}

func (c *ClusterManager) executeSlotMigration(ctx context.Context, tasks []slotTask, groups map[string]*group) {
	var completed atomic.Int64
	totalSlots := int64(len(tasks))
	ctxProgress, cancelProgress := context.WithCancel(ctx)
	defer cancelProgress()
	go c.reportMigrationProgress(ctxProgress, &completed, totalSlots)
	var wg sync.WaitGroup
	errCh := make(chan error, len(tasks))
	for _, g := range groups {
		sourceCli, err := data.CreateRedisClient(fmt.Sprintf("127.0.0.1:%d", g.fromHostPort))
		if err != nil {
			cancelProgress()
			log.Printf("connect source %s fail: %v", g.fromIP, err)
			return
		}
		defer sourceCli.Close()
		taskCh := make(chan slotTask, len(g.tasks))
		for _, t := range g.tasks {
			taskCh <- t
		}
		close(taskCh)
		for w := 0; w < migrationWorkers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for t := range taskCh {
					if err := migrateSlotShared(ctx, sourceCli, t); err != nil {
						errCh <- fmt.Errorf("slot %d from %s to %s: %w", t.slot, t.fromIP, t.toIP, err)
					}
					completed.Add(1)
				}
			}()
		}
	}
	wg.Wait()
	cancelProgress()
	close(errCh)
	for err := range errCh {
		log.Printf("slot migration: %v", err)
	}
	fmt.Printf("  slot migration: %d/%d (100%%)\n", totalSlots, totalSlots)
}

func (c *ClusterManager) reportMigrationProgress(ctx context.Context, completed *atomic.Int64, total int64) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			done := completed.Load()
			if done < total {
				fmt.Printf("  slot migration: %d/%d (%.1f%%)\n", done, total, float64(done)/float64(total)*100)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *ClusterManager) invalidateCacheAfterMigration(tasks []slotTask) {
	minSlot, maxSlot := tasks[0].slot, tasks[0].slot
	for i := range tasks {
		if tasks[i].slot < minSlot {
			minSlot = tasks[i].slot
		}
		if tasks[i].slot > maxSlot {
			maxSlot = tasks[i].slot
		}
	}
	c.cacheInvalidator.InvalidateSlots(minSlot, maxSlot)
}

// migrateSlotShared migrates a single slot using a shared source client.
func migrateSlotShared(ctx context.Context, sourceCli *redis.Client, t slotTask) error {
	destCli, err := data.CreateRedisClient(fmt.Sprintf("127.0.0.1:%d", t.destHostPort))
	if err != nil {
		return fmt.Errorf("connect dest fail: %w", err)
	}
	defer destCli.Close()
	if err := setupSlotMigration(ctx, destCli, sourceCli, t); err != nil {
		return err
	}
	keys := sourceCli.ClusterGetKeysInSlot(ctx, t.slot, 1000).Val()
	if len(keys) > 0 {
		migrateSlotKeys(ctx, sourceCli, t, keys)
	}
	return finalizeSlotMigration(ctx, destCli, sourceCli, t)
}

func setupSlotMigration(ctx context.Context, destCli, sourceCli *redis.Client, t slotTask) error {
	if _, err := utils.ExecuteClusterCommand(ctx, destCli, "cluster", "SETSLOT",
		strconv.Itoa(t.slot), "IMPORTING", t.fromID); err != nil {
		return fmt.Errorf("IMPORTING slot %d: %w", t.slot, err)
	}
	if _, err := utils.ExecuteClusterCommand(ctx, sourceCli, "CLUSTER", "SETSLOT",
		strconv.Itoa(t.slot), "MIGRATING", t.toID); err != nil {
		return fmt.Errorf("MIGRATING slot %d: %w", t.slot, err)
	}
	return nil
}

func migrateSlotKeys(ctx context.Context, sourceCli *redis.Client, t slotTask, keys []string) {
	if len(keys) <= smallKeyBatch {
		migrateKeyBatch(ctx, sourceCli, t, keys, 5000)
		return
	}
	normal, big, _ := classifyKeys(ctx, sourceCli, keys)
	for _, entry := range big {
		if err := migrateBigKey(ctx, sourceCli, t, entry.key, entry.size); err != nil {
			log.Printf("big key %s (%.1fMB) migration failed: %v",
				entry.key, float64(entry.size)/1024/1024, err)
		}
	}
	if len(normal) == 0 {
		return
	}
	chunkSize := len(normal) / chunkDivisor
	if chunkSize < 10 {
		chunkSize = 10
	}
	for i := 0; i < len(normal); i += chunkSize {
		end := i + chunkSize
		if end > len(normal) {
			end = len(normal)
		}
		active := filterExistingKeys(ctx, sourceCli, normal[i:end])
		if len(active) > 0 {
			migrateKeyBatch(ctx, sourceCli, t, active, 5000)
		}
	}
}

func finalizeSlotMigration(ctx context.Context, destCli, sourceCli *redis.Client, t slotTask) error {
	if _, err := utils.ExecuteClusterCommand(ctx, destCli, "CLUSTER", "SETSLOT",
		strconv.Itoa(t.slot), "NODE", t.toID); err != nil {
		return fmt.Errorf("SETSLOT NODE dest slot %d: %w", t.slot, err)
	}
	if _, err := utils.ExecuteClusterCommand(ctx, sourceCli, "CLUSTER", "SETSLOT",
		strconv.Itoa(t.slot), "NODE", t.toID); err != nil {
		return fmt.Errorf("SETSLOT NODE source slot %d: %w", t.slot, err)
	}
	return nil
}

// classifyKeys separates keys into normal and big based on MEMORY USAGE.
func classifyKeys(ctx context.Context, cli *redis.Client, keys []string) (normal []string, big []keyEntry, failed []string) {
	for _, key := range keys {
		size, err := cli.MemoryUsage(ctx, key, 0).Result()
		if err != nil {
			failed = append(failed, key)
		} else if size >= bigKeyThreshold {
			big = append(big, keyEntry{key, size})
		} else {
			normal = append(normal, key)
		}
	}
	return
}

// migrateBigKey migrates a single large key using COPY mode for safety.
func migrateBigKey(ctx context.Context, sourceCli *redis.Client, t slotTask, key string, size int64) error {
	timeoutSec := size / 1024 / 1024
	if timeoutSec < 5 {
		timeoutSec = 5
	}
	if timeoutSec > 60 {
		timeoutSec = 60
	}
	fmt.Printf("  migrating big key %s (%.1fMB, timeout=%ds)\n", key, float64(size)/1024/1024, timeoutSec)
	args := []interface{}{t.toIP, t.containerPort, key, 0, timeoutSec * 1000, "COPY", "REPLACE"}
	if err := sourceCli.Do(ctx, append([]interface{}{"MIGRATE"}, args...)...).Err(); err != nil {
		return fmt.Errorf("MIGRATE big key %s: %w", key, err)
	}
	if err := sourceCli.Del(ctx, key).Err(); err != nil {
		log.Printf("big key %s copied but source delete failed: %v", key, err)
	}
	return nil
}

// migrateKeyBatch sends a batch of keys via a single MIGRATE command.
func migrateKeyBatch(ctx context.Context, sourceCli *redis.Client, t slotTask, keys []string, timeoutMs int) {
	port := strconv.Itoa(int(t.containerPort))
	args := []interface{}{t.toIP, port, "", 0, timeoutMs, "KEYS"}
	for _, key := range keys {
		args = append(args, key)
	}
	if err := sourceCli.Do(ctx, append([]interface{}{"MIGRATE"}, args...)...).Err(); err != nil {
		log.Printf("MIGRATE slot %d (%d keys): %v", t.slot, len(keys), err)
	}
}

// filterExistingKeys returns keys that exist on the source node.
func filterExistingKeys(ctx context.Context, cli *redis.Client, keys []string) []string {
	var result []string
	for _, key := range keys {
		exists, err := cli.Exists(ctx, key).Result()
		if err != nil {
			continue
		}
		if exists > 0 {
			result = append(result, key)
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Verification helpers
// ---------------------------------------------------------------------------

// verifyNodeTypeSet polls all nodes until the master/slave topology matches expectations.
func (c *ClusterManager) verifyNodeTypeSet(ctx context.Context, masterToSlave map[string][]string, clusterName string) (bool, error) {
	for _, node := range c.nodeManager.GetNodes() {
		if node.ClusterName != clusterName {
			continue
		}
		if ok, err := c.checkNodeTypeWithRetry(ctx, node, masterToSlave, clusterName); err != nil {
			return false, err
		} else if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (c *ClusterManager) checkNodeTypeWithRetry(ctx context.Context, node *data.RuntimeNode, masterToSlave map[string][]string, clusterName string) (bool, error) {
	tryTimes := 10
	for i := 0; i < tryTimes; i++ {
		if err := c.UpdateAfterSetNodeRole(ctx, node, clusterName); err != nil {
			fmt.Printf("try sync fail %d\n", i+1)
			time.Sleep(2 * time.Second)
			continue
		}
		if c.equalClusterNodeType(masterToSlave, c.MasterToSlave) {
			return true, nil
		}
		fmt.Printf("%v try %d /10 sync fail\n", node.Name, i+1)
		time.Sleep(3 * time.Second)
	}
	return false, fmt.Errorf("failed to verify node type set")
}

// equalClusterNodeType compares two master-to-slave maps for structural equality.
func (c *ClusterManager) equalClusterNodeType(m1 map[string][]string, m2 map[string][]string) bool {
	if len(m1) != len(m2) {
		return false
	}
	for k, v := range m1 {
		v2, exists := m2[k]
		if !exists || len(v) != len(v2) {
			return false
		}
	}
	return true
}

// waitForMeetSync waits until all expected nodes appear in the cluster.
func (c *ClusterManager) waitForMeetSync(client *redis.Client, ctx context.Context, expectedNodes int) error {
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		nodesInfo, err := client.ClusterNodes(ctx).Result()
		if err != nil {
			return fmt.Errorf("failed to get cluster nodes info: %v", err)
		}
		if c.countActiveNodes(nodesInfo) == expectedNodes {
			return nil
		}
		time.Sleep(2 * time.Second)
		fmt.Printf("ClusterManager not fully synchronized, retrying... (%d/%d)\n", i+1, maxRetries)
	}
	return fmt.Errorf("cluster did not synchronize within the expected time")
}

func (c *ClusterManager) countActiveNodes(nodesInfo string) int {
	lines := strings.Split(nodesInfo, "\n")
	count := 0
	for _, line := range lines {
		fields := strings.Split(line, " ")
		if len(fields) < 8 {
			continue
		}
		if strings.Contains(fields[2], "fail") {
			continue
		}
		if strings.Contains(line, "connected") {
			count++
		}
	}
	return count
}

// VerifyAllocateSlots verifies that all master nodes have the expected number of slots allocated.
func (c *ClusterManager) VerifyAllocateSlots(ctx context.Context, clusterName string) error {
	for _, container := range c.nodeManager.GetNodes() {
		if err := c.checkSlotCounts(ctx, container, clusterName); err != nil {
			return err
		}
	}
	return nil
}

func (c *ClusterManager) checkSlotCounts(ctx context.Context, container *data.RuntimeNode, clusterName string) error {
	tryTimes := 10
	for j := 0; j < tryTimes; j++ {
		if err := c.UpdateSlots(ctx, container, clusterName); err != nil {
			return err
		}
		if len(c.MasterIDs) == 0 {
			break
		}
		slotsPerMaster := TotalSlots / len(c.MasterIDs)
		remainder := TotalSlots % len(c.MasterIDs)
		ok := true
		for i := 0; i < len(c.MasterIDs); i++ {
			expectedSlots := slotsPerMaster
			if i == len(c.MasterIDs)-1 && remainder != 0 {
				expectedSlots = slotsPerMaster + remainder
			}
			masterNode := c.IDToClusterNode[c.MasterIDs[i]]
			if masterNode == nil || masterNode.SlotsNum != expectedSlots {
				ok = false
				break
			}
		}
		if ok {
			return nil
		}
		fmt.Printf("slot verification attempt %d/%d, retrying...\n", j+1, tryTimes)
		time.Sleep(2 * time.Second)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Cluster node parsing
// ---------------------------------------------------------------------------

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
	return c.ParseRedisClusterNodes(ctx, nodesInfo, clusterName)
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
	if len(failedIDs) > 0 {
		c.forgetFailedClusterNodes(ctx, clusterName, failedIDs)
	}
	return parsed, nil
}

func (c *ClusterManager) forgetFailedClusterNodes(ctx context.Context, clusterName string, failedIDs []string) {
	if c.nodeManager == nil || len(c.nodeManager.GetNodes()) == 0 {
		return
	}
	clusterIndex := -1
	for i := 0; i < len(c.nodeManager.GetNodes()); i++ {
		if c.nodeManager.GetNodes()[i].ClusterName == clusterName {
			clusterIndex = i
			break
		}
	}
	if clusterIndex < 0 {
		return
	}
	client, err := data.CreateRedisClient(c.nodeManager.GetNodes()[clusterIndex].ClientConnAddr())
	if err != nil {
		log.Printf("failed to create Redis client for forget: %v", err)
		return
	}
	defer client.Close()
	for _, id := range failedIDs {
		if _, ferr := client.ClusterForget(ctx, id).Result(); ferr != nil {
			log.Printf("ClusterForget failed: %v", ferr)
		}
	}
}
