package model

import (
	"context"
	"fmt"
	"log"
	"redisClusterManager/cluster/config"
	"redisClusterManager/cluster/utils"
	"sort"
	"sync"
	"sync/atomic"
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
	nodeManager       PodManager
	NodesPerShard     int
	cacheInvalidator  config.CacheInvalidator
}

func NewClusterManager(nodesPerShard int, nodeManager PodManager) *ClusterManager {
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

// SetCacheInvalidator registers a callback for cache invalidation after slot migration.
func (c *ClusterManager) SetCacheInvalidator(inv config.CacheInvalidator) {
	c.cacheInvalidator = inv
}

func (c *ClusterManager) CreateSource(ctx context.Context, clusterName string, sum int) error {
	var err error
	err = c.nodeManager.CreatePods(ctx, sum, clusterName)
	if err != nil {
		return fmt.Errorf("create pods fail %v", err)
	}

	fmt.Println("pods info list follows:")

	for _, node := range c.nodeManager.GetNodes() {
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
	if len(c.nodeManager.GetNodes()) > 0 {
		for ; meetNodeIndex < len(c.nodeManager.GetNodes()); meetNodeIndex++ {
			if c.nodeManager.GetNodes()[meetNodeIndex].ClusterName == clusterName {
				break
			}
		}
		if meetNodeIndex >= len(c.nodeManager.GetNodes()) {
			return fmt.Errorf("no node found for cluster %s: %w", clusterName, ErrClusterNotFound)
		}
		cliRedis, err = c.nodeManager.GetNodes()[meetNodeIndex].CreateRedisClient()
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
	err = c.UpdateAfterMeet(ctx, c.nodeManager.GetNodes()[meetNodeIndex], clusterName)
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

func (c *ClusterManager) AddShards(ctx context.Context, shardCount int, clusterName string) error {
	var err error
	sum := shardCount * c.NodesPerShard
	err = c.CreateSource(ctx, clusterName, sum)
	if err != nil {
		return err
	}
	// Find a login node for meeting
	meetNodeIndex := 0
	for ; meetNodeIndex < c.nodeManager.GetNodeCount()-sum; meetNodeIndex++ {
		if c.nodeManager.GetNodes()[meetNodeIndex].ClusterName == clusterName {
			break
		}
	}
	if meetNodeIndex >= c.nodeManager.GetNodeCount()-sum {
		return fmt.Errorf("no existing node found for cluster %s", clusterName)
	}
	cliRedis, err := c.nodeManager.GetNodes()[meetNodeIndex].CreateRedisClient()
	if err != nil {
		return fmt.Errorf("create redis client fail: %w", err)
	}
	defer cliRedis.Close()
	err = c.MeetNodes(cliRedis, ctx, clusterName)
	if err != nil {
		return fmt.Errorf("meet nodes fail: %w", err)
	}
	err = c.UpdateAfterMeet(ctx, c.nodeManager.GetNodes()[meetNodeIndex], clusterName)
	if err != nil {
		return err
	}
	c.EmptyMasters = make([]*ClusterNode, 0)
	newNodeStartIndex := c.nodeManager.GetNodeCount() - sum
	var masterToSlave = make(map[string][]string)
	masterToSlave = c.MasterToSlave
	IDToIP := make(map[string]string)
	count := 0
	masterID := ""
	for i := newNodeStartIndex; i < newNodeStartIndex+sum; i++ {
		ip := c.nodeManager.GetNodes()[i].ConIp
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
		masterRuntime := c.nodeManager.GetNodeByHost(masterIP); 
			if masterRuntime == nil || masterRuntime.ClusterName != clusterName {
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

func (c *ClusterManager) SortInfo() {
	c.sortClusterNodesByIP(c.ClusterNodeList)
	c.sortClusterNodesByIP(c.EmptyMasters)
}

func (c *ClusterManager) UpdateAfterSetNodeRole(ctx context.Context, LoginNode *RuntimeNode, clusterName string) error {
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
	if c.nodeManager.GetNodeCount() == 0 {
		return nil
	}

	c.EmptyMasters = make([]*ClusterNode, 0)

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
	slaveNode := c.nodeManager.GetNodeByHost(slaveAddr)
	if slaveNode == nil {
		return fmt.Errorf("slave node not found for IP %s", slaveAddr)
	}
	cli, err := CreateRedisClient(ctx, slaveNode.ClientConnAddr())
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
	slotsPerMaster := config.TotalSlots / numMasters
	for i := 0; i < numMasters; i++ {
		startPoint := i * slotsPerMaster
		endPoint := startPoint + slotsPerMaster - 1
		if i == numMasters-1 {
			endPoint = config.TotalSlots - 1
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
		cliClusterMaster, err := CreateRedisClient(ctx, port.ClientConnAddr())
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
	if len(c.nodeManager.GetNodes()) == 0 {
		return fmt.Errorf("no nodes available: %w", ErrNoNodesAvailable)
	}
	time.Sleep(time.Duration(len(c.ClusterNodeList)/3) * time.Second)
	client, err := CreateRedisClient(ctx, c.nodeManager.GetNodes()[0].ClientConnAddr())
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

// concurrency of slot migration workers.
const migrationWorkers = 16

// Slot migration thresholds.
const (
	smallKeyBatch    = 50          // keys: single MIGRATE is faster than chunking
	chunkDivisor     = 10          // split into this many chunks for large key counts
	bigKeyThreshold  = 10 * 1024 * 1024 // 10MB: treat as "big key"
)

type slotTask struct {
	slot         int
	fromID       string
	fromIP       string
	toID         string
	toIP         string
	destHostPort uint16
	fromHostPort uint16
	containerPort uint16
}

func (c *ClusterManager) MigratesSlotsToEmptyNode(ctx context.Context, clusterName string) error {
	if len(c.EmptyMasters) == 0 {
		return fmt.Errorf("no Empty master")
	}
	if len(c.MasterIDs) == 0 {
		return fmt.Errorf("no master nodes for slot migration")
	}

	newV := config.TotalSlots / len(c.MasterIDs)

	// Phase 1: collect all slot migration tasks.
	var tasks []slotTask
	toSlots := make(map[string]int) // toNodeID -> target slot count

	empIdx := 0
	for _, masterID := range c.MasterIDs {
		masterNode, ok := c.IDToClusterNode[masterID]
		if !ok || masterNode.SlotsNum == 0 || masterNode.ClusterName != clusterName || masterNode.SlotsNum <= newV {
			continue
		}
		for _, slot := range masterNode.Slots {
			for i := slot.End; i >= slot.Start && empIdx < len(c.EmptyMasters); i-- {
				toID := c.EmptyMasters[empIdx].ID
				if masterID == toID {
					break
				}
				destRuntime := c.nodeManager.GetNodeByHost(c.EmptyMasters[empIdx].IP)
				fromRuntime := c.nodeManager.GetNodeByHost(masterNode.IP)
				var destPort, fromPort, conPort uint16
				if destRuntime != nil {
					destPort = destRuntime.HostPort
					conPort = destRuntime.ConPort
				}
				if fromRuntime != nil {
					fromPort = fromRuntime.HostPort
				}
				tasks = append(tasks, slotTask{
					slot:          i,
					fromID:        masterID,
					fromIP:        masterNode.IP,
					toID:          toID,
					toIP:          c.EmptyMasters[empIdx].IP,
					destHostPort:  destPort,
					fromHostPort:  fromPort,
					containerPort: conPort,
				})
				toSlots[toID]++
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
	}

	if len(tasks) == 0 {
		return nil
	}

	// Phase 2: group tasks by source IP and migrate concurrently.
	type group struct {
		tasks         []slotTask
		fromIP        string
		fromHostPort  uint16
	}
	groups := make(map[string]*group)
	for i := range tasks {
		t := &tasks[i]
		key := t.fromIP
		if groups[key] == nil {
			groups[key] = &group{fromIP: t.fromIP, fromHostPort: t.fromHostPort}
		}
		groups[key].tasks = append(groups[key].tasks, *t)
	}

	fmt.Printf("slot migration: %d slots across %d source groups (%d workers each)\n", len(tasks), len(groups), migrationWorkers)

	var completed atomic.Int64
	totalSlots := int64(len(tasks))

	// Progress reporter goroutine.
	ctxProgress, cancelProgress := context.WithCancel(ctx)
	defer cancelProgress()
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				done := completed.Load()
				if done < totalSlots {
					fmt.Printf("  slot migration: %d/%d (%.1f%%)\n", done, totalSlots, float64(done)/float64(totalSlots)*100)
				}
			case <-ctxProgress.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	errCh := make(chan error, len(tasks))

	for _, g := range groups {
		// Shared source client per group.
		sourceCli, err := CreateRedisClient(ctx, fmt.Sprintf("127.0.0.1:%d", g.fromHostPort))
		if err != nil {
			cancelProgress()
			return fmt.Errorf("connect source %s fail: %w", g.fromIP, err)
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

	// Collect errors (non-fatal).
	for err := range errCh {
		log.Printf("slot migration: %v", err)
	}

	fmt.Printf("  slot migration: %d/%d (100%%)\n", totalSlots, totalSlots)

	// Notify cache layer to invalidate affected slots.
	if c.cacheInvalidator != nil && len(tasks) > 0 {
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

	return c.PrintClusterNodesInfo(ctx)
}

// migrateSlotShared migrates a single slot using a shared source client.
// Handles: empty slots (instant), small batches (single MIGRATE), large batches (chunked), and big keys (COPY mode).
func migrateSlotShared(ctx context.Context, sourceCli *redis.Client, t slotTask) error {
	destCli, err := CreateRedisClient(ctx, fmt.Sprintf("127.0.0.1:%d", t.destHostPort))
	if err != nil {
		return fmt.Errorf("connect dest fail: %w", err)
	}
	defer destCli.Close()

	// IMPORTING on dest
	_, err = utils.ExecuteClusterCommand(ctx, destCli, "cluster", "SETSLOT", strconv.Itoa(t.slot), "IMPORTING", t.fromID)
	if err != nil {
		return fmt.Errorf("IMPORTING slot %d: %w", t.slot, err)
	}
	// MIGRATING on source
	_, err = utils.ExecuteClusterCommand(ctx, sourceCli, "CLUSTER", "SETSLOT", strconv.Itoa(t.slot), "MIGRATING", t.toID)
	if err != nil {
		return fmt.Errorf("MIGRATING slot %d: %w", t.slot, err)
	}

	// Migrate keys if any.
	keys := sourceCli.ClusterGetKeysInSlot(ctx, t.slot, 1000).Val()
	if len(keys) == 0 {
		// Scenario A: empty slot — skip data migration.
		goto finalize
	}

	if len(keys) <= smallKeyBatch {
		// Scenario B: few keys — single MIGRATE is fastest.
		migrateKeyBatch(ctx, sourceCli, t, keys, 5000)
	} else {
		// Scenario C/D: many keys or possible big keys.
		normal, big, _ := classifyKeys(ctx, sourceCli, keys)
		// Big keys: migrate individually with COPY mode.
		for _, entry := range big {
			if err := migrateBigKey(ctx, sourceCli, t, entry.key, entry.size); err != nil {
				log.Printf("big key %s (%.1fMB) migration failed: %v", entry.key, float64(entry.size)/1024/1024, err)
			}
		}
		// Normal keys: chunked migration.
		if len(normal) > 0 {
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
	}

finalize:
	// SETSLOT NODE on dest
	_, err = utils.ExecuteClusterCommand(ctx, destCli, "CLUSTER", "SETSLOT", strconv.Itoa(t.slot), "NODE", t.toID)
	if err != nil {
		return fmt.Errorf("SETSLOT NODE dest slot %d: %w", t.slot, err)
	}
	// SETSLOT NODE on source
	_, err = utils.ExecuteClusterCommand(ctx, sourceCli, "CLUSTER", "SETSLOT", strconv.Itoa(t.slot), "NODE", t.toID)
	if err != nil {
		return fmt.Errorf("SETSLOT NODE source slot %d: %w", t.slot, err)
	}
	return nil
}

type keyEntry struct {
	key  string
	size int64
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
	timeoutSec := size / 1024 / 1024 // 1 second per MB
	if timeoutSec < 5 {
		timeoutSec = 5
	}
	if timeoutSec > 60 {
		timeoutSec = 60
	}

	fmt.Printf("  migrating big key %s (%.1fMB, timeout=%ds)\n", key, float64(size)/1024/1024, timeoutSec)

	// COPY mode: keep source key until we confirm the destination has it.
	args := []interface{}{t.toIP, t.containerPort, key, 0, timeoutSec * 1000, "COPY", "REPLACE"}
	if err := sourceCli.Do(ctx, append([]interface{}{"MIGRATE"}, args...)...).Err(); err != nil {
		// COPY keeps the source key — safe to retry.
		return fmt.Errorf("MIGRATE big key %s: %w", key, err)
	}
	// Key is now on the destination. Safe to delete from source.
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

func (c *ClusterManager) sortClusterNodesByIP(nodes []*ClusterNode) {
	sort.Sort(ByIP(nodes))
}

func (c *ClusterManager) GetClusterNodes(ctx context.Context, LoginNode *RuntimeNode, clusterName string) ([]ClusterNode, error) {
	if len(c.nodeManager.GetNodes()) == 0 {
		return nil, fmt.Errorf("no pods found: %w", ErrClusterNotFound)
	}
	client, err := CreateRedisClient(ctx, LoginNode.ClientConnAddr())
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
	for _, node := range c.nodeManager.GetNodes() {
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
	for _, container := range c.nodeManager.GetNodes() {
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
			slotsPerMaster := config.TotalSlots / len(cluster.MasterIDs)
			remainder := config.TotalSlots % len(cluster.MasterIDs)
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
			client, err := CreateRedisClient(ctx, c.nodeManager.GetNodes()[clusterIndex].ClientConnAddr())
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
				log.Printf("failed to parse slots for node %s: %v", node.ID, err)
			continue
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

