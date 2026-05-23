package model

import (
	"context"
	"fmt"
	"sort"

	"redisClusterManager/cluster/data"

	"github.com/go-redis/redis/v8"
)

type ByIP []*data.ClusterNode

func (a ByIP) Len() int           { return len(a) }
func (a ByIP) Less(i, j int) bool { return a[i].IP < a[j].IP }
func (a ByIP) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

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

func NewClusterManager(nodesPerShard int, nodeManager PodManager) *ClusterManager {
	clusterManager := ClusterManager{
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

	return &clusterManager
}

// SetCacheInvalidator registers a callback for cache invalidation after slot migration.
func (c *ClusterManager) SetCacheInvalidator(inv CacheInvalidator) {
	c.cacheInvalidator = inv
}

// CreatePodsForCluster creates pods for the cluster.
func (c *ClusterManager) CreatePodsForCluster(ctx context.Context, clusterName string, sum int) error {
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
	err = c.CreatePodsForCluster(ctx, clusterName, sum)
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

// AddShards adds new shards to an existing cluster.
func (c *ClusterManager) AddShards(ctx context.Context, shardCount int, clusterName string) error {
	var err error
	sum := shardCount * c.NodesPerShard
	err = c.CreatePodsForCluster(ctx, clusterName, sum)
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
	c.EmptyMasters = make([]*data.ClusterNode, 0)
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
		masterRuntime := c.nodeManager.GetNodeByHost(masterIP)
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

func (c *ClusterManager) SortInfo() {
	c.sortClusterNodesByIP(c.ClusterNodeList)
	c.sortClusterNodesByIP(c.EmptyMasters)
}

func (c *ClusterManager) sortClusterNodesByIP(nodes []*data.ClusterNode) {
	sort.Sort(ByIP(nodes))
}
