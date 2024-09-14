package model

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"redisStudy/utils"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
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
	Replica           int
}

func NewClusterManager(ctx context.Context, replica int) (context.Context, *ClusterManager) {

	ctx, containersManager := NewContainersManager(ctx)
	clusterManager := ClusterManager{
		EmptyMasters:      make([]ClusterNode, 0),
		IDToClusterNode:   make(map[string]ClusterNode),
		IPToClusterID:     make(map[string]string),
		AlreadyMeetNode:   make(map[string]bool),
		MasterToSlave:     make(map[string][]string),
		AlreadySetCluster: make(map[string]bool),
		ClusterNodeList:   make([]ClusterNode, 0),
		MasterIDs:         make([]string, 0),
		MasterSet:         make(map[string]bool),
		containersManager: containersManager,
		Replica:           replica,
	}

	return ctx, &clusterManager
}

func (c *ClusterManager) CreateClusterNodes(shared int, ctx context.Context) (context.Context, error) {
	var err error
	sum := shared * c.Replica
	err = c.containersManager.CreateContainers(ctx, sum)
	if err != nil {
		return ctx, fmt.Errorf("create container fail %v", err)
	}

	ctx, err = c.containersManager.UpdateNewContainersInfo(ctx)
	if err != nil {
		return ctx, err
	}

	fmt.Printf("finish created, %d sharder, %d 个replica ", shared, c.Replica)
	fmt.Println("containers info list：")

	// 打印当前容器信息
	for _, node := range c.containersManager.Nodes {
		fmt.Println("containerName: ", node.Name, "containerID: ", node.ID, "HostIP: ", node.HostIP, "HostPort:", node.HostPort, "ContainerIP:", node.ConIp, "containerPort: ", node.ConPort)
	}

	// 创建redis client
	if len(c.containersManager.Nodes) > 0 {
		cliRedis, err = c.containersManager.Nodes[0].CreateRedisClient(ctx)
		defer func() {
			if err := cliRedis.Close(); err != nil {
				fmt.Printf("Error closing Redis client: %v\n", err)
			}
		}()

	} else {
		// 处理容器列表为空的情况
		fmt.Println("No containers available.")
	}

	fmt.Println("start Meet...")
	ctx, err = c.MeetNodes(cliRedis, ctx)
	if err != nil {
		return ctx, err
	}
	// 获取集群信息
	ctx, err = c.UpdateClusterNodes(ctx, c.containersManager.Nodes[0])
	if err != nil {
		fmt.Printf("Fail to ger ClusterManager Info : %v", err)
		return ctx, err
	}

	return ctx, err
}

func (c *ClusterManager) UpdateContainerInfo(ctx context.Context) (context.Context, error) {
	ctx, err := c.containersManager.UpdateNewContainersInfo(ctx)
	if err != nil {
		return ctx, err
	}
	return ctx, nil
}

func (c *ClusterManager) GetContainerNum() int {
	return c.containersManager.Num
}

func (c *ClusterManager) AddShaders(ctx context.Context, shaderNum int) (context.Context, error) {
	ctx, err := c.GetContainersInfo(ctx)
	if err != nil {
		return ctx, err
	}
	ctx, err = c.UpdateContainerInfo(ctx)
	if err != nil {
		return ctx, err
	}

	for i := 0; i < shaderNum; i++ {
		ctx, _, err := c.addShaderAndReplica(ctx)
		if err != nil {
			return ctx, err
		}
	}

	ctx, err = c.GetContainersInfo(ctx)
	if err != nil {
		return ctx, err
	}

	ctx, err = c.UpdateClusterNodes(ctx, c.containersManager.Nodes[0])
	if err != nil {
		return ctx, err
	}
	return ctx, nil
}

func (c *ClusterManager) UpdateClusterNodes(ctx context.Context, LoginNode *ContainerNode) (context.Context, error) {
	var err error
	containersManager := c.containersManager
	if containersManager.Num == 0 {
		//fmt.Println("nodes num is 0")
		return ctx, nil
	}

	nodes, err := c.getClusterNodes(ctx, containersManager, LoginNode)
	if err != nil {
		return ctx, err
	}
	if len(nodes) == 0 {
		fmt.Println("the num of cluster nodes is 0")
		return ctx, nil
	}
	c.resetClusterData()

	// 创建新节点时更新以下信息

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
	// 主从关系有变动时更新以下信息

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

// MeetNodes 添加新节点到集群
func (c *ClusterManager) MeetNodes(client *redis.Client, ctx context.Context) (context.Context, error) {
	var err error
	containersManager := c.containersManager
	nodes := containersManager.Nodes
	for _, node := range nodes {
		_, exist := c.AlreadyMeetNode[node.HostIP]
		if !exist {
			_, err = client.ClusterMeet(ctx, node.ConIp, "6379").Result()
			if err != nil {
				return ctx, fmt.Errorf("could not meet node %v: %v", node.ConIp, err)
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

	for _, node := range containersManager.Nodes {
		cli, err := node.CreateRedisClient(ctx)
		Clients = append(Clients, cli)
		err = c.waitForMeetSync(cli, ctx, len(nodes))
		if err != nil {
			return ctx, fmt.Errorf("failed to wait for cluster sync: %v", err)
		}
	}
	return ctx, nil
}

// SetAllNodeType 设置主从节点
func (c *ClusterManager) SetAllNodeType(ctx context.Context) (context.Context, error) {
	var err error
	containersManager := c.containersManager
	ctx, err = c.UpdateClusterNodes(ctx, containersManager.Nodes[0])
	if err != nil {
		return ctx, fmt.Errorf("failed to get cluster nodes info: %v", err)
	}
	// 计算主节点数量
	n := len(c.ClusterNodeList) / c.Replica
	expectedNodes := len(c.ClusterNodeList)
	var masterToSlave = make(map[string][]string)
	for i := 0; i < n; i++ {
		// 获取主节点的 IP 和 ID
		masterIP := c.ClusterNodeList[i].IP
		masterID := c.IPToClusterID[masterIP]
		masterToSlave[masterID] = make([]string, 0)

		// 从节点的起始和结束位置
		slaveStart := n + (i * (c.Replica - 1))
		slaveEnd := slaveStart + c.Replica - 1

		// 遍历设置从节点
		for j := slaveStart; j < slaveEnd && j < expectedNodes; j++ {
			slaveIP := c.ClusterNodeList[j].IP
			slaveID := c.IPToClusterID[slaveIP]

			// 检查是否已经设置主从节点
			if c.AlreadySetCluster[masterID] || c.AlreadySetCluster[slaveID] {
				continue
			}
			masterToSlave[masterID] = append(masterToSlave[masterID], slaveID)
			// 调用 SetNodeAsSlave 函数设置从节点
			ctx, err = c.SetNodeAsSlave(ctx, masterIP, slaveIP)
			if err != nil {
				return ctx, err
			}
			// 记录已经设置的主从节点
			c.AlreadySetCluster[masterID] = true
			c.AlreadySetCluster[slaveID] = true
		}

	}
	_, err = c.verifyNodeTypeSet(ctx, masterToSlave)
	if err != nil {
		return ctx, fmt.Errorf("同步失败")
	}
	return ctx, err
}

// SetNodeAsSlave 设置节点为从节点
func (c *ClusterManager) SetNodeAsSlave(ctx context.Context, masterIP string, slaveIP string) (context.Context, error) {
	var err error
	// 获取从节点的 IP 地址 (不带端口)
	containersManager := c.containersManager
	ctx, err = containersManager.UpdateNewContainersInfo(ctx)
	if err != nil {
		return ctx, fmt.Errorf("failed to get container info: %v", err)
	}
	slaveAddr := slaveIP
	// 创建 Redis 客户端
	ctx, err = c.UpdateClusterNodes(ctx, containersManager.Nodes[0])

	if err != nil {
		return ctx, fmt.Errorf("failed to get cluster nodes info: %v", err)
	}
	cli := CreateRedisClient(ctx, "127.0.0.1", containersManager.IPToNode[slaveAddr].HostPort)
	println("-----------------------------")
	// 打印从节点信息
	slaveAddrPort := fmt.Sprintf("%v:%d", slaveIP, containersManager.IPToNode[slaveAddr].HostPort)
	println("slave:", slaveAddrPort)
	println("id:", c.IPToClusterID[slaveIP])

	// 获取主节点的 ID
	masterID := c.IPToClusterID[masterIP]
	// 执行 ClusterReplicate 命令
	cmdMessage := cli.ClusterReplicate(ctx, masterID)
	if err := cmdMessage.Err(); err != nil {
		fmt.Printf("Error executing ClusterReplicate for slave %s %v\n", slaveAddrPort, err)
		return ctx, fmt.Errorf("failed to set node %s as replica of master %s: %v", slaveAddrPort, masterIP, err)
	}
	// 成功设置为从节点
	fmt.Printf("Node %s set as replica of master %s\n", slaveAddrPort, masterIP)
	return ctx, nil
}

func (c *ClusterManager) AddClusterNode(ctx context.Context) (context.Context, *ContainerNode, error) {
	// 获取初始的容器信息
	var err error
	containersManager := c.containersManager
	//
	ctx, err = containersManager.UpdateNewContainersInfo(ctx)
	if err != nil {
		return ctx, &ContainerNode{}, err
	}

	if err := containersManager.CreateContainers(ctx, 1); err != nil {
		return ctx, &ContainerNode{}, err
	}

	// 获取更新后的容器信息
	ctx, err = containersManager.UpdateNewContainersInfo(ctx)
	if err != nil {
		return ctx, &ContainerNode{}, err
	}

	// 创建 Redis 客户端并使节点互相发现
	cliRedis, err := containersManager.Nodes[0].CreateRedisClient(ctx)
	if err != nil {
		return ctx, &ContainerNode{}, fmt.Errorf("create redis client fail")
	}
	defer func() {
		err = cliRedis.Close()
		if err != nil {
			log.Printf("Redsi %v", err)
		}
	}()
	ctx, err = c.MeetNodes(cliRedis, ctx)
	if err != nil {
		return ctx, &ContainerNode{}, err
	}

	// 获取更新后的集群节点信息
	ctx, err = c.containersManager.UpdateNewContainersInfo(ctx)
	if err != nil {
		return ctx, &ContainerNode{}, err
	}

	newContainerNode := containersManager.Nodes[len(containersManager.Nodes)-1]
	time.Sleep(1 * time.Second) // 给新节点一些时间来初始化

	return ctx, newContainerNode, nil
}
func (c *ClusterManager) addShaderAndReplica(ctx context.Context) (context.Context, string, error) {
	var err error
	containersManager := c.containersManager
	ctx, err = containersManager.UpdateNewContainersInfo(ctx)
	if err != nil {
		return ctx, "", fmt.Errorf(err.Error())
	}
	ctx, err = c.UpdateClusterNodes(ctx, containersManager.Nodes[0])
	if err != nil {
		return ctx, "", fmt.Errorf(err.Error())
	}
	if len(containersManager.Nodes) == 0 {
		return ctx, "", fmt.Errorf("Empty ClusterManager  Please Create cluster first !")
	}

	// 创建节点
	ctx, masterNode, err := c.AddClusterNode(ctx)
	mNode := &masterNode
	if mNode == nil {
		return ctx, "", fmt.Errorf("masterNode is nil, cannot set slaves")
	}
	if err != nil {
		println("error retrieving masterNode: %v", err)
		return ctx, "", fmt.Errorf(err.Error())
	}

	var slaveIPs []string

	// 添加从节点
	for i := 0; i < (c.Replica - 1); i++ {
		ctx, slaveNode, err := c.AddClusterNode(ctx)
		if err != nil {
			log.Printf(err.Error())
		}
		if slaveNode == nil {
			return ctx, "", fmt.Errorf("create slaveNode fail")
		}
		slaveIPs = append(slaveIPs, slaveNode.ConIp)
	}
	// 设置主从关系
	for _, slaveIP := range slaveIPs {
		ctx, err = c.SetNodeAsSlave(ctx, masterNode.ConIp, slaveIP)
		if err != nil {
			return ctx, "", fmt.Errorf(err.Error())
		}
	}
	return ctx, masterNode.ID, err
}

func (c *ClusterManager) AllocateSlots(ctx context.Context) (context.Context, error) {
	var err error
	containersManager := c.containersManager
	time.Sleep(1 * time.Second)
	ctx, err = c.UpdateClusterNodes(ctx, containersManager.Nodes[0])
	if err != nil {
		panic(err)
	}
	println("start allocate...")
	numMasters := len(c.MasterIDs)
	if numMasters == 0 {
		println("no current master node to be allocate slots。")
		return ctx, fmt.Errorf("no available master nodes for slot allocation")
	}
	slotsPerMaster := totalSlots / numMasters
	for i := 0; i < numMasters; i++ {
		startPoint := i * slotsPerMaster
		endPoint := startPoint + slotsPerMaster - 1
		// 确保最后一个主节点处理剩余槽位
		if i == numMasters-1 {
			endPoint = totalSlots - 1
		}

		masterId := c.MasterIDs[i]
		port := containersManager.IPToNode[c.IDToClusterNode[masterId].IP].HostPort
		cliClusterMaster := CreateRedisClient(ctx, "127.0.0.1", port)
		var slots = make([]int, 0)
		for j := startPoint; j <= endPoint; j++ {
			//cliClusterMaster.ClusterAddSlots(ctx, j)
			slots = append(slots, j)
		}
		cliClusterMaster.ClusterAddSlots(ctx, slots...)
	}
	time.Sleep(3 * time.Second)
	err = c.VerifyAllocateSlots(ctx, containersManager)
	if err != nil {
		return ctx, fmt.Errorf("failed to verify slot allocation: %v", err)
	}
	return ctx, nil
}

func (c *ClusterManager) GetContainersInfo(ctx context.Context) (context.Context, error) {
	ctx, err := c.containersManager.UpdateNewContainersInfo(ctx)
	if err != nil {
		return ctx, err
	}
	return ctx, nil
}

func (c *ClusterManager) PrintClusterNodesInfo(ctx context.Context) error {
	var err error
	containersManager := c.containersManager
	ctx, err = containersManager.UpdateNewContainersInfo(ctx)
	if err != nil {
		return err
	}
	ctx, err = c.UpdateClusterNodes(ctx, containersManager.Nodes[0])
	if err != nil {
		return err
	}

	client := CreateRedisClient(ctx, containersManager.Nodes[0].HostIP, containersManager.Nodes[0].HostPort)

	nodesInfo, err := client.ClusterNodes(ctx).Result()
	if err != nil {
		println("failed to get cluster nodes info: %v", err)
	}
	println("\ncluster nodes lines:")
	// 解析返回结果，提取所有的 node ID 和对应的 IP+Port
	lines := strings.Split(nodesInfo, "\n")
	for _, line := range lines {
		println(line)
	}
	return nil
}

// MigrateSlot 迁移 slot
func (c *ClusterManager) MigrateSlot(ctx context.Context, slot int, sourceNodeID, destNodeID string) error {
	var err error
	containersManager := c.containersManager
	sourceNode := c.IDToClusterNode[sourceNodeID]
	destNode := c.IDToClusterNode[destNodeID]
	desCli := CreateRedisClient(ctx, containersManager.IPToNode[destNode.IP].HostIP, containersManager.IPToNode[destNode.IP].HostPort)
	if _, exist := c.MasterToSlave[sourceNode.ID]; !exist {
		println(sourceNodeID)
	}
	sourceCli := CreateRedisClient(ctx, containersManager.IPToNode[sourceNode.IP].HostIP, containersManager.IPToNode[sourceNode.IP].HostPort)

	// Step 1: 设置 slot 状态为迁移中 (MIGRATING)
	_, err = utils.ExecuteClusterCommand(ctx, sourceCli, "CLUSTER", "SETSLOT", strconv.Itoa(slot), "MIGRATING", destNodeID)
	if err != nil {
		return fmt.Errorf("failed to set slot as MIGRATING: %v", err)
	}
	//fmt.Printf("Slot %d set to MIGRATING state\n", slot)

	// Step 2: 在目标节点上接收 slot (IMPORTING)
	_, err = utils.ExecuteClusterCommand(ctx, desCli, "cluster", "SETSLOT", strconv.Itoa(slot), "IMPORTING", destNodeID)
	if err != nil {
		return fmt.Errorf("failed to set slot as IMPORTING: %v", err)
	}
	//fmt.Printf("Slot %d set to IMPORTING state on destination node\n", slot)

	// Step 3: 迁移 slot 中的数据

	cmdstring := sourceCli.ClusterGetKeysInSlot(ctx, slot, 1000)
	keys := cmdstring.Val()
	port := fmt.Sprintf("%v", destNode.Port)
	for _, key := range keys {
		_, err := desCli.Migrate(ctx, destNode.IP, port, key, 0, 5000).Result()
		if err != nil {
			log.Printf("Failed to migrate key %s: %v", key, err)
			continue
		}
		//fmt.Printf("Migrated key: %s from slot: %d\n", key, slot)
	}

	// Step 4: 在目标节点上设置 slot 归属
	_, err = utils.ExecuteClusterCommand(ctx, desCli, "CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", destNodeID)
	if err != nil {
		return fmt.Errorf("failed to set slot %d to NODE %s on destination: %v", slot, destNodeID, err)
	}
	//fmt.Printf("Slot %d assigned to node %s\n", slot, destNodeID)
	// step 5 :在原节点 设置slot
	_, err = utils.ExecuteClusterCommand(ctx, sourceCli, "CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", destNodeID)
	if err != nil {
		return fmt.Errorf("failed to set slot %d to NODE %s on destination: %v", slot, destNodeID, err)
	}

	return nil
}

func (c *ClusterManager) MigratesSlotsToEmptyNode(ctx context.Context) error {
	var err error
	if err != nil {
		log.Fatalf("Failed to update containers: %v", err)
		return err
	}

	if len(c.EmptyMasters) == 0 {
		return fmt.Errorf("no Empty master")
	}

	newVolum := totalSlots / len(c.MasterIDs)
	// empty master index
	index := 0
	if err != nil {
		return fmt.Errorf("failed to get cluster nodes info: %v", err)
	}
	for _, masterID := range c.MasterIDs {
		masterNode := c.IDToClusterNode[masterID]
		fromId := masterID
		if masterNode.SlotsNum == 0 {
			continue
		}
		if masterNode.SlotsNum > newVolum {
			for _, slot := range masterNode.Slots {
				start := slot.Start
				end := slot.End
				// 将 slots 迁移到空的节点
				for i := end; i >= start && index < len(c.EmptyMasters); i-- {

					toId := c.EmptyMasters[index].ID
					err = c.MigrateSlot(ctx, i, fromId, toId)
					if fromId == toId {
						break
					}
					//log.Println("迁移节点:", fromId, "slot:", i, "to:", toId)
					if err != nil {
						log.Printf("Failed to migrate slot %d: %v", i, err)
						return err
					}
					c.EmptyMasters[index].SlotsNum++
					masterNode.SlotsNum--
					if c.EmptyMasters[index].SlotsNum == newVolum {
						index++
						toId = c.MasterIDs[index]
					}
					if masterNode.SlotsNum == newVolum {
						break
					}
				}
				if masterNode.SlotsNum == newVolum {
					break
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

// 内部函数
// sortClusterNodesByIP sorts the ClusterNodeList by IP address.
func (c *ClusterManager) sortClusterNodesByIP(nodes []ClusterNode) {
	sort.Sort(ByIP(nodes))
}

func (c *ClusterManager) getClusterNodes(ctx context.Context, containersManager *ContainersManager, LoginNode *ContainerNode) ([]ClusterNode, error) {
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
}

// 当节点为slave 节点进行操作
func (c *ClusterManager) processSlaveNode(node ClusterNode) error {
	if _, exists := c.MasterSet[node.MasterID]; !exists {
		return fmt.Errorf("master node %s not found for slave %s", node.MasterID, node.ID)
	}
	c.MasterToSlave[node.MasterID] = append(c.MasterToSlave[node.MasterID], node.ID)
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

func (c *ClusterManager) equals(c2 *ClusterManager) bool {
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

func (c *ClusterManager) verifyNodeTypeSet(ctx context.Context, masterToSlave map[string][]string) (bool, error) {
	containersManger := c.containersManager
	for _, node := range containersManger.Nodes {
		tryTimes := 10
		i := 0
		for i < tryTimes {
			_, err := c.UpdateClusterNodes(ctx, node)
			if err != nil {
				println("try sync fail", i)
				i++
				time.Sleep(2 * time.Second)
				continue
			}
			i++
			ok := c.equalClusterNodeType(masterToSlave, c.MasterToSlave)
			if ok == true {
				break
			}
			fmt.Printf("try %v /10 sync fail\n", i)
			time.Sleep(2 * time.Second)
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

// waitForMeetSync 等待集群同步
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
			if strings.Contains(line, "connected") {
				activeNodes++
			}
		}

		// 检查活跃节点是否与预期节点数匹配
		if activeNodes == expectedNodes {
			return nil
		}
		time.Sleep(2 * time.Second)
		fmt.Printf("ClusterManager not fully synchronized, retrying... (%d/%d)\n", i+1, maxRetries)
	}

	return fmt.Errorf("cluster did not synchronize within the expected time")
}

func (c *ClusterManager) VerifyAllocateSlots(ctx context.Context, containers *ContainersManager) error {
	var err error
	for _, container := range containers.Nodes {
		cluster := c
		tryTimes := 10
		for j := 0; j < tryTimes; j++ {
			_, err := cluster.UpdateClusterNodes(ctx, container)
			if err != nil {
				return err
			}
			Flag := false
			for i := 0; i < len(cluster.MasterIDs); i++ {
				if i == len(cluster.MasterIDs)-1 && totalSlots%len(cluster.MasterIDs) != 0 {
					if cluster.IDToClusterNode[cluster.MasterIDs[i]].SlotsNum == totalSlots%len(cluster.MasterIDs) {
						Flag = true
					}
				} else {
					if cluster.IDToClusterNode[cluster.MasterIDs[i]].SlotsNum == totalSlots/len(cluster.MasterIDs) {
						Flag = true
					}
				}
				if Flag == false {
					break
				}
			}
		}

	}
	return err
}
