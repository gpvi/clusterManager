package model

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"redisStudy/utils"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ByIP 用于根据IP 排序 ClusterNode
type ByIP []*ClusterNode

func (a ByIP) Len() int { return len(a) }

func (a ByIP) Less(i, j int) bool { return a[i].IP < a[j].IP }

func (a ByIP) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

type ClusterManager struct {
	EmptyMasters      []*ClusterNode          // 当前没有分配任何slot的主节点列表
	IDToClusterNode   map[string]*ClusterNode // 将节点 ID 映射到对应的 ClusterNode 结构体
	IPToClusterID     map[string]string       // 将 IP 地址映射到对应的集群节点 ID
	AlreadyMeetNode   map[string]bool         // 记录已经通过 "meet" 命令连接过的节点
	MasterToSlave     map[string][]string     // 将主节点 ID 映射到其从节点 ID 列表
	AlreadySetCluster map[string]bool         // 记录已经被设置为集群一部分的节点
	ClusterNodeList   []*ClusterNode          // 集群中所有节点（包括主节点和从节点）的列表
	MasterIDs         []string                // 集群中所有主节点的 ID 列表
	MasterSet         map[string]bool         // 主节点 ID 集合，用于快速查找
	containersManager *ContainersManager      // 指向负责管理容器操作的 ContainersManager 指针
	Replica           int                     // 每个主节点要分配的副本（从节点）数量
}

func NewClusterManager(replica int) *ClusterManager {
	containersManager := NewContainersManager()
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
		containersManager: containersManager,
		Replica:           replica,
	}

	return &clusterManager
}

func (c *ClusterManager) CreateClusterNodes(shared int, ctx context.Context, clusterName string) error {
	var err error
	sum := shared * c.Replica
	err = c.containersManager.CreateContainers(ctx, sum, clusterName)
	if err != nil {
		return fmt.Errorf("create container fail %v", err)
	}

	fmt.Printf("finish created, %d sharder, %d 个replica ", shared, c.Replica)
	fmt.Println("containers info list：")

	// 打印当前容器信息
	for _, node := range c.containersManager.Nodes {
		fmt.Println("clusterName:", node.ClusterName, "containerName: ", node.Name, "containerID: ", node.ID, "HostIP: ", node.HostIP, "HostPort:", node.HostPort, "ContainerIP:", node.ConIp, "containerPort: ", node.ConPort)
	}

	meetNodeIndex := 0
	// 创建redis client
	if len(c.containersManager.Nodes) > 0 {
		for ; meetNodeIndex < len(c.containersManager.Nodes); meetNodeIndex++ {
			if c.containersManager.Nodes[meetNodeIndex].ClusterName == clusterName {
				break
			}
		}
		cliRedis, err = c.containersManager.Nodes[meetNodeIndex].CreateRedisClient()
		if err != nil {
			return fmt.Errorf("create redis client fail")
		}
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
	err = c.MeetNodes(cliRedis, ctx, clusterName)
	if err != nil {
		return fmt.Errorf("meet nodes fail %v", err)
	}
	err = c.UpdateAfterMeet(ctx, c.containersManager.Nodes[0], clusterName)
	if err != nil {
		return err
	}
	return err
}

func (c *ClusterManager) GetContainerNum() int {
	return c.containersManager.Num
}

func (c *ClusterManager) AddShaders(ctx context.Context, shaderNum int, clusterName string) error {
	var err error
	//addAfterNum := shaderNum + c.GetContainerNum()
	sum := shaderNum * c.Replica
	// 增加容器完成meet
	err = c.CreateClusterNodes(shaderNum, ctx, clusterName)
	if err != nil {
		return err
	}
	// 分配主从
	// 新节点开始的index
	c.EmptyMasters = make([]*ClusterNode, 0)
	newNodeStartIndex := c.containersManager.Num - sum
	var masterToSlave = make(map[string][]string)
	masterToSlave = c.MasterToSlave
	IDToIP := make(map[string]string)
	count := 0
	masterID := ""
	for i := newNodeStartIndex; i < newNodeStartIndex+sum; i++ {
		ip := c.containersManager.Nodes[i].ConIp
		ID := c.IPToClusterID[ip]
		IDToIP[ID] = ip
		if count == c.Replica {
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

	// 设置主从
	for k, v := range masterToSlave {
		masterID = k
		masterIP := IDToIP[masterID]

		if _, exist := IDToIP[masterID]; !exist {
			continue
		}
		if c.containersManager.IPToNode[masterIP].ClusterName != clusterName {
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

func (c *ClusterManager) UpdateAfterMeet(ctx context.Context, LoginNode *ContainerNode, clusterName string) error {
	var err error
	containersManager := c.containersManager
	if containersManager.Num == 0 {
		//fmt.Println("nodes num is 0")
		return nil
	}

	nodes, err := c.GetClusterNodes(ctx, LoginNode, clusterName)
	if err != nil {
		return err
	}
	// 更新
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
	// 排序确保唯一性
	c.sortClusterNodesByIP(c.ClusterNodeList)
	c.sortClusterNodesByIP(c.EmptyMasters)

}

func (c *ClusterManager) UpdateAfterSetNodeRole(ctx context.Context, LoginNode *ContainerNode, clusterName string) error {
	var err error
	c.MasterToSlave = make(map[string][]string)
	c.MasterIDs = make([]string, 0)
	c.MasterSet = make(map[string]bool)
	containersManager := c.containersManager
	if containersManager.Num == 0 {
		//fmt.Println("nodes num is 0")
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

func (c *ClusterManager) UpdateSlots(ctx context.Context, LoginNode *ContainerNode, clusterName string) error {
	var err error
	containersManager := c.containersManager
	if containersManager.Num == 0 {
		//fmt.Println("nodes num is 0")
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

// MeetNodes 添加新节点到集群
func (c *ClusterManager) MeetNodes(client *redis.Client, ctx context.Context, clusterName string) error {
	var err error
	containersManager := c.containersManager
	nodes := containersManager.Nodes
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

	for _, node := range containersManager.Nodes {
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

// SetAllNodeRole 设置主从节点
func (c *ClusterManager) SetAllNodeRole(ctx context.Context, clusterName string) error {
	var err error
	// 计算主节点数量
	n := len(c.ClusterNodeList) / c.Replica
	expectedNodes := len(c.ClusterNodeList)
	var masterToSlave = make(map[string][]string)
	for i := 0; i < n; i++ {
		// 获取主节点的 IP 和 ID
		if c.ClusterNodeList[i].ClusterName != clusterName {
			continue
		}
		masterIP := c.ClusterNodeList[i].IP
		masterID := c.IPToClusterID[masterIP]
		masterToSlave[masterID] = make([]string, 0)
		c.MasterIDs = append(c.MasterIDs, masterID)

		// 从节点的起始和结束位置
		slaveStart := n + (i * (c.Replica - 1))
		slaveEnd := slaveStart + c.Replica - 1

		// 遍历设置从节点
		for j := slaveStart; j < slaveEnd && j < expectedNodes; j++ {
			if c.ClusterNodeList[j].ClusterName != clusterName {
				continue
			}
			slaveIP := c.ClusterNodeList[j].IP
			slaveID := c.IPToClusterID[slaveIP]

			// 检查是否已经设置主从节点
			if c.AlreadySetCluster[masterID] && c.AlreadySetCluster[slaveID] {
				continue
			}
			masterToSlave[masterID] = append(masterToSlave[masterID], slaveID)
			// 调用 SetNodeAsSlave 函数设置从节点
			err = c.SetNodeAsSlave(ctx, masterIP, slaveIP, clusterName)
			if err != nil {
				return err
			}
			// 记录已经设置的主从节点
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

// SetNodeAsSlave 设置节点为从节点
func (c *ClusterManager) SetNodeAsSlave(ctx context.Context, masterIP string, slaveIP string, clusterName string) error {
	var err error
	// 获取从节点的 IP 地址 (不带端口)
	containersManager := c.containersManager
	slaveAddr := slaveIP
	// 创建 Redis 客户端
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
		return fmt.Errorf("failed to set node %s as replica of master %s: %v", slaveAddrPort, masterIP, err)
	}
	// 成功设置为从节点
	fmt.Printf("Node %s set as replica of master %s\n", slaveAddrPort, masterIP)
	// 更新主从映射
	err = c.UpdateAfterSetNodeRole(ctx, containersManager.Nodes[0], clusterName)
	if err != nil {
		return fmt.Errorf("failed to get cluster nodes info: %v", err)
	}
	return nil
}

func (c *ClusterManager) AddClusterNode(ctx context.Context, clusterName string) (*ContainerNode, error) {
	// 获取初始的容器信息
	var err error
	containersManager := c.containersManager
	if err := containersManager.CreateContainers(ctx, 1, clusterName); err != nil {
		return &ContainerNode{}, err
	}

	// 创建 Redis 客户端并使节点互相发现
	cliRedis, err := containersManager.Nodes[0].CreateRedisClient()
	if err != nil {
		return &ContainerNode{}, fmt.Errorf("create redis client fail")
	}
	defer func() {
		err = cliRedis.Close()
		if err != nil {
			log.Printf("Redsi %v", err)
		}
	}()
	err = c.MeetNodes(cliRedis, ctx, clusterName)
	if err != nil {
		return &ContainerNode{}, err
	}

	newContainerNode := containersManager.Nodes[len(containersManager.Nodes)-1]
	return newContainerNode, nil
}

func (c *ClusterManager) AllocateSlots(ctx context.Context, clusterName string) error {
	var err error
	containersManager := c.containersManager
	err = c.UpdateSlots(ctx, containersManager.Nodes[0], clusterName)
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
		// 确保最后一个主节点处理剩余槽位
		if i == numMasters-1 {
			endPoint = TotalSlots - 1
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
	err = c.VerifyAllocateSlots(ctx, containersManager, clusterName)
	if err != nil {
		return fmt.Errorf("failed to verify slot allocation: %v", err)
	}
	return nil
}

func (c *ClusterManager) PrintClusterNodesInfo(ctx context.Context) error {
	time.Sleep(time.Duration(len(c.ClusterNodeList)/3) * time.Second)
	var err error
	containersManager := c.containersManager
	client := CreateRedisClient(ctx, containersManager.Nodes[0].HostIP, containersManager.Nodes[0].HostPort)
	defer func() {
		err = client.Close()
		fmt.Println(err)
	}()
	nodesInfo, err := client.ClusterNodes(ctx).Result()
	if err != nil {
		println("failed to get cluster nodes info: %v", err)
	}
	println("\ncluster nodes lines:")
	// 解析返回结果，提取所有的 node ID 和对应的 IP+Port
	lines := strings.Split(nodesInfo, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		fields := strings.Split(line, " ")
		if len(fields) < 8 {
			continue // 跳过字段数不够的行
		}
		// 排除状态中包含 fail 的节点
		if strings.Contains(fields[2], "fail") {
			continue // 如果节点包含 fail 状态，则跳过
		}
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
		return fmt.Errorf("source node %s is not a master node", sourceNodeID)
	}
	sourceCli := CreateRedisClient(ctx, containersManager.IPToNode[sourceNode.IP].HostIP, containersManager.IPToNode[sourceNode.IP].HostPort)
	defer func() {
		err = desCli.Close()
		if err != nil {
			fmt.Printf("Redsi %v", err)
		}
	}()

	// Step 1: 在目标节点上接收 slot (IMPORTING)
	_, err = utils.ExecuteClusterCommand(ctx, desCli, "cluster", "SETSLOT", strconv.Itoa(slot), "IMPORTING", sourceNodeID)
	if err != nil {
		return fmt.Errorf("failed to set slot as IMPORTING: %v", err)
	}
	//fmt.Printf("Slot %d set to IMPORTING state on destination node\n", slot)

	// Step 2: 设置 slot 状态为迁移中 (MIGRATING)
	_, err = utils.ExecuteClusterCommand(ctx, sourceCli, "CLUSTER", "SETSLOT", strconv.Itoa(slot), "MIGRATING", destNodeID)
	if err != nil {
		return fmt.Errorf("failed to set slot as MIGRATING: %v", err)
	}
	//fmt.Printf("Slot %d set to MIGRATING state\n", slot)

	// Step 3: 迁移 slot 中的数据

	cmdstring := sourceCli.ClusterGetKeysInSlot(ctx, slot, 1000)
	keys := cmdstring.Val()
	port := fmt.Sprintf("%v", destNode.Port)
	if len(keys) > 0 {
		// 确定每个批次的大小（总键数的 1/10）
		chunkSize := len(keys) / 10
		if chunkSize == 0 {
			chunkSize = 1 // 确保至少迁移一个键
		}

		// 按批次迁移
		for i := 0; i < len(keys); i += chunkSize {
			end := i + chunkSize
			if end > len(keys) {
				end = len(keys) // 调整结束索引以防超出切片长度
			}

			// 创建当前批次的切片
			currentChunk := keys[i:end]

			// 使用 KEYS 参数构建 MIGRATE 命令
			migrateArgs := []interface{}{destNode.IP, port, "", 0, 5000 * time.Millisecond, "KEYS"}

			// 检查键的存在性并添加到参数中
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

			// 确保有键可迁移
			if len(migrateArgs) > 6 { // 6 是 migrateArgs 的基础长度
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

	//log.Println("Keys migrated successfully.")

	// Step 4: 在目标节点上设置 slot 归属
	_, err = utils.ExecuteClusterCommand(ctx, desCli, "CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", sourceNodeID)
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

func (c *ClusterManager) MigratesSlotsToEmptyNode(ctx context.Context, clusterName string) error {
	var err error

	if len(c.EmptyMasters) == 0 {
		return fmt.Errorf("no Empty master")
	}

	newV := TotalSlots / len(c.MasterIDs)
	// empty master index
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
				// 将 slots 迁移到空的节点
				var toId string
				for i := end; i >= start && index < len(c.EmptyMasters); i-- {
					toId = c.EmptyMasters[index].ID
					if fromId == toId {
						break
					}
					err = c.MigrateSlot(ctx, i, fromId, toId)
					//log.Println("迁移节点:", fromId, "slot:", i, "to:", toId)
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

// sortClusterNodesByIP sorts the ClusterNodeList by IP address.
func (c *ClusterManager) sortClusterNodesByIP(nodes []*ClusterNode) {
	sort.Sort(ByIP(nodes))
}

func (c *ClusterManager) GetClusterNodes(ctx context.Context, LoginNode *ContainerNode, clusterName string) ([]ClusterNode, error) {
	var err error
	containersManager := c.containersManager
	if len(containersManager.Nodes) == 0 {
		return nil, fmt.Errorf("no containers found")
	}
	client := CreateRedisClient(ctx, LoginNode.HostIP, LoginNode.HostPort)
	defer func() {
		err = client.Close()
		if err != nil {
			fmt.Printf("Error closing Redis client: %v", err)
		}
	}()

	// 执行 CLUSTER NODES 命令获取集群中的所有节点信息
	nodesInfo, err := client.ClusterNodes(ctx).Result()
	nodes, err := c.ParseRedisClusterNodes(ctx, nodesInfo, clusterName)

	return nodes, nil
}

// 计算当前节点的
func (c *ClusterManager) calculateSlots(slots []SlotRange) int {
	totalSlots := 0
	for _, slot := range slots {
		totalSlots += slot.End - slot.Start + 1
	}
	return totalSlots
}

func (c *ClusterManager) verifyNodeTypeSet(ctx context.Context, masterToSlave map[string][]string, clusterName string) (bool, error) {
	containersManger := c.containersManager
	for _, node := range containersManger.Nodes {
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
			fields := strings.Split(line, " ")
			if len(fields) < 8 {
				continue // 跳过字段数不够的行
			}
			// 排除状态中包含 fail 的节点
			if strings.Contains(fields[2], "fail") {
				client.ClusterForget(ctx, fields[0])
				continue // 如果节点包含 fail 状态，则跳过
			}
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

func (c *ClusterManager) VerifyAllocateSlots(ctx context.Context, containers *ContainersManager, clusterName string) error {
	var err error
	for _, container := range containers.Nodes {
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
	lines := strings.Split(data, "\n") // 将数据按行分割
	var nodes []ClusterNode            // 存储解析后的节点信息
	clusterIndex := 0
	for ; clusterIndex < len(c.containersManager.Nodes); clusterIndex++ {
		if c.containersManager.Nodes[clusterIndex].ClusterName == clusterName {
			break
		}
	}
	client := CreateRedisClient(ctx, c.containersManager.Nodes[clusterIndex].HostIP, c.containersManager.Nodes[clusterIndex].HostPort)
	for _, line := range lines {
		if len(line) == 0 {
			continue // 跳过空行
		}

		fields := strings.Split(line, " ")
		if len(fields) < 8 {
			continue // 跳过字段数不够的行
		}
		// 排除状态中包含 fail 的节点
		if strings.Contains(fields[2], "fail") {
			client.ClusterForget(ctx, fields[0])
			continue // 如果节点包含 fail 状态，则跳过
		}

		ipPort := strings.Split(fields[1], "@")[0]
		ip, port := utils.ParseIPPort(ipPort) // 解析 IP 和端口
		portUint16, err := utils.StringToUint16(port)
		if err != nil {
			log.Printf("Error parsing port: %v", err)
			continue
		}
		// 不是当前集群所属节点
		if c.containersManager.IPToNode[ip].ClusterName != clusterName {
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
		// 如果是master节点，并且有插槽范围，解析插槽
		if node.NodeType == Master && len(fields) > 8 {
			slots, err := ParseSlots(fields[8:])
			if err != nil {
				return nil, err
			}
			node.Slots = slots
		}
		nodes = append(nodes, node) // 将有效节点添加到结果列表中
	}
	return nodes, nil
}

// parseNodeType 解析节点类型（master 或 slave）
func parseNodeType(field string) string {
	if strings.Contains(field, "master") {
		return Master
	}
	return Slave
}

// parseAdditionalFlags 解析其他附加标志 (如 myself)
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
