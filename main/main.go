package main

import (
	"context"
	"fmt"
	"github.com/containers/common/libnetwork/types"
	"github.com/containers/podman/v5/pkg/bindings"
	"github.com/containers/podman/v5/pkg/bindings/containers"
	types2 "github.com/containers/podman/v5/pkg/domain/entities/types"
	"github.com/containers/podman/v5/pkg/specgen"
	"github.com/go-redis/redis/v8"
	"github.com/opencontainers/runtime-spec/specs-go"
	"log"
	"math"
	"os"
	"path/filepath"
	"redisStudy/Data"
	"strings"
	"time"
)

type ContainerInfo = Data.ContainerInfo

type ClusterNodeInfo = Data.ClusterNodeInfo

/*
全局变量说明：
1. AllContainerInfoList：所有容器的信息列表，用于存储获取到的容器信息。
2. IPToContainerInfoMapping：IP到容器信息的映射，用于快速查找指定IP对应的容器信息。
3. ContainIdToClusterInfoMapping：容器ID到集群信息的映射，用于快速查找指定容器对应的集群信息。
4. ContainerIdToContainerINfoMapping：容器ID到容器信息的映射，用于快速查找指定容器对应的信息。
5. AlreadyMeetNode：记录已经存在的节点，用于避免重复添加。
6. ClusterIDList：记录已经存在的集群ID，用于避免重复创建。
7. MasterToSlaveMapping：主节点到从节点的映射，用于快速查找指定主节点对应的从节点。
8. AlreadySetCluster：记录已经设置的集群，用于避免重复设置。
9. PodmanClient：用于与Podman进行通信的Client对象。

*/

const totalSlots = 16384

var True = true

// 指定本地的配置文件路径
var redisHostConfigPath = "/Users/zhuoqun.niu/Desktop/redis/config"

// 容器路径
var redisConfigPath = "/data/redis/config"

// 宿主机路径
var redisHostDataPath = "/Users/zhuoqun.niu/Desktop/redis/data"

// 容器路径
var redisConfigDataPath = "/data/redis/data"

// Container 相关数据
var IPToContainerInfoMapping = make(map[string]ContainerInfo)

var AllContainerInfoList = make([]ContainerInfo, 0)

var ContainerIdToContainerINfoMapping = make(map[string]ContainerInfo)

var ContainerNum = 0

var ClusterIdClusterInfoMapping = make(map[string]ClusterNodeInfo)

// cluster 相关
var IPToClusterIDMapping = make(map[string]string)

var AlreadyMeetNode = make(map[string]bool)

var ClusterIDList = make([]string, 0)

var MasterToSlaveMapping = make(map[string][]string)

var AlreadySetCluster = make(map[string]bool)

var ClusterNodeList = make([]Data.ClusterNodeInfo, 0)

var masterIDs = make([]string, 0)

var masterSet = make(map[string]bool)

func PrintClusterNodesInfo(ctx context.Context) error {
	client, ctxRedis := CreateRedisClient(AllContainerInfoList[0].IP, AllContainerInfoList[0].Port)
	ctx = ctxRedis
	nodesInfo, err := client.ClusterNodes(ctx).Result()
	if err != nil {
		println("failed to get cluster nodes info: %v", err)
	}
	println("cluster nodes:")
	println("---------------------------------------")
	// 解析返回结果，提取所有的 node ID 和对应的 IP+Port
	lines := strings.Split(nodesInfo, "\n")
	for _, line := range lines {
		println(line)
	}
	println("---------------------------------------")
	return nil
}

func GetContainerInfo(ctx context.Context) error {
	containerList, err := containers.List(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}
	ContainerNum = len(containerList)
	println("containers 信息:")
	println("---------------------------------------")

	for _, container := range containerList {
		if _, exist := ContainerIdToContainerINfoMapping[container.ID]; exist {
			continue
		}
		inspect, err := containers.Inspect(ctx, container.ID, nil)
		if err != nil {
			fmt.Printf("failed to inspect container %s: %v\n", container.ID, err)
			continue
		}

		for _, network := range inspect.NetworkSettings.Networks {
			containerNode := ContainerInfo{
				Name:    container.Names[0],
				IP:      "127.0.0.1", // 这个可能是占位符，如果需要可以更新
				ConIp:   network.IPAddress,
				Port:    container.Ports[0].HostPort,
				ConPort: container.Ports[0].ContainerPort,
				Id:      container.ID,
			}
			println(containerNode.Id, containerNode.Name, containerNode.IP, containerNode.Port, containerNode.ConIp)
			AllContainerInfoList = append(AllContainerInfoList, containerNode)
			IPToContainerInfoMapping[network.IPAddress] = containerNode
			ContainerIdToContainerINfoMapping[container.ID] = containerNode
		}

	}
	println("---------------------------------------")

	return nil
}

func GetClusterNodesInfo(ctx context.Context) error {
	client, ctx := CreateRedisClient(AllContainerInfoList[0].IP, AllContainerInfoList[0].Port)
	// 执行 CLUSTER NODES 命令获取集群中的所有节点信息
	nodesInfo, err := client.ClusterNodes(ctx).Result()
	nodes, err := Data.ParseRedisClusterNodes(nodesInfo)
	if err != nil {
		println("failed to get cluster nodes info: %v", err)
	}
	// 解析返回结果，提取所有的 node ID 和对应的 IP+Port
	err = PrintClusterNodesInfo(ctx)
	// 清空切片
	masterIDs = []string{}
	ClusterNodeList = []ClusterNodeInfo{}

	// 清空映射
	masterSet = make(map[string]bool)
	MasterToSlaveMapping = make(map[string][]string)
	ClusterIdClusterInfoMapping = make(map[string]ClusterNodeInfo)
	IPToClusterIDMapping = make(map[string]string)

	for _, node := range nodes {
		if node.NodeType == "master" {
			masterIDs = append(masterIDs, node.ID)
			masterSet[node.ID] = true
			MasterToSlaveMapping[node.ID] = make([]string, 0)
		}
		if node.NodeType == "slave" {
			if _, exist := masterSet[node.MasterID]; exist {
				MasterToSlaveMapping[node.MasterID] = append(MasterToSlaveMapping[node.MasterID], node.ID)
			}
		}
		ClusterIdClusterInfoMapping[node.ID] = node
		IPToClusterIDMapping[node.IP] = node.ID

		ClusterNodeList = append(ClusterNodeList, node)
	}
	return nil
}

// GetMasterNodeIDs 获取集群中所有主节点的 ID
func GetMasterNodeIDs(client *redis.Client, ctx context.Context) ([]string, error) {
	nodesInfo, err := client.ClusterNodes(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster nodes info: %v", err)
	}

	lines := strings.Split(nodesInfo, "\n")

	masterIDs := make([]string, 0)

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) > 2 {
			fmt.Printf("Parsed fields: %v\n", fields)
			// 调试输出
			if strings.Contains(fields[2], "master") {
				nodeID := fields[0]
				_, exist := masterSet[nodeID]
				if !exist {
					masterIDs = append(masterIDs, nodeID)
					masterSet[nodeID] = true
				}

			}
		}
	}

	return masterIDs, nil
}

// CreateContainers 创建指定数量的容器
func CreateContainers(ctx context.Context, nodeCount int) {
	for i := 1; i <= nodeCount; i++ {
		CreateContainer(ctx, i)
	}
}

// CreatePodmanConnection 创建连接
func CreatePodmanConnection() context.Context {
	conn, err := bindings.NewConnection(context.Background(), "unix:///Users/zhuoqun.niu/.local/share/containers/podman/machine/podman.sock")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return conn
}

// CreateContainer 创建容器
func CreateContainer(ctx context.Context, nodeId int) error {
	startConfigPath := filepath.Join(redisConfigPath, "redis.conf")

	s := specgen.NewSpecGenerator("myredis", false)

	s.Name = fmt.Sprintf("redis-%d", nodeId)

	s.Mounts = []specs.Mount{
		{
			Source:      redisHostConfigPath,
			Destination: redisConfigPath,
			Type:        "bind",
			Options:     []string{"ro"},
		},
		{
			Source:      redisHostDataPath,
			Destination: redisConfigDataPath,
			Type:        "bind",
			Options:     []string{"ro"},
		},
	}

	s.Labels = map[string]string{
		"cluster": "cluster1",
		"env":     "prod",
	}

	s.PortMappings = []types.PortMapping{
		{
			ContainerPort: 6379,
			HostPort:      0, // Redis server port, 0 indicates a random host port should be chosen
			Protocol:      "tcp",
		},
		{
			ContainerPort: 6379, // cluster-announce-port, same as Redis server port
			HostPort:      0,    // Random host port
			Protocol:      "tcp",
		},
		{
			ContainerPort: 16379, // cluster-announce-bus-port (Redis Cluster bus port)
			HostPort:      0,     // Random host port
			Protocol:      "tcp",
		},
	}

	s.Command = []string{"redis-server", startConfigPath}

	createResponse, err := containers.CreateWithSpec(ctx, s, nil)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("Container created:", createResponse.ID)

	if err := containers.Start(ctx, createResponse.ID, nil); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("Container started.")
	return err
}

// CreateRedisClient 执行 redis-cli --cluster create 命令
func CreateRedisClient(ip string, port uint16) (*redis.Client, context.Context) {
	ctx := context.Background()
	addr := fmt.Sprintf("%s:%d", ip, port)

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	// 测试连接
	_, err := client.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("could not connect to Redis: %v", err)
	}

	return client, ctx
}

// SetNodeAsSlave 设置节点为从节点
func SetNodeAsSlave(masterIP string, slaveIP string, expectedNodes int) error {
	// 获取从节点的 IP 地址 (不带端口)
	slaveAddr := strings.Split(slaveIP, ":")[0]

	// 创建 Redis 客户端
	cli, ctx := CreateRedisClient("127.0.0.1", IPToContainerInfoMapping[slaveAddr].Port)

	// 打印从节点信息
	slaveAddrPort := fmt.Sprintf("localhost:%d", IPToContainerInfoMapping[slaveAddr].Port)
	println("slave:", slaveAddrPort)
	println("id:", IPToClusterIDMapping[slaveIP])

	// 轮询等待集群同步
	err := WaitForClusterSync(cli, ctx, expectedNodes)
	if err != nil {
		println(err)
		return fmt.Errorf("failed to wait for cluster sync: %v", err)
	}

	// 获取主节点的 ID
	masterID := IPToClusterIDMapping[masterIP]
	// 执行 ClusterReplicate 命令
	cmdMessage := cli.ClusterReplicate(ctx, masterID)
	if err := cmdMessage.Err(); err != nil {
		fmt.Printf("Error executing ClusterReplicate for slave %s %v\n", slaveAddrPort, err)
		return fmt.Errorf("failed to set node %s as replica of master %s: %v", slaveAddrPort, masterIP, err)
	}

	// 成功设置为从节点
	fmt.Printf("Node %s set as replica of master %s\n", slaveAddrPort, masterIP)
	return nil
}

// SetAllMasterSlave 设置主从节点
func SetAllMasterSlave() error {
	// 计算主节点数量
	n := len(ClusterNodeList) / 2
	expectedNodes := len(ClusterNodeList)

	for i := 0; i < n; i++ {
		// 获取主节点和从节点的 IP
		masterIP := ClusterNodeList[i].IP
		slaveIP := ClusterNodeList[i+n].IP
		// 记录已经设置的主从节点
		masterID := IPToClusterIDMapping[masterIP]
		slaveID := IPToClusterIDMapping[slaveIP]
		AlreadySetCluster[masterID] = true
		AlreadySetCluster[slaveID] = true

		// 调用 SetNodeAsSlave 函数设置从节点
		err := SetNodeAsSlave(masterIP, slaveIP, expectedNodes)
		if err != nil {
			return err
		}
	}

	return nil
}

// MeetNodes 添加新节点到集群
func MeetNodes(client *redis.Client, ctx context.Context) error {
	nodes := AllContainerInfoList
	for _, node := range nodes {
		_, exist := AlreadyMeetNode[node.IP]
		if !exist {
			_, err := client.ClusterMeet(ctx, node.ConIp, "6379").Result()
			if err != nil {
				return fmt.Errorf("could not meet node %v: %v", node.ConIp, err)
			}
		}

	}
	return nil
}

// DeleteContainer 删除容器
func DeleteContainer(ctx context.Context, container types2.ListContainer) {
	if container.State == "exited" {
		// Stop the container before removing
		err := containers.Stop(ctx, container.ID, nil)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("Container stopped:", container.ID)
	}
	report, err := containers.Remove(ctx, container.ID, &containers.RemoveOptions{
		Force: &True,
	})
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Container removed:", report)
	}
}

// DeleteAllContainers 删除所有容器
func DeleteAllContainers(ctx context.Context) {
	// Stop and remove all containers
	containerList, err := containers.List(ctx, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, container := range containerList {
		DeleteContainer(ctx, container)
	}

}

// WaitForClusterSync 等待集群同步
func WaitForClusterSync(client *redis.Client, ctx context.Context, expectedNodes int) error {
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

		fmt.Printf("Cluster not fully synchronized, retrying... (%d/%d)\n", i+1, maxRetries)
		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("cluster did not synchronize within the expected time")
}

// AllocateSlots 分配槽位
func AllocateSlots(ctx context.Context) {
	err := GetClusterNodesInfo(ctx)
	if err != nil {
		panic(err)
	}
	println("开始将槽位分配给 Redis 主节点...")
	numMasters := len(masterIDs)
	if numMasters == 0 {
		println("没有可用的主节点进行槽位分配。")
		return
	}
	slotsPerMaster := totalSlots / numMasters
	for i := 0; i < numMasters; i++ {
		startPoint := i * slotsPerMaster
		endPoint := startPoint + slotsPerMaster - 1

		// 确保最后一个主节点处理剩余槽位
		if i == numMasters-1 {
			endPoint = totalSlots - 1
		}

		masterId := masterIDs[i]

		port := IPToContainerInfoMapping[ClusterIdClusterInfoMapping[masterId].IP].Port

		if err != nil {
			panic(err)
		}
		cliClusterMaster, ctxClusterMaster := CreateRedisClient("127.0.0.1", port)
		for j := startPoint; j <= endPoint; j++ {
			// log.Println("分配槽位", j, "到主节点", masterId)
			cliClusterMaster.ClusterAddSlots(ctxClusterMaster, j)
		}
	}
}

// DataInit 初始化数据
func DataInit(ctxPodman context.Context) {
	// 容器信息获取
	println("操作前podmanContains信息：")
	println("--------------------------------------------------")
	err := GetContainerInfo(ctxPodman)
	println("--------------------------------------------------")
	if err != nil {
		println(err)
	}
	if len(AllContainerInfoList) != 0 {
	}
}

// AddClusterNode 添加新节点到集群

func CreateAction(ctxPodman context.Context, num int) {

	DataInit(ctxPodman)

	CreateContainers(ctxPodman, num)

	println("操作后podmanContainer信息:")
	println("--------------------------------------------------")
	err := GetContainerInfo(ctxPodman)
	println("--------------------------------------------------")
	if err != nil {
		println(err)
	}
	cliRedis, ctxRedis := CreateRedisClient(AllContainerInfoList[0].IP, AllContainerInfoList[0].Port)
	println("执行Meet操作：")
	println("--------------------------------------------------")
	err = MeetNodes(cliRedis, ctxPodman)
	println("--------------------------------------------------")
	println("等待更新节点状态中......")
	time.Sleep(2 * time.Second)

	err = GetClusterNodesInfo(ctxRedis)
	err = SetAllMasterSlave()
	time.Sleep(4 * time.Second)
	if err != nil {
		log.Fatal(err)
	}
	// 开始分配slots, 需要
	masterIDs, err = GetMasterNodeIDs(cliRedis, ctxRedis)

	AllocateSlots(ctxPodman)
	err = PrintClusterNodesInfo(ctxPodman)
	if err != nil {
		println(err)
	}

}

func AddClusterNode(ctx context.Context) (ContainerInfo, error) {
	// 获取初始的容器信息
	if err := GetContainerInfo(ctx); err != nil {
		return ContainerInfo{}, err
	}

	// 创建一个新的容器
	nodeId := ContainerNum + 1
	if err := CreateContainer(ctx, nodeId); err != nil {
		return ContainerInfo{}, err
	}
	time.Sleep(1 * time.Second)

	// 获取更新后的容器信息
	if err := GetContainerInfo(ctx); err != nil {
		return ContainerInfo{}, err
	}

	// 创建 Redis 客户端并使节点互相发现
	println("--------------------------------------------------")
	log.Println("%v %v", AllContainerInfoList[0].IP, AllContainerInfoList[0].Port)
	println("---------------------------------------------------")
	cliRedis, ctx := CreateRedisClient(AllContainerInfoList[0].IP, AllContainerInfoList[0].Port)
	if err := MeetNodes(cliRedis, ctx); err != nil {
		return ContainerInfo{}, err
	}
	time.Sleep(3 * time.Second)

	// 获取更新后的集群节点信息
	if err := GetClusterNodesInfo(ctx); err != nil {
		return ContainerInfo{}, err
	}

	newContainerNode := AllContainerInfoList[len(AllContainerInfoList)-1]
	time.Sleep(2 * time.Second) // 给新节点一些时间来初始化

	return newContainerNode, nil
}

func AddClusterSlave(ctx context.Context) error {
	// 添加一个新的集群节点
	containerNode, err := AddClusterNode(ctx)
	newclusterNode := ClusterIdClusterInfoMapping[IPToClusterIDMapping[containerNode.ConIp]]
	if err != nil {
		return err
	}

	// 找到从节点最少的主节点
	minSlavesOfMaster := math.MaxInt
	var minMaster string

	for masterID, slaves := range MasterToSlaveMapping {
		if len(slaves) < minSlavesOfMaster && masterID != newclusterNode.ID {
			minSlavesOfMaster = len(slaves)
			minMaster = masterID
		}
	}

	// 将新节点设置为选择的主节点的从节点
	slaveIP := containerNode.IP
	slavePort := containerNode.Port
	err = GetClusterNodesInfo(ctx)
	cliRedis, _ := CreateRedisClient(slaveIP, slavePort)
	if _, err := cliRedis.ClusterReplicate(ctx, minMaster).Result(); err != nil {
		return err
	}

	return nil
}

func AddAction(ctx context.Context, masterNum int, slaveNum int) error {
	var err error

	// 添加主节点
	for i := 0; i < masterNum; i++ {
		if _, err = AddClusterNode(ctx); err != nil {
			log.Printf("添加主节点 %d 时发生错误: %v", i, err)
			return err
		}
	}
	// 添加从节点
	for i := 0; i < slaveNum; i++ {
		if err = AddClusterSlave(ctx); err != nil {
			log.Printf("添加从节点 %d 时发生错误: %v", i, err)
			return err
		}
	}

	return nil
}

func main() {
	// 首先连接podman
	ctxPodman := CreatePodmanConnection()
	CreateAction(ctxPodman, 6)
	//err := AddAction(ctxPodman, 0, 5)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//time.Sleep(4 * time.Second)
	//println("------------------------result:")
	//err = GetClusterNodesInfo(ctxPodman)
	//err = PrintClusterNodesInfo(ctxPodman)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//for k, v := range masterSet {
	//	log.Println(k, v)
	//}

	//defer DeleteAllContainers(ctxPodman)
}
