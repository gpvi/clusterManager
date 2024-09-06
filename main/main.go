package main

import (
	"context"
	"fmt"
	"github.com/containers/common/libnetwork/types"
	"github.com/containers/podman/v5/pkg/bindings"
	"github.com/containers/podman/v5/pkg/bindings/containers"
	"github.com/containers/podman/v5/pkg/specgen"
	"github.com/go-redis/redis/v8"
	"github.com/opencontainers/runtime-spec/specs-go"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type ContainerInfo struct {
	Name  string
	IP    string
	Port  uint16
	ConIp string
	Id    string
}

var TRUE = true
var FALSE = false

type ClusterNodeInfo struct {
	NodeID string
	IP     string
	Port   string
}

// 总槽数
const totalSlots = 16384

var masterIDs = make([]string, 0)

var masterSet = make(map[string]bool)

// 指定本地的配置文件路径
var redisHostConfigPath = "/Users/zhuoqun.niu/Desktop/redis/config"

// 容器路径
var redisConfigPath = "/data/redis/config"

// 宿主机路径
var redisHostDataPath = "/Users/zhuoqun.niu/Desktop/redis/data"

// 容器路径
var redisConfigDataPath = "/data/redis/data"

var ClusterIdClusterInfoMapping = make(map[string]ClusterNodeInfo)

var IPToContainerInfoMapping = make(map[string]ContainerInfo)

var ContainIdToClusterInfoMapping = make(map[string]ContainerInfo)

var AllContainerInfoList = make([]ContainerInfo, 0)

var AlreadyMeetNode = make(map[string]bool)

var AllRedisClusterList = make([]ClusterNodeInfo, 0)

var ContainNum = 0

var ContainerNum = 0

var AlreadySetCluster = make(map[string]bool)

func CreateConnection() context.Context {
	conn, err := bindings.NewConnection(context.Background(), "unix:///Users/zhuoqun.niu/.local/share/containers/podman/machine/podman.sock")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)

	}
	return conn
}

func CreateContainer(ctx context.Context, nodeId int) {
	ContainerNum = ContainerNum + 1
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
}

func DeleteAllContainer(ctx context.Context) {
	// Stop and remove all containers
	containerList, err := containers.List(ctx, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	forceFlg := true
	for _, container := range containerList {
		if container.State == "exited" {
			// Stop the container before removing
			err := containers.Stop(ctx, container.ID, nil)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println("Container stopped:", container.ID)
		}
		report, err := containers.Remove(ctx, container.ID, &containers.RemoveOptions{
			Force: &forceFlg,
		})
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("Container removed:", report)
		}
	}
}

func GetContainerInfo(ctx context.Context) ([]ContainerInfo, error) {
	containerList, err := containers.List(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	var ipArr []ContainerInfo

	for _, container := range containerList {
		// 计数当前容器数量
		ContainerNum += 1
		if _, exist := ContainIdToClusterInfoMapping[container.ID]; exist {
			continue
		}

		inspect, err := containers.Inspect(ctx, container.ID, nil)
		if err != nil {
			fmt.Printf("failed to inspect container %s: %v\n", container.ID, err)
			continue
		}

		for _, network := range inspect.NetworkSettings.Networks {
			containerNode := ContainerInfo{
				Name:  container.Names[0],
				IP:    "127.0.0.1", // 这个可能是占位符，如果需要可以更新
				ConIp: network.IPAddress,
				Port:  container.Ports[0].HostPort,
				Id:    container.ID,
			}
			fmt.Printf("Node信息： %v \n", containerNode)
			AllContainerInfoList = append(AllContainerInfoList, containerNode)
			ipArr = append(ipArr, containerNode)
			IPToContainerInfoMapping[network.IPAddress] = containerNode
			ContainIdToClusterInfoMapping[container.ID] = containerNode
		}
	}

	return ipArr, nil
}

func CreateContainers(ctx context.Context, nodeCount int) {
	for i := 1; i <= nodeCount; i++ {
		CreateContainer(ctx, i)
	}

}

// CreateClient 执行 redis-cli --cluster create 命令
func CreateClient(ip string, port uint16) (*redis.Client, context.Context) {
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

func MeetNodes(client *redis.Client, ctx context.Context, nodes []ContainerInfo) error {

	for _, node := range nodes {
		_, exist := AlreadyMeetNode[node.IP]
		if !exist {
			_, err := client.ClusterMeet(ctx, node.ConIp, "6379").Result()
			if err != nil {
				return fmt.Errorf("could not meet node %v: %v", node.ConIp, err)
			}
			fmt.Printf("Node %v added to the cluster\n", node)
		}

	}
	return nil
}

func GetClusterIdToIPPortMapping(client *redis.Client, ctx context.Context) (map[string]string, []string, error) {
	// 执行 CLUSTER NODES 命令获取集群中的所有节点信息
	nodesInfo, err := client.ClusterNodes(ctx).Result()
	if err != nil {
		println("failed to get cluster nodes info: %v", err)
	}
	// 解析返回结果，提取所有的 node ID 和对应的 IP+Port
	lines := strings.Split(nodesInfo, "\n")
	println(lines)
	nodeMapping := make(map[string]ClusterNodeInfo)
	nodeIDs := make([]string, 0)
	ipPortToNodeID := make(map[string]string)
	ipList := make([]string, 0)
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) > 1 && fields[len(fields)-1] == "connected" {
			nodeIDs = append(nodeIDs, fields[0])
			nodeID := fields[0]
			ipPort := fields[1]
			// ipPort @后边是集群通信port
			ipPort = strings.Split(ipPort, "@")[0]
			// 此处port 为6739
			ip, port := parseIPPort(ipPort)
			NodeInfo := ClusterNodeInfo{
				NodeID: nodeID,
				IP:     ip,
				Port:   port,
			}

			nodeMapping[nodeID] = NodeInfo
			AllRedisClusterList = append(AllRedisClusterList, NodeInfo)

			ipList = append(ipList, ip)
			ipPortToNodeID[ip] = nodeID

		}
	}

	return ipPortToNodeID, ipList, nil
}

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

// parseIPPort 将 IP:Port 格式的字符串分割为 IP 和 Port
func parseIPPort(ipPort string) (string, string) {
	parts := strings.Split(ipPort, ":")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", ""
}

// SetMasterSlave 设置主从节点
func SetMasterSlave(ipPortList []string, ipPortMapping map[string]string) error {
	// 计算主节点数量
	n := len(ipPortList) / 2
	expectedNodes := len(ipPortList)
	for i := 0; i < n; i++ {
		masterID := ipPortMapping[ipPortList[i]]
		slaveID := ipPortMapping[ipPortList[i+n]]
		AlreadySetCluster[masterID] = true
		AlreadySetCluster[slaveID] = true
		slaveIP := strings.Split(ipPortList[i+n], ":")[0]
		cli, ctx := CreateClient("127.0.0.1", IPToContainerInfoMapping[slaveIP].Port)
		slaveAddr := fmt.Sprintf("localhost:%d", IPToContainerInfoMapping[slaveIP].Port)
		// 创建从节点的 Redis 客户端
		println("slave:", slaveAddr)
		println("id:", ipPortMapping[ipPortList[i+n]])
		// 轮询等待集群同步
		err := WaitForClusterSync(cli, ctx, expectedNodes)
		if err != nil {
			println(err)
		}
		// 执行 ClusterReplicate 命令
		cmdMessage := cli.ClusterReplicate(ctx, masterID)
		if err := cmdMessage.Err(); err != nil {
			fmt.Printf("Error executing ClusterReplicate for slave %s %v\n", slaveAddr, err)
			return fmt.Errorf("failed to set node %s as replica of master %s: %v", slaveAddr, ipPortList[i], err)
		}

		fmt.Printf("Node %s set as replica of master %s\n", slaveAddr, ipPortList[i])
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
	for _, line := range lines {
		log.Println(line)
	}
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

func AllocateSlots() {
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
		port := ClusterIdClusterInfoMapping[masterId].Port
		uintPort, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			println("解析端口号时出错:", err)
			continue
		}
		uint16Port := uint16(uintPort)

		cliClusterMaster, ctxClusterMaster := CreateClient("127.0.0.1", uint16Port)
		for j := startPoint; j <= endPoint; j++ {
			// log.Println("分配槽位", j, "到主节点", masterId)
			cliClusterMaster.ClusterAddSlots(ctxClusterMaster, j)
		}
	}
}

func Process(ctxPodman context.Context) {
	println("操作前podmanContains信息：")
	println("--------------------------------------------------")
	_, err := GetContainerInfo(ctxPodman)
	println("--------------------------------------------------")
	CreateContainers(ctxPodman, 6)

	println("操作后podmanContainer信息:")
	println("--------------------------------------------------")
	_, err = GetContainerInfo(ctxPodman)
	println("--------------------------------------------------")
	if err != nil {
		println(err)
	}
	cliRedis, ctxRedis := CreateClient(AllContainerInfoList[0].IP, AllContainerInfoList[0].Port)
	err = MeetNodes(cliRedis, ctxPodman, AllContainerInfoList)
	time.Sleep(2 * time.Second)
	ipNodeMapping, ipPortsMapping, err := GetClusterIdToIPPortMapping(cliRedis, ctxRedis)
	err = SetMasterSlave(ipPortsMapping, ipNodeMapping)
	for ip, node := range ipNodeMapping {
		port := strconv.Itoa(int(IPToContainerInfoMapping[ip].Port))
		ClusterIdClusterInfoMapping[node] = ClusterNodeInfo{
			NodeID: node,
			IP:     ip,
			Port:   port,
		}

		println(ip, IPToContainerInfoMapping[ip].Port, node)
	}
	time.Sleep(4 * time.Second)
	if err != nil {
		log.Fatal(err)
	}
	// 开始分配slots, 需要
	masterIDs, err = GetMasterNodeIDs(cliRedis, ctxRedis)

}

func AddNewContainerTOCLUSTER(ctx context.Context) {
	nodeId := ContainerNum + 1
	CreateContainer(ctx, nodeId)
	ContainerInfoList, err := GetContainerInfo(ctx)
	if err != nil {
		println(err)
	}
	for _, node := range ContainerInfoList {
		println(node.IP)
	}
	cliRedis, ctx := CreateClient(AllContainerInfoList[0].IP, AllContainerInfoList[0].Port)
	err = MeetNodes(cliRedis, ctx, AllContainerInfoList)
	if err != nil {
		println(err)
	}
	// 之前集群中存在无从节点的主节点
	if len(masterIDs)%2 != 0 {

	}

}

func main() {
	// 首先连接podman
	ctxPodman := CreateConnection()
	//
	Process(ctxPodman)
	AllocateSlots()
	//AddNewContainerTOCLUSTER(ctxPodman)

	//AddNewContainerToCluster(ctxPodman)
	defer DeleteAllContainer(ctxPodman)
}
