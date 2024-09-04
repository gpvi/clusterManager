package main

import (
	"context"
	"fmt"
	"github.com/containers/common/libnetwork/types"
	"github.com/containers/podman/v5/pkg/bindings"
	"github.com/containers/podman/v5/pkg/bindings/containers"
	"github.com/containers/podman/v5/pkg/bindings/images"
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

const totalSlots = 16384

// 指定本地的配置文件路径
var redisHostConfigPath string = "/Users/zhuoqun.niu/Desktop/redis/config"

// 容器路径
var redisConfigPath string = "/data/redis/config"

// 宿主机路径
var redisHostDataPath string = "/Users/zhuoqun.niu/Desktop/redis/data"

// 容器路径
var redisConfigDataPath string = "/data/redis/data"

// 容器唯一计数
var count int = 1

type ContainerInfo struct {
	Name  string
	IP    string
	Port  uint16
	ConIp string
	Id    string
}

type ClusterInfo struct {
	NodeID string
	IP     string
	Port   string
}

func createConnection() context.Context {
	conn, err := bindings.NewConnection(context.Background(), "unix:///Users/zhuoqun.niu/.local/share/containers/podman/machine/podman.sock")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
		panic(err)
	}
	return conn
}

func createContainer(conn context.Context, nodeid int) {
	startConfigPath := filepath.Join(redisConfigPath, "redis.conf")
	//s := specgen.NewSpecGenerator("redis:7.4.0-alpine", false)
	s := specgen.NewSpecGenerator("myredis2", false)

	s.Name = fmt.Sprintf("redis-%d", nodeid)

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

	createResponse, err := containers.CreateWithSpec(conn, s, nil)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("Container created:", createResponse.ID)

	if err := containers.Start(conn, createResponse.ID, nil); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("Container started.")
}

func pullImage(ctx context.Context) {
	//_, err := images.Pull(ctx, "redis:7.4.0-alpine", nil)
	_, err := images.Pull(ctx, "redis:latest", nil)

	if err != nil {
		fmt.Println(err)
		panic(err)
		os.Exit(1)
	}

}

func deleteAllContainer(ctx context.Context) {
	// Stop and remove all containers
	containerList, err := containers.List(ctx, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	force_flg := true
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
			Force: &force_flg,
		})
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("Container removed:", report)
		}
	}
}

var iptonodeMapping = make(map[string]ContainerInfo)

func getContainerIPs(ctx context.Context) ([]ContainerInfo, error) {

	containerList, err := containers.List(ctx, nil)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	var ipArr []ContainerInfo
	for _, container := range containerList {
		inspect, err := containers.Inspect(ctx, container.ID, nil)
		if err != nil {
			fmt.Println(err)
			continue
		}

		for _, network := range inspect.NetworkSettings.Networks {
			ipArr = append(ipArr, ContainerInfo{
				Name:  container.Names[0],
				IP:    "127.0.0.1",
				ConIp: network.IPAddress,
				Port:  container.Ports[0].HostPort,
				Id:    container.ID,
			})
			iptonodeMapping[network.IPAddress] = ContainerInfo{
				Name:  container.Names[0],
				IP:    network.IPAddress,
				Port:  container.Ports[0].HostPort,
				ConIp: network.IPAddress,
			}

		}
		//for _, info := range ipArr {
		//	fmt.Printf("Container Name: %s, IP Address: %s\n", info.Name, info.IP)
		//}
	}
	return ipArr, nil
}

func createCluster(ctx context.Context, nodeCount int) {
	for i := 1; i <= nodeCount; i++ {
		createContainer(ctx, i)
	}
}

func StopAllContainers(ctx context.Context) {
	containerList, err := containers.List(ctx, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, container := range containerList {
		if err := containers.Stop(ctx, container.ID, nil); err != nil {
			fmt.Println(err)
		}
	}
}

// 执行 redis-cli --cluster create 命令
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

		_, err := client.ClusterMeet(ctx, node.ConIp, "6379").Result()
		if err != nil {
			return fmt.Errorf("could not meet node %v: %v", node, err)
		}
		fmt.Printf("Node %v added to the cluster\n", node)
	}
	return nil
}

func GetNodeIDToIPPortMapping(client *redis.Client, ctx context.Context) (map[string]string, []string, error) {
	// 执行 CLUSTER NODES 命令获取集群中的所有节点信息

	nodesInfo, err := client.ClusterNodes(ctx).Result()
	if err != nil {
		println("failed to get cluster nodes info: %v", err)
	}

	// 解析返回结果，提取所有的 node ID 和对应的 IP+Port
	lines := strings.Split(nodesInfo, "\n")
	println(lines)
	nodeMapping := make(map[string]ClusterInfo)
	nodeIDs := []string{}
	ipPortToNodeID := make(map[string]string)
	ipList := []string{}
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) > 1 && fields[len(fields)-1] == "connected" {
			nodeIDs = append(nodeIDs, fields[0])
			nodeID := fields[0]
			ipPort := fields[1]
			ipPort = strings.Split(ipPort, "@")[0]
			ip, port := parseIPPort(ipPort)

			nodeMapping[nodeID] = ClusterInfo{
				NodeID: nodeID,
				IP:     ip,
				Port:   port,
			}
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

// 设置主从节点
func SetMasterSlave(ipPortList []string, ipPortMapping map[string]string) error {
	// 延迟确保状态同步

	n := len(ipPortList) / 2
	expectedNodes := len(ipPortList)
	for i := 0; i < n; i++ {
		masterID := ipPortMapping[ipPortList[i]]
		slaveIP := strings.Split(ipPortList[i+n], ":")[0]
		cli, ctx := CreateClient("127.0.0.1", iptonodeMapping[slaveIP].Port)
		slaveAddr := fmt.Sprintf("localhost:%d", iptonodeMapping[slaveIP].Port)
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
		println(line)
	}
	masterIDs := []string{}

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) > 2 {
			fmt.Printf("Parsed fields: %v\n", fields) // 调试输出

			if strings.Contains(fields[2], "master") {
				nodeID := fields[0]
				masterIDs = append(masterIDs, nodeID)
			}
		}
	}

	return masterIDs, nil
}

func main() {
	// podman
	ctx_podman := createConnection()
	createCluster(ctx_podman, 6)
	name_ips, _ := getContainerIPs(ctx_podman)
	cli, cli_ctx := CreateClient(name_ips[0].IP, name_ips[0].Port)
	MeetNodes(cli, ctx_podman, name_ips)
	time.Sleep(2 * time.Second)
	ip_node_mapping, ip_ports, err := GetNodeIDToIPPortMapping(cli, cli_ctx)
	SetMasterSlave(ip_ports, ip_node_mapping)
	idToIPPort := make(map[string]ClusterInfo)

	for ip, node := range ip_node_mapping {
		port := strconv.Itoa(int(iptonodeMapping[ip].Port))
		idToIPPort[node] = ClusterInfo{
			NodeID: node,
			IP:     ip,
			Port:   port,
		}

		println(ip, iptonodeMapping[ip].Port, node)
	}
	time.Sleep(4 * time.Second)

	if err != nil {
		log.Fatal(err)
	}

	masterIDs, err := GetMasterNodeIDs(cli, cli_ctx)

	for i := 0; i < len(masterIDs); i++ {
		stratPoint := i * totalSlots / len(masterIDs)
		endpPoint := stratPoint + totalSlots/len(masterIDs) - 1
		masterId := masterIDs[i]
		port := idToIPPort[masterId].Port
		uintPort, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			println(err)
		}
		uint_16_port := uint16(uintPort)
		cli_temp, ctx_temp := CreateClient("127.0.0.1", uint_16_port)
		for j := stratPoint; j <= endpPoint; j++ {
			cli_temp.ClusterAddSlots(ctx_temp, j)
		}
	}
}
