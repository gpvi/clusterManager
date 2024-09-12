package model

import (
	"context"
	"fmt"
	"github.com/containers/common/libnetwork/types"
	"github.com/containers/podman/v5/pkg/bindings"
	"github.com/containers/podman/v5/pkg/bindings/containers"
	"github.com/containers/podman/v5/pkg/specgen"
	"github.com/opencontainers/runtime-spec/specs-go"
	"log"
	"os"
	"path/filepath"
)

type ContainerNode struct {
	Name    string
	HostIP  string
	IP      string
	Port    uint16
	ConIp   string
	ConPort uint16
	Id      string
}

// Containers 结构体存储 Podman 容器相关的信息
type Containers struct {
	IPToNode  map[string]*ContainerNode // IP 地址到 ContainerNode 的映射
	Num       int                       // 容器数量
	IDToNode  map[string]*ContainerNode // 容器 ID 到 ContainerNode 的映射
	Nodes     []*ContainerNode          // 所有容器的节点信息列表
	ctxPodman context.Context
}

// CreatePodmanConnection 创建连接

func CreatePodmanConnection() (context.Context, error) {
	conn, err := bindings.NewConnection(context.Background(), "unix:///Users/zhuoqun.niu/.local/share/containers/podman/machine/podman.sock")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return conn, err
}

// NewContainers 构造函数，用于初始化 Containers 结构体并分配必要的内存
func NewContainers() (*Containers, error) {
	ctx, err := CreatePodmanConnection()
	return &Containers{
		IPToNode:  make(map[string]*ContainerNode), // 初始化 IP 映射
		IDToNode:  make(map[string]*ContainerNode), // 初始化 ID 映射
		Nodes:     make([]*ContainerNode, 0, 10),   // 初始化容器列表并预留容量
		Num:       0,                               // 初始容器数量为 0
		ctxPodman: ctx,
	}, err
}

// AddContainerNode 添加一个新的 ContainerNode 到容器信息
func (c *Containers) AddContainerNode(node *ContainerNode) {
	c.IPToNode[node.ConIp] = node
	c.IDToNode[node.Id] = node
	c.Nodes = append(c.Nodes, node)
	c.Num = len(c.Nodes) // 更新容器数量
}

// createContainer 创建容器
func (c *Containers) createContainer(ctx context.Context, nodeId int) error {
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

	//fmt.Println("Container created:", createResponse.ID)

	if err := containers.Start(ctx, createResponse.ID, nil); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	//fmt.Println("Container started.")
	return err
}

func (c *Containers) AddContainers(ctx context.Context, nodeNum int) error {
	var err error
	start := c.Num + 1
	end := c.Num + nodeNum
	for i := start; i <= end; i++ {
		err = c.createContainer(ctx, i)
		if err != nil {
			fmt.Println(err)
		}
	}
	return err
}

func (c *Containers) UpdateContainers() error {
	ctx := c.ctxPodman
	// 获取当前 Podman 的容器列表
	containerList, err := containers.List(c.ctxPodman, nil)
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err) // 列表获取失败时返回错误
	}

	containerNum := len(containerList) // 容器数量
	if containerNum <= 0 {
		log.Println("当前Podman容器数量为0.") // 容器数量为 0 时打印日志提示
	}

	// 遍历获取到的容器列表
	for _, container := range containerList {

		// 检查当前容器是否已存在于映射中，避免重复处理
		if _, exist := c.IDToNode[container.ID]; exist {
			continue // 如果容器已存在，跳过该容器
		}

		// 获取容器的详细信息
		inspect, err := containers.Inspect(ctx, container.ID, nil)
		if err != nil {
			fmt.Printf("failed to inspect container %s: %v\n", container.ID, err) // 检查容器失败时打印错误
			return err
		}

		// 循环遍历容器的网络设置（通常只有一个网络）
		for _, network := range inspect.NetworkSettings.Networks {
			// 创建 ContainerNode 实例，存储容器的相关信息
			containerNode := ContainerNode{
				Name:    container.Names[0],          // 容器名称
				IP:      "127.0.0.1",                 // 容器本地 IP
				ConIp:   network.IPAddress,           // 容器的网络 IP
				Port:    container.Ports[0].HostPort, // 容器主机端口
				Id:      container.ID,                // 容器 ID
				ConPort: 6379,                        // 容器内部 Redis 端口（假设为 Redis 容器）
			}

			// 将当前容器信息加入 Containers
			c.Nodes = append(c.Nodes, &containerNode)      // 添加到容器列表
			c.IDToNode[container.ID] = &containerNode      // 更新 ID 映射
			c.IPToNode[network.IPAddress] = &containerNode // 更新 IP 映射
		}
		c.Num = containerNum
	}

	// 返回获取到的容器信息
	return nil
}

// GetContainersInfoFromPodman 从 Podman 中获取所有容器的详细信息，并将其存储在 Containers 中
func GetContainersInfoFromPodman(ctx context.Context) (*Containers, error) {
	containerInfo, err := NewContainers()
	if containerInfo == nil || err != nil {
		log.Println("创建容器信息实例失败 in GetContainersInfoFromPodman", err)
		return containerInfo, err
	}
	// 获取当前 Podman 的容器列表
	containerList, err := containers.List(containerInfo.ctxPodman, nil)
	if err != nil {
		return &Containers{}, fmt.Errorf("failed to list containers: %w", err) // 列表获取失败时返回错误
	}

	containerNum := len(containerList) // 容器数量
	if containerNum <= 0 {
		log.Println("当前Podman容器数量为0.") // 容器数量为 0 时打印日志提示
	}

	// 遍历获取到的容器列表
	for _, container := range containerList {

		// 检查当前容器是否已存在于映射中，避免重复处理
		if _, exist := containerInfo.IDToNode[container.ID]; exist {
			continue // 如果容器已存在，跳过该容器
		}

		// 获取容器的详细信息
		inspect, err := containers.Inspect(ctx, container.ID, nil)
		if err != nil {
			fmt.Printf("failed to inspect container %s: %v\n", container.ID, err) // 检查容器失败时打印错误
			return nil, err
		}

		// 循环遍历容器的网络设置（通常只有一个网络）
		for _, network := range inspect.NetworkSettings.Networks {
			// 创建 ContainerNode 实例，存储容器的相关信息
			containerNode := ContainerNode{
				Name:    container.Names[0],          // 容器名称
				IP:      "127.0.0.1",                 // 容器本地 IP
				ConIp:   network.IPAddress,           // 容器的网络 IP
				Port:    container.Ports[0].HostPort, // 容器主机端口
				Id:      container.ID,                // 容器 ID
				ConPort: 6379,                        // 容器内部 Redis 端口（假设为 Redis 容器）
			}

			// 将当前容器信息加入 Containers
			containerInfo.Nodes = append(containerInfo.Nodes, &containerNode) // 添加到容器列表
			containerInfo.IDToNode[container.ID] = &containerNode             // 更新 ID 映射
			containerInfo.IPToNode[network.IPAddress] = &containerNode        // 更新 IP 映射
		}
	}

	// 返回获取到的容器信息
	return containerInfo, nil
}
