package model

import (
	"context"
	"fmt"
	"github.com/containers/common/libnetwork/types"
	"github.com/containers/podman/v5/libpod/define"
	"github.com/containers/podman/v5/pkg/bindings"
	"github.com/containers/podman/v5/pkg/bindings/containers"
	"github.com/containers/podman/v5/pkg/specgen"
	"github.com/go-redis/redis/v8"
	"github.com/opencontainers/runtime-spec/specs-go"
	"log"
	"path/filepath"
	"redisStudy/utils"
	"time"
)

type ContainerNode struct {
	Name     string
	HostIP   string
	HostPort uint16
	ConIp    string
	ConPort  uint16
	ID       string
}

// CreateRedisClient 初始化和连接一个 Redis 客户端，如果已经存在则检查是否有效。
func (node *ContainerNode) CreateRedisClient(ctx context.Context) (*redis.Client, error) {
	// 创建一个新的 Redis 客户端
	addr := fmt.Sprintf("%s:%d", node.HostIP, node.HostPort)
	cli := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "", // 如果需要设置密码则填入
		DB:       0,  // 默认数据库
	})

	// Ping 以验证连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := cli.Ping(ctx).Result()
	if err != nil {
		cli = nil // 如果创建失败，确保客户端为 nil
		return nil, fmt.Errorf("failed to create Redis client for node %s: %v\n", node.ID, err)
	}
	fmt.Printf("Redis client successfully connected to node %s at %s \n", node.ID, addr)
	return cli, nil
}

// CloseRedisClient 关闭 Redis 客户端连接。
func (node *ContainerNode) CloseRedisClient(cli *redis.Client) error {
	if cli != nil {
		err := cli.Close()
		if err != nil {
			return fmt.Errorf("failed to close Redis client for node %s: %v", node.ID, err)
		}
		fmt.Printf("Redis client for node %s has been closed", node.ID)
	}
	return nil
}

// ContainersManager 结构体存储 Podman 容器相关的信息
type ContainersManager struct {
	IPToNode        map[string]*ContainerNode // IP 地址到 ContainerNode 的映射
	Num             int                       // 容器数量
	IDToNode        map[string]*ContainerNode // 容器 ID 到 ContainerNode 的映射
	Nodes           []*ContainerNode          // 所有容器的节点信息列表
	ContainersIDSet map[string]bool
}

// AddContainerNode 添加一个新的 ContainerNode 到容器信息
func (c *ContainersManager) AddContainerNode(node *ContainerNode) {
	c.IPToNode[node.ConIp] = node
	c.IDToNode[node.ID] = node
	c.Nodes = append(c.Nodes, node)
	c.Num = len(c.Nodes) // 更新容器数量
}

func (c *ContainersManager) CreateContainers(ctx context.Context, nodeNum int) error {
	var err error
	start := c.Num + 1
	end := c.Num + nodeNum
	for i := start; i <= end; i++ {
		containerID, err := c.CreateContainer(ctx, i)
		if err != nil {
			return err
		}
		// 记录由自己创建的容器ID
		c.ContainersIDSet[containerID] = true
	}
	for id := range c.ContainersIDSet {
		var inspect *define.InspectContainerData
		for {
			inspect, err = containers.Inspect(ctx, id, nil)
			if err != nil {
				return fmt.Errorf("failed to inspect container %s: %v", id, err)
			}
			if inspect.State.Running {
				fmt.Printf("container %v is running\n", id)
				break
			}

		}
		hostPort, err := utils.StringToUint16(inspect.NetworkSettings.Ports["6379/tcp"][0].HostPort)
		if err != nil {
			return fmt.Errorf("failed to parse host port for container %s: %v", id, err)
		}

		conIP := inspect.NetworkSettings.Networks["podman"].IPAddress

		containerNode := ContainerNode{
			Name:     inspect.Name,
			HostIP:   "127.0.0.1",
			HostPort: hostPort,
			ConIp:    conIP,
			ID:       inspect.ID,
			ConPort:  6379,
		}
		c.Nodes = append(c.Nodes, &containerNode) // 添加到容器列表
		c.IDToNode[inspect.ID] = &containerNode   // 更新 ID 映射
		c.IPToNode[conIP] = &containerNode        // 更新 IP 映射
		c.Num++
	}

	return err
}

// CreateContainer 创建容器
func (c *ContainersManager) CreateContainer(ctx context.Context, index int) (string, error) {
	startConfigPath := filepath.Join(redisConfigPath, "redis.conf")
	s := specgen.NewSpecGenerator("myredis", false)

	s.Name = fmt.Sprintf("redis-%d", index)

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
		"env": "prod",
	}

	s.PortMappings = []types.PortMapping{
		{
			ContainerPort: 6379,
			HostPort:      0, // Redis server port, 0 indicates a random host port should be chosen
			Protocol:      "tcp",
		},
		{
			ContainerPort: 16379, // cluster-announce-bus-port (Redis ClusterManager bus port)
			HostPort:      0,     // Random host port
			Protocol:      "tcp",
		},
	}

	s.Command = []string{"redis-server", startConfigPath}

	createResponse, err := containers.CreateWithSpec(ctx, s, nil)
	if err != nil {
		return "", err
	}
	//fmt.Println("Container created:", createResponse.ID)
	// 返回 createResponse
	if err := containers.Start(ctx, createResponse.ID, nil); err != nil {
		fmt.Println(err)
		return "", fmt.Errorf("container start fail:%v", err)
	}

	return createResponse.ID, err
}

func (c *ContainersManager) GetCurContainersNum(ctx context.Context) error {
	// 获取当前 Podman 的容器列表
	containerList, err := containers.List(ctx, nil)
	if err != nil {
		return err
	}
	c.Num = len(containerList)

	return nil
}

// CreatePodmanConnection 创建连接
func CreatePodmanConnection(ctx context.Context) (context.Context, error) {
	conn, err := bindings.NewConnection(ctx, "unix:///Users/zhuoqun.niu/.local/share/containers/podman/machine/podman.sock")
	if err != nil {
		return ctx, fmt.Errorf("create podman conection fail:%v ", err)
	}
	return conn, err
}

// NewContainersManager 构造函数，用于初始化 ContainersManager 结构体并分配必要的内存
func NewContainersManager() *ContainersManager {
	return &ContainersManager{
		IPToNode:        make(map[string]*ContainerNode), // 初始化 IP 映射
		IDToNode:        make(map[string]*ContainerNode), // 初始化 ID 映射
		Nodes:           make([]*ContainerNode, 0, 10),   // 初始化容器列表并预留容量
		Num:             0,                               // 初始容器数量为 0
		ContainersIDSet: make(map[string]bool),
	}
}

func CreateRedisClient(ctx context.Context, ip string, port uint16) *redis.Client {
	addr := fmt.Sprintf("%s:%d", ip, port)

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	// 测试连接
	_, err := client.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("could not connect to Redis: %v", err)
	}
	return client
}

func (c *ContainersManager) UpdateAllContainersInfo(ctx context.Context) error {
	// 获取当前 Podman 的容器列表
	containerList, err := containers.List(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err) // 列表获取失败时返回错误
	}
	for _, container := range containerList {
		if _, exist := c.ContainersIDSet[container.ID]; exist {
			continue
		}
		// 当前容器还未加入管理器
		c.Num++ // 容器数量
		inspect, err := containers.Inspect(ctx, container.ID, nil)
		if err != nil {
			fmt.Printf("failed to inspect container %s: %v\n", container.ID, err) // 检查容器失败时打印错误
			return err
		}

		// 循环遍历容器的网络设置（通常只有一个网络）
		for _, network := range inspect.NetworkSettings.Networks {
			// 创建 ContainerNode 实例，存储容器的相关信息
			containerNode := ContainerNode{
				Name:     container.Names[0],
				HostIP:   "127.0.0.1",
				ConIp:    network.IPAddress,
				HostPort: container.Ports[0].HostPort,
				ID:       container.ID,
				ConPort:  6379,
			}

			// 将当前容器信息加入 ContainersManager
			c.Nodes = append(c.Nodes, &containerNode)      // 添加到容器列表
			c.IDToNode[container.ID] = &containerNode      // 更新 ID 映射
			c.IPToNode[network.IPAddress] = &containerNode // 更新 IP 映射
		}
	}
	return nil
}
