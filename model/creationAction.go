package model

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
)

var cliRedis *redis.Client
var ctxRedis context.Context

// CreateContainers 创建指定数量的容器
func CreateContainers(ctx context.Context, nodeCount int) {
	for i := 1; i <= nodeCount; i++ {
		err := CreateContainer(ctx, i)
		if err != nil {
			fmt.Println(err)
		}
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

	//fmt.Println("Container created:", createResponse.ID)

	if err := containers.Start(ctx, createResponse.ID, nil); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	//fmt.Println("Container started.")
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
func CreateAction(shared int, replica int) error {
	ctxPodman := CreatePodmanConnection()
	sum := shared * replica
	_, _, err := DataInit()
	if err != nil {
		log.Printf("数据初始化失败 %v", err)
	}

	CreateContainers(ctxPodman, sum)
	containerInfo, err := GetContainersInfo(ctxPodman)
	if err != nil {
		return err
	}
	if containerInfo == nil {
		return fmt.Errorf("容器信息为空，容器创建失败")
	}

	log.Printf("完成集群容器创建共 %d 个sharder,规格为 %d 个replica", shared, replica)
	println("容器信息如下：")

	for _, node := range containerInfo.AllContainerInfoList {
		println("容器名: ", node.Name, "容器ID: ", node.Id, "容器宿主地址: ", node.IP, "容器映射端口:", node.Port, "容器IP:", node.ConIp, "容器端口: ", node.ConPort)
	}
	// 创建redis client
	if len(containerInfo.AllContainerInfoList) > 0 {
		cliRedis, ctxRedis = CreateRedisClient(containerInfo.AllContainerInfoList[0].IP, containerInfo.AllContainerInfoList[0].Port)
		// 用匿名函数处理 defer 中的错误
		defer func() {
			if err := cliRedis.Close(); err != nil {
				fmt.Printf("Error closing Redis client: %v\n", err)
			}
		}()
		// 进行其他 Redis 操作
	} else {
		// 处理容器列表为空的情况
		fmt.Println("No containers available.")
	}
	println("开始执行Meet操作...")
	err = MeetNodes(cliRedis, ctxPodman, containerInfo)
	println("更新节点状态...")

	// 获取集群信息
	err = GetClusterNodesInfo(ctxPodman)
	if err != nil {
		log.Printf("获取集群信息失败: %v", err)
	}

	err = SetAllMasterSlave(replica)
	if err != nil {
		log.Printf("设置节点为master/slave失败: %v", err)
	}
	err = GetClusterNodesInfo(ctxPodman)
	if err != nil {
		log.Printf("获取集群信息失败: %v", err)
	}
	err = AllocateSlots(ctxPodman)
	if err != nil {
		return fmt.Errorf("分配slots失败:%v", err)
	}

	err = PrintClusterNodesInfo(ctxPodman)
	if err != nil {
		return fmt.Errorf("打印集群信息失败: %v", err)
	}
	return err
}
