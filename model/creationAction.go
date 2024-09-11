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
	"time"
)

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
func CreateAction(shared int, replica int) {
	ctxPodman := CreatePodmanConnection()
	// shard >= 3
	// replica >= 1
	// replica == 2  一主一从
	sum := shared * replica

	DataInit()
	CreateContainers(ctxPodman, sum)
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

	time.Sleep(3 * time.Second)

	err = GetClusterNodesInfo(ctxRedis)
	err = SetAllMasterSlave(replica)

	println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	time.Sleep(4 * time.Second)
	if err != nil {
		log.Fatal(err)
	}
	AllocateSlots(ctxPodman)
	err = PrintClusterNodesInfo(ctxPodman)
	if err != nil {
		println(err)
	}

}
