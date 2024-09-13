package model

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
)

var cliRedis *redis.Client

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
func CreateClusterAction(ctx context.Context, shared int, replica int) (context.Context, error) {
	var err error
	// 创建container 和 cluster 对象
	containersManager, err := NewContainersManager(ctx)
	if err != nil {
		return ctx, fmt.Errorf("create containers object fail: %v", err)
	}
	//获取初始化容器信息
	err = containersManager.UpdateAllContainersInfo(ctx)
	// 判断创建操作是否合法
	if containersManager.Num != 0 {
		return ctx, fmt.Errorf("already exist containers，please operate after delete  before containers")
	}
	// 创建集群信息对象
	clusterInfo := NewClusterManager()
	sum := shared * replica

	err = containersManager.CreateContainers(ctx, sum)
	if err != nil {
		return ctx, fmt.Errorf("create container fail %v", err)
	}

	err = containersManager.UpdateAllContainersInfo(ctx)
	if err != nil {
		return ctx, err
	}

	fmt.Printf("finish created, %d sharder, %d 个replica ", shared, replica)
	fmt.Println("containers info list：")

	// 打印当前容器信息
	for _, node := range containersManager.Nodes {
		fmt.Println("containerName: ", node.Name, "containerID: ", node.ID, "HostIP: ", node.HostIP, "HostPort:", node.HostPort, "ContainerIP:", node.ConIp, "containerPort: ", node.ConPort)
	}

	// 创建redis client
	if len(containersManager.Nodes) > 0 {
		cliRedis, err = containersManager.Nodes[0].CreateRedisClient(ctx)
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
	err = MeetNodes(cliRedis, ctx, containersManager, clusterInfo)
	if err != nil {
		return ctx, err
	}
	// 获取集群信息
	ctx, err = clusterInfo.UpdateClusterNodes(ctx, containersManager, containersManager.Nodes[0])
	if err != nil {
		fmt.Printf("Fail to ger ClusterManager Info : %v", err)
		return ctx, err
	}

	// 设置主从关系
	err = SetAllNodeType(ctx, containersManager, clusterInfo, replica)
	if err != nil {
		fmt.Printf("set master/slave fail: %v", err)
		return ctx, err
	}

	err = AllocateSlots(ctx, containersManager, clusterInfo)
	if err != nil {
		return ctx, fmt.Errorf("allocate slots fail:%v", err)
	}

	err = PrintClusterNodesInfo(ctx, containersManager, clusterInfo)
	if err != nil {
		return ctx, fmt.Errorf("print cluster info fail: %v", err)
	}
	return ctx, err
}
