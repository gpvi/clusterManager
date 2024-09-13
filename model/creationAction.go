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
func CreateAction(ctx context.Context, shared int, replica int) (context.Context, error) {
	var err error
	// 创建container 和 cluster 对象
	containers, err := NewContainers(ctx)
	if err != nil {
		return ctx, fmt.Errorf("create containers object fail: %v", err)
	}
	//获取初始化容器信息
	err = containers.UpdateNodesInfo(ctx)
	// 判断创建操作是否合法
	if containers.Num != 0 {
		return ctx, fmt.Errorf("already exist containers，please operate after delete  before containers")
	}
	// 创建集群信息对象
	cluster := NewCluster()
	sum := shared * replica

	err = containers.AddContainers(ctx, sum)
	if err != nil {
		return ctx, fmt.Errorf("create container fail %v", err)
	}

	fmt.Printf("finish created, %d sharder, %d 个replica ", shared, replica)
	fmt.Println("containers info list：")

	// 打印当前容器信息
	for _, node := range containers.Nodes {
		fmt.Println("containerName: ", node.Name, "containerID: ", node.ID, "HostIP: ", node.HostIP, "HostPort:", node.HostPort, "ContainerIP:", node.ConIp, "containerPort: ", node.ConPort)
	}
	// 创建redis client
	if len(containers.Nodes) > 0 {
		cliRedis, err = containers.Nodes[0].CreateRedisClient(ctx)
		defer func() {
			if err := cliRedis.Close(); err != nil {
				fmt.Printf("Error closing Redis client: %v\n", err)
			}
		}()

	} else {
		// 处理容器列表为空的情况
		fmt.Println("No containers available.")
	}

	fmt.Println("开始执行Meet操作...")
	err = MeetNodes(cliRedis, ctx, containers, cluster)
	fmt.Println("更新节点状态...")
	println("----------------------------------------")
	// 获取集群信息
	ctx, err = cluster.UpdateClusterNodesInfo(ctx, containers, containers.Nodes[0])
	if err != nil {
		fmt.Printf("获取集群信息失败: %v", err)
		return ctx, err
	}

	// 设置主从关系
	err = SetAllNodeType(ctx, containers, cluster, replica)
	if err != nil {
		fmt.Printf("设置节点为master/slave失败: %v", err)
		return ctx, err
	}

	// 获取cluster信息
	ctx, err = cluster.UpdateClusterNodesInfo(ctx, containers, containers.Nodes[0])
	if err != nil {
		fmt.Printf("获取集群信息失败: %v", err)
		return ctx, err
	}
	err = AllocateSlots(ctx, containers, cluster)
	if err != nil {
		return ctx, fmt.Errorf("分配slots失败:%v", err)
	}

	err = PrintClusterNodesInfo(ctx, containers, cluster)
	if err != nil {
		return ctx, fmt.Errorf("打印集群信息失败: %v", err)
	}
	return ctx, err
}
