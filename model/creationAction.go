package model

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
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
	var err error
	containers := NewContainers()
	ctxPodman := containers.ctxPodman
	sum := shared * replica
	_, _, err = DataInit(containers)

	if err != nil {
		log.Printf("数据初始化失败 %v", err)
	}
	err = containers.AddContainers(ctxPodman, sum)
	if err != nil {
		log.Printf("容器创建失败 %v", err)
	}

	//CreateContainers(ctxPodman, sum)
	containerInfo, err := GetContainersInfoFromPodman(ctxPodman)
	if err != nil {
		return err
	}
	if containerInfo == nil {
		return fmt.Errorf("容器信息为空，容器创建失败")
	}

	log.Printf("完成集群容器创建共 %d 个sharder,规格为 %d 个replica", shared, replica)
	println("容器信息如下：")

	for _, node := range containerInfo.Nodes {
		println("容器名: ", node.Name, "容器ID: ", node.Id, "容器宿主地址: ", node.IP, "容器映射端口:", node.Port, "容器IP:", node.ConIp, "容器端口: ", node.ConPort)
	}
	// 创建redis client
	if len(containerInfo.Nodes) > 0 {
		cliRedis, ctxRedis = CreateRedisClient(containerInfo.Nodes[0].IP, containerInfo.Nodes[0].Port)
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
