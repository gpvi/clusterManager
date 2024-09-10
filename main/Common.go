package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"strings"
	"time"
)

func PrintClusterNodesInfo(ctx context.Context) error {
	client, ctxRedis := CreateRedisClient(AllContainerInfoList[0].IP, AllContainerInfoList[0].Port)
	ctx = ctxRedis
	nodesInfo, err := client.ClusterNodes(ctx).Result()
	if err != nil {
		println("failed to get cluster nodes info: %v", err)
	}
	println("cluster nodes lines:")
	println("---------------------------------------")
	// 解析返回结果，提取所有的 node ID 和对应的 IP+Port
	lines := strings.Split(nodesInfo, "\n")
	for _, line := range lines {
		println(line)
	}
	println("cluster nodes lines end")
	println("---------------------------------------")

	return nil
}

// SetNodeAsSlave 设置节点为从节点
func SetNodeAsSlave(masterIP string, slaveIP string) error {
	// 获取从节点的 IP 地址 (不带端口)
	slaveAddr := slaveIP
	// 创建 Redis 客户端
	println("**************")
	cli, ctx := CreateRedisClient("127.0.0.1", IPToContainerInfoMapping[slaveAddr].Port)

	// 打印从节点信息
	slaveAddrPort := fmt.Sprintf("localhost:%d", IPToContainerInfoMapping[slaveAddr].Port)
	println("slave:", slaveAddrPort)
	println("id:", IPToClusterIDMapping[slaveIP])

	// 轮询等待集群同步
	expectedNodes := ContainerNum
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
func SetAllMasterSlave(replica int) error {
	var err error
	// 计算主节点数量
	n := len(ClusterNodeList) / replica
	expectedNodes := len(ClusterNodeList)

	for i := 0; i < n; i++ {
		// 获取主节点的 IP 和 ID
		masterIP := ClusterNodeList[i].IP
		masterID := IPToClusterIDMapping[masterIP]

		// 从节点的起始和结束位置
		slaveStart := n + (i * (replica - 1))
		slaveEnd := slaveStart + replica - 1

		// 遍历设置从节点
		for j := slaveStart; j < slaveEnd && j < expectedNodes; j++ {
			slaveIP := ClusterNodeList[j].IP
			slaveID := IPToClusterIDMapping[slaveIP]

			// 检查是否已经设置主从节点
			if AlreadySetCluster[masterID] || AlreadySetCluster[slaveID] {
				continue
			}
			// 调用 SetNodeAsSlave 函数设置从节点
			err = SetNodeAsSlave(masterIP, slaveIP)
			if err != nil {
				return err
			}
			// 记录已经设置的主从节点
			AlreadySetCluster[masterID] = true
			AlreadySetCluster[slaveID] = true
		}
	}

	return err
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
	time.Sleep(2 * time.Second)
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

		cliClusterMaster, ctxClusterMaster := CreateRedisClient("127.0.0.1", port)
		for j := startPoint; j <= endPoint; j++ {
			// log.Println("分配槽位", j, "到主节点", masterId)
			cliClusterMaster.ClusterAddSlots(ctxClusterMaster, j)
		}
	}
}

// DataInit 初始化数据
func DataInit() (context.Context, error) {
	// 容器信息获取
	ctxPodman := CreatePodmanConnection()
	println("操作前podmanContains信息：")
	println("--------------------------------------------------")
	err := GetContainerInfo(ctxPodman)
	if err != nil {
		println(err)
	}
	if len(AllContainerInfoList) != 0 {
		err = GetClusterNodesInfo(ctxPodman)
	}
	return ctxPodman, err
}

func ExecuteClusterCommand(ctx context.Context, client *redis.Client, args ...interface{}) (string, error) {
	result, err := client.Do(ctx, args...).Text()
	if err != nil {
		return "", err
	}
	return result, nil
}
