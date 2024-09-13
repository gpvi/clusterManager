package model

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"strings"
	"time"
)

func PrintClusterNodesInfo(ctx context.Context, containers *ContainersManager, cluster *ClusterManager) error {
	var err error
	err = containers.UpdateAllContainersInfo(ctx)
	if err != nil {
		return err
	}
	ctx, err = cluster.UpdateClusterNodes(ctx, containers, containers.Nodes[0])
	if err != nil {
		return err
	}

	client := CreateRedisClient(ctx, containers.Nodes[0].HostIP, containers.Nodes[0].HostPort)

	nodesInfo, err := client.ClusterNodes(ctx).Result()
	if err != nil {
		println("failed to get cluster nodes info: %v", err)
	}
	println("\ncluster nodes lines:")
	// 解析返回结果，提取所有的 node ID 和对应的 IP+Port
	lines := strings.Split(nodesInfo, "\n")
	for _, line := range lines {
		println(line)
	}
	return nil
}

// SetNodeAsSlave 设置节点为从节点
func SetNodeAsSlave(ctx context.Context, containers *ContainersManager, cluster *ClusterManager, masterIP string, slaveIP string) (context.Context, error) {
	var err error
	// 获取从节点的 IP 地址 (不带端口)
	err = containers.UpdateAllContainersInfo(ctx)
	if err != nil {
		return ctx, fmt.Errorf("failed to get container info: %v", err)
	}
	slaveAddr := slaveIP
	// 创建 Redis 客户端
	ctx, err = cluster.UpdateClusterNodes(ctx, containers, containers.Nodes[0])

	if err != nil {
		return ctx, fmt.Errorf("failed to get cluster nodes info: %v", err)
	}
	cli := CreateRedisClient(ctx, "127.0.0.1", containers.IPToNode[slaveAddr].HostPort)
	println("-----------------------------")
	// 打印从节点信息
	slaveAddrPort := fmt.Sprintf("%v:%d", slaveIP, containers.IPToNode[slaveAddr].HostPort)
	println("slave:", slaveAddrPort)
	println("id:", cluster.IPToClusterID[slaveIP])

	// 获取主节点的 ID
	masterID := cluster.IPToClusterID[masterIP]
	// 执行 ClusterReplicate 命令
	cmdMessage := cli.ClusterReplicate(ctx, masterID)
	if err := cmdMessage.Err(); err != nil {
		fmt.Printf("Error executing ClusterReplicate for slave %s %v\n", slaveAddrPort, err)
		return ctx, fmt.Errorf("failed to set node %s as replica of master %s: %v", slaveAddrPort, masterIP, err)
	}

	// 成功设置为从节点
	fmt.Printf("Node %s set as replica of master %s\n", slaveAddrPort, masterIP)
	_, err = VerifyNodeTypeSet(ctx, containers)
	if err != nil {
		return ctx, fmt.Errorf("sync fail%v", err)
	}
	return ctx, nil
}

// SetAllNodeType 设置主从节点
func SetAllNodeType(ctx context.Context, containers *ContainersManager, cluster *ClusterManager, replica int) error {
	var err error
	ctx, err = cluster.UpdateClusterNodes(ctx, containers, containers.Nodes[0])
	if err != nil {
		return fmt.Errorf("failed to get cluster nodes info: %v", err)
	}
	// 计算主节点数量
	n := len(cluster.ClusterNodeList) / replica
	expectedNodes := len(cluster.ClusterNodeList)

	for i := 0; i < n; i++ {
		// 获取主节点的 IP 和 ID
		masterIP := cluster.ClusterNodeList[i].IP
		masterID := cluster.IPToClusterID[masterIP]

		// 从节点的起始和结束位置
		slaveStart := n + (i * (replica - 1))
		slaveEnd := slaveStart + replica - 1

		// 遍历设置从节点
		for j := slaveStart; j < slaveEnd && j < expectedNodes; j++ {
			slaveIP := cluster.ClusterNodeList[j].IP
			slaveID := cluster.IPToClusterID[slaveIP]

			// 检查是否已经设置主从节点
			if cluster.AlreadySetCluster[masterID] || cluster.AlreadySetCluster[slaveID] {
				continue
			}
			// 调用 SetNodeAsSlave 函数设置从节点
			ctx, err = SetNodeAsSlave(ctx, containers, cluster, masterIP, slaveIP)
			if err != nil {
				return err
			}
			// 记录已经设置的主从节点
			cluster.AlreadySetCluster[masterID] = true
			cluster.AlreadySetCluster[slaveID] = true
		}

	}
	_, err = VerifyNodeTypeSet(ctx, containers)
	if err != nil {
		return fmt.Errorf("同步失败")
	}
	return err
}

func VerifyNodeTypeSet(ctx context.Context, containers *ContainersManager) (bool, error) {
	cluster := NewClusterManager()
	_, err := cluster.UpdateClusterNodes(ctx, containers, containers.Nodes[len(containers.Nodes)-1])
	if err != nil {
		return false, fmt.Errorf("failed to get container info: %v", err)
	}
	for _, node := range containers.Nodes {
		tryTimes := 10
		i := 0
		for i < tryTimes {
			clusterTemp := NewClusterManager()
			_, err = clusterTemp.UpdateClusterNodes(ctx, containers, node)
			_, err = cluster.UpdateClusterNodes(ctx, containers, containers.Nodes[len(containers.Nodes)-1])

			if err != nil {
				println("try sync fail", i)
				i++
				time.Sleep(2 * time.Second)
				continue
			}

			i++
			ok := CompareCluster(cluster, clusterTemp)
			if ok == true {
				break
			}
			fmt.Printf("try %v /10 sync fail\n", i)
			time.Sleep(2 * time.Second)
		}
		if i == tryTimes {
			return false, fmt.Errorf("failed to verify node type set")
		}
	}
	return true, nil
}

func CompareCluster(c1 *ClusterManager, c2 *ClusterManager) bool {
	ok := true
	if len(c1.MasterIDs) == len(c2.MasterIDs) {
		for k, v := range c1.MasterToSlave {
			l1 := len(v)
			l2 := len(c2.MasterToSlave[k])
			if l1 != l2 {
				ok = false
				break
			}
		}
	} else {
		ok = false
	}
	return ok
}

// MeetNodes 添加新节点到集群
//
//	设置为成员函数
func MeetNodes(client *redis.Client, ctx context.Context, info *ContainersManager, cluster *ClusterManager) error {
	var err error
	nodes := info.Nodes
	for _, node := range nodes {
		_, exist := cluster.AlreadyMeetNode[node.HostIP]
		if !exist {
			_, err = client.ClusterMeet(ctx, node.ConIp, "6379").Result()
			if err != nil {
				return fmt.Errorf("could not meet node %v: %v", node.ConIp, err)
			}
		}
	}

	clis := make([]*redis.Client, 0)
	defer func() {
		for _, cli := range clis {
			err := cli.Close()
			if err != nil {
				return
			}
		}
	}()

	for _, node := range info.Nodes {
		cli, err := node.CreateRedisClient(ctx)
		clis = append(clis, cli)
		err = WaitForClusterSync(cli, ctx, len(nodes))
		if err != nil {
			return fmt.Errorf("failed to wait for cluster sync: %v", err)
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
		time.Sleep(2 * time.Second)
		fmt.Printf("ClusterManager not fully synchronized, retrying... (%d/%d)\n", i+1, maxRetries)
	}

	return fmt.Errorf("cluster did not synchronize within the expected time")
}

// AllocateSlots 分配槽位
func AllocateSlots(ctx context.Context, containers *ContainersManager, cluster *ClusterManager) error {
	var err error
	time.Sleep(1 * time.Second)
	ctx, err = cluster.UpdateClusterNodes(ctx, containers, containers.Nodes[0])
	if err != nil {
		panic(err)
	}
	println("start allocate...")
	numMasters := len(cluster.MasterIDs)
	if numMasters == 0 {
		println("no current master node to be allocate slots。")
		return fmt.Errorf("no available master nodes for slot allocation")
	}
	slotsPerMaster := totalSlots / numMasters
	for i := 0; i < numMasters; i++ {
		startPoint := i * slotsPerMaster
		endPoint := startPoint + slotsPerMaster - 1
		// 确保最后一个主节点处理剩余槽位
		if i == numMasters-1 {
			endPoint = totalSlots - 1
		}

		masterId := cluster.MasterIDs[i]
		port := containers.IPToNode[cluster.IDToClusterNode[masterId].IP].HostPort
		cliClusterMaster := CreateRedisClient(ctx, "127.0.0.1", port)

		for j := startPoint; j <= endPoint; j++ {
			cliClusterMaster.ClusterAddSlots(ctx, j)
		}
	}
	time.Sleep(3 * time.Second)
	VerifyAllocateSlots(ctx, containers)
	return nil
}

func VerifyAllocateSlots(ctx context.Context, containers *ContainersManager) bool {

	for _, container := range containers.Nodes {
		cluster := NewClusterManager()
		tryTimes := 10
		for j := 0; j < tryTimes; j++ {
			_, err := cluster.UpdateClusterNodes(ctx, containers, container)
			if err != nil {
				return false
			}
			Flag := false
			for i := 0; i < len(cluster.MasterIDs); i++ {
				if i == len(cluster.MasterIDs)-1 && totalSlots%len(cluster.MasterIDs) != 0 {
					if cluster.IDToClusterNode[cluster.MasterIDs[i]].SlotsNum == totalSlots%len(cluster.MasterIDs) {
						Flag = true
					}
				} else {
					if cluster.IDToClusterNode[cluster.MasterIDs[i]].SlotsNum == totalSlots/len(cluster.MasterIDs) {
						Flag = true
					}
				}
				if Flag == false {
					break
				}
			}
		}

	}
	return false
}

func ExecuteClusterCommand(ctx context.Context, client *redis.Client, args ...interface{}) (string, error) {
	result, err := client.Do(ctx, args...).Text()
	if err != nil {
		return "", err
	}
	return result, nil
}

// parseInt64 简单的字符串转 int64
func parseInt64(value string) int64 {
	var result int64
	_, err := fmt.Sscanf(value, "%d", &result)
	if err != nil {
		fmt.Printf("Error parsing int64: %v\n", err)
	}
	return result
}
