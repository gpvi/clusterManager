package model

import (
	"fmt"
	"log"
	"redisStudy/utils"
	"strings"
)

// NodeType 表示节点的类型，如 master 或 slave
type NodeType string

type SlotRange struct {
	Start int
	End   int
}

const (
	Master NodeType = "master"
	Slave  NodeType = "slave"
)

// ClusterNode 结构体表示 Redis 集群中的一个节点
/*
ID：节点的唯一标识符。
Address：节点的 IP 地址和端口号。
NodeType：节点的角色（master 或 slave）。
MasterID：对于 slave 节点，它存储的是主节点的 ID；对于 master 节点，该字段为 "-"。
PingSent：上次发送 ping 的时间戳。
PongRecv：上次接收 pong 的时间戳。
ConfigEpoch：节点的配置纪元，用于 Redis 的主从切换机制。
LinkState：节点的连接状态（connected 或 disconnected）。
Slots：master 节点负责的槽范围（0-5460 等）。
AdditionalFlags：存储其他的附加标志，例如 myself。
*/
type ClusterNode struct {
	ID              string // 节点ID
	IP              string
	Port            uint16
	NodeType        NodeType    // 节点类型 (master 或 slave)
	MasterID        string      // 对于slave节点，表示主节点的ID；对于master节点则为"-"
	PingSent        int64       // 上次发送ping的时间戳
	PongRecv        int64       // 上次接收到pong的时间戳
	ConfigEpoch     int64       // 节点的配置纪元 (用于实现故障转移)
	LinkState       string      // 节点的连接状态 (connected 或 disconnected)
	Slots           []SlotRange // 负责的插槽范围 (对master节点有效)
	AdditionalFlags []string    // 其他标志，如 myself
	SlotsNum        int
}

// ParseSlots 解析 Redis 节点的槽位范围
func ParseSlots(slotStrs []string) ([]SlotRange, error) {
	var slots []SlotRange
	for _, slotStr := range slotStrs {
		if strings.Contains(slotStr, "-") {
			// 解析槽范围 (如 "0-5460")
			parts := strings.Split(slotStr, "-")
			var start, end int
			_, err := fmt.Sscanf(parts[0], "%d", &start)
			if err != nil {
				println(err)
			}

			_, err = fmt.Sscanf(parts[1], "%d", &end)
			slots = append(slots, SlotRange{Start: start, End: end})
		} else {
			// 单个槽 (如 "6000")
			var singleSlot int
			_, err := fmt.Sscanf(slotStr, "%d", &singleSlot)
			if err != nil {
				println(err)
			}
			slots = append(slots, SlotRange{Start: singleSlot, End: singleSlot})
		}
	}
	return slots, nil
}

// ParseRedisClusterNodes 解析 Redis cluster nodes 命令的输出
func ParseRedisClusterNodes(data string) ([]ClusterNode, error) {
	lines := strings.Split(data, "\n") // 将数据按行分割
	var nodes []ClusterNode            // 存储解析后的节点信息

	for _, line := range lines {
		if len(line) == 0 {
			continue // 跳过空行
		}

		fields := strings.Split(line, " ")
		if len(fields) < 8 {
			continue // 跳过字段数不够的行
		}

		// 排除状态中包含 fail 的节点
		if strings.Contains(fields[2], "fail") {
			continue // 如果节点包含 fail 状态，则跳过
		}

		ipPort := strings.Split(fields[1], "@")[0]
		ip, port := utils.ParseIPPort(ipPort) // 解析 IP 和端口
		portUint16, err := utils.StringToUint16(port)
		if err != nil {
			log.Printf("Error parsing port: %v", err)
			continue
		}

		node := ClusterNode{
			ID:              fields[0],
			IP:              ip,
			Port:            portUint16,
			NodeType:        parseNodeType(fields[2]),
			MasterID:        fields[3],
			PingSent:        parseInt64(fields[4]),
			PongRecv:        parseInt64(fields[5]),
			ConfigEpoch:     parseInt64(fields[6]),
			LinkState:       fields[7],
			AdditionalFlags: parseAdditionalFlags(fields[2]),
		}

		// 如果是master节点，并且有插槽范围，解析插槽
		if node.NodeType == Master && len(fields) > 8 {
			slots, err := ParseSlots(fields[8:])
			if err != nil {
				return nil, err
			}
			node.Slots = slots
		}

		nodes = append(nodes, node) // 将有效节点添加到结果列表中
	}

	return nodes, nil
}

// parseNodeType 解析节点类型（master 或 slave）
func parseNodeType(field string) NodeType {
	if strings.Contains(field, "master") {
		return Master
	}
	return Slave
}

// parseAdditionalFlags 解析其他附加标志 (如 myself)
func parseAdditionalFlags(field string) []string {
	flags := strings.Split(field, ",")
	var additionalFlags []string
	for _, flag := range flags {
		if flag != "master" && flag != "slave" {
			additionalFlags = append(additionalFlags, flag)
		}
	}
	return additionalFlags
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
