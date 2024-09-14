package model

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"redisStudy/utils"
	"strconv"
	"strings"
)

const (
	Master string = "master"
	Slave  string = "slave"
)

type SlotRange struct {
	Start int
	End   int
}
type ClusterNode struct {
	ID              string        // 节点ID
	IP              string        // IP地址
	Port            uint16        // 端口号
	NodeType        string        // 节点类型 (master 或 slave)
	MasterID        string        // 对于slave节点，表示主节点的ID；对于master节点则为"-"
	PingSent        int64         // 上次发送ping的时间戳
	PongRecv        int64         // 上次接收到pong的时间戳
	ConfigEpoch     int64         // 节点的配置纪元 (用于实现故障转移)
	LinkState       string        // 节点的连接状态 (connected 或 disconnected)
	Slots           []SlotRange   // 负责的插槽范围 (对master节点有效)
	AdditionalFlags []string      // 其他标志，如 myself
	SlotsNum        int           // 节点负责的槽的数量
	RedisClient     *redis.Client // Redis客户端
}

func (n1 *ClusterNode) Equals(n2 *ClusterNode) bool {
	if n1 == nil || n2 == nil {
		return n1 == n2
	}

	if n1.ID != n2.ID ||
		n1.IP != n2.IP ||
		n1.Port != n2.Port ||
		n1.NodeType != n2.NodeType ||
		n1.MasterID != n2.MasterID ||
		n1.PingSent != n2.PingSent ||
		n1.PongRecv != n2.PongRecv ||
		n1.ConfigEpoch != n2.ConfigEpoch ||
		n1.LinkState != n2.LinkState ||
		n1.SlotsNum != n2.SlotsNum {
		return false
	}

	if len(n1.Slots) != len(n2.Slots) {
		return false
	}
	for i := range n1.Slots {
		if n1.Slots[i] != n2.Slots[i] {
			return false
		}
	}

	if len(n1.AdditionalFlags) != len(n2.AdditionalFlags) {
		return false
	}
	for i := range n1.AdditionalFlags {
		if n1.AdditionalFlags[i] != n2.AdditionalFlags[i] {
			return false
		}
	}

	return true
}
func ParseSlots(slotStrs []string) ([]SlotRange, error) {
	var slots []SlotRange
	for _, slotStr := range slotStrs {
		if strings.Contains(slotStr, "-") {
			start, end, err := parseRange(slotStr)
			if err != nil {
				return nil, err
			}
			slots = append(slots, SlotRange{Start: start, End: end})
		} else {
			singleSlot, err := parseSingleSlot(slotStr)
			if err != nil {
				return nil, err
			}
			slots = append(slots, SlotRange{Start: singleSlot, End: singleSlot})
		}
	}
	return slots, nil
}

func parseRange(slotStr string) (int, int, error) {
	parts := strings.Split(slotStr, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range format: %v", parts)
	}
	start, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, err
	}
	end, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, err
	}
	return start, end, nil
}

func parseSingleSlot(slotStr string) (int, error) {
	singleSlot, err := strconv.Atoi(slotStr)
	if err != nil {
		return 0, err
	}
	return singleSlot, nil
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
			PingSent:        utils.ParseInt64(fields[4]),
			PongRecv:        utils.ParseInt64(fields[5]),
			ConfigEpoch:     utils.ParseInt64(fields[6]),
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
func parseNodeType(field string) string {
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
