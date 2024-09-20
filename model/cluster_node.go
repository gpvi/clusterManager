package model

import (
	"fmt"
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
	ID              string      // 节点ID
	IP              string      // IP地址
	Port            uint16      // 端口号
	NodeType        string      // 节点类型 (master 或 slave)
	MasterID        string      // 对于slave节点，表示主节点的ID；对于master节点则为"-"
	PingSent        int64       // 上次发送ping的时间戳
	PongRecv        int64       // 上次接收到pong的时间戳
	ConfigEpoch     int64       // 节点的配置纪元 (用于实现故障转移)
	LinkState       string      // 节点的连接状态 (connected 或 disconnected)
	Slots           []SlotRange // 负责的插槽范围 (对master节点有效)
	AdditionalFlags []string    // 其他标志，如 myself
	SlotsNum        int         // 节点负责的槽的数量
	ClusterName     string      // 所属集群名字
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
