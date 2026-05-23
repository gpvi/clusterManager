package data

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"
)

// NodeAddress holds the two addresses a Redis node exposes.
type NodeAddress struct {
	ClusterAddr string // address for CLUSTER MEET (hostname:port or IP:port)
	ClientAddr  string // address for management client (127.0.0.1:HostPort)
}

// RuntimeNode represents a running Redis container/pod.
type RuntimeNode struct {
	HostIP      string      // host-visible IP
	HostPort    uint16      // mapped host port
	ConIp       string      // container IP (or 127.0.0.1 for host networking)
	ConPort     uint16      // container Redis port
	Hostname    string      // DNS-resolvable name (empty if DNS disabled)
	Name        string      // container/pod name
	ID          string      // container/pod ID
	ClusterName string      // owning cluster name
	Address     NodeAddress // dual-address view
}

// ClientConnAddr returns the address for management commands (127.0.0.1:HostPort).
func (n *RuntimeNode) ClientConnAddr() string {
	if n.Address.ClientAddr != "" {
		return n.Address.ClientAddr
	}
	return fmt.Sprintf("%s:%d", n.HostIP, n.HostPort)
}

// ClusterMeetAddr returns the address for CLUSTER MEET (hostname:port or ConIp:ConPort).
func (n *RuntimeNode) ClusterMeetAddr() string {
	if n.Hostname != "" {
		return fmt.Sprintf("%s:%d", n.Hostname, n.ConPort)
	}
	return fmt.Sprintf("%s:%d", n.ConIp, n.ConPort)
}

// CreateRedisClient creates a Redis client for management commands on this node.
func (n *RuntimeNode) CreateRedisClient() (*redis.Client, error) {
	return CreateRedisClient(n.ClientConnAddr())
}

// CreateRedisClient creates a Redis client connected to the given address.
func CreateRedisClient(addr string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return client, nil
}

const (
	Master string = "master"
	Slave  string = "slave"
)

type SlotRange struct {
	Start int
	End   int
}
type ClusterNode struct {
	ID          string      // 节点ID
	IP          string      // IP地址
	Port        uint16      // 端口号
	NodeType    string      // 节点类型 (master 或 slave)
	MasterID    string      // 对于slave节点，表示主节点的ID；对于master节点则为"-"
	LinkState   string      // 节点的连接状态 (connected 或 disconnected)
	Slots       []SlotRange // 负责的插槽范围 (对master节点有效)
	SlotsNum    int         // 节点负责的槽的数量
	ClusterName string      // 所属集群名字
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
