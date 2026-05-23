package model

import (
	"fmt"

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
	return CreateRedisClient(nil, n.ClientConnAddr())
}

// CreateRedisClient creates a Redis client connected to the given address.
func CreateRedisClient(_ interface{}, addr string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return client, nil
}
