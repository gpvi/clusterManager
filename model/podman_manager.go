package model

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type PodmanNodeManager struct {
	config    *RuntimeConfig
	IPToNode  map[string]*RuntimeNode
	IDToNode  map[string]*RuntimeNode
	Nodes     []*RuntimeNode
	Num       int
}

func NewPodmanNodeManager(cfg *RuntimeConfig) *PodmanNodeManager {
	return &PodmanNodeManager{
		config:   cfg,
		IPToNode: make(map[string]*RuntimeNode),
		IDToNode: make(map[string]*RuntimeNode),
		Nodes:    make([]*RuntimeNode, 0, 10),
		Num:      0,
	}
}

func (c *PodmanNodeManager) AddRuntimeNode(node *RuntimeNode) {
	c.IPToNode[node.ConIp] = node
	c.IDToNode[node.ID] = node
	c.Nodes = append(c.Nodes, node)
	c.Num = len(c.Nodes)
	if node.Address.ClientAddr == "" {
		node.Address = NodeAddress{
			ClusterAddr: fmt.Sprintf("%s:%d", node.ConIp, node.ConPort),
			ClientAddr:  fmt.Sprintf("%s:%d", node.HostIP, node.HostPort),
		}
	}
}

func (c *PodmanNodeManager) HasCluster(clusterName string) bool {
	for _, node := range c.Nodes {
		if node.ClusterName == clusterName {
			return true
		}
	}
	return false
}

func (c *PodmanNodeManager) GetNodes() []*RuntimeNode   { return c.Nodes }
func (c *PodmanNodeManager) GetNodeByIP(ip string) *RuntimeNode { return c.IPToNode[ip] }
func (c *PodmanNodeManager) GetNodeCount() int           { return c.Num }

func (c *PodmanNodeManager) CountByCluster(clusterName string) int {
	count := 0
	for _, node := range c.Nodes {
		if node.ClusterName == clusterName {
			count++
		}
	}
	return count
}

func (c *PodmanNodeManager) CreatePods(ctx context.Context, nodeNum int, clusterName string) error {
	start := c.Num + 1
	end := c.Num + nodeNum

	port := int(c.config.RedisContainerPort)
	busPort := port + 10000
	configHostPath := c.config.RedisHostConfigPath
	configMountPath := c.config.RedisConfigPath
	dataHostPath := filepath.Join(c.config.RedisHostDataPath, clusterName)
	dataMountPath := c.config.RedisConfigDataPath

	for i := start; i <= end; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		containerName := fmt.Sprintf("%s-redis-%d", clusterName, i)

		if err := os.MkdirAll(dataHostPath, 0755); err != nil {
			return fmt.Errorf("failed to create data dir %s: %w", dataHostPath, err)
		}

		args := []string{
			"run", "-d",
			"--name", containerName,
			"-p", strconv.Itoa(port),
			"-p", strconv.Itoa(busPort),
			"-v", fmt.Sprintf("%s:%s", configHostPath, configMountPath),
			"-v", fmt.Sprintf("%s:%s", dataHostPath, dataMountPath),
			c.config.ImageName,
			"redis-server", configMountPath + "/redis.conf",
			"--port", strconv.Itoa(port),
			"--cluster-announce-bus-port", strconv.Itoa(busPort),
		}

		cmd := exec.CommandContext(ctx, "podman", args...)
		cmd.Stderr = os.Stderr
		out, err := cmd.Output()
		if err != nil {
			return fmt.Errorf("failed to run container %s: %w", containerName, err)
		}
		containerID := strings.TrimSpace(string(out))
		fmt.Printf("Container created: %s (%s)\n", containerName, containerID[:12])

		// Wait for container to be running and get its IP
		var containerIP string
		var hostPort uint16
		deadline := time.Now().Add(2 * time.Minute)
		for {
			if time.Now().After(deadline) {
				c.cleanupContainers(clusterName, start, i)
				return fmt.Errorf("timed out waiting for container %s to be ready", containerName)
			}

			select {
			case <-ctx.Done():
				c.cleanupContainers(clusterName, start, i)
				return ctx.Err()
			default:
			}

			containerIP, err = c.getContainerIP(containerID)
			if err == nil && containerIP != "" {
				hostPort, err = c.getContainerHostPort(containerID, port)
				if err == nil && hostPort != 0 {
					break
				}
			}
			time.Sleep(2 * time.Second)
		}

		fmt.Printf("Container %s is running with IP %s (host port: %d)\n", containerName, containerIP, hostPort)

		c.AddRuntimeNode(&RuntimeNode{
			Name:        containerName,
			HostIP:      "127.0.0.1",
			HostPort:    hostPort,
			ConIp:       containerIP,
			ID:          containerID[:12],
			ConPort:     c.config.RedisContainerPort,
			ClusterName: clusterName,
		})
	}

	return nil
}

func (c *PodmanNodeManager) getContainerIP(containerID string) (string, error) {
	cmd := exec.Command("podman", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", containerID)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	ip := strings.TrimSpace(string(out))
	if ip == "<nil>" {
		return "", fmt.Errorf("container IP is nil")
	}
	return ip, nil
}

func (c *PodmanNodeManager) getContainerHostPort(containerID string, containerPort int) (uint16, error) {
	args := []string{"port", containerID, strconv.Itoa(containerPort)}
	cmd := exec.Command("podman", args...)
	out, err := cmd.Output()
	if err != nil {
		return 0, err
	}
	// Output format: "0.0.0.0:12345" or "12345/tcp -> 0.0.0.0:12345"
	line := strings.TrimSpace(string(out))
	// Try to extract port number
	idx := strings.LastIndex(line, ":")
	if idx < 0 {
		return 0, fmt.Errorf("unexpected port output: %s", line)
	}
	portStr := strings.TrimSpace(line[idx+1:])
	portVal, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return 0, fmt.Errorf("failed to parse port from %q: %w", line, err)
	}
	return uint16(portVal), nil
}

func (c *PodmanNodeManager) cleanupContainers(clusterName string, fromIdx, toIdx int) {
	bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := fromIdx; i <= toIdx; i++ {
		name := fmt.Sprintf("%s-redis-%d", clusterName, i)
		cmd := exec.CommandContext(bgCtx, "podman", "rm", "-f", name)
		if err := cmd.Run(); err != nil {
			fmt.Printf("cleanup: failed to remove container %s: %v\n", name, err)
		}
	}
}

func (c *PodmanNodeManager) ListPodsByCluster(ctx context.Context, clusterName string) error {
	c.Nodes = c.Nodes[:0]
	c.IPToNode = make(map[string]*RuntimeNode)
	c.IDToNode = make(map[string]*RuntimeNode)
	c.Num = 0

	filter := fmt.Sprintf("name=%s-redis", clusterName)
	cmd := exec.CommandContext(ctx, "podman", "ps", "--filter", filter, "--format", "{{.Names}}")
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	names := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}

		containerIP, err := c.getContainerIP(name)
		if err != nil {
			fmt.Printf("skipping container %s: failed to get IP: %v\n", name, err)
			continue
		}

		hostPort, err := c.getContainerHostPort(name, int(c.config.RedisContainerPort))
		if err != nil {
			fmt.Printf("skipping container %s: failed to get host port: %v\n", name, err)
			continue
		}

		fullID, err := c.getContainerFullID(name)
		if err != nil {
			fmt.Printf("skipping container %s: failed to get ID: %v\n", name, err)
			continue
		}

		node := &RuntimeNode{
			Name:        name,
			HostIP:      "127.0.0.1",
			HostPort:    hostPort,
			ConIp:       containerIP,
			ID:          fullID[:12],
			ConPort:     c.config.RedisContainerPort,
			ClusterName: clusterName,
		}
		c.Nodes = append(c.Nodes, node)
		c.IDToNode[node.ID] = node
		c.IPToNode[node.ConIp] = node
		c.Num++
	}

	return nil
}

func (c *PodmanNodeManager) getContainerFullID(name string) (string, error) {
	cmd := exec.Command("podman", "inspect", "-f", "{{.Id}}", name)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func (c *PodmanNodeManager) DeleteResources(ctx context.Context, clusterName string) error {
	filter := fmt.Sprintf("name=%s-redis", clusterName)
	cmd := exec.CommandContext(ctx, "podman", "ps", "-a", "--filter", filter, "--format", "{{.Names}}")
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	names := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		rmCmd := exec.CommandContext(ctx, "podman", "rm", "-f", name)
		if err := rmCmd.Run(); err != nil {
			fmt.Printf("Error removing container %s: %v\n", name, err)
		} else {
			fmt.Printf("Container removed: %s\n", name)
		}
	}
	return nil
}

func (c *PodmanNodeManager) SaveToJSON(filename string) error {
	return saveNodesToJSON(filename, c.Nodes)
}
