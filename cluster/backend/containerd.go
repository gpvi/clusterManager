//go:build containerd

package backend

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"redisClusterManager/cluster/config"
	"redisClusterManager/cluster/model"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/namespaces"
	"github.com/opencontainers/runtime-spec/specs-go"
)

const containerdNamespace = "cluster-manager"

type ContainerdNodeManager struct {
	client *containerd.Client
	config *config.RuntimeConfig
	base   baseNodeManager
}

func NewContainerdNodeManager(cfg *config.RuntimeConfig) (*ContainerdNodeManager, error) {
	client, err := containerd.New(cfg.ContainerdSocket)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to containerd at %s: %w", cfg.ContainerdSocket, err)
	}
	return &ContainerdNodeManager{
		client: client,
		config: cfg,
		base: baseNodeManager{
			IPToNode:   make(map[string]*model.RuntimeNode),
			HostToNode: make(map[string]*model.RuntimeNode),
			IDToNode:   make(map[string]*model.RuntimeNode),
		},
	}, nil
}

func (c *ContainerdNodeManager) AddRuntimeNode(node *model.RuntimeNode) {
	c.base.addNode(node)
	if node.Address.ClientAddr == "" {
		node.Address = model.NodeAddress{
			ClusterAddr: fmt.Sprintf("%s:%d", node.ConIp, node.ConPort),
			ClientAddr:  fmt.Sprintf("%s:%d", node.HostIP, node.HostPort),
		}
	}
}

func (c *ContainerdNodeManager) HasCluster(clusterName string) bool   { return c.base.hasCluster(clusterName) }
func (c *ContainerdNodeManager) GetNodes() []*model.RuntimeNode        { return c.base.getNodes() }
func (c *ContainerdNodeManager) GetNodeByIP(ip string) *model.RuntimeNode { return c.base.getNodeByIP(ip) }
func (c *ContainerdNodeManager) GetNodeByHost(host string) *model.RuntimeNode { return c.base.getNodeByHost(host) }
func (c *ContainerdNodeManager) GetNodeCount() int                     { return c.base.getNodeCount() }

func (c *ContainerdNodeManager) CountByCluster(clusterName string) int { return c.base.countByCluster(clusterName) }

func (c *ContainerdNodeManager) CreatePods(ctx context.Context, nodeNum int, clusterName string) error {
	ctx = namespaces.WithNamespace(ctx, containerdNamespace)
	start := c.base.Num + 1
	end := c.base.Num + nodeNum
	basePort := int(c.config.RedisContainerPort)

	image, err := c.client.Pull(ctx, c.config.ImageName, containerd.WithPullUnpack)
	if err != nil {
		return fmt.Errorf("pull image %s fail: %w", c.config.ImageName, err)
	}

	for i := start; i <= end; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		containerName := fmt.Sprintf("%s-redis-%d", clusterName, i)
		port := basePort + (i - 1)
		busPort := port + 10000
		snapshotID := fmt.Sprintf("%s-snapshot", containerName)

		configHostPath, err := filepath.Abs(c.config.RedisHostConfigPath)
		if err != nil {
			configHostPath = c.config.RedisHostConfigPath
		}

		dataHostPath := filepath.Join(c.config.RedisHostDataPath, clusterName)

		spec := defaultSpec()
		redisArgs := []string{
			"redis-server",
			c.config.RedisConfigPath + "/redis.conf",
			"--port", strconv.Itoa(port),
			"--cluster-announce-bus-port", strconv.Itoa(busPort),
		}
		// Only add --cluster-announce-ip when DNS is enabled.
		if c.config.DNSEnabled() {
			redisArgs = append(redisArgs, "--cluster-announce-ip", containerName)
		}
		spec.Process.Args = redisArgs
		spec.Mounts = []specs.Mount{
			{
				Destination: c.config.RedisConfigPath,
				Type:        "bind",
				Source:      configHostPath,
				Options:     []string{"rbind", "ro"},
			},
			{
				Destination: c.config.RedisConfigDataPath,
				Type:        "bind",
				Source:      dataHostPath,
				Options:     []string{"rbind", "rw"},
			},
		}

		container, err := c.client.NewContainer(
			ctx,
			containerName,
			containerd.WithImage(image),
			containerd.WithNewSnapshot(snapshotID, image),
			containerd.WithSpec(&spec),
			containerd.WithContainerLabels(map[string]string{
				"cluster-name": clusterName,
				"managed-by":   "clusterManager",
				"node-index":   strconv.Itoa(i),
			}),
		)
		if err != nil {
			return fmt.Errorf("create container %s fail: %w", containerName, err)
		}

		task, err := container.NewTask(ctx, cio.NullIO)
		if err != nil {
			container.Delete(ctx)
			return fmt.Errorf("create task for %s fail: %w", containerName, err)
		}

		if err := task.Start(ctx); err != nil {
			task.Delete(ctx)
			container.Delete(ctx)
			return fmt.Errorf("start task for %s fail: %w", containerName, err)
		}

		containerID := container.ID()
		fmt.Printf("Container started: %s (%s)\n", containerName, containerID[:12])

		node := model.RuntimeNode{
			Name:        containerName,
			HostIP:      "127.0.0.1",
			HostPort:    uint16(port),
			ConIp:       "127.0.0.1",
			ID:          containerID[:12],
			ConPort:     uint16(port),
			ClusterName: clusterName,
		}
		// Only set hostname when DNS is enabled.
		if c.config.DNSEnabled() {
			node.Hostname = c.config.BuildHostname(clusterName, i)
		}
		c.AddRuntimeNode(&node)
	}

	return nil
}

// defaultSpec returns a minimal OCI spec with host networking (no network namespace).
func defaultSpec() specs.Spec {
	return specs.Spec{
		Version: "1.0.0",
		Process: &specs.Process{
			Terminal: false,
			User:     specs.User{UID: 0, GID: 0},
			Env:      []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
			Cwd:      "/",
			Capabilities: &specs.LinuxCapabilities{
				Bounding:    defaultCaps(),
				Permitted:   defaultCaps(),
				Inheritable: defaultCaps(),
				Effective:   defaultCaps(),
			},
			Rlimits: []specs.POSIXRlimit{
				{Type: "RLIMIT_NOFILE", Hard: 1024, Soft: 1024},
			},
		},
		Root: &specs.Root{
			Path:     "rootfs",
			Readonly: false,
		},
		Mounts: []specs.Mount{
			{
				Destination: "/proc",
				Type:        "proc",
				Source:      "proc",
			},
			{
				Destination: "/dev",
				Type:        "tmpfs",
				Source:      "tmpfs",
				Options:     []string{"nosuid", "strictatime", "mode=755", "size=65536k"},
			},
			{
				Destination: "/dev/pts",
				Type:        "devpts",
				Source:      "devpts",
				Options:     []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620"},
			},
			{
				Destination: "/sys",
				Type:        "sysfs",
				Source:      "sysfs",
				Options:     []string{"nosuid", "noexec", "nodev", "ro"},
			},
		},
		Linux: &specs.Linux{
			Namespaces: []specs.LinuxNamespace{
				{Type: specs.PIDNamespace},
				{Type: specs.IPCNamespace},
				{Type: specs.UTSNamespace},
				{Type: specs.MountNamespace},
				// No NetworkNamespace = host networking
			},
		},
		// ContainerdWithSpec will merge the image config on top of this.
	}
}

func defaultCaps() []string {
	return []string{
		"CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_FSETID", "CAP_FOWNER",
		"CAP_MKNOD", "CAP_NET_RAW", "CAP_SETGID", "CAP_SETUID",
		"CAP_SETFCAP", "CAP_SETPCAP", "CAP_NET_BIND_SERVICE",
		"CAP_SYS_CHROOT", "CAP_KILL", "CAP_AUDIT_WRITE",
	}
}

func (c *ContainerdNodeManager) ListPodsByCluster(ctx context.Context, clusterName string) error {
	ctx = namespaces.WithNamespace(ctx, containerdNamespace)
	c.base.resetMaps()

	containers, err := c.client.Containers(ctx,
		fmt.Sprintf("labels.\"cluster-name\"==%s", clusterName),
		"labels.\"managed-by\"==clusterManager",
	)
	if err != nil {
		return fmt.Errorf("list containers fail: %w", err)
	}

	for _, container := range containers {
		info, err := container.Info(ctx, containerd.WithoutRefreshedMetadata)
		if err != nil {
			fmt.Printf("skipping container %s: failed to get info: %v\n", container.ID(), err)
			continue
		}

		nodeIndexStr := info.Labels["node-index"]
		nodeIndex, _ := strconv.Atoi(nodeIndexStr)
		port := int(c.config.RedisContainerPort) + (nodeIndex - 1)

		node := &model.RuntimeNode{
			Name:        container.ID(),
			HostIP:      "127.0.0.1",
			HostPort:    uint16(port),
			ConIp:       "127.0.0.1",
			ID:          container.ID()[:12],
			ConPort:     uint16(port),
			ClusterName: info.Labels["cluster-name"],
		}
		if c.config.DNSEnabled() {
			node.Hostname = c.config.BuildHostname(clusterName, nodeIndex)
		}
		c.base.Nodes = append(c.base.Nodes, node)
		c.base.IDToNode[node.ID] = node
		c.base.IPToNode[node.ConIp] = node
		c.base.Num++
	}

	return nil
}

func (c *ContainerdNodeManager) DeleteResources(ctx context.Context, clusterName string) error {
	ctx = namespaces.WithNamespace(ctx, containerdNamespace)

	containers, err := c.client.Containers(ctx,
		fmt.Sprintf("labels.\"cluster-name\"==%s", clusterName),
		"labels.\"managed-by\"==clusterManager",
	)
	if err != nil {
		return fmt.Errorf("list containers fail: %w", err)
	}

	for _, container := range containers {
		containerName := container.ID()
		if task, taskErr := container.Task(ctx, nil); taskErr == nil {
			task.Delete(ctx)
		}
		if err := container.Delete(ctx); err != nil {
			fmt.Printf("Error removing container %s: %v\n", containerName, err)
		} else {
			fmt.Printf("Container removed: %s\n", containerName)
		}
	}
	return nil
}

var _ model.PodManager = (*ContainerdNodeManager)(nil)

func newContainerdOrError(cfg *config.RuntimeConfig) (model.PodManager, error) {
	return NewContainerdNodeManager(cfg)
}
