package backend

import (
	"fmt"

	"redisClusterManager/cluster/config"
	"redisClusterManager/cluster/model"
)

func NewNodeManager(cfg *config.RuntimeConfig) (model.PodManager, error) {
	switch cfg.Backend {
	case "podman":
		return NewPodmanNodeManager(cfg), nil
	case "containerd":
		return newContainerdOrError(cfg)
	default:
		return nil, fmt.Errorf("unsupported backend: %s", cfg.Backend)
	}
}

// Compile-time interface checks.
var _ model.PodManager = (*PodmanNodeManager)(nil)
