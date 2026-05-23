package model

import "fmt"

func NewNodeManager(cfg *RuntimeConfig) (PodManager, error) {
	switch cfg.Backend {
	case "podman":
		return NewPodmanNodeManager(cfg), nil
	case "containerd":
		return newContainerdOrError(cfg)
	default:
		return nil, fmt.Errorf("unsupported backend: %s", cfg.Backend)
	}
}
