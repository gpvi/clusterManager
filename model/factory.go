package model

import "fmt"

func NewNodeManager(cfg *RuntimeConfig) (PodManager, error) {
	switch cfg.Backend {
	case "k8s":
		clientset, _, err := NewK8sClientset(cfg.KubeConfigPath)
		if err != nil {
			return nil, fmt.Errorf("create k8s clientset fail: %w", err)
		}
		return NewK8sNodeManager(clientset, cfg.KubeNamespace, cfg), nil
	case "podman":
		return NewPodmanNodeManager(cfg), nil
	case "containerd":
		return newContainerdOrError(cfg)
	default:
		return nil, fmt.Errorf("unsupported backend: %s", cfg.Backend)
	}
}
