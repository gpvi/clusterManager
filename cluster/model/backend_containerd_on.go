//go:build containerd

package model

var _ PodManager = (*ContainerdNodeManager)(nil)

func newContainerdOrError(cfg *RuntimeConfig) (PodManager, error) {
	return NewContainerdNodeManager(cfg)
}
