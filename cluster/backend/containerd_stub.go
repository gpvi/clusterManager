//go:build !containerd

package backend

import (
	"fmt"

	"redisClusterManager/cluster/config"
	"redisClusterManager/cluster/model"
)

func newContainerdOrError(cfg *config.RuntimeConfig) (model.PodManager, error) {
	return nil, fmt.Errorf("containerd backend not compiled in: rebuild with -tags containerd")
}
