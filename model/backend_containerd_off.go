//go:build !containerd

package model

import "fmt"

func newContainerdOrError(cfg *RuntimeConfig) (PodManager, error) {
	return nil, fmt.Errorf("containerd backend not compiled in: rebuild with -tags containerd")
}
