package model

import "context"

// PodManager defines the interface for managing pods in a Kubernetes cluster.
// K8sNodeManager implements this interface.
type PodManager interface {
	CreatePods(ctx context.Context, nodeNum int, clusterName string) error
	ListPodsByCluster(ctx context.Context, clusterName string) error
	DeleteResources(ctx context.Context, clusterName string) error
	HasCluster(clusterName string) bool
	CountByCluster(clusterName string) int
}
