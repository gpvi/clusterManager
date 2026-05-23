package model

import "context"

// PodManager defines the interface for managing Redis cluster pods on Kubernetes.
type PodManager interface {
	CreatePods(ctx context.Context, nodeNum int, clusterName string) error
	ListPodsByCluster(ctx context.Context, clusterName string) error
	DeleteResources(ctx context.Context, clusterName string) error
	HasCluster(clusterName string) bool
	CountByCluster(clusterName string) int
	GetNodes() []*RuntimeNode
	GetNodeByIP(ip string) *RuntimeNode
	GetNodeByHost(host string) *RuntimeNode
	GetNodeCount() int
}

// CacheInvalidator is called when Redis slot migration completes,
// so the cache layer can evict stale entries for the affected key range.
type CacheInvalidator interface {
	InvalidateSlots(start, end int)
}

// Compile-time checks that default node managers satisfy PodManager.
var _ PodManager = (*K8sNodeManager)(nil)
var _ PodManager = (*PodmanNodeManager)(nil)
