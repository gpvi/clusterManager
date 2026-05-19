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
	GetNodeCount() int
	SaveToJSON(filename string) error
}

// Compile-time checks that default node managers satisfy PodManager.
var _ PodManager = (*K8sNodeManager)(nil)
var _ PodManager = (*PodmanNodeManager)(nil)
