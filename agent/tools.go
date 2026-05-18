package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"redisClusterManager/api/v1"
	"redisClusterManager/model"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// --- GVR for RedisCluster CRD ---

var redisClusterGVR = schema.GroupVersionResource{
	Group:    v1.GroupName,
	Version:  v1.Version,
	Resource: v1.ResourceName,
}

// --- Request / Response types ---

type CreateClusterRequest struct {
	Name          string `json:"name"`
	Shards        int    `json:"shards"`
	NodesPerShard int    `json:"nodes_per_shard"`
	RedisPort     uint16 `json:"redis_port,omitempty"`
	Image         string `json:"image,omitempty"`
}

type CreateClusterResponse struct {
	Name    string `json:"name"`
	Message string `json:"message"`
	Success bool   `json:"success"`
}

type GetStatusRequest struct {
	Name string `json:"name"`
}

type GetStatusResponse struct {
	Name        string     `json:"name"`
	Phase       string     `json:"phase"`
	MasterCount int        `json:"master_count"`
	TotalNodes  int        `json:"total_nodes"`
	Nodes       []NodeInfo `json:"nodes"`
}

type NodeInfo struct {
	ID      string   `json:"id"`
	IP      string   `json:"ip"`
	Role    string   `json:"role"`
	Slots   []string `json:"slots,omitempty"`
	Healthy bool     `json:"healthy"`
}

type ScaleClusterRequest struct {
	Name   string `json:"name"`
	Shards int    `json:"shards"`
}

type DiagnoseRequest struct {
	ClusterName string `json:"cluster_name"`
}

type DiagnoseResponse struct {
	ClusterName string              `json:"cluster_name"`
	Nodes       []DiagnoseNodeResult `json:"nodes"`
	Summary     string              `json:"summary"`
	Healthy     bool                `json:"healthy"`
}

type DiagnoseNodeResult struct {
	PodName         string   `json:"pod_name"`
	PodIP           string   `json:"pod_ip"`
	NodeID          string   `json:"node_id,omitempty"`
	Role            string   `json:"role,omitempty"`
	Connected       bool     `json:"connected"`
	ClusterNodes    string   `json:"cluster_nodes,omitempty"`
	InfoReplication string   `json:"info_replication,omitempty"`
	Errors          []string `json:"errors,omitempty"`
}

type EventInfo struct {
	Type           string `json:"type"`
	Reason         string `json:"reason"`
	Message        string `json:"message"`
	Count          int32  `json:"count"`
	FirstTimestamp string `json:"first_timestamp"`
	LastTimestamp  string `json:"last_timestamp"`
}

// --- Tool functions ---

// CreateClusterTool creates a RedisCluster CR via K8s API.
func CreateClusterTool(ctx context.Context, clientset kubernetes.Interface, restConfig *rest.Config, req CreateClusterRequest) (*CreateClusterResponse, error) {
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	spec := map[string]interface{}{
		"shards":        req.Shards,
		"nodesPerShard": req.NodesPerShard,
	}
	if req.RedisPort != 0 {
		spec["redisPort"] = req.RedisPort
	}
	if req.Image != "" {
		spec["image"] = req.Image
	}

	cluster := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": v1.GroupName + "/" + v1.Version,
			"kind":       v1.Kind,
			"metadata": map[string]interface{}{
				"name": req.Name,
			},
			"spec": spec,
		},
	}

	_, err = dynamicClient.Resource(redisClusterGVR).Create(ctx, cluster, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create RedisCluster CR: %w", err)
	}

	return &CreateClusterResponse{
		Name:    req.Name,
		Message: fmt.Sprintf("RedisCluster %s created successfully", req.Name),
		Success: true,
	}, nil
}

// GetStatusTool reads CR Status directly from K8s API and returns structured topology.
func GetStatusTool(ctx context.Context, clientset kubernetes.Interface, restConfig *rest.Config, req GetStatusRequest) (*GetStatusResponse, error) {
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	obj, err := dynamicClient.Resource(redisClusterGVR).Get(ctx, req.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get RedisCluster CR %q: %w", req.Name, err)
	}

	resp := &GetStatusResponse{
		Name:  req.Name,
		Phase: "Unknown",
	}

	statusObj, ok := obj.Object["status"].(map[string]interface{})
	if !ok {
		return resp, nil
	}

	resp.Phase = stringField(statusObj, "phase")
	resp.MasterCount = intField(statusObj, "masterCount")
	resp.TotalNodes = intField(statusObj, "totalNodes")

	if rawNodes, ok := statusObj["nodes"].([]interface{}); ok {
		for _, raw := range rawNodes {
			m, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			ni := NodeInfo{
				ID:      stringField(m, "id"),
				IP:      stringField(m, "ip"),
				Role:    stringField(m, "role"),
				Healthy: boolField(m, "healthy"),
			}
			if rawSlots, ok := m["slots"].([]interface{}); ok {
				for _, s := range rawSlots {
					if sStr, ok := s.(string); ok {
						ni.Slots = append(ni.Slots, sStr)
					}
				}
			}
			resp.Nodes = append(resp.Nodes, ni)
		}
	}

	return resp, nil
}

// ScaleClusterTool patches the RedisCluster CR spec.shards.
func ScaleClusterTool(ctx context.Context, clientset kubernetes.Interface, restConfig *rest.Config, name string, shards int) error {
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"shards": shards,
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal patch: %w", err)
	}

	_, err = dynamicClient.Resource(redisClusterGVR).Patch(ctx, name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch RedisCluster CR %q: %w", name, err)
	}

	return nil
}

// ListClustersTool lists all RedisCluster CRs.
func ListClustersTool(ctx context.Context, clientset kubernetes.Interface, restConfig *rest.Config) ([]string, error) {
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	list, err := dynamicClient.Resource(redisClusterGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list RedisCluster CRs: %w", err)
	}

	names := make([]string, 0, len(list.Items))
	for _, item := range list.Items {
		names = append(names, item.GetName())
	}
	sort.Strings(names)
	return names, nil
}

// DiagnoseTool connects to each Redis pod, runs CLUSTER NODES and INFO replication,
// and returns structured findings.
func DiagnoseTool(ctx context.Context, cfg *model.RuntimeConfig, clusterName string) (*DiagnoseResponse, error) {
	clientset, _, err := model.NewK8sClientset(cfg.KubeConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s clientset: %w", err)
	}

	labelSelector := fmt.Sprintf("cluster-name=%s,managed-by=clusterManager", clusterName)
	podList, err := clientset.CoreV1().Pods(cfg.KubeNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	if len(podList.Items) == 0 {
		return &DiagnoseResponse{
			ClusterName: clusterName,
			Healthy:     false,
			Summary:     "no pods found for cluster",
		}, nil
	}

	var results []DiagnoseNodeResult
	allHealthy := true

	for _, pod := range podList.Items {
		res := DiagnoseNodeResult{
			PodName: pod.Name,
			PodIP:   pod.Status.PodIP,
		}

		if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
			res.Connected = false
			res.Errors = append(res.Errors, fmt.Sprintf("pod not ready (phase: %s)", pod.Status.Phase))
			allHealthy = false
			results = append(results, res)
			continue
		}

		// Find NodePort service for this pod.
		svcList, err := clientset.CoreV1().Services(cfg.KubeNamespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("pod=%s", pod.Name),
		})
		if err != nil || len(svcList.Items) == 0 {
			res.Connected = false
			res.Errors = append(res.Errors, "no NodePort service found")
			allHealthy = false
			results = append(results, res)
			continue
		}

		nodePort := uint16(svcList.Items[0].Spec.Ports[0].NodePort)

		redisClient, err := model.CreateRedisClient(ctx, "127.0.0.1", nodePort)
		if err != nil {
			res.Connected = false
			res.Errors = append(res.Errors, fmt.Sprintf("redis connection failed: %v", err))
			allHealthy = false
			results = append(results, res)
			continue
		}

		res.Connected = true

		// Run CLUSTER NODES.
		clusterNodesOutput, err := redisClient.ClusterNodes(ctx).Result()
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("CLUSTER NODES failed: %v", err))
		} else {
			res.ClusterNodes = clusterNodesOutput
			// Parse the "myself" line to extract node ID and role.
			lines := strings.Split(clusterNodesOutput, "\n")
			for _, line := range lines {
				if strings.Contains(line, "myself") {
					fields := strings.Split(line, " ")
					if len(fields) >= 3 {
						res.NodeID = fields[0]
						if strings.Contains(fields[2], "master") {
							res.Role = "master"
						} else {
							res.Role = "slave"
						}
					}
					break
				}
			}
		}

		// Run INFO replication.
		infoOutput, err := redisClient.Info(ctx, "replication").Result()
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("INFO replication failed: %v", err))
		} else {
			res.InfoReplication = infoOutput
		}

		redisClient.Close()
		results = append(results, res)
	}

	summary := fmt.Sprintf("diagnosed %d node(s), all healthy: %v", len(results), allHealthy)
	return &DiagnoseResponse{
		ClusterName: clusterName,
		Nodes:       results,
		Summary:     summary,
		Healthy:     allHealthy,
	}, nil
}

// GetEventsTool lists K8s Events for the cluster.
func GetEventsTool(ctx context.Context, clientset kubernetes.Interface, namespace, clusterName string) ([]EventInfo, error) {
	events, err := clientset.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list events: %w", err)
	}

	var result []EventInfo
	for _, ev := range events.Items {
		if strings.Contains(ev.InvolvedObject.Name, clusterName) {
			firstTS := ev.FirstTimestamp.Time
			lastTS := ev.LastTimestamp.Time
			if firstTS.IsZero() {
				firstTS = ev.EventTime.Time
			}
			if lastTS.IsZero() {
				lastTS = ev.EventTime.Time
			}

			result = append(result, EventInfo{
				Type:           ev.Type,
				Reason:         ev.Reason,
				Message:        ev.Message,
				Count:          ev.Count,
				FirstTimestamp: firstTS.Format(time.RFC3339),
				LastTimestamp:  lastTS.Format(time.RFC3339),
			})
		}
	}

	return result, nil
}

// --- helpers for extracting values from unstructured JSON ---

func stringField(obj map[string]interface{}, key string) string {
	if v, ok := obj[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func intField(obj map[string]interface{}, key string) int {
	if v, ok := obj[key]; ok {
		switch tv := v.(type) {
		case float64:
			return int(tv)
		case int64:
			return int(tv)
		case int:
			return tv
		}
	}
	return 0
}

func boolField(obj map[string]interface{}, key string) bool {
	if v, ok := obj[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}
