package controller

import (
	"context"
	"fmt"
	"log"
	"time"

	v1 "redisClusterManager/api/v1"
	"redisClusterManager/model"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	reconcileInterval = 10 * time.Second
)

var (
	gvr = schema.GroupVersionResource{
		Group:    v1.GroupName,
		Version:  v1.Version,
		Resource: v1.ResourceName,
	}
)

// RedisClusterController implements the reconcile loop for RedisCluster CRs.
type RedisClusterController struct {
	clientset     kubernetes.Interface
	namespace     string
	restConfig    *rest.Config
	dynamicClient dynamic.Interface
	runtimeCfg    *model.RuntimeConfig
}

// NewController creates a new RedisClusterController.
func NewController(clientset kubernetes.Interface, restConfig *rest.Config, namespace string) *RedisClusterController {
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		log.Fatalf("failed to create dynamic client: %v", err)
	}

	// Try to load runtime config (file-based), fall back to defaults for operator use.
	runtimeCfg, err := model.InitConfig()
	if err != nil {
		log.Printf("warning: failed to load runtime config: %v, using defaults", err)
		runtimeCfg = &model.RuntimeConfig{
			KubeNamespace:       namespace,
			ImageName:           "redis:7-alpine",
			RedisContainerPort:  6379,
			RedisHostConfigPath: "setup/redis/config",
		}
	}

	// The controller always uses the configured namespace.
	runtimeCfg.KubeNamespace = namespace

	return &RedisClusterController{
		clientset:     clientset,
		namespace:     namespace,
		restConfig:    restConfig,
		dynamicClient: dynamicClient,
		runtimeCfg:    runtimeCfg,
	}
}

// Run starts the reconcile loop with a 10 s ticker.
func (c *RedisClusterController) Run(ctx context.Context) error {
	log.Printf("starting RedisClusterController reconcile loop (interval: %s)", reconcileInterval)
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	// Reconcile immediately on startup.
	if err := c.reconcileAll(ctx); err != nil {
		log.Printf("initial reconcile error: %v", err)
	}

	for {
		select {
		case <-ticker.C:
			if err := c.reconcileAll(ctx); err != nil {
				log.Printf("reconcile error: %v", err)
			}
		case <-ctx.Done():
			log.Println("shutting down controller")
			return ctx.Err()
		}
	}
}

// reconcileAll lists all RedisCluster CRs and reconciles each one.
func (c *RedisClusterController) reconcileAll(ctx context.Context) error {
	crs, err := c.getCRs(ctx)
	if err != nil {
		return fmt.Errorf("failed to list RedisCluster CRs: %w", err)
	}

	for i := range crs {
		if err := c.reconcile(ctx, &crs[i]); err != nil {
			log.Printf("error reconciling %s/%s: %v", crs[i].Namespace, crs[i].Name, err)
		}
	}
	return nil
}

// getCRs lists all RedisCluster CRs from the API server.
func (c *RedisClusterController) getCRs(ctx context.Context) ([]v1.RedisCluster, error) {
	unstructList, err := c.dynamicClient.Resource(gvr).Namespace(c.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	crs := make([]v1.RedisCluster, 0, len(unstructList.Items))
	for _, u := range unstructList.Items {
		var cr v1.RedisCluster
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &cr); err != nil {
			return nil, fmt.Errorf("failed to convert unstructured to RedisCluster: %w", err)
		}
		crs = append(crs, cr)
	}
	return crs, nil
}

// updateStatus patches the status subresource of the given CR.
func (c *RedisClusterController) updateStatus(ctx context.Context, cr *v1.RedisCluster) error {
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cr)
	if err != nil {
		return fmt.Errorf("failed to convert RedisCluster to unstructured: %w", err)
	}

	u := &unstructured.Unstructured{Object: obj}
	_, err = c.dynamicClient.Resource(gvr).Namespace(c.namespace).UpdateStatus(ctx, u, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update status for %s/%s: %w", cr.Namespace, cr.Name, err)
	}
	return nil
}

// buildRuntimeConfig creates a RuntimeConfig for the given CR, merging
// controller-level defaults with per-CR spec overrides.
func (c *RedisClusterController) buildRuntimeConfig(cr *v1.RedisCluster) *model.RuntimeConfig {
	cfg := &model.RuntimeConfig{}
	*cfg = *c.runtimeCfg // shallow copy of controller-level config

	if cr.Spec.Image != "" {
		cfg.ImageName = cr.Spec.Image
	}
	if cr.Spec.RedisPort != 0 {
		cfg.RedisContainerPort = cr.Spec.RedisPort
	}
	return cfg
}

// buildNodeStatus constructs a slice of v1.NodeStatus from cluster nodes.
func buildNodeStatus(clusterNodes []model.ClusterNode) []v1.NodeStatus {
	var result []v1.NodeStatus
	for _, cn := range clusterNodes {
		ns := v1.NodeStatus{
			ID:       cn.ID,
			IP:       cn.IP,
			Port:     cn.Port,
			Role:     cn.NodeType,
			MasterID: cn.MasterID,
			Healthy:  cn.LinkState == "connected",
		}
		for _, slot := range cn.Slots {
			if slot.Start == slot.End {
				ns.Slots = append(ns.Slots, fmt.Sprintf("%d", slot.Start))
			} else {
				ns.Slots = append(ns.Slots, fmt.Sprintf("%d-%d", slot.Start, slot.End))
			}
		}
		result = append(result, ns)
	}
	return result
}

// assessHealth evaluates the health of cluster nodes and returns the overall
// health status, master count, and the computed node status slice.
func assessHealth(nodes []model.ClusterNode) (healthy bool, masterCount int, statusNodes []v1.NodeStatus) {
	statusNodes = buildNodeStatus(nodes)
	allHealthy := true
	mc := 0
	for _, ns := range statusNodes {
		if ns.Role == model.Master {
			mc++
		}
		if !ns.Healthy {
			allHealthy = false
		}
	}
	return allHealthy, mc, statusNodes
}

// ---------------------------------------------------------------------------
// Reconcile
// ---------------------------------------------------------------------------

// reconcile implements the state machine for a single CR.
//
// State transitions:
//
//	""  (empty)  ──► Creating
//	Creating     ──► Ready
//	Ready        ──► Degraded  (on health-check failure)
//	Degraded     ──► Ready     (on repair success)
func (c *RedisClusterController) reconcile(ctx context.Context, cr *v1.RedisCluster) error {
	switch cr.Status.Phase {
	case "":
		return c.handleInitial(ctx, cr)
	case v1.PhaseCreating:
		return c.handleCreating(ctx, cr)
	case v1.PhaseReady:
		return c.handleReady(ctx, cr)
	case v1.PhaseDegraded:
		return c.handleDegraded(ctx, cr)
	default:
		log.Printf("unknown phase %q for %s/%s", cr.Status.Phase, cr.Namespace, cr.Name)
		return nil
	}
}

// handleInitial transitions from "" to Creating by launching the required
// Kubernetes pods and services.
func (c *RedisClusterController) handleInitial(ctx context.Context, cr *v1.RedisCluster) error {
	log.Printf("reconciling %s/%s: initial state -> Creating", cr.Namespace, cr.Name)

	runtimeCfg := c.buildRuntimeConfig(cr)
	nodeManager := model.NewK8sNodeManager(c.clientset, c.namespace, runtimeCfg)

	totalNodes := cr.Spec.Shards * cr.Spec.NodesPerShard
	if err := nodeManager.CreatePods(ctx, totalNodes, cr.Name); err != nil {
		return fmt.Errorf("failed to create pods for cluster %s: %w", cr.Name, err)
	}

	now := metav1.Now()
	cr.Status.Phase = v1.PhaseCreating
	cr.Status.Conditions = append(cr.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             "False",
		Reason:             "CreatingPods",
		Message:            fmt.Sprintf("Creating %d pods for cluster %s", totalNodes, cr.Name),
		LastTransitionTime: now,
	})
	return c.updateStatus(ctx, cr)
}

// handleCreating waits for pods to become ready, then forms the Redis cluster
// (MEET, set roles, allocate slots).
func (c *RedisClusterController) handleCreating(ctx context.Context, cr *v1.RedisCluster) error {
	log.Printf("reconciling %s/%s: Creating -> Ready", cr.Namespace, cr.Name)

	runtimeCfg := c.buildRuntimeConfig(cr)
	nodeManager := model.NewK8sNodeManager(c.clientset, c.namespace, runtimeCfg)

	// 1. List existing pods for this cluster.
	if err := nodeManager.ListPodsByCluster(ctx, cr.Name); err != nil {
		return fmt.Errorf("failed to list pods for cluster %s: %w", cr.Name, err)
	}

	expectedNodes := cr.Spec.Shards * cr.Spec.NodesPerShard
	if nodeManager.CountByCluster(cr.Name) < expectedNodes {
		log.Printf("cluster %s: waiting for pods (%d/%d ready)", cr.Name, nodeManager.CountByCluster(cr.Name), expectedNodes)
		return nil
	}

	// 2. Find the first ready node (login node) to run Redis commands against.
	var loginNode *model.RuntimeNode
	for _, node := range nodeManager.Nodes {
		if node.ClusterName == cr.Name {
			loginNode = node
			break
		}
	}
	if loginNode == nil {
		return fmt.Errorf("no login node found for cluster %s (pods exist but no service)", cr.Name)
	}

	// 3. Create a Redis client to the login node.
	redisClient, err := loginNode.CreateRedisClient()
	if err != nil {
		return fmt.Errorf("failed to create Redis client for cluster %s: %w", cr.Name, err)
	}
	defer redisClient.Close()

	// 4. Build a ClusterManager and form the cluster.
	clusterManager := model.NewClusterManager(cr.Spec.NodesPerShard, nodeManager)

	// Meet nodes (idempotent: CLUSTER MEET on an existing member is a no-op).
	if err := clusterManager.MeetNodes(redisClient, ctx, cr.Name); err != nil {
		return fmt.Errorf("meet nodes failed for cluster %s: %w", cr.Name, err)
	}
	// Update the internal node-tracking maps after the MEET phase.
	if err := clusterManager.UpdateAfterMeet(ctx, loginNode, cr.Name); err != nil {
		return fmt.Errorf("update after meet failed for cluster %s: %w", cr.Name, err)
	}

	// 5. Set master / slave roles (idempotent).
	if err := clusterManager.SetAllNodeRole(ctx, cr.Name); err != nil {
		cr.Status.Phase = v1.PhaseDegraded
		if err := c.updateStatus(ctx, cr); err != nil {
			return fmt.Errorf("failed to update status for %s: %w", cr.Name, err)
		}
		return fmt.Errorf("set node roles failed for cluster %s: %w", cr.Name, err)
	}

	// 6. Allocate slots — skip if already allocated (idempotency).
	clusterManager.UpdateSlots(ctx, loginNode, cr.Name)
	if len(clusterManager.EmptyMasters) > 0 {
		log.Printf("cluster %s: allocating slots for %d empty masters", cr.Name, len(clusterManager.EmptyMasters))
		if err := clusterManager.AllocateSlots(ctx, cr.Name); err != nil {
			cr.Status.Phase = v1.PhaseDegraded
			if err := c.updateStatus(ctx, cr); err != nil {
				return fmt.Errorf("failed to update status for %s: %w", cr.Name, err)
			}
			return fmt.Errorf("allocate slots failed for cluster %s: %w", cr.Name, err)
		}
	} else {
		log.Printf("cluster %s: slots already allocated, skipping", cr.Name)
	}

	// 7. Mark the cluster as Ready.
	now := metav1.Now()
	cr.Status.Phase = v1.PhaseReady
	cr.Status.MasterCount = cr.Spec.Shards
	cr.Status.TotalNodes = expectedNodes
	cr.Status.SlotBalance = v1.SlotBalanced
	cr.Status.Conditions = append(cr.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             "True",
		Reason:             "ClusterCreated",
		Message:            fmt.Sprintf("Redis cluster %s with %d shards is ready", cr.Name, cr.Spec.Shards),
		LastTransitionTime: now,
	})
	return c.updateStatus(ctx, cr)
}

// handleReady performs a health check and transitions to Degraded if problems
// are detected.
func (c *RedisClusterController) handleReady(ctx context.Context, cr *v1.RedisCluster) error {
	runtimeCfg := c.buildRuntimeConfig(cr)
	nodeManager := model.NewK8sNodeManager(c.clientset, c.namespace, runtimeCfg)

	// 1. List pods to verify expected count.
	if err := nodeManager.ListPodsByCluster(ctx, cr.Name); err != nil {
		return fmt.Errorf("health check: failed to list pods: %w", err)
	}
	expectedNodes := cr.Spec.Shards * cr.Spec.NodesPerShard
	if nodeManager.CountByCluster(cr.Name) < expectedNodes {
		log.Printf("health check: cluster %s has %d/%d pods available -> Degraded",
			cr.Name, nodeManager.CountByCluster(cr.Name), expectedNodes)
		cr.Status.Phase = v1.PhaseDegraded
		if err := c.updateStatus(ctx, cr); err != nil {
			return fmt.Errorf("failed to update status for %s: %w", cr.Name, err)
		}
		return nil
	}

	// 2. Find a login node to inspect the Redis cluster.
	var loginNode *model.RuntimeNode
	for _, node := range nodeManager.Nodes {
		if node.ClusterName == cr.Name {
			loginNode = node
			break
		}
	}
	if loginNode == nil {
		cr.Status.Phase = v1.PhaseDegraded
		if err := c.updateStatus(ctx, cr); err != nil {
			return fmt.Errorf("failed to update status for %s: %w", cr.Name, err)
		}
		return nil
	}

	// 3. Connect to Redis and run CLUSTER NODES.
	redisClient, err := loginNode.CreateRedisClient()
	if err != nil {
		log.Printf("health check: cannot connect to %s/%s: %v", cr.Namespace, cr.Name, err)
		cr.Status.Phase = v1.PhaseDegraded
		if err := c.updateStatus(ctx, cr); err != nil {
			return fmt.Errorf("failed to update status for %s: %w", cr.Name, err)
		}
		return nil
	}
	defer redisClient.Close()

	nodesInfo, err := redisClient.ClusterNodes(ctx).Result()
	if err != nil {
		log.Printf("health check: CLUSTER NODES failed for %s/%s: %v", cr.Namespace, cr.Name, err)
		cr.Status.Phase = v1.PhaseDegraded
		if err := c.updateStatus(ctx, cr); err != nil {
			return fmt.Errorf("failed to update status for %s: %w", cr.Name, err)
		}
		return nil
	}

	clusterManager := model.NewClusterManager(cr.Spec.NodesPerShard, nodeManager)
	nodes, err := clusterManager.ParseRedisClusterNodes(ctx, nodesInfo, cr.Name)
	if err != nil {
		return fmt.Errorf("health check: ParseRedisClusterNodes failed: %w", err)
	}

	// 4. Evaluate health.
	allHealthy, masterCount, statusNodes := assessHealth(nodes)

	cr.Status.MasterCount = masterCount
	cr.Status.TotalNodes = len(nodes)
	cr.Status.Nodes = statusNodes

	if nodeManager.CountByCluster(cr.Name) >= expectedNodes {
		cr.Status.SlotBalance = v1.SlotBalanced
	}

	if !allHealthy || masterCount < cr.Spec.Shards {
		log.Printf("health check: cluster %s/%s unhealthy (%d masters, %d nodes) -> Degraded",
			cr.Namespace, cr.Name, masterCount, len(nodes))
		cr.Status.Phase = v1.PhaseDegraded
		now := metav1.Now()
		cr.Status.Conditions = append(cr.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             "False",
			Reason:             "HealthCheckFailed",
			Message:            fmt.Sprintf("Cluster %s is degraded: %d/%d masters healthy", cr.Name, masterCount, cr.Spec.Shards),
			LastTransitionTime: now,
		})
	} else {
		log.Printf("health check: cluster %s/%s is healthy", cr.Namespace, cr.Name)
	}

	return c.updateStatus(ctx, cr)
}

// handleDegraded attempts to repair a degraded cluster by re-running MEET and
// master / slave role assignment. If the cluster becomes healthy again the
// phase is set back to Ready.
func (c *RedisClusterController) handleDegraded(ctx context.Context, cr *v1.RedisCluster) error {
	log.Printf("reconciling %s/%s: Degraded -> attempting repair", cr.Namespace, cr.Name)

	runtimeCfg := c.buildRuntimeConfig(cr)
	nodeManager := model.NewK8sNodeManager(c.clientset, c.namespace, runtimeCfg)

	if err := nodeManager.ListPodsByCluster(ctx, cr.Name); err != nil {
		return fmt.Errorf("repair: failed to list pods: %w", err)
	}
	expectedNodes := cr.Spec.Shards * cr.Spec.NodesPerShard
	if nodeManager.CountByCluster(cr.Name) < expectedNodes {
		log.Printf("repair: cluster %s has %d/%d pods -> waiting for pods to recover",
			cr.Name, nodeManager.CountByCluster(cr.Name), expectedNodes)
		return nil
	}

	var loginNode *model.RuntimeNode
	for _, node := range nodeManager.Nodes {
		if node.ClusterName == cr.Name {
			loginNode = node
			break
		}
	}
	if loginNode == nil {
		return fmt.Errorf("repair: no login node available for cluster %s", cr.Name)
	}

	redisClient, err := loginNode.CreateRedisClient()
	if err != nil {
		return fmt.Errorf("repair: cannot connect to %s: %w", loginNode.Name, err)
	}
	defer redisClient.Close()

	clusterManager := model.NewClusterManager(cr.Spec.NodesPerShard, nodeManager)

	// Re-MEET — this is idempotent.
	if err := clusterManager.MeetNodes(redisClient, ctx, cr.Name); err != nil {
		log.Printf("repair: re-MEET failed (non-fatal): %v", err)
	}
	if err := clusterManager.UpdateAfterMeet(ctx, loginNode, cr.Name); err != nil {
		log.Printf("repair: UpdateAfterMeet failed (non-fatal): %v", err)
	}

	// Re-apply master / slave roles.
	if err := clusterManager.SetAllNodeRole(ctx, cr.Name); err != nil {
		log.Printf("repair: SetAllNodeRole failed (non-fatal): %v", err)
	}

	// Check health after repair.
	nodesInfo, err := redisClient.ClusterNodes(ctx).Result()
	if err != nil {
		return fmt.Errorf("repair: CLUSTER NODES failed: %w", err)
	}
	nodes, err := clusterManager.ParseRedisClusterNodes(ctx, nodesInfo, cr.Name)
	if err != nil {
		return fmt.Errorf("repair: ParseRedisClusterNodes failed: %w", err)
	}

	allHealthy, masterCount, statusNodes := assessHealth(nodes)

	cr.Status.MasterCount = masterCount
	cr.Status.TotalNodes = len(nodes)
	cr.Status.Nodes = statusNodes

	now := metav1.Now()
	if allHealthy && masterCount >= cr.Spec.Shards {
		log.Printf("repair: cluster %s/%s restored to healthy", cr.Namespace, cr.Name)
		cr.Status.Phase = v1.PhaseReady
		cr.Status.Conditions = append(cr.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             "True",
			Reason:             "RepairSucceeded",
			Message:            fmt.Sprintf("Cluster %s repaired and healthy", cr.Name),
			LastTransitionTime: now,
		})
	} else {
		log.Printf("repair: cluster %s/%s still degraded after repair attempt (%d masters, %d total)",
			cr.Namespace, cr.Name, masterCount, len(nodes))
		cr.Status.Conditions = append(cr.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             "False",
			Reason:             "RepairFailed",
			Message:            fmt.Sprintf("Repair attempted but cluster is still degraded: %d/%d masters healthy", masterCount, cr.Spec.Shards),
			LastTransitionTime: now,
		})
	}

	return c.updateStatus(ctx, cr)
}


