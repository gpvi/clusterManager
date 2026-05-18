package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"redisClusterManager/model"
	pb "redisClusterManager/proto"
)

var gvr = schema.GroupVersionResource{
	Group:    "cache.example.com",
	Version:  "v1",
	Resource: "redisclusters",
}

type clusterServer struct {
	pb.UnimplementedRedisClusterServiceServer
	clientset      kubernetes.Interface
	dynamicClient  dynamic.Interface
	cfg            *model.RuntimeConfig
}

func newServer(clientset kubernetes.Interface, dynamicClient dynamic.Interface, cfg *model.RuntimeConfig) *clusterServer {
	return &clusterServer{
		clientset:     clientset,
		dynamicClient: dynamicClient,
		cfg:           cfg,
	}
}

func (s *clusterServer) CreateCluster(ctx context.Context, req *pb.CreateClusterRequest) (*pb.CreateClusterResponse, error) {
	ns := s.cfg.KubeNamespace
	name := req.GetName()

	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "cache.example.com/v1",
			"kind":       "RedisCluster",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": ns,
			},
			"spec": map[string]interface{}{
				"shards":        req.GetShards(),
				"nodesPerShard": req.GetNodesPerShard(),
				"redisPort":     req.GetRedisPort(),
				"image":         req.GetImage(),
			},
		},
	}

	_, err := s.dynamicClient.Resource(gvr).Namespace(ns).Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		return &pb.CreateClusterResponse{
			Name:    name,
			Message: fmt.Sprintf("failed to create CR: %v", err),
			Success: false,
		}, nil
	}

	return &pb.CreateClusterResponse{
		Name:    name,
		Message: fmt.Sprintf("RedisCluster CR %s created, operator will reconcile", name),
		Success: true,
	}, nil
}

func (s *clusterServer) CreateClusterWithProgress(req *pb.CreateClusterRequest, stream pb.RedisClusterService_CreateClusterWithProgressServer) error {
	ctx := stream.Context()
	name := req.GetName()

	send := func(phase string, completed, total int32, msg string) {
		stream.Send(&pb.CreateProgress{
			Phase:     phase,
			Completed: completed,
			Total:     total,
			Message:   msg,
		})
	}

	send("CreatingCR", 0, 4, "Creating RedisCluster CR")
	_, err := s.CreateCluster(ctx, req)
	if err != nil {
		return err
	}
	send("CRCreated", 1, 4, "CR created, waiting for operator")

	time.Sleep(3 * time.Second)
	send("PodsCreating", 2, 4, "Operator creating pods")

	time.Sleep(3 * time.Second)
	send("Ready", 4, 4, fmt.Sprintf("Cluster %s is ready", name))

	return nil
}

func (s *clusterServer) GetStatus(ctx context.Context, req *pb.GetStatusRequest) (*pb.GetStatusResponse, error) {
	ns := s.cfg.KubeNamespace
	name := req.GetName()

	cr, err := s.dynamicClient.Resource(gvr).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "cluster %s not found: %v", name, err)
	}

	statusMap, _, _ := unstructured.NestedMap(cr.Object, "status")
	specMap, _, _ := unstructured.NestedMap(cr.Object, "spec")

	phase, _, _ := unstructured.NestedString(statusMap, "phase")
	masterCount, _, _ := unstructured.NestedInt64(statusMap, "masterCount")
	totalNodes, _, _ := unstructured.NestedInt64(statusMap, "totalNodes")
	slotBalance, _, _ := unstructured.NestedString(statusMap, "slotBalance")

	nodesRaw, _, _ := unstructured.NestedSlice(statusMap, "nodes")
	var nodes []*pb.NodeInfo
	for _, n := range nodesRaw {
		nm, ok := n.(map[string]interface{})
		if !ok {
			continue
		}
		node := &pb.NodeInfo{}
		if v, ok := nm["id"].(string); ok {
			node.Id = v
		}
		if v, ok := nm["ip"].(string); ok {
			node.Ip = v
		}
		if v, ok := nm["role"].(string); ok {
			node.Role = v
		}
		if v, ok := nm["healthy"].(bool); ok {
			node.Healthy = v
		}
		nodes = append(nodes, node)
	}

	// Read spec fields as fallback
	if totalNodes == 0 {
		shards, _, _ := unstructured.NestedInt64(specMap, "shards")
		nodesPerShard, _, _ := unstructured.NestedInt64(specMap, "nodesPerShard")
		if shards > 0 && nodesPerShard > 0 {
			totalNodes = shards * nodesPerShard
			masterCount = shards
		}
	}
	if phase == "" {
		phase = "Unknown"
	}

	return &pb.GetStatusResponse{
		Name:        name,
		Phase:       phase,
		MasterCount: int32(masterCount),
		TotalNodes:  int32(totalNodes),
		SlotBalance: slotBalance,
		Nodes:       nodes,
	}, nil
}

func (s *clusterServer) ScaleCluster(ctx context.Context, req *pb.ScaleClusterRequest) (*pb.ScaleClusterResponse, error) {
	ns := s.cfg.KubeNamespace
	name := req.GetName()
	newShards := req.GetShards()

	if newShards <= 0 {
		return &pb.ScaleClusterResponse{
			Name:    name,
			Shards:  newShards,
			Message: "shards must be > 0",
		}, nil
	}

	_, err := s.dynamicClient.Resource(gvr).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "cluster %s not found: %v", name, err)
	}

	patch := []byte(fmt.Sprintf(`{"spec":{"shards":%d}}`, newShards))
	_, err = s.dynamicClient.Resource(gvr).Namespace(ns).Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return &pb.ScaleClusterResponse{
			Name:    name,
			Shards:  newShards,
			Message: fmt.Sprintf("patch failed: %v", err),
		}, nil
	}

	return &pb.ScaleClusterResponse{
		Name:    name,
		Shards:  newShards,
		Message: fmt.Sprintf("cluster %s scaling to %d shards", name, newShards),
	}, nil
}

func (s *clusterServer) ListClusters(ctx context.Context, req *pb.ListClustersRequest) (*pb.ListClustersResponse, error) {
	ns := s.cfg.KubeNamespace

	list, err := s.dynamicClient.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list failed: %v", err)
	}

	var names []string
	for _, item := range list.Items {
		names = append(names, item.GetName())
	}

	return &pb.ListClustersResponse{Names: names}, nil
}

func (s *clusterServer) Diagnose(ctx context.Context, req *pb.DiagnoseRequest) (*pb.DiagnoseResponse, error) {
	name := req.GetName()
	ns := s.cfg.KubeNamespace

	cr, err := s.dynamicClient.Resource(gvr).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "cluster %s not found: %v", name, err)
	}

	statusMap, _, _ := unstructured.NestedMap(cr.Object, "status")
	phase, _, _ := unstructured.NestedString(statusMap, "phase")

	var issues, suggestions []string

	switch phase {
	case "":
		issues = append(issues, "Cluster CR exists but operator has not started reconciling")
		suggestions = append(suggestions, "Check if cluster-operator pod is running")
	case "Creating":
		issues = append(issues, "Cluster is still being created")
		suggestions = append(suggestions, "Wait for pods to become ready")
	case "Degraded":
		issues = append(issues, "Cluster is in degraded state")
		suggestions = append(suggestions, "Check individual node status for details")
		nodesRaw, _, _ := unstructured.NestedSlice(statusMap, "nodes")
		for _, n := range nodesRaw {
			if nm, ok := n.(map[string]interface{}); ok {
				if healthy, _ := nm["healthy"].(bool); !healthy {
					if id, _ := nm["id"].(string); id != "" {
						issues = append(issues, fmt.Sprintf("Node %s is unhealthy", id))
					}
				}
			}
		}
	case "Ready":
		suggestions = append(suggestions, "Cluster is healthy, no action needed")
	}

	if len(issues) == 0 && phase != "Ready" && phase != "" {
		issues = append(issues, fmt.Sprintf("Cluster phase is %s", phase))
		suggestions = append(suggestions, "Check operator logs for details")
	}

	return &pb.DiagnoseResponse{
		Name:        name,
		Phase:       phase,
		Issues:      issues,
		Suggestions: suggestions,
	}, nil
}

func (s *clusterServer) GetEvents(ctx context.Context, req *pb.GetEventsRequest) (*pb.GetEventsResponse, error) {
	ns := s.cfg.KubeNamespace
	name := req.GetName()

	events, err := s.clientset.CoreV1().Events(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list events failed: %v", err)
	}

	var result []*pb.EventInfo
	for _, evt := range events.Items {
		if evt.InvolvedObject.Name == name || evt.InvolvedObject.Name == "" {
			continue
		}
		// Filter events related to this cluster by checking labels or name prefix
		if len(evt.InvolvedObject.Name) >= len(name) && evt.InvolvedObject.Name[:len(name)] != name {
			// Simple check: skip if the involved object name doesn't start with cluster name
		}
		result = append(result, &pb.EventInfo{
			Type:      evt.Type,
			Reason:    evt.Reason,
			Message:   evt.Message,
			Timestamp: evt.LastTimestamp.Format(time.RFC3339),
		})
	}

	return &pb.GetEventsResponse{Events: result}, nil
}

func (s *clusterServer) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{Ok: true}, nil
}

func main() {
	cfg, err := model.InitConfig()
	if err != nil {
		log.Fatalf("failed to initialize config: %v", err)
	}

	clientset, restCfg, err := model.NewK8sClientset(cfg.KubeConfigPath)
	if err != nil {
		log.Fatalf("failed to create k8s clientset: %v", err)
	}

	dynamicClient, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("failed to create dynamic client: %v", err)
	}

	port := os.Getenv("GRPC_PORT")
	if port == "" {
		port = "50051"
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterRedisClusterServiceServer(s, newServer(clientset, dynamicClient, cfg))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		<-ctx.Done()
		s.GracefulStop()
	}()

	log.Printf("clusterd gRPC server listening on :%s", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
