package model

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

type RuntimeNode struct {
	Name        string
	HostIP      string
	HostPort    uint16
	ConIp       string
	ConPort     uint16
	ID          string
	ClusterName string
}

type ContainerInfo struct {
	HostIP      string `json:"host_ip"`
	HostPort    uint16 `json:"host_port"`
	ConIp       string `json:"con_ip"`
	ConPort     uint16 `json:"con_port"`
	ID          string `json:"id"`
	Name        string `json:"name"`
	ClusterName string `json:"cluster_name"`
}

func (node *RuntimeNode) CreateRedisClient() (*redis.Client, error) {
	addr := fmt.Sprintf("%s:%d", node.HostIP, node.HostPort)
	cli := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := cli.Ping(ctx).Result()
	if err != nil {
		cli.Close()
		return nil, fmt.Errorf("failed to create Redis client for node %s: %w", node.ID, err)
	}
	fmt.Printf("Redis client successfully connected to node %s at %s\n", node.ID, addr)
	return cli, nil
}

type K8sNodeManager struct {
	clientset kubernetes.Interface
	namespace string
	config    *RuntimeConfig
	IPToNode  map[string]*RuntimeNode
	Num       int
	IDToNode  map[string]*RuntimeNode
	Nodes     []*RuntimeNode
}

func NewK8sNodeManager(clientset kubernetes.Interface, namespace string, config *RuntimeConfig) *K8sNodeManager {
	return &K8sNodeManager{
		clientset: clientset,
		namespace: namespace,
		config:    config,
		IPToNode:  make(map[string]*RuntimeNode),
		IDToNode:  make(map[string]*RuntimeNode),
		Nodes:     make([]*RuntimeNode, 0, 10),
		Num:       0,
	}
}

func (c *K8sNodeManager) AddRuntimeNode(node *RuntimeNode) {
	c.IPToNode[node.ConIp] = node
	c.IDToNode[node.ID] = node
	c.Nodes = append(c.Nodes, node)
	c.Num = len(c.Nodes)
}

func (c *K8sNodeManager) HasCluster(clusterName string) bool {
	for _, node := range c.Nodes {
		if node.ClusterName == clusterName {
			return true
		}
	}
	return false
}

func (c *K8sNodeManager) GetNodes() []*RuntimeNode  { return c.Nodes }
func (c *K8sNodeManager) GetNodeByIP(ip string) *RuntimeNode { return c.IPToNode[ip] }
func (c *K8sNodeManager) GetNodeCount() int         { return c.Num }

func (c *K8sNodeManager) CountByCluster(clusterName string) int {
	count := 0
	for _, node := range c.Nodes {
		if node.ClusterName == clusterName {
			count++
		}
	}
	return count
}

func (c *K8sNodeManager) CreatePods(ctx context.Context, nodeNum int, clusterName string) error {
	start := c.Num + 1
	end := c.Num + nodeNum
	const podReadyTimeout = 5 * time.Minute

	configMapName := clusterName + "-redis-config"
	if err := c.createConfigMap(ctx, clusterName, configMapName); err != nil {
		return fmt.Errorf("failed to create ConfigMap: %w", err)
	}

	podTemplate := func(i int) *corev1.Pod {
		podName := fmt.Sprintf("%s-redis-%d", clusterName, i)
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: c.namespace,
				Labels: map[string]string{
					"app":          "redis-cluster",
					"cluster-name": clusterName,
					"pod-name":     podName,
					"managed-by":   "clusterManager",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "redis",
						Image: c.config.ImageName,
						Command: []string{
							"redis-server",
							"/data/redis/config/redis.conf",
							"--port", strconv.Itoa(int(c.config.RedisContainerPort)),
							"--cluster-announce-bus-port", strconv.Itoa(int(c.config.RedisContainerPort + 10000)),
						},
						Ports: []corev1.ContainerPort{
							{Name: "redis", ContainerPort: int32(c.config.RedisContainerPort)},
							{Name: "bus", ContainerPort: int32(c.config.RedisContainerPort + 10000)},
						},
						Env: []corev1.EnvVar{
							{
								Name: "POD_IP",
								ValueFrom: &corev1.EnvVarSource{
									FieldRef: &corev1.ObjectFieldSelector{
										FieldPath: "status.podIP",
									},
								},
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "redis-config",
								MountPath: "/data/redis/config",
								ReadOnly:  true,
							},
						},
					},
				},
				Volumes: []corev1.Volume{
					{
						Name: "redis-config",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: configMapName,
								},
							},
						},
					},
				},
				RestartPolicy: corev1.RestartPolicyNever,
			},
		}
	}

	// Phase 1: create pods concurrently
	type createResult struct {
		name string
		err  error
	}
	createCh := make(chan createResult, nodeNum)
	for i := start; i <= end; i++ {
		go func(idx int) {
			pod := podTemplate(idx)
			created, err := c.clientset.CoreV1().Pods(c.namespace).Create(ctx, pod, metav1.CreateOptions{})
			if err != nil {
				createCh <- createResult{err: fmt.Errorf("failed to create pod %s: %w", pod.Name, err)}
				return
			}
			createCh <- createResult{name: created.Name}
		}(i)
	}

	podNames := make([]string, 0, nodeNum)
	for i := 0; i < nodeNum; i++ {
		r := <-createCh
		if r.err != nil {
			return r.err
		}
		podNames = append(podNames, r.name)
		fmt.Printf("Pod created: %s\n", r.name)
	}

	// Phase 2: wait for all pods to be ready concurrently (with timeout)
	type readyResult struct {
		node RuntimeNode
		err  error
	}
	readyCh := make(chan readyResult, len(podNames))
	for _, podName := range podNames {
		go func(name string) {
			var pod *corev1.Pod
			var err error
			deadline := time.Now().Add(podReadyTimeout)
			for {
				if time.Now().After(deadline) {
					readyCh <- readyResult{err: fmt.Errorf("timed out waiting for pod %s to become ready", name)}
					return
				}
				select {
				case <-ctx.Done():
					readyCh <- readyResult{err: ctx.Err()}
					return
				default:
				}

				pod, err = c.clientset.CoreV1().Pods(c.namespace).Get(ctx, name, metav1.GetOptions{})
				if err != nil {
					readyCh <- readyResult{err: fmt.Errorf("failed to get pod %s: %w", name, err)}
					return
				}
				if pod.Status.Phase == corev1.PodRunning && pod.Status.PodIP != "" {
					fmt.Printf("Pod %s is running with IP %s\n", name, pod.Status.PodIP)
					break
				}
				fmt.Printf("Waiting for pod %s (phase=%s)...\n", name, pod.Status.Phase)
				time.Sleep(2 * time.Second)
			}

			nodePort, err := c.createServiceForPod(ctx, pod, clusterName)
			if err != nil {
				readyCh <- readyResult{err: fmt.Errorf("failed to create service for pod %s: %w", name, err)}
				return
			}

			readyCh <- readyResult{node: RuntimeNode{
				Name:        pod.Name,
				HostIP:      "127.0.0.1",
				HostPort:    nodePort,
				ConIp:       pod.Status.PodIP,
				ID:          string(pod.UID),
				ConPort:     c.config.RedisContainerPort,
				ClusterName: clusterName,
			}}
		}(podName)
	}

	for range podNames {
		r := <-readyCh
		if r.err != nil {
			return r.err
		}
		c.AddRuntimeNode(&r.node)
	}

	return nil
}

func (c *K8sNodeManager) createServiceForPod(ctx context.Context, pod *corev1.Pod, clusterName string) (uint16, error) {
	svcName := pod.Name + "-svc"

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: c.namespace,
			Labels: map[string]string{
				"app":          "redis-cluster",
				"cluster-name": clusterName,
				"pod":          pod.Name,
				"managed-by":   "clusterManager",
			},
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeNodePort,
			Selector: map[string]string{
				"pod-name": pod.Name,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "redis",
					Port:       int32(c.config.RedisContainerPort),
					TargetPort: intstr.FromInt(int(c.config.RedisContainerPort)),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "cluster-bus",
					Port:       int32(c.config.RedisContainerPort + 10000),
					TargetPort: intstr.FromInt(int(c.config.RedisContainerPort + 10000)),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	created, err := c.clientset.CoreV1().Services(c.namespace).Create(ctx, svc, metav1.CreateOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to create service: %w", err)
	}

	nodePort := uint16(created.Spec.Ports[0].NodePort)
	fmt.Printf("Service created: %s (NodePort: %d -> pod %s)\n", svcName, nodePort, pod.Name)
	return nodePort, nil
}

func (c *K8sNodeManager) createConfigMap(ctx context.Context, clusterName, configMapName string) error {
	configFilePath := filepath.Join(c.config.RedisHostConfigPath, "redis.conf")
	configData, err := os.ReadFile(configFilePath)
	if err != nil {
		return fmt.Errorf("failed to read redis.conf from %s: %w", configFilePath, err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: c.namespace,
			Labels: map[string]string{
				"app":          "redis-cluster",
				"cluster-name": clusterName,
				"managed-by":   "clusterManager",
			},
		},
		Data: map[string]string{
			"redis.conf": string(configData),
		},
	}

	_, err = c.clientset.CoreV1().ConfigMaps(c.namespace).Create(ctx, cm, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create ConfigMap: %w", err)
	}
	fmt.Printf("ConfigMap created: %s\n", configMapName)
	return nil
}

func (c *K8sNodeManager) ListPodsByCluster(ctx context.Context, clusterName string) error {
	// Reset to avoid double-counting on repeated calls
	c.Nodes = c.Nodes[:0]
	c.IPToNode = make(map[string]*RuntimeNode)
	c.IDToNode = make(map[string]*RuntimeNode)
	c.Num = 0

	labelSelector := fmt.Sprintf("cluster-name=%s,managed-by=clusterManager", clusterName)
	podList, err := c.clientset.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	for _, pod := range podList.Items {
		if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
			continue
		}

		svcList, err := c.clientset.CoreV1().Services(c.namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("pod=%s", pod.Name),
		})
		if err != nil {
			return fmt.Errorf("failed to list services for pod %s: %w", pod.Name, err)
		}

		if len(svcList.Items) == 0 || len(svcList.Items[0].Spec.Ports) == 0 || svcList.Items[0].Spec.Ports[0].NodePort == 0 {
			continue
		}
		hostPort := uint16(svcList.Items[0].Spec.Ports[0].NodePort)

		node := RuntimeNode{
			Name:        pod.Name,
			HostIP:      "127.0.0.1",
			HostPort:    hostPort,
			ConIp:       pod.Status.PodIP,
			ID:          string(pod.UID),
			ConPort:     c.config.RedisContainerPort,
			ClusterName: pod.Labels["cluster-name"],
		}
		c.Nodes = append(c.Nodes, &node)
		c.IDToNode[node.ID] = &node
		c.IPToNode[node.ConIp] = &node
		c.Num++
	}
	return nil
}

func (c *K8sNodeManager) DeleteResources(ctx context.Context, clusterName string) error {
	labelSelector := fmt.Sprintf("cluster-name=%s,managed-by=clusterManager", clusterName)

	svcList, err := c.clientset.CoreV1().Services(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return fmt.Errorf("failed to list services: %w", err)
	}
	for _, svc := range svcList.Items {
		err := c.clientset.CoreV1().Services(c.namespace).Delete(ctx, svc.Name, metav1.DeleteOptions{})
		if err != nil {
			fmt.Printf("Error deleting service %s: %v\n", svc.Name, err)
		} else {
			fmt.Printf("Service deleted: %s\n", svc.Name)
		}
	}

	podList, err := c.clientset.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}
	for _, pod := range podList.Items {
		err := c.clientset.CoreV1().Pods(c.namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
		if err != nil {
			fmt.Printf("Error deleting pod %s: %v\n", pod.Name, err)
		} else {
			fmt.Printf("Pod deleted: %s\n", pod.Name)
		}
	}

	cmList, err := c.clientset.CoreV1().ConfigMaps(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		fmt.Printf("Error listing ConfigMaps: %v\n", err)
	} else {
		for _, cm := range cmList.Items {
			err := c.clientset.CoreV1().ConfigMaps(c.namespace).Delete(ctx, cm.Name, metav1.DeleteOptions{})
			if err != nil {
				fmt.Printf("Error deleting ConfigMap %s: %v\n", cm.Name, err)
			} else {
				fmt.Printf("ConfigMap deleted: %s\n", cm.Name)
			}
		}
	}

	return nil
}
func (c *K8sNodeManager) SaveToJSON(filename string) error {
	return saveNodesToJSON(filename, c.Nodes)
}
func saveNodesToJSON(filename string, nodes []*RuntimeNode) error {
	var containerInfos []ContainerInfo

	for _, node := range nodes {
		containerInfos = append(containerInfos, ContainerInfo{
			HostIP:      node.HostIP,
			HostPort:    node.HostPort,
			ConIp:       node.ConIp,
			ConPort:     node.ConPort,
			ID:          node.ID,
			Name:        node.Name,
			ClusterName: node.ClusterName,
		})
	}

	data, err := json.MarshalIndent(containerInfos, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling JSON: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return fmt.Errorf("error creating state dir: %w", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("error writing to file: %w", err)
	}
	return nil
}

func CreateRedisClient(ctx context.Context, ip string, port uint16) (*redis.Client, error) {
	addr := fmt.Sprintf("%s:%d", ip, port)

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	_, err := client.Ping(ctx).Result()
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("could not connect to Redis at %s: %w", addr, err)
	}
	return client, nil
}
