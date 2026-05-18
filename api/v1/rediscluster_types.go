package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	GroupName    = "cache.example.com"
	Version      = "v1"
	Kind         = "RedisCluster"
	ResourceName = "redisclusters"

	PhasePending   = "Pending"
	PhaseCreating  = "Creating"
	PhaseReady     = "Ready"
	PhaseDegraded  = "Degraded"
	PhaseDeleting  = "Deleting"

	SlotBalanced    = "balanced"
	SlotMigrating   = "migrating"
	SlotUnbalanced  = "unbalanced"
)

var GroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

// RedisClusterSpec defines the desired state of a Redis cluster.
type RedisClusterSpec struct {
	// Number of shards (master nodes).
	Shards int `json:"shards"`

	// Number of nodes per shard (1 master + N-1 slaves).
	NodesPerShard int `json:"nodesPerShard"`

	// Redis container port (default 6379).
	RedisPort uint16 `json:"redisPort,omitempty"`

	// Redis container image name.
	Image string `json:"image,omitempty"`

	// Name of a Secret containing the Redis password (key: "password").
	PasswordSecret string `json:"passwordSecret,omitempty"`
}

// NodeStatus reports the observed state of a single cluster node.
type NodeStatus struct {
	ID       string   `json:"id"`
	IP       string   `json:"ip"`
	Port     uint16   `json:"port"`
	Role     string   `json:"role"` // master | slave
	MasterID string   `json:"masterId,omitempty"`
	Slots    []string `json:"slots,omitempty"` // "0-5460" etc.
	Healthy  bool     `json:"healthy"`
}

// RedisClusterStatus defines the observed state.
type RedisClusterStatus struct {
	// Current lifecycle phase.
	Phase string `json:"phase,omitempty"`

	// Observed number of master nodes.
	MasterCount int `json:"masterCount,omitempty"`

	// Observed total number of nodes.
	TotalNodes int `json:"totalNodes,omitempty"`

	// Slot distribution state: balanced | migrating | unbalanced.
	SlotBalance string `json:"slotBalance,omitempty"`

	// Per-node status.
	Nodes []NodeStatus `json:"nodes,omitempty"`

	// Conditions represent the latest available observations.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// RedisCluster is the Schema for the redisclusters API.
type RedisCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RedisClusterSpec   `json:"spec,omitempty"`
	Status RedisClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RedisClusterList contains a list of RedisCluster.
type RedisClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RedisCluster `json:"items"`
}

var (
	SchemeBuilder      = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme        = SchemeBuilder.AddToScheme
	SchemeGroupVersion = GroupVersion
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&RedisCluster{},
		&RedisClusterList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

// DeepCopyInto copies all properties into another RedisClusterSpec.
func (in *RedisClusterSpec) DeepCopyInto(out *RedisClusterSpec) {
	*out = *in
}

// DeepCopy creates a deep copy of RedisClusterSpec.
func (in *RedisClusterSpec) DeepCopy() *RedisClusterSpec {
	if in == nil {
		return nil
	}
	out := new(RedisClusterSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *NodeStatus) DeepCopyInto(out *NodeStatus) {
	*out = *in
	if in.Slots != nil {
		out.Slots = make([]string, len(in.Slots))
		copy(out.Slots, in.Slots)
	}
}

func (in *NodeStatus) DeepCopy() *NodeStatus {
	if in == nil {
		return nil
	}
	out := new(NodeStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *RedisClusterStatus) DeepCopyInto(out *RedisClusterStatus) {
	*out = *in
	if in.Nodes != nil {
		out.Nodes = make([]NodeStatus, len(in.Nodes))
		for i := range in.Nodes {
			in.Nodes[i].DeepCopyInto(&out.Nodes[i])
		}
	}
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		for i := range in.Conditions {
			in.Conditions[i].DeepCopyInto(&out.Conditions[i])
		}
	}
}

func (in *RedisClusterStatus) DeepCopy() *RedisClusterStatus {
	if in == nil {
		return nil
	}
	out := new(RedisClusterStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *RedisCluster) DeepCopyInto(out *RedisCluster) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *RedisCluster) DeepCopy() *RedisCluster {
	if in == nil {
		return nil
	}
	out := new(RedisCluster)
	in.DeepCopyInto(out)
	return out
}

func (in *RedisCluster) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *RedisClusterList) DeepCopyInto(out *RedisClusterList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]RedisCluster, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *RedisClusterList) DeepCopy() *RedisClusterList {
	if in == nil {
		return nil
	}
	out := new(RedisClusterList)
	in.DeepCopyInto(out)
	return out
}

func (in *RedisClusterList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}
