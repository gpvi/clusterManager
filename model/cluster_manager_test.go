package model

import (
	"testing"
)

// TestNewClusterManager verifies ClusterManager initialization.
func TestNewClusterManager(t *testing.T) {
	nm := NewK8sNodeManager(nil, "default", &RuntimeConfig{RedisContainerPort: 6379})
	cm := NewClusterManager(2, nm)

	if cm.NodesPerShard != 2 {
		t.Errorf("expected NodesPerShard=2, got %d", cm.NodesPerShard)
	}
	if cm.EmptyMasters == nil {
		t.Error("EmptyMasters should be initialized")
	}
	if cm.IDToClusterNode == nil {
		t.Error("IDToClusterNode should be initialized")
	}
	if cm.IPToClusterID == nil {
		t.Error("IPToClusterID should be initialized")
	}
	if cm.AlreadyMeetNode == nil {
		t.Error("AlreadyMeetNode should be initialized")
	}
	if cm.MasterToSlave == nil {
		t.Error("MasterToSlave should be initialized")
	}
	if cm.AlreadySetCluster == nil {
		t.Error("AlreadySetCluster should be initialized")
	}
	if cm.ClusterNodeList == nil {
		t.Error("ClusterNodeList should be initialized")
	}
	if cm.MasterIDs == nil {
		t.Error("MasterIDs should be initialized")
	}
	if cm.MasterSet == nil {
		t.Error("MasterSet should be initialized")
	}
	if len(cm.MasterIDs) != 0 {
		t.Error("MasterIDs should be empty initially")
	}
}

// TestK8sNodeManager_Init verifies K8sNodeManager initialization.
func TestK8sNodeManager_Init(t *testing.T) {
	cfg := &RuntimeConfig{RedisContainerPort: 6379}
	nm := NewK8sNodeManager(nil, "test-ns", cfg)

	if nm.namespace != "test-ns" {
		t.Errorf("expected namespace test-ns, got %s", nm.namespace)
	}
	if nm.config != cfg {
		t.Error("config should be set")
	}
	if nm.Num != 0 {
		t.Errorf("expected Num=0, got %d", nm.Num)
	}
	if nm.IPToNode == nil {
		t.Error("IPToNode should be initialized")
	}
	if nm.IDToNode == nil {
		t.Error("IDToNode should be initialized")
	}
	if nm.Nodes == nil {
		t.Error("Nodes should be initialized")
	}
	if nm.PodSet == nil {
		t.Error("PodSet should be initialized")
	}
}

// TestK8sNodeManager_AddAndQuery verifies AddRuntimeNode and query methods.
func TestK8sNodeManager_AddAndQuery(t *testing.T) {
	nm := NewK8sNodeManager(nil, "default", &RuntimeConfig{RedisContainerPort: 6379})

	node := &RuntimeNode{
		Name:        "test-redis-1",
		HostIP:      "127.0.0.1",
		HostPort:    30000,
		ConIp:       "10.0.0.1",
		ID:          "pod-uid-1",
		ConPort:     6379,
		ClusterName: "test-cluster",
	}
	nm.AddRuntimeNode(node)

	if nm.Num != 1 {
		t.Errorf("expected Num=1, got %d", nm.Num)
	}
	if len(nm.Nodes) != 1 {
		t.Errorf("expected 1 node, got %d", len(nm.Nodes))
	}

	// Verify maps
	if nm.IPToNode["10.0.0.1"] != node {
		t.Error("IPToNode should contain the node by ConIp")
	}
	if nm.IDToNode["pod-uid-1"] != node {
		t.Error("IDToNode should contain the node by ID")
	}

	// HasCluster
	if !nm.HasCluster("test-cluster") {
		t.Error("HasCluster should return true for test-cluster")
	}
	if nm.HasCluster("other-cluster") {
		t.Error("HasCluster should return false for other-cluster")
	}

	// CountByCluster
	if nm.CountByCluster("test-cluster") != 1 {
		t.Errorf("expected CountByCluster=1, got %d", nm.CountByCluster("test-cluster"))
	}
	if nm.CountByCluster("other-cluster") != 0 {
		t.Errorf("expected CountByCluster=0, got %d", nm.CountByCluster("other-cluster"))
	}
}

// TestK8sNodeManager_MultipleNodes verifies adding multiple nodes.
func TestK8sNodeManager_MultipleNodes(t *testing.T) {
	nm := NewK8sNodeManager(nil, "default", &RuntimeConfig{RedisContainerPort: 6379})

	nm.AddRuntimeNode(&RuntimeNode{ConIp: "10.0.0.1", ID: "uid-1", ClusterName: "a"})
	nm.AddRuntimeNode(&RuntimeNode{ConIp: "10.0.0.2", ID: "uid-2", ClusterName: "a"})
	nm.AddRuntimeNode(&RuntimeNode{ConIp: "10.0.0.3", ID: "uid-3", ClusterName: "b"})

	if nm.Num != 3 {
		t.Errorf("expected Num=3, got %d", nm.Num)
	}

	if nm.CountByCluster("a") != 2 {
		t.Errorf("expected CountByCluster(a)=2, got %d", nm.CountByCluster("a"))
	}
	if nm.CountByCluster("b") != 1 {
		t.Errorf("expected CountByCluster(b)=1, got %d", nm.CountByCluster("b"))
	}
	if nm.CountByCluster("c") != 0 {
		t.Errorf("expected CountByCluster(c)=0, got %d", nm.CountByCluster("c"))
	}

	if !nm.HasCluster("a") {
		t.Error("HasCluster(a) should be true")
	}
	if !nm.HasCluster("b") {
		t.Error("HasCluster(b) should be true")
	}
	if nm.HasCluster("c") {
		t.Error("HasCluster(c) should be false")
	}
}

// TestParseSlots_Valid verifies parsing of various valid slot formats.
func TestParseSlots_Valid(t *testing.T) {
	tests := []struct {
		name    string
		input   []string
		wantLen int
		want0   SlotRange
		want1   SlotRange
		want2   SlotRange
	}{
		{
			name:    "range+single+single",
			input:   []string{"0-5460", "6000-6000", "7890"},
			wantLen: 3,
			want0:   SlotRange{Start: 0, End: 5460},
			want1:   SlotRange{Start: 6000, End: 6000},
			want2:   SlotRange{Start: 7890, End: 7890},
		},
		{
			name:    "single slot only",
			input:   []string{"10000"},
			wantLen: 1,
			want0:   SlotRange{Start: 10000, End: 10000},
		},
		{
			name:    "full range",
			input:   []string{"0-16383"},
			wantLen: 1,
			want0:   SlotRange{Start: 0, End: 16383},
		},
		{
			name:    "multiple ranges",
			input:   []string{"0-5000", "5001-10000", "10001-16383"},
			wantLen: 3,
			want0:   SlotRange{Start: 0, End: 5000},
			want1:   SlotRange{Start: 5001, End: 10000},
			want2:   SlotRange{Start: 10001, End: 16383},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slots, err := ParseSlots(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(slots) != tt.wantLen {
				t.Fatalf("expected %d slot ranges, got %d", tt.wantLen, len(slots))
			}
			if tt.wantLen >= 1 {
				if slots[0].Start != tt.want0.Start || slots[0].End != tt.want0.End {
					t.Errorf("slot 0: expected %d-%d, got %d-%d", tt.want0.Start, tt.want0.End, slots[0].Start, slots[0].End)
				}
			}
			if tt.wantLen >= 2 {
				if slots[1].Start != tt.want1.Start || slots[1].End != tt.want1.End {
					t.Errorf("slot 1: expected %d-%d, got %d-%d", tt.want1.Start, tt.want1.End, slots[1].Start, slots[1].End)
				}
			}
			if tt.wantLen >= 3 {
				if slots[2].Start != tt.want2.Start || slots[2].End != tt.want2.End {
					t.Errorf("slot 2: expected %d-%d, got %d-%d", tt.want2.Start, tt.want2.End, slots[2].Start, slots[2].End)
				}
			}
		})
	}
}

// TestParseSlots_Empty verifies parsing an empty input returns empty result.
func TestParseSlots_Empty(t *testing.T) {
	slots, err := ParseSlots(nil)
	if err != nil {
		t.Fatalf("unexpected error for nil input: %v", err)
	}
	if len(slots) != 0 {
		t.Errorf("expected 0 slot ranges, got %d", len(slots))
	}

	slots, err = ParseSlots([]string{})
	if err != nil {
		t.Fatalf("unexpected error for empty input: %v", err)
	}
	if len(slots) != 0 {
		t.Errorf("expected 0 slot ranges, got %d", len(slots))
	}
}

// TestParseSlots_InvalidRange verifies error on invalid range format.
func TestParseSlots_InvalidRange(t *testing.T) {
	_, err := ParseSlots([]string{"abc-xyz"})
	if err == nil {
		t.Error("expected error for invalid slot range 'abc-xyz'")
	}
}

// TestParseSlots_InvalidSingle verifies error on invalid single slot.
func TestParseSlots_InvalidSingle(t *testing.T) {
	_, err := ParseSlots([]string{"abc"})
	if err == nil {
		t.Error("expected error for invalid single slot 'abc'")
	}
}

// TestParseSlots_MalformedRange verifies error on malformed range format.
func TestParseSlots_MalformedRange(t *testing.T) {
	_, err := ParseSlots([]string{"0-1-2"})
	if err == nil {
		t.Error("expected error for malformed range '0-1-2'")
	}
}

// TestCalculateSlots verifies the slot count calculation.
func TestCalculateSlots(t *testing.T) {
	nm := NewK8sNodeManager(nil, "default", &RuntimeConfig{RedisContainerPort: 6379})
	cm := NewClusterManager(2, nm)

	tests := []struct {
		name  string
		slots []SlotRange
		want  int
	}{
		{name: "empty", slots: []SlotRange{}, want: 0},
		{name: "single", slots: []SlotRange{{Start: 0, End: 0}}, want: 1},
		{name: "range", slots: []SlotRange{{Start: 0, End: 5460}}, want: 5461},
		{name: "full range one slot", slots: []SlotRange{{Start: 0, End: 16383}}, want: 16384},
		{name: "multiple ranges", slots: []SlotRange{{Start: 0, End: 5000}, {Start: 5001, End: 10000}}, want: 10001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cm.calculateSlots(tt.slots)
			if got != tt.want {
				t.Errorf("expected %d, got %d", tt.want, got)
			}
		})
	}
}

// TestParseNodeType verifies node type string parsing.
func TestParseNodeType(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{input: "master", want: Master},
		{input: "master -", want: Master},
		{input: "myself,master", want: Master},
		{input: "slave", want: Slave},
		{input: "myself,slave", want: Slave},
		{input: "handshake", want: Slave},
		{input: "noflags", want: Slave},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseNodeType(tt.input)
			if got != tt.want {
				t.Errorf("parseNodeType(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// TestEqualClusterNodeType verifies the equality check for master-to-slave maps.
func TestEqualClusterNodeType(t *testing.T) {
	nm := NewK8sNodeManager(nil, "default", &RuntimeConfig{RedisContainerPort: 6379})
	cm := NewClusterManager(2, nm)

	tests := []struct {
		name string
		a    map[string][]string
		b    map[string][]string
		want bool
	}{
		{
			name: "both empty",
			a:    map[string][]string{},
			b:    map[string][]string{},
			want: true,
		},
		{
			name: "same length different keys",
			a:    map[string][]string{"a": {"1"}},
			b:    map[string][]string{"b": {"1"}},
			want: false,
		},
		{
			name: "same keys different lengths",
			a:    map[string][]string{"a": {"1", "2"}},
			b:    map[string][]string{"a": {"1"}},
			want: false,
		},
		{
			name: "identical maps",
			a:    map[string][]string{"a": {"1", "2"}, "b": {"3"}},
			b:    map[string][]string{"a": {"1", "2"}, "b": {"3"}},
			want: true,
		},
		{
			name: "nil vs empty",
			a:    nil,
			b:    map[string][]string{},
			want: false,
		},
		{
			name: "matching regardless of values",
			a:    map[string][]string{"a": {"1", "2", "3"}},
			b:    map[string][]string{"a": {"3", "2", "1"}},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cm.equalClusterNodeType(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("equalClusterNodeType() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestByIPSorting verifies the ByIP sort implementation.
func TestByIPSorting(t *testing.T) {
	nodes := []*ClusterNode{
		{IP: "10.0.0.3"},
		{IP: "10.0.0.1"},
		{IP: "10.0.0.2"},
	}
	cm := &ClusterManager{}
	cm.sortClusterNodesByIP(nodes)

	expected := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}
	for i, n := range nodes {
		if n.IP != expected[i] {
			t.Errorf("position %d: expected %s, got %s", i, expected[i], n.IP)
		}
	}
}
