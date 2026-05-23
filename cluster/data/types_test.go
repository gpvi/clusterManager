package data

import (
	"fmt"
	"strings"
	"testing"
)

func TestParseClusterNodeLines_WithFailFlag(t *testing.T) {
	data := `failnode1 10.0.0.1:6379@16379 fail - 0 0 0 disconnected`
	lines := strings.Split(data, "\n")
	parsed, failedIDs := ParseClusterNodeLines(lines, nil)
	if len(failedIDs) != 1 {
		t.Errorf("expected 1 failed ID, got %d", len(failedIDs))
	}
	if len(parsed) != 0 {
		t.Errorf("expected 0 parsed nodes, got %d", len(parsed))
	}
}

func TestParseClusterNodeLines_EmptyInput(t *testing.T) {
	parsed, failedIDs := ParseClusterNodeLines(nil, nil)
	if len(parsed) != 0 || len(failedIDs) != 0 {
		t.Error("expected empty results for nil input")
	}
}

func TestParseClusterNodeLines_BadSlotFormat(t *testing.T) {
	data := `node1 10.0.0.2:6379@16379 master - 0 0 0 connected abc-xyz`
	lines := strings.Split(data, "\n")
	parsed, _ := ParseClusterNodeLines(lines, func(ip string) bool { return true })
	// Bad slot format should log and skip this node
	if len(parsed) != 0 {
		t.Errorf("expected 0 nodes (bad slot), got %d", len(parsed))
	}
}

func TestParseClusterNodeLines_ClusterFilter(t *testing.T) {
	data := `node1 10.0.0.1:6379 master - 0 0 0 connected
node2 10.0.0.2:6379 master - 0 0 0 connected`
	lines := strings.Split(data, "\n")
	// Only accept 10.0.0.1
	filter := func(ip string) bool { return ip == "10.0.0.1" }
	parsed, _ := ParseClusterNodeLines(lines, filter)
	if len(parsed) != 1 {
		t.Errorf("expected 1 filtered node, got %d", len(parsed))
	}
	if parsed[0].IP != "10.0.0.1" {
		t.Errorf("expected 10.0.0.1, got %s", parsed[0].IP)
	}
}

func TestParseClusterNodeLines(t *testing.T) {
	data := `147ff14ec72be84cff55a4e05b3d32e2d727da95 10.88.3.59:6379@16379 master - 0 1725596407022 3 connected 0 2-5460 6000 7890
c205ed8fd181f95b61d11525effdc478864c91d8 10.88.3.61:6379@16379 master - 0 1725596405004 0 connected 5461-10921
42eecdb2638230925c8ce268c2f16f35edb20d4a 10.88.3.60:6379@16379 master - 0 1725596406012 2 connected 10922-14814
7964bf21b2239e4c0df2663995f1333343380598 10.88.3.63:6379@16379 slave 147ff14ec72be84cff55a4e05b3d32e2d727da95 0 1725596405000 3 connected
426acff8f0d8eecf66f8979ba2fbe0038d020713 10.88.3.62:6379@16379 slave c205ed8fd181f95b61d11525effdc478864c91d8 0 1725596404000 0 connected
52af4b5e914aa0e780694dc5831adb6b05bffd43 10.88.3.58:6379@16379 myself,slave 42eecdb2638230925c8ce268c2f16f35edb20d4a 0 1725596406000 2 connected`

	lines := strings.Split(data, "\n")
	parsed, failedIDs := ParseClusterNodeLines(lines, func(ip string) bool { return true })

	if len(failedIDs) != 0 {
		t.Errorf("expected 0 failed IDs, got %d", len(failedIDs))
	}
	if len(parsed) != 6 {
		t.Errorf("expected 6 nodes, got %d", len(parsed))
	}

	masters := 0
	slaves := 0
	for _, node := range parsed {
		if node.NodeType == Master {
			masters++
			if len(node.Slots) == 0 {
				t.Errorf("master %s has no slots", node.ID)
			}
		} else {
			slaves++
		}
	}
	if masters != 3 {
		t.Errorf("expected 3 masters, got %d", masters)
	}
	if slaves != 3 {
		t.Errorf("expected 3 slaves, got %d", slaves)
	}

	fmt.Printf("Parsed %d nodes successfully (%d masters, %d slaves)\n", len(parsed), masters, slaves)
	for _, node := range parsed {
		fmt.Printf("ID: %s, IP: %s, Port: %d, Type: %s, MasterID: %s",
			node.ID, node.IP, node.Port, node.NodeType, node.MasterID)
		if len(node.Slots) > 0 {
			fmt.Printf(", SlotsNum: %d", len(node.Slots))
		}
		fmt.Println()
	}
}
