package model

import (
	"fmt"
	"strings"
	"testing"
)

func TestParseClusterNodeLines(t *testing.T) {
	data := `147ff14ec72be84cff55a4e05b3d32e2d727da95 10.88.3.59:6379@16379 master - 0 1725596407022 3 connected 0 2-5460 6000 7890
c205ed8fd181f95b61d11525effdc478864c91d8 10.88.3.61:6379@16379 master - 0 1725596405004 0 connected 5461-10921
42eecdb2638230925c8ce268c2f16f35edb20d4a 10.88.3.60:6379@16379 master - 0 1725596406012 2 connected 10922-14814
7964bf21b2239e4c0df2663995f1333343380598 10.88.3.63:6379@16379 slave 147ff14ec72be84cff55a4e05b3d32e2d727da95 0 1725596405000 3 connected
426acff8f0d8eecf66f8979ba2fbe0038d020713 10.88.3.62:6379@16379 slave c205ed8fd181f95b61d11525effdc478864c91d8 0 1725596404000 0 connected
52af4b5e914aa0e780694dc5831adb6b05bffd43 10.88.3.58:6379@16379 myself,slave 42eecdb2638230925c8ce268c2f16f35edb20d4a 0 1725596406000 2 connected`

	lines := strings.Split(data, "\n")
	parsed, failedIDs := parseClusterNodeLines(lines, func(ip string) bool { return true })

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
