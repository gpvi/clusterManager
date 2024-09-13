package model

import (
	"context"
	"testing"
)

//	func TestMigrateSlot(t *testing.T) {
//		ctx, _, err := DataInit()
//		formId := masterIDs[0]
//		toId := masterIDs[1]
//		slotID := 1000
//		err = MigrateSlot(ctx, slotID, formId, toId)
//		if err != nil {
//			t.Fatalf("Error migrating slot %d: %v", slotID, err)
//		}
//	}
//
//	func Test_Migrate_slots(t *testing.T) {
//		ctx, _, err := DataInit()
//		if err != nil {
//			t.Fatal(err)
//		}
//
//		if len(masterIDs) < 2 {
//			t.Fatal("Not enough master nodes")
//		}
//
//		formId := masterIDs[0]
//		toId := masterIDs[1]
//
//		// 迁移 formId 的后 100 个 slot
//		count := 0
//		slots := ClusterIdClusterInfoMapping[formId].Slots
//		lSlots := len(slots)
//		index := lSlots - 1
//
//		for count < 100 && index >= 0 {
//			slot := slots[index]
//			start := slot.Start
//			end := slot.End
//
//			for i := end; i >= start && count < 100; i-- {
//				t.Logf("Migrating slot %d from %s to %s", i, formId, toId)
//				err := MigrateSlot(ctx, i, formId, toId)
//				if err != nil {
//					t.Fatalf("Error migrating slot %d: %v", i, err)
//				}
//				count++
//			}
//			index--
//		}
//
//		err = PrintClusterNodesInfo(ctx)
//		if err != nil {
//			t.Fatal(err)
//		}
//	}
//
//	func TestMigratesSlotsToEmptyNode(t *testing.T) {
//		ctx, _, err := DataInit()
//		if err != nil {
//			t.Fatal(err)
//		}
//		err = MigratesSlotsToEmptyNode(ctx)
//		if err != nil {
//			t.Fatal(err)
//		}
//	}
func TestAddAction(t *testing.T) {
	ctx := context.Background()
	ctx, err := CreatePodmanConnection(ctx)
	if err != nil {
		t.Fatal(err)
	}
	containers, err := NewContainersManager(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cluster := NewClusterManager()
	err = ScaleCluster(ctx, containers, cluster, 1, 2)

	if err != nil {
		t.Errorf("ScaleCluster() error = %v", err)
	}
}
