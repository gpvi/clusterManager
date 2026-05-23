package controller

import (
	"testing"

	"redisClusterManager/model"
)

func TestAssessHealth_AllHealthy(t *testing.T) {
	nodes := []model.ClusterNode{
		{ID: "m1", IP: "10.0.0.1", NodeType: "master", LinkState: "connected", Slots: []model.SlotRange{{Start: 0, End: 5460}}},
		{ID: "s1", IP: "10.0.0.2", NodeType: "slave", MasterID: "m1", LinkState: "connected"},
	}
	healthy, masterCount, statusNodes := assessHealth(nodes)
	if !healthy {
		t.Error("expected healthy cluster")
	}
	if masterCount != 1 {
		t.Errorf("expected 1 master, got %d", masterCount)
	}
	if len(statusNodes) != 2 {
		t.Errorf("expected 2 status nodes, got %d", len(statusNodes))
	}
}

func TestAssessHealth_Degraded(t *testing.T) {
	nodes := []model.ClusterNode{
		{ID: "m1", IP: "10.0.0.1", NodeType: "master", LinkState: "disconnected", Slots: []model.SlotRange{{Start: 0, End: 5460}}},
	}
	healthy, _, _ := assessHealth(nodes)
	if healthy {
		t.Error("expected degraded cluster (disconnected node)")
	}
}

func TestAssessHealth_Empty(t *testing.T) {
	healthy, masterCount, statusNodes := assessHealth(nil)
	if !healthy {
		t.Error("empty cluster should be healthy")
	}
	if masterCount != 0 {
		t.Errorf("expected 0 masters, got %d", masterCount)
	}
	if len(statusNodes) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(statusNodes))
	}
}

func TestBuildNodeStatus(t *testing.T) {
	nodes := []model.ClusterNode{
		{ID: "m1", IP: "10.0.0.1", Port: 6379, NodeType: "master", MasterID: "-", LinkState: "connected",
			Slots: []model.SlotRange{{Start: 0, End: 5460}, {Start: 6000, End: 6000}}},
	}
	result := buildNodeStatus(nodes)
	if len(result) != 1 {
		t.Fatalf("expected 1 node, got %d", len(result))
	}
	if result[0].ID != "m1" {
		t.Errorf("expected m1, got %s", result[0].ID)
	}
	if result[0].Role != "master" {
		t.Errorf("expected master, got %s", result[0].Role)
	}
	if len(result[0].Slots) != 2 {
		t.Errorf("expected 2 slot strings, got %d", len(result[0].Slots))
	}
	if result[0].Slots[0] != "0-5460" {
		t.Errorf("expected '0-5460', got %s", result[0].Slots[0])
	}
	if result[0].Slots[1] != "6000" {
		t.Errorf("expected '6000', got %s", result[0].Slots[1])
	}
}
