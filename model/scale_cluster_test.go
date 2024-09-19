package model

import (
	"context"
	"testing"
)

func TestAddAction(t *testing.T) {
	ctx := context.Background()
	ctx, err := CreatePodmanConnection(ctx)
	if err != nil {
		t.Fatal(err)
	}
	clusterName := "myCluster"
	err = ScaleClusterAction(ctx, 1, clusterName)
	if err != nil {
		t.Errorf("ScaleCluster() error = %v", err)
	}
}
