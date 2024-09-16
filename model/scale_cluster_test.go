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
	err = ScaleClusterAction(ctx, 1)
	if err != nil {
		t.Errorf("ScaleCluster() error = %v", err)
	}
}
