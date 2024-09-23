package model

import (
	"context"
	"testing"
)

func TestCreation(t *testing.T) {
	var err error

	ctx := context.Background()
	ctx, err = CreatePodmanConnection(ctx)
	clusterName := "myCluster"
	RedisContainerPort = 6379
	err = CreateClusterAction(ctx, 3, 2, clusterName)
	if err != nil {
		t.Fatal(err)
	}
}
