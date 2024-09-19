package model

import (
	"context"
	"testing"
)

func TestDeleteAllAction(t *testing.T) {
	ctx := context.Background()
	ctxPodman, err := CreatePodmanConnection(ctx)
	if err != nil {
		t.Fatal(err)
	}
	clusterName := "myCluster"
	DeleteAllContainers(ctxPodman, clusterName)
}
