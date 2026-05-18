//go:build integration
// +build integration

package model

import (
	"context"
	"testing"
)

func TestCreation(t *testing.T) {
	requireIntegrationTest(t)

	ctx := context.Background()
	clusterName := "myCluster"
	RedisContainerPort = 6379
	err := CreateClusterAction(ctx, 3, 2, clusterName)
	if err != nil {
		t.Fatal(err)
	}
}
