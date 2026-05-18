//go:build integration
// +build integration

package model

import (
	"context"
	"testing"
)

func TestAddAction(t *testing.T) {
	requireIntegrationTest(t)

	ctx := context.Background()
	clusterName := "myCluster"
	RedisContainerPort = 6379
	err := ScaleClusterAction(ctx, 1, clusterName)
	if err != nil {
		t.Errorf("ScaleCluster() error = %v", err)
	}
}
