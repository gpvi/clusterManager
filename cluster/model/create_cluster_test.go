//go:build integration
// +build integration

package model

import (
	"context"
	"testing"
)

func TestCreation(t *testing.T) {
	requireIntegrationTest(t)

	cfg, err := InitConfig()
	if err != nil {
		t.Fatal(err)
	}
	cfg.RedisContainerPort = 6379

	ctx := context.Background()
	clusterName := "myCluster"
	err = CreateClusterAction(ctx, cfg, 3, 2, clusterName)
	if err != nil {
		t.Fatal(err)
	}
}
