//go:build integration
// +build integration

package action

import (
	"context"
	"testing"

	"redisClusterManager/cluster/model"
)

func TestCreation(t *testing.T) {
	requireIntegrationTest(t)

	cfg, err := model.InitConfig()
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
