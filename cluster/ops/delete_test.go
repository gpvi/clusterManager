//go:build integration
// +build integration

package ops

import (
	"context"
	"testing"

	"redisClusterManager/cluster/config"
)

func TestDeleteAllAction(t *testing.T) {
	requireIntegrationTest(t)

	cfg, err := config.InitConfig()
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	clusterName := "myCluster"
	err = DeleteAllContainers(ctx, cfg, clusterName)
	if err != nil {
		t.Fatal(err)
	}
}
