//go:build integration
// +build integration

package action

import (
	"context"
	"testing"

	"redisClusterManager/cluster/model"
)

func TestDeleteAllAction(t *testing.T) {
	requireIntegrationTest(t)

	cfg, err := model.InitConfig()
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
