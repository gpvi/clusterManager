//go:build integration
// +build integration

package action

import (
	"context"
	"testing"

	"redisClusterManager/cluster/model"
)

func TestAddAction(t *testing.T) {
	requireIntegrationTest(t)

	cfg, err := model.InitConfig()
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	clusterName := "myCluster"
	err = ScaleClusterAction(ctx, cfg, 1, clusterName)
	if err != nil {
		t.Errorf("ScaleCluster() error = %v", err)
	}
}
