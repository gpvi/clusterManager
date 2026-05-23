//go:build integration
// +build integration

package model

import (
	"context"
	"testing"
)

func TestDeleteAllAction(t *testing.T) {
	requireIntegrationTest(t)

	cfg, err := InitConfig()
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
