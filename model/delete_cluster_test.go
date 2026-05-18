//go:build integration
// +build integration

package model

import (
	"context"
	"testing"
)

func TestDeleteAllAction(t *testing.T) {
	requireIntegrationTest(t)

	ctx := context.Background()
	clusterName := "myCluster"
	err := DeleteAllContainers(ctx, clusterName)
	if err != nil {
		t.Fatal(err)
	}
}
