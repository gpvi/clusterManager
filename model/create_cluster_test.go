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
	err := CreateClusterAction(ctx, 3, 2, clusterName, 6379)
	if err != nil {
		t.Fatal(err)
	}
}
