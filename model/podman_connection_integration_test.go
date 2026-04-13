//go:build integration
// +build integration

package model

import (
	"context"
	"testing"
)

func TestCreatePodmanConnectionReal(t *testing.T) {
	requireIntegrationTest(t)

	ctx := context.Background()
	conn, err := CreatePodmanConnection(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if conn == nil {
		t.Fatal("expected non-nil connection context")
	}
}
