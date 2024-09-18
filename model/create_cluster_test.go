package model

import (
	"context"
	"testing"
)

func TestCreation(t *testing.T) {
	var err error
	ctx := context.Background()
	ctx, err = CreatePodmanConnection(ctx)
	err = CreateClusterAction(ctx, 3, 2)
	if err != nil {
		t.Fatal(err)
	}
}
