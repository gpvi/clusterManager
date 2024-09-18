package model

import (
	"context"
	"log"
	"testing"
)

func TestCreation(t *testing.T) {
	var err error
	ctx := context.Background()
	ctx, err = CreatePodmanConnection(ctx)
	err = CreateClusterAction(ctx, 3, 4)
	if err != nil {
		log.Printf("Error: %v", err)
	}
}
