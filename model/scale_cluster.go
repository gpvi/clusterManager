package model

import (
	"context"
)

func ScaleClusterAction(ctx context.Context, masterNum int, replica int) (context.Context, error) {
	var err error

	ctx, clusterManager := NewClusterManager(ctx, replica)
	ctx, err = clusterManager.AddShaders(ctx, masterNum)
	if err != nil {
		return ctx, err
	}

	err = clusterManager.MigratesSlotsToEmptyNode(ctx)
	if err != nil {
		return ctx, err
	}
	return ctx, nil
}
