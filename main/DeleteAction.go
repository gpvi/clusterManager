package main

import (
	"context"
	"fmt"
	"github.com/containers/podman/v5/pkg/bindings/containers"
	types2 "github.com/containers/podman/v5/pkg/domain/entities/types"
)

// DeleteContainer 删除容器
func DeleteContainer(ctx context.Context, container types2.ListContainer) {
	if container.State == "exited" {
		// Stop the container before removing
		err := containers.Stop(ctx, container.ID, nil)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("Container stopped:", container.ID)
	}

	report, err := containers.Remove(ctx, container.ID, &containers.RemoveOptions{
		Force: &True,
	})
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Container removed:", report)
	}
}

// DeleteAllContainers 删除所有容器
func DeleteAllContainers(ctx context.Context) {
	// Stop and remove all containers
	containerList, err := containers.List(ctx, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, container := range containerList {
		DeleteContainer(ctx, container)
	}

}
