package model

import (
	"context"
	"fmt"
	"github.com/containers/podman/v5/pkg/bindings/containers"
	types2 "github.com/containers/podman/v5/pkg/domain/entities/types"
	"log"
	"os"
	"redisStudy/utils"
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
func DeleteAllContainers(ctx context.Context, clusterName string) {
	// Stop and remove all containers

	if utils.FileExists(ConfigSaveFileName) {
		fmt.Printf("File %s already exists, deleting...\n", ConfigSaveFileName)
		err := os.Remove(ConfigSaveFileName) // 删除文件
		if err != nil {
			log.Fatalf("Error deleting file: %s", err)
		}
	}
	containerList, err := containers.List(ctx, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, container := range containerList {
		if container.Labels["clusterName"] == clusterName {
			DeleteContainer(ctx, container)
		}
	}

}
