package model

import (
	"context"
	"fmt"
	"github.com/containers/podman/v5/pkg/bindings/containers"
	types2 "github.com/containers/podman/v5/pkg/domain/entities/types"
	"os"
	"redisStudy/utils"
)

// DeleteContainer 删除容器
func DeleteContainer(ctx context.Context, container types2.ListContainer) error {

	if container.State == "exited" {
		// Stop the container before removing
		err := containers.Stop(ctx, container.ID, nil)
		if err != nil {
			fmt.Println(err)
			return fmt.Errorf("stop container %v fail error:%v", container.ID, err)
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
	return nil
}

// DeleteAllContainers 删除cluster中所有容器
func DeleteAllContainers(ctx context.Context, clusterName string) error {
	// Stop and remove all containers
	// 读取相关配置
	var err error
	err = InitConfig()
	if err != nil {
		return fmt.Errorf("init config fail: %v", err)
	}

	if utils.FileExists(ConfigSaveFileName) {
		fmt.Printf("File %s already exists, deleting...\n", ConfigSaveFileName)
		err := os.Remove(ConfigSaveFileName) // 删除文件
		if err != nil {
			return fmt.Errorf("error deleting file: %v", err)
		}
	}

	containerFile := "containers.json"
	if utils.FileExists(containerFile) {
		fmt.Printf("File %s already exists, deleting...\n", containerFile)
		err := os.Remove(containerFile) // 删除文件
		if err != nil {
			return fmt.Errorf("error deleting file: %v", err)
		}
	}

	containerList, err := containers.List(ctx, nil)
	if err != nil {
		return fmt.Errorf("can not find the containers error : %v", err)
	}
	for _, container := range containerList {
		if container.Labels["clusterName"] == clusterName {
			err = DeleteContainer(ctx, container)
			if err != nil {
				return fmt.Errorf("delete container %v fail error:%v", container.ID, err)
			}
		}
	}
	return nil
}
