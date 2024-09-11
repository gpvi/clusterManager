package model

import (
	"context"
	"fmt"
	"github.com/containers/podman/v5/pkg/bindings/containers"
	"log"
)

type ContainerInfo struct {
	IPToContainerInfoMapping map[string]ContainerNode
	ContainerNum             int
	IDToContainerNodeMapping map[string]ContainerNode
	AllContainerInfoList     []ContainerNode
}

func NewContainerInfo() *ContainerInfo {
	return &ContainerInfo{
		IPToContainerInfoMapping: make(map[string]ContainerNode, 10), // 预留容量
		IDToContainerNodeMapping: make(map[string]ContainerNode, 10),
		AllContainerInfoList:     make([]ContainerNode, 0, 10),
		ContainerNum:             0,
	}
}

var IPToContainerInfoMapping = make(map[string]ContainerNode)
var ContainIdToClusterInfoMapping = make(map[string]ContainerNode)
var AllContainerInfoList = make([]ContainerNode, 0)

func GetContainersInfo(ctx context.Context) (*ContainerInfo, error) {
	containerList, err := containers.List(ctx, nil)
	if err != nil {
		return &ContainerInfo{}, fmt.Errorf("failed to list containers: %w", err)
	}

	containerNum := len(containerList)
	if containerNum <= 0 {
		log.Println("当前Podman容器数量为0.")
	}

	// 创建容器消息体
	containerInfo := NewContainerInfo()
	for _, container := range containerList {

		if _, exist := containerInfo.IDToContainerNodeMapping[container.ID]; exist {
			continue
		}
		inspect, err := containers.Inspect(ctx, container.ID, nil)
		if err != nil {
			fmt.Printf("failed to inspect container %s: %v\n", container.ID, err)
			continue
		}
		// 循环只有一次
		for _, network := range inspect.NetworkSettings.Networks {
			containerNode := ContainerNode{
				Name:    container.Names[0],
				IP:      "127.0.0.1", // 本地ip
				ConIp:   network.IPAddress,
				Port:    container.Ports[0].HostPort,
				Id:      container.ID,
				ConPort: 6379,
			}
			containerInfo.AllContainerInfoList = append(containerInfo.AllContainerInfoList, containerNode)
			containerInfo.IDToContainerNodeMapping[container.ID] = containerNode
			containerInfo.IPToContainerInfoMapping[network.IPAddress] = containerNode

			// 全局变量赋值
			AllContainerInfoList = append(AllContainerInfoList, containerNode)
			IPToContainerInfoMapping[network.IPAddress] = containerNode
			ContainIdToClusterInfoMapping[container.ID] = containerNode
		}
	}

	return containerInfo, nil
}
