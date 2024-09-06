package DataStruct

import (
	"context"
	"fmt"
	"github.com/containers/podman/v5/pkg/bindings/containers"
)

type ContainerInfo struct {
	Name  string
	IP    string
	Port  uint16
	ConIp string
	Id    string
}

var IPToContainerInfoMapping = make(map[string]ContainerInfo)
var ContainerNum = 0
var ContainIdToClusterInfoMapping = make(map[string]ContainerInfo)
var AllContainerInfoList = make([]ContainerInfo, 0)

func GetContainerInfo(ctx context.Context) ([]ContainerInfo, error) {
	containerList, err := containers.List(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}
	var ipArr []ContainerInfo
	ContainerNum += len(containerList)
	for _, container := range containerList {
		// 计数当前容器数量

		if _, exist := ContainIdToClusterInfoMapping[container.ID]; exist {
			continue
		}

		inspect, err := containers.Inspect(ctx, container.ID, nil)
		if err != nil {
			fmt.Printf("failed to inspect container %s: %v\n", container.ID, err)
			continue
		}

		for _, network := range inspect.NetworkSettings.Networks {
			containerNode := ContainerInfo{
				Name:  container.Names[0],
				IP:    "127.0.0.1", // 这个可能是占位符，如果需要可以更新
				ConIp: network.IPAddress,
				Port:  container.Ports[0].HostPort,
				Id:    container.ID,
			}
			fmt.Printf("Node信息： %v \n", containerNode)
			AllContainerInfoList = append(AllContainerInfoList, containerNode)
			ipArr = append(ipArr, containerNode)
			IPToContainerInfoMapping[network.IPAddress] = containerNode
			ContainIdToClusterInfoMapping[container.ID] = containerNode
		}
	}

	return ipArr, nil
}
