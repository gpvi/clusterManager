package model

type ContainerNode struct {
	Name    string
	HostIP  string
	IP      string
	Port    uint16
	ConIp   string
	ConPort uint16
	Id      string
}

//
//func GetContainersInfo(ctx context.Context) ([]ContainerNode, error) {
//	containerList, err := containers.List(ctx, nil)
//	if err != nil {
//		return nil, fmt.Errorf("failed to list containers: %w", err)
//	}
//	var ipArr []ContainerNode
//	ContainerNum += len(containerList)
//	for _, container := range containerList {
//		// 计数当前容器数量
//
//		if _, exist := ContainIdToClusterInfoMapping[container.ID]; exist {
//			continue
//		}
//
//		inspect, err := containers.Inspect(ctx, container.ID, nil)
//		if err != nil {
//			fmt.Printf("failed to inspect container %s: %v\n", container.ID, err)
//			continue
//		}
//
//		for _, network := range inspect.NetworkSettings.Networks {
//			containerNode := ContainerNode{
//				Name:  container.Names[0],
//				IP:    "127.0.0.1", // 本地ip
//				ConIp: network.IPAddress,
//				Port:  container.Ports[0].HostPort,
//				Id:    container.ID,
//			}
//			fmt.Printf("Node信息： %v \n", containerNode)
//			AllContainerInfoList = append(AllContainerInfoList, containerNode)
//			ipArr = append(ipArr, containerNode)
//			IPToContainerInfoMapping[network.IPAddress] = containerNode
//			ContainIdToClusterInfoMapping[container.ID] = containerNode
//		}
//	}
//
//	return ipArr, nil
//}
