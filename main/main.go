package main

////
//import (
//	"log"
//	"redisStudy/model"
//)
//
//type ContainerInfo = model.ContainerInfo
//
//type ClusterNodeInfo = model.ClusterNodeInfo
//
///*
//全局变量说明：
//1. AllContainerInfoList：所有容器的信息列表，用于存储获取到的容器信息。
//2. IPToContainerInfoMapping：IP到容器信息的映射，用于快速查找指定IP对应的容器信息。
//3. ContainIdToClusterInfoMapping：容器ID到集群信息的映射，用于快速查找指定容器对应的集群信息。
//4. ContainerIdToContainerINfoMapping：容器ID到容器信息的映射，用于快速查找指定容器对应的信息。
//5. AlreadyMeetNode：记录已经存在的节点，用于避免重复添加。
//6. ClusterIDList：记录已经存在的集群ID，用于避免重复创建。
//7. MasterToSlaveMapping：主节点到从节点的映射，用于快速查找指定主节点对应的从节点。
//8. AlreadySetCluster：记录已经设置的集群，用于避免重复设置。
//9. PodmanClient：用于与Podman进行通信的Client对象。
//
//*/
//
//const totalSlots = 16384
//
//var True = true
//
//// 指定本地的配置文件路径
//var redisHostConfigPath = "/Users/zhuoqun.niu/Desktop/redis/config"
//
//// 容器路径
//var redisConfigPath = "/data/redis/config"
//
//// 宿主机路径
//var redisHostDataPath = "/Users/zhuoqun.niu/Desktop/redis/data"
//
//// 容器路径
//var redisConfigDataPath = "/data/redis/data"
//
//// Container 相关数据
//var EmptyMasterNodes = make([]ClusterNodeInfo, 0)
//
//var IPToContainerInfoMapping = make(map[string]ContainerInfo)
//
//var AllContainerInfoList = make([]ContainerInfo, 0)
//
//var ContainerIdToContainerINfoMapping = make(map[string]ContainerInfo)
//
//var ContainerNum = 0
//
//var ClusterIdClusterInfoMapping = make(map[string]ClusterNodeInfo)
//
//var IPToClusterIDMapping = make(map[string]string)
//
//var AlreadyMeetNode = make(map[string]bool)
//
//var ClusterIDList = make([]string, 0)
//
//var MasterToSlaveMapping = make(map[string][]string)
//
//var AlreadySetCluster = make(map[string]bool)
//
//var ClusterNodeList = make([]model.ClusterNodeInfo, 0)
//
//var masterIDs = make([]string, 0)
//
//var masterSet = make(map[string]bool)

func main() {
	// 首先连接podman
	//ctxPodman := CreatePodmanConnection()
	//CreateAction(3, 2)
	//err := UpdateClusterNodesInfo(ctxPodman)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//err = AddAction(ctxPodman, 3, 2)
	//if err != nil {
	//	println(err)
	//}
	//defer DeleteAllContainers(ctxPodman)
}
