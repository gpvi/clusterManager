
## 项目结构
```
redisStudy
├─ Design.md
├─ README.md
├─ cluster
├─ cmd
│  ├─ create.go
│  ├─ delete.go
│  ├─ root.go
│  └─ scale.go
├─ config
│  ├─ conf.json
│  └─ readConf_test.go
├─ go.mod
├─ go.sum
├─ main.go
├─ model
│  ├─ cluster_manager.go
│  ├─ cluster_node.go
│  ├─ cluster_node_test.go
│  ├─ config.go
│  ├─ config_test.go
│  ├─ containers_manger.go
│  ├─ create_cluster.go
│  ├─ create_cluster_test.go
│  ├─ delete_cluster.go
│  ├─ delete_cluster_test.go
│  ├─ redis_cluster_config.json
│  ├─ scale_cluster.go
│  └─ scale_cluster_test.go
├─ redis_cluster_config.json
└─ utils
   ├─ uils_test.go
   └─ utils.go
```

主要类关系

```plantuml
@startuml
!define RECTANGLE class

RECTANGLE ClusterNode {
  + ID : string                     
  + IP : string                      
  + Port : uint16                    
  + NodeType : string                
  + MasterID : string                
  + PingSent : int64                
  + PongRecv : int64                 
  + ConfigEpoch : int64             
  + LinkState : string               
  + Slots : []SlotRange              
  + AdditionalFlags : []string       
  + SlotsNum : int                    
  + ClusterName : string             
}

RECTANGLE ClusterManager {
  + EmptyMasters : []*ClusterNode
  + IDToClusterNode : map[string]*ClusterNode
  + IPToClusterID : map[string]string
  + AlreadyMeetNode : map[string]bool
  + MasterToSlave : map[string][]string
  + AlreadySetCluster : map[string]bool
  + ClusterNodeList : []*ClusterNode
  + MasterIDs : []string
  + MasterSet : map[string]bool
  + Replica : int
  - containersManager : *ContainersManager

  + CreateCluster(clusterName : string, shaderNum : int, replicaNum : int) : error
  + CreateClusterNodes(sharedNum : int, ctx : context.Context, clusterName : string) : error
  + MeetNodes(client : redis.Client, ctx : context.Context, clusterName : string) : error
  + SetAllNodeRole(ctx : context.Context, clusterName : string) : error
  + SetNodeAsSlave(ctx : context.Context, masterIP : string, slaveIP : string, clusterName : string) : error
  + AddClusterNode(ctx : context.Context, clusterName : string) : (ContainerNode, error)
  + VerifyAllocateSlots(ctx : context.Context, containers : ContainersManager, clusterName : string) : error
  + ParseRedisClusterNodes(ctx : context.Context, data : string, clusterName : string) : ([]ClusterNode, error)
  + GetClusterNodes(ctx : context.Context, loginNode : ContainerNode, clusterName : string) : ([]ClusterNode, error)
  - sortClusterNodesByIP(nodes : []*ClusterNode) : void
  + MigrateSlot(ctx : context.Context, slot : int, sourceNodeID : string, destNodeID : string) : error
  + AllocateSlots(ctx : context.Context, clusterName : string) : error
  + MigratesSlotsToEmptyNode(ctx : context.Context, clusterName : string) : error
  + calculateSlots(slots : []SlotRange) : int
  - verifyNodeTypeSet(ctx : context.Context, masterToSlave : map[string][]string, clusterName : string) : (bool, error)
  - addShaderAndReplica(ctx : context.Context, clusterName : string) : (string, error)
}

RECTANGLE ContainerNode {
  - Name: string
  - HostIP: string
  - HostPort: uint16
  - ConIp: string
  - ConPort: uint16
  - ID: string
  - ClusterName: string

  + CreateRedisClient(ctx: context.Context): *redis.Client
  + CloseRedisClient(cli: *redis.Client): error
}

RECTANGLE ContainersManager {
  - IPToNode: map[string]*ContainerNode
  - Num: int
  - IDToNode: map[string]*ContainerNode
  - Nodes: []*ContainerNode
  - ContainersIDSet: map[string]bool

  + AddContainerNode(node: *ContainerNode)
  + CreateContainers(ctx: context.Context, nodeNum: int, clusterName: string) : error
  + CreateContainer(ctx: context.Context, index: int, clusterName : string) : (string, error)
  + GetCurContainersNum(ctx : context.Context) : error
  + UpdateAllContainersInfo(ctx : context.Context) : error
}

RECTANGLE redis {
  + NewClient(options : *Options) : *Client
  + Ping(ctx : context.Context) : *Result
}

ClusterManager"1" -->"1" ContainersManager : uses
ClusterManager"1" --> "*"ClusterNode : manages
ClusterNode "1" --> "1"ContainerNode : related by IP
ContainerNode "1"-->"1" redis : uses
ContainersManager "1" --> "*" ContainerNode : manages

@enduml


  
```

