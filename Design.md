
## 项目结构
```
redisStudy
├─ cluster
├─ cmd
│  ├─ create.go
│  ├─ delete.go
│  ├─ root.go
│  └─ scale.go
├─ go.mod
├─ go.sum
├─ main.go
├─ model
│  ├─ cluster_manager.go
│  ├─ cluster_node.go
│  ├─ cluster_node_test.go
│  ├─ config.go
│  ├─ containers_manger.go
│  ├─ create_cluster.go
│  ├─ create_cluster_test.go
│  ├─ delete_cluster.go
│  ├─ delete_cluster_test.go
│  ├─ scale_cluster.go
│  └─ scale_cluster_test.go
├─ utils
│  └─ utils.go
└─ 项目说明.md

```

```mermaid
flowchart TD
A[开始] --> B[创建clusterManager]
B --> C[初始化容器信息]
C--> D{是否满足创建条件
（当前容器数为0）}
D-->|是| E[开始创建集群]
D-->|否| F[提示当前容器数不为0，无法创建集群]
F-->End["创建集群结束"]
E-->G[由参数（sharder和replica）
创建redis容器并启动]
G-->H[执行meet操作，将各个节点加入集群,将集群信息写入管理器]
H-->I[根据replica设置主从角色]
I-->J[设置Slots并验证]
J-->End["创建集群结束"]
```
删除


