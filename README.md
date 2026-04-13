

## 项目介绍
此项目在MacOS开发环境下实现了对单个集群的创建、删除以及扩容功能。
## 文件说明
- 此项目中环境配置（Redis配置，镜像构建以及项目编译）见文件[INSTALLATION_GUIDE.md](./INSTALLATION_GUIDE.md)
- 此项目的具体结构设计见文件[DESIGN.md](./DESIGN.md)
- 为了方便回顾，已补充整理文档到 [docs/00-项目总览.md](./docs/00-项目总览.md)

## 回顾导航
- [项目总览](./docs/00-项目总览.md)
- [架构与目录](./docs/01-架构与目录.md)
- [核心流程](./docs/02-核心流程.md)
- [配置与运行](./docs/03-配置与运行.md)
- [当前状态与问题清单](./docs/04-当前状态与问题清单.md)

## 使用说明 
### 创建集群
```shell
./cluster create -s <shardCount> -r <nodesPerShard> -n <clusterName> -p <port>
```
参数解释：
- s: shardCount, 集群中 shard 的数量，默认为 3
- r: nodesPerShard, 每个 shard 中 Redis 节点总数，包含 1 个 master，默认为 2
- n: clusterName, 集群的名称，默认为cluster

**Example** :
~~~
./cluster create -s 3 -r 2 -n test -p
~~~
正确运行结果：
![./img/img_2.png](img/img_2.png)

### 删除集群
```shell
./cluster delete -n <clusterName>
```
参数解释：
- n: clusterName, 集群的名称，必须显式声明

**Example**
~~~
./cluster delete -n <clusterName>
~~~
正确运行结果：
![img/imgDel.png](img/imgDel.png)

### 扩容集群

```shell
./cluster scale -n <clusterName> -s <shardCount> 
```
参数解释：
- n: clusterName, 集群的名称，必须显式声明
- s: shardCount, 增加的 shard 数量，默认为 1

**Example:**
~~~
./cluster scale -n test -s 3 
~~~
正确运行结果：
![img/omgScale.png](img/imgScale.png)
