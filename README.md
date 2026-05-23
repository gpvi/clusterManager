# Redis Data Platform

基于 Go 的 Redis Cluster 本地编排工具 + 分布式内存缓存，支持**多后端**容器运行环境和 **Kubernetes Operator** 模式。

## 运行模式

| 模式 | 入口 | 说明 |
|------|------|------|
| **CLI 工具** | `main.go` | 命令行直接管理 Redis Cluster（创建/扩容/删除） |
| **Cache Node** | `cmd/cache.go` | 启动分布式内存缓存节点（GeeCache），支持 gRPC + SWIM gossip |
| **K8s Operator** | `cmd/operator/main.go` | 声明式管理，通过 CRD 描述集群，controller 自动调和 |
| **gRPC Server** | `cmd/clusterd/main.go` | 提供 gRPC API，Agent 通过远程调用管理集群 |

## 支持的后端

- **Kubernetes** — 通过 K8s API 管理 Pod/Service/ConfigMap（默认）
- **Podman** — 通过 podman CLI 管理 Redis 容器（已实机验证）
- **Containerd** — 通过 containerd Go API 直接管理容器（需 `-tags containerd` 编译）

## 项目结构

```
.
├── main.go                  # CLI 工具入口
├── cmd/
│   ├── root.go              # cobra 根命令
│   ├── create.go            # create 子命令
│   ├── scale.go             # scale 子命令
│   ├── delete.go            # delete 子命令
│   ├── operator/main.go     # K8s Operator 入口
│   └── clusterd/main.go     # gRPC server 入口
├── controller/
│   └── rediscluster_controller.go  # CRD reconcile loop
├── model/
│   ├── interfaces.go        # PodManager 接口定义
│   ├── factory.go           # 后端工厂方法
│   ├── config.go            # 配置加载（YAML + 环境变量）
│   ├── store.go             # SQLite 持久化
│   ├── cluster_manager.go   # 集群操作核心逻辑
│   ├── cluster_node.go      # 集群节点模型
│   ├── k8s_manager.go       # K8s 后端实现
│   ├── podman_manager.go    # Podman 后端实现
│   ├── containerd_manager.go # Containerd 后端实现
│   ├── create_cluster.go    # 创建集群流程
│   ├── scale_cluster.go     # 扩容集群流程
│   ├── delete_cluster.go    # 删除集群流程
│   └── errors.go            # 错误定义
├── api/v1/
│   └── rediscluster_types.go # CRD 类型定义
├── proto/
│   ├── rediscluster.proto       # clusterd gRPC proto
│   ├── cache.proto              # cache peer gRPC proto
│   ├── cache.pb.go / cache_grpc.go
│   └── rediscluster.pb.go / rediscluster_grpc.pb.go
├── cache/                       # 分布式缓存子系统 (GeeCache)
│   ├── run.go / init.go / starter.go  # 缓存节点入口
│   ├── geecache.go              # Group 缓存命名空间
│   ├── cache.go / byteview.go   # LRU 缓存核心
│   ├── hotkey.go                # 热点 key 检测与复制
│   ├── ratelimit.go             # DB 负载限流
│   ├── redis_getter.go          # Redis 后端 Getter
│   ├── invalidator.go           # slot 迁移缓存失效
│   ├── lru/                     # LRU 淘汰算法
│   ├── consistenthash/          # 一致性哈希
│   ├── singleflight/            # 请求合并
│   └── peer/                    # gRPC peer + memberlist
├── config/
│   ├── conf.yaml            # 默认配置文件
│   ├── crd/rediscluster.yaml # CRD 定义
│   ├── deploy/              # K8s 部署清单
│   │   ├── rbac.yaml
│   │   ├── operator.yaml
│   │   └── clusterd.yaml
│   └── examples/            # CR 示例
└── utils/                   # 工具函数
```

## 快速开始 — CLI 模式

```bash
# 编译
make build
# 或: go build -o cluster .

# 创建集群（K8s 后端，3 shard × 2 节点）
./cluster create -n mycluster -s 3 -r 2

# Podman 后端
./cluster create --backend podman -n mycluster -s 3 -r 2

# 扩容集群
./cluster scale -n mycluster -s 4

# 删除集群
./cluster delete -n mycluster
```

## 快速开始 — Cache Node

```bash
# 启动一个缓存节点（独立模式，不需要 Redis）
./cluster cache --port=8001 --gossip=9001

# 启动缓存节点 + HTTP API 网关 + 指定种子节点
./cluster cache --port=8002 --gossip=9002 --api --seeds=10.0.1.1:8001

# 完整示例：Redis Cluster + Cache 加速层
# 1. 创建 Redis 集群
./cluster create -n mycluster -s 3 -r 2

# 2. 启动缓存节点（使用 Redis 集群作为 L2 后端）
./cluster cache --port=8001 --gossip=9001
```

缓存架构：
- **L1**: GeeCache 内存缓存 (~1μs)，热点 key 自动复制，single-flight 防击穿
- **L2**: Redis Cluster (~0.5ms)，由 clusterManager 编排管理
- **L3**: 数据库 / API（Getter 回调回源）

## 快速开始 — Operator 模式

```bash
# 1. 部署 CRD
kubectl apply -f config/crd/rediscluster.yaml

# 2. 部署 RBAC + Operator
kubectl apply -f config/deploy/rbac.yaml
kubectl apply -f config/deploy/operator.yaml

# 3. 创建 RedisCluster CR
kubectl apply -f config/examples/dev-cluster.yaml

# 4. 查看状态
kubectl get rediscluster -o yaml
```

Operator 通过 reconcile loop 管理 CR 生命周期，状态机如下：

```
""  ──► Creating ──► Ready ⇄ Degraded
```

## 快速开始 — gRPC Server

```bash
# 部署 clusterd
kubectl apply -f config/deploy/clusterd.yaml

# 通过 gRPC 调用（需要 grpcurl 或自建客户端）
grpcurl -plaintext <clusterd-ip>:50051 list
```

gRPC 提供的 API：
- `CreateCluster` — 创建 CR
- `CreateClusterWithProgress` — 创建并返回进度流
- `GetStatus` — 查询集群状态
- `ScaleCluster` — 扩容
- `ListClusters` — 列出所有集群
- `Diagnose` — 诊断集群问题
- `GetEvents` — 获取 K8s Event
- `Health` — 健康检查

## 全局参数

| 参数 | 简写 | 默认值 | 说明 |
|------|------|--------|------|
| `--backend` | `-b` | `k8s` | 后端选择：k8s, podman, containerd |
| `--containerd-socket` | | 自动 | containerd socket 路径 |
| `--db` | | `runtime/cluster.db` | SQLite 数据库路径 |

## create 参数

```bash
./cluster create -s <shardCount> -r <nodesPerShard> -n <clusterName> -p <port>
```

| 参数 | 简写 | 默认值 | 说明 |
|------|------|--------|------|
| `--clusterName` | `-n` | `cluster` | 集群名称 |
| `--shards` | `-s` | `3` | shard 数量 |
| `--nodes-per-shard` | `-r` | `2` | 每个 shard 节点数（含 master） |
| `--port` | `-p` | `6379` | Redis 容器端口 |

## scale 参数

```bash
./cluster scale -n <clusterName> -s <shardCount>
```

扩容时自动执行 slot 迁移，16 个并发 worker，带实时进度输出。

## delete 参数

```bash
./cluster delete -n <clusterName>
```

## 配置

默认配置在 [config/conf.yaml](config/conf.yaml)，支持环境变量覆盖：

| 环境变量 | 说明 |
|----------|------|
| `CLUSTER_BACKEND` | 后端选择 |
| `CLUSTER_DB_PATH` | SQLite 数据库路径 |
| `CLUSTER_IMAGE_NAME` | Redis 镜像名 |
| `CLUSTER_REDIS_PORT` | Redis 端口 |
| `CLUSTER_STATE_DIR` | 运行时状态目录 |
| `KUBECONFIG` | K8s 配置文件路径 |
| `KUBE_NAMESPACE` | K8s 命名空间 |
| `BASE_NODE_PORT` | NodePort 起始端口 |
| `CLUSTER_CONTAINERD_SOCKET` | containerd socket |
| `GRPC_PORT` | gRPC server 端口（仅 clusterd） |
| `CACHE_PORT` | 缓存 gRPC 端口 |
| `CACHE_GOSSIP_PORT` | Gossip 协议端口 |
| `CACHE_TLS_MODE` | TLS 模式: insecure, server, mutual |
| `CACHE_MAX_BYTES` | 单节点最大缓存 (bytes) |
| `CACHE_TTL` | 默认 TTL (秒) |

## 文档导航

| 文档 | 说明 |
|------|------|
| [docs/00-项目总览.md](docs/00-项目总览.md) | 项目目标与范围 |
| [docs/01-架构与目录.md](docs/01-架构与目录.md) | 架构分层与目录结构 |
| [docs/02-核心流程.md](docs/02-核心流程.md) | 创建/扩容/删除核心流程 |
| [docs/03-配置与运行.md](docs/03-配置与运行.md) | 配置项、环境变量、运行方式 |
| [docs/05-CLI使用手册.md](docs/05-CLI使用手册.md) | 完整 CLI 参考 |
| [docs/06-开发与测试指南.md](docs/06-开发与测试指南.md) | 开发环境与测试 |
| [docs/07-故障排查.md](docs/07-故障排查.md) | 常见问题排查 |
| [docs/optimization-analysis.md](docs/optimization-analysis.md) | 热点 slot & 大 key 迁移优化分析 |
| [docs/INTEGRATION_DESIGN.md](docs/INTEGRATION_DESIGN.md) | GeeCache + clusterManager 整合设计方案 |

## Makefile

```bash
make build             # 编译 CLI 工具
make build-containerd  # 编译（含 containerd 后端）
make test              # 运行所有测试
make test-cache        # 仅缓存子系统测试
make test-model        # 仅集群管理测试
make test-race         # 竞态检测测试
make clean             # 清理构建产物和缓存
```
