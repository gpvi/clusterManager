# clusterManager

基于 Go 的 Redis Cluster 本地编排工具，支持**多后端**容器运行环境：

- **Podman** — 通过 podman CLI 管理 Redis 容器（已实机验证）
- **Kubernetes** — 通过 K8s API 管理 Pod/Service/ConfigMap
- **Containerd** — 通过 containerd Go API 直接管理容器（需 `-tags containerd` 编译）

功能：
- 创建 Redis Cluster（自动 MEET / 主从分配 / slot 分配）
- 扩容集群（新增 shard + 并发 slot 迁移 + 进度报告）
- 删除集群（清理容器 + 运行时文件）
- SQLite 持久化状态与操作审计

## 快速开始

```powershell
# 编译（默认不含 containerd 后端）
go build -o cluster .
# 编译（含 containerd 后端）
go build -tags containerd -o cluster .

# 创建集群（Podman 后端）
./cluster create --backend podman -n mycluster -s 3 -r 2
# 扩容集群
./cluster scale --backend podman -n mycluster -s 1
# 删除集群
./cluster delete --backend podman -n mycluster

# Kubernetes 后端（默认）
./cluster create -n mycluster -s 3 -r 2
# Containerd 后端
./cluster create --backend containerd -n mycluster -s 3 -r 2
```

## 全局参数

| 参数 | 简写 | 默认值 | 说明 |
|------|------|--------|------|
| `--backend` | `-b` | `k8s` | 后端选择：k8s, podman, containerd |
| `--containerd-socket` | | 自动 | containerd socket 路径 |
| `--db` | | `runtime/cluster.db` | SQLite 数据库路径 |

## 创建集群

```shell
./cluster create -s <shardCount> -r <nodesPerShard> -n <clusterName> -p <port>
```

| 参数 | 简写 | 默认值 | 说明 |
|------|------|--------|------|
| `--clusterName` | `-n` | `cluster` | 集群名称 |
| `--shards` | `-s` | `3` | shard 数量 |
| `--nodes-per-shard` | `-r` | `2` | 每个 shard 节点数（含 master） |
| `--port` | `-p` | `6379` | Redis 容器端口 |

## 扩容集群

```shell
./cluster scale -n <clusterName> -s <shardCount>
```

扩容时自动执行 slot 迁移，16 个并发 worker，带实时进度输出。

## 删除集群

```shell
./cluster delete -n <clusterName>
```

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
