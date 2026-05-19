# CLI 使用手册

## 1. 构建

```powershell
go build -o cluster .
# 含 containerd 后端
go build -tags containerd -o cluster .
```

程序入口是 `main.go`，Cobra 根命令在 `cmd/root.go`。

## 2. 命令总览

```text
cluster create   创建 Redis Cluster
cluster scale    为已有集群新增 shard（含并发 slot 迁移）
cluster delete   删除指定集群及其容器资源
```

## 3. 全局参数

| 参数 | 简写 | 类型 | 默认值 | 说明 |
|------|------|------|--------|------|
| `--backend` | `-b` | string | `k8s` | 后端选择：`k8s` / `podman` / `containerd` |
| `--containerd-socket` | | string | 自动检测 | containerd socket 路径（仅 containerd 后端） |
| `--db` | | string | `runtime/cluster.db` | SQLite 数据库路径（空则不启用持久化） |

也可通过环境变量配置：
- `CLUSTER_BACKEND` — 等价 `--backend`
- `CLUSTER_CONTAINERD_SOCKET` — containerd socket
- `CLUSTER_DB_PATH` — SQLite 路径

## 4. 创建集群

### 基本用法

```powershell
./cluster create --backend podman -n mycluster -s 3 -r 2 -p 6379
```

### 参数

| 参数 | 简写 | 类型 | 默认值 | 说明 |
|------|------|------|--------|------|
| `--clusterName` | `-n` | string | `cluster` | 集群名称 |
| `--shards` | `-s` | int | `3` | shard 数量（即 master 数量） |
| `--nodes-per-shard` | `-r` | int | `2` | 每个 shard 的节点数（含 master） |
| `--port` | `-p` | uint16 | `6379` | Redis 容器端口 |

### 示例

```powershell
# Podman 后端：3 分片 x 2 副本 = 6 容器
./cluster create --backend podman -n mycluster -s 3 -r 2

# K8s 后端（默认）
./cluster create -n mycluster -s 3 -r 2

# Containerd 后端
./cluster create --backend containerd -n mycluster -s 5 -r 3
```

### 创建后

- Podman：容器命名为 `<clusterName>-redis-<n>`，运行在 bridge 网络中
- K8s：带标签 `cluster-name=<name>` 的 Pod / Service / ConfigMap
- 运行时状态写入 `<runtime_state_dir>/<clusterName>/`
- SQLite 数据库中写入 cluster + container 记录和操作日志

## 5. 扩容集群

### 基本用法

```powershell
./cluster scale --backend podman --clusterName mycluster --shards 2
```

### 参数

| 参数 | 简写 | 类型 | 默认值 | 说明 |
|------|------|------|--------|------|
| `--clusterName` | `-n` | string | (必填) | 集群名称 |
| `--shards` | `-s` | int | `1` | 要新增的 shard 数量 |

### 扩容流程

1. 创建新容器（数量 = shards × nodes_per_shard）
2. CLUSTER MEET 将新节点加入集群
3. 分配主从角色
4. 并发 slot 迁移（16 workers），实时进度输出
5. 更新 SQLite 状态

```
slot migration: 8192 slots across 1 source groups (16 workers each)
  slot migration: 444/8192 (5.4%)
  ...
  slot migration: 8192/8192 (100%)
```

## 6. 删除集群

### 基本用法

```powershell
./cluster delete --backend podman --clusterName mycluster
```

### 参数

| 参数 | 简写 | 类型 | 默认值 | 说明 |
|------|------|------|--------|------|
| `--clusterName` | `-n` | string | (必填) | 集群名称 |

### 删除后

- 清理该集群所有容器（按命名前缀过滤，不影响其他容器）
- 删除运行时状态文件和目录
- SQLite 中标记删除

## 7. 各后端差异

| 特性 | Podman | Kubernetes | Containerd |
|------|--------|------------|------------|
| 运行依赖 | podman CLI | kubeconfig | containerd socket |
| 网络模型 | bridge + port mapping | PodIP + NodePort | host 网络 + 顺序端口 |
| 容器命名 | `{name}-redis-{n}` | `{name}-redis-{n}` | `{name}-redis-{n}` |
| 物理隔离 | 是（bridge 网络） | 是（Pod 网络） | 否（共享 host 网络） |
| 编译要求 | 默认 | 默认 | `-tags containerd` |

## 8. 废弃参数

以下参数仍可用但已废弃：

| 参数 | 替代 |
|------|------|
| `--shaderNum` | `--shards` |
| `--replica` | `--nodes-per-shard` |
