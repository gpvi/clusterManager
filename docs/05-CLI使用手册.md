# CLI 使用手册

## 1. 构建

```powershell
go build -o cluster .
# 或
make build
```

程序入口是 `main.go`，Cobra 根命令在 `cmd/root.go`。

## 2. 命令总览

```text
cluster create   创建 Redis Cluster
cluster scale    为已有集群新增 shard
cluster delete   删除指定集群及其 K8s 资源
```

所有命令需要可访问的 Kubernetes 集群（通过 `config/conf.yaml` 中的 `kube_config_path` 或 `KUBECONFIG` 环境变量）。

## 3. 创建集群

### 基本用法

```powershell
./cluster create --clusterName mycluster --shards 3 --nodes-per-shard 2 --port 6379
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
# 创建 3 分片 x 2 副本 = 6 节点的集群
./cluster create -n mycluster -s 3 -r 2

# 创建 5 分片集群
./cluster create -n bigcluster -s 5 -r 3
```

### 创建后

- K8s Namespace 中会出现带标签 `cluster-name=<name>` 的 Pod 和 Service
- 运行时状态写入 `<runtime_state_dir>/<clusterName>/`
- 输出 `CLUSTER NODES` 确认集群状态

## 4. 扩容集群

### 基本用法

```powershell
./cluster scale --clusterName mycluster --shards 2
```

### 参数

| 参数 | 简写 | 类型 | 默认值 | 说明 |
|------|------|------|--------|------|
| `--clusterName` | `-n` | string | (必填) | 集群名称 |
| `--shards` | `-s` | int | `1` | 要新增的 shard 数量 |

## 5. 删除集群

### 基本用法

```powershell
./cluster delete --clusterName mycluster
```

### 参数

| 参数 | 简写 | 类型 | 默认值 | 说明 |
|------|------|------|--------|------|
| `--clusterName` | `-n` | string | (必填) | 集群名称 |

### 删除后

- 清理该集群所有 K8s 资源（Service、Pod、ConfigMap）
- 删除运行时状态文件
- 不影响其他集群

## 6. 废弃参数

以下参数仍可用但已废弃（仅保留以兼容旧脚本）：

| 参数 | 替代 |
|------|------|
| `--shaderNum` | `--shards` |
| `--replica` | `--nodes-per-shard` |
