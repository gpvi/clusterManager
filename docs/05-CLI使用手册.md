# CLI 使用手册

## 1. 可执行文件

默认构建产物名为 `cluster`：

```powershell
./scripts/build.ps1
```

等价命令：

```powershell
go build -tags containers_image_openpgp -o cluster .
```

程序入口是 `main.go`，Cobra 根命令在 `cmd/root.go`。

## 2. 命令总览

```text
cluster create   创建 Redis Cluster
cluster scale    为已有集群新增 shard
cluster delete   删除指定集群及其容器
```

所有命令都会先创建 Podman API 连接。连接参数来自 `config/conf.yaml`、环境变量，或 `podman system connection list --format json` 的默认连接。

## 3. 创建集群

```powershell
./cluster create -s 3 -r 2 -n demo -p 6379
```

参数：

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `-s`, `--shards` | `3` | 要创建的 shard 数量 |
| `-r`, `--nodes-per-shard` | `2` | 每个 shard 的 Redis 节点数，包含 1 个 master |
| `-n`, `--clusterName` | `cluster` | 集群名称 |
| `-p`, `--port` | `6379` | Redis 容器内服务端口 |

兼容参数：

- `--shaderNum`：已废弃，等价于 `--shards`
- `--replica`：已废弃，等价于 `--nodes-per-shard`

约束：

- `shards` 必须大于 `0`
- `nodes-per-shard` 必须至少为 `2`
- `clusterName` 不能为空
- 同名集群已存在时会拒绝创建

创建成功后会生成：

```text
runtime/<clusterName>/containers.json
runtime/<clusterName>/run_time_config.yaml
```

## 4. 扩容集群

```powershell
./cluster scale -n demo -s 1
```

参数：

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `-n`, `--clusterName` | 空 | 要扩容的集群名称 |
| `-s`, `--shards` | `1` | 要新增的 shard 数量 |

兼容参数：

- `--shaderNum`：已废弃，等价于 `--shards`

扩容会读取：

```text
runtime/<clusterName>/run_time_config.yaml
```

该文件用于恢复创建时的 `nodes_per_shard` 和 Redis 端口。如果它丢失，扩容无法知道原集群的节点布局。

## 5. 删除集群

```powershell
./cluster delete -n demo
```

参数：

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `-n`, `--clusterName` | 空 | 要删除的集群名称 |

删除逻辑：

1. 删除 `runtime/<clusterName>/run_time_config.yaml`
2. 删除 `runtime/<clusterName>/containers.json`
3. 枚举 Podman 容器
4. 删除标签 `clusterName=<clusterName>` 的容器
5. 如果运行时目录为空，则删除该目录

## 6. 推荐操作顺序

首次运行建议按下面顺序：

```powershell
./scripts/build.ps1
./cluster create -s 3 -r 2 -n demo
./cluster scale -n demo -s 1
./cluster delete -n demo
```

如果 Redis 镜像还不存在，需要先构建镜像：

```bash
./scripts/build-image.sh
```

## 7. 常用检查命令

查看 Podman 连接：

```powershell
podman system connection list
```

查看项目创建的容器：

```powershell
podman ps -a --filter label=clusterName=demo
```

查看 Redis Cluster 节点：

```powershell
redis-cli -p <mapped-host-port> cluster nodes
```

`mapped-host-port` 可以从 `runtime/<clusterName>/containers.json` 或 `podman ps` 中获取。
