# 项目介绍

`clusterManager` 是一个基于 Go 和 Podman API 的 Redis Cluster 本地编排工具，当前支持：

- 创建集群
- 删除集群
- 扩容集群

当前已经实机验证通过的运行形态是：

- Windows 客户端
- Podman machine / WSL 中的远端 Podman service
- Redis 容器运行在 `podman` bridge 网络中

## 快速开始

推荐优先使用仓库内脚本：

```powershell
./scripts/build.ps1
./scripts/test.ps1
```

其中：

- `scripts/build.ps1`：使用 `containers_image_openpgp` tag 编译 CLI，避免额外安装 `gpgme`
- `scripts/test.ps1`：运行默认测试集，并自动准备本地 `.gocache` / 最小 `CONTAINERS_CONF`
- `scripts/build-image.sh`：构建 Redis 测试镜像 `myredis`

如果手动执行命令，推荐使用：

```powershell
go build -tags containers_image_openpgp -o cluster .
go test -tags containers_image_openpgp ./...
```

## 使用说明

创建集群：

```shell
./cluster create -s <shardCount> -r <nodesPerShard> -n <clusterName> -p <port>
```

- `s`: shard 数量，默认 `3`
- `r`: 每个 shard 中的节点数，包含 1 个 master，默认 `2`
- `n`: 集群名，默认 `cluster`

删除集群：

```shell
./cluster delete -n <clusterName>
```

扩容集群：

```shell
./cluster scale -n <clusterName> -s <shardCount>
```

## 文档导航

- 环境配置、镜像构建与运行说明见 [INSTALLATION_GUIDE.md](./INSTALLATION_GUIDE.md)
- 历史设计草稿见 [DESIGN.md](./DESIGN.md)
- 完整文档入口见 [docs/README.md](./docs/README.md)
- [项目总览](./docs/00-项目总览.md)
- [架构与目录](./docs/01-架构与目录.md)
- [核心流程](./docs/02-核心流程.md)
- [配置与运行](./docs/03-配置与运行.md)
- [当前状态与问题清单](./docs/04-当前状态与问题清单.md)
- [CLI 使用手册](./docs/05-CLI使用手册.md)
- [开发与测试指南](./docs/06-开发与测试指南.md)
- [故障排查](./docs/07-故障排查.md)
