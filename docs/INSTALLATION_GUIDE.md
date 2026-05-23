# 环境配置

## 1. 安装并启动 Podman

推荐先确认本机 Podman CLI 和 Podman machine 正常：

```powershell
podman --version
podman machine init
podman machine start
podman system connection list
```

当前项目已验证通过的模式是：

- Windows 上运行 CLI 和 Go 程序
- Podman machine / WSL 中运行远端 Podman service
- 通过 `ssh://.../podman.sock` 连接远端服务

## 2. 准备 Redis 配置

项目默认使用：

- [setup/redis/config/redis.conf](./setup/redis/config/redis.conf)
- `setup/redis/data/`

如果要替换为你自己的 Redis 配置，至少要保留 cluster 相关配置。

## 3. 构建 Redis 镜像

推荐直接运行：

```bash
./scripts/build-image.sh
```

该脚本会执行：

```bash
podman build -t myredis -f ./setup/redisv3.dockerfile .
```

## 4. 修改配置文件

主配置文件位置：`config/conf.yaml`

当前关键项：

- `paths.redis_host_config_path`
- `paths.redis_host_data_path`
- `paths.runtime_state_dir`
- `podman.endpoint`
- `podman.network_name`
- `configs.image_name`
- `configs.redis_port`

### Windows + Podman machine 说明

如果 Go 程序运行在 Windows，但 Podman service 运行在 Linux VM 中，容器 bind mount 的宿主机路径必须是 **Podman machine 内可见的 Linux 路径**。

例如：

```powershell
$env:REDIS_HOST_CONFIG_PATH="/mnt/e/Projects/clusterManager/clusterManager-main/setup/redis/config"
$env:REDIS_HOST_DATA_PATH="/mnt/e/Projects/clusterManager/clusterManager-main/setup/redis/data"
```

项目当前已经支持这类路径，不会再把 `/mnt/...` 误解析成 Windows 相对路径。

## 5. 构建项目

推荐使用脚本：

```powershell
./scripts/build.ps1
```

等价命令：

```powershell
go build -tags containers_image_openpgp -o cluster .
```

这里使用 `containers_image_openpgp` 是为了避免为 Podman 依赖链额外安装 `gpgme`。

## 6. 运行测试

默认测试：

```powershell
./scripts/test.ps1
```

该脚本会自动准备：

- 仓库内 `.gocache`
- 用于 Windows 本地测试的最小 `CONTAINERS_CONF`

等价命令：

```powershell
go test -tags containers_image_openpgp ./...
```

真实 Podman 集成测试需要额外开启：

```powershell
$env:CLUSTER_RUN_INTEGRATION_TESTS="1"
go test -tags "integration containers_image_openpgp" ./model -run TestCreation -v
```

## 7. 运行时文件

运行时状态现在按集群名保存到 `runtime/<clusterName>/` 下，主要包括：

- `containers.json`
- `run_time_config.yaml`

这些文件属于运行态产物，不建议提交到 Git。

更多命令示例见 [docs/05-CLI使用手册.md](./docs/05-CLI使用手册.md)，常见问题见 [docs/07-故障排查.md](./docs/07-故障排查.md)。
