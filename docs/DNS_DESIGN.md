# DNS 集成设计方案

## 1. 现状: IP-based 寻址的瓶颈

当前所有 Redis 节点间通信都依赖**原始 IP 地址**:

```
CLUSTER MEET 10.244.1.5 6379    ← Pod IP (K8s)
CLUSTER MEET 10.88.0.3 6379     ← bridge IP (Podman)
```

| 问题 | 影响 |
|------|------|
| Pod 重启 IP 变化 | 集群拓扑破裂, 需重新 MEET |
| NAT/overlay 网络跨主机不可达 | K8s 跨节点 Pod IP 直连不通 |
| `GetNodeByIP()` 硬依赖 IP 精确匹配 | `ClusterNode.IP` 必须等于 `RuntimeNode.ConIp` |
| 无 StatefulSet 序号主机名 | 虽然有 `<pod>-svc` 但从未用于寻址 |
| `cluster-announce-ip` 被注释掉 | Redis 自报地址 仍是 Pod IP |

---

## 2. 设计方案: DNS-first 寻址

### 2.1 核心思路

**用 DNS 主机名替代原始 IP 作为集群节点的"身份标识"**，IP 降级为"连接实现细节"。

```
现在:  CLUSTER MEET 10.244.1.5 6379
之后:  CLUSTER MEET mycluster-redis-0.mycluster-svc.default.svc.cluster.local 6379
```

### 2.2 架构变更

```
                    ┌──────────────────────────┐
                    │    RuntimeNode            │
                    │                           │
                    │  Hostname  (NEW)          │  ← DNS 可解析名称
                    │  ConIp     (保留)         │  ← 用于日志/调试
                    │  HostPort  (保留)         │  ← 管理端口映射
                    │  ConPort                  │
                    │                           │
                    │  ClusterMeetAddr()  (NEW) │  → "hostname:port"
                    │  ClientConnAddr()         │  → "127.0.0.1:HostPort"
                    └──────────────────────────┘
```

```
Before (IP-based):
  CLUSTER MEET  → node.ConIp                  (10.244.1.5)
  GetNodeByIP() → IPToNode[ip]                (string match)

After (DNS-based):
  CLUSTER MEET  → node.ClusterMeetAddr()       (hostname:port)
  GetNodeByHost() → HostToNode[hostname]       (string match)
  cluster-announce-ip → hostname               (Redis 自报 DNS)
```

### 2.3 数据模型变更

#### RuntimeNode 新增字段

```go
type RuntimeNode struct {
    // existing...
    HostIP    string
    HostPort  uint16
    ConIp     string   // retained: container IP for logging/debug
    ConPort   uint16
    
    // NEW
    Hostname  string   // DNS-resolvable name, e.g. "mycluster-redis-0.svc.cluster.local"
    
    Address   NodeAddress
}

// ClusterMeetAddr returns the address other Redis nodes should use to MEET this node.
// In DNS mode this returns hostname:port; falls back to ConIp:ConPort otherwise.
func (n *RuntimeNode) ClusterMeetAddr() string {
    if n.Hostname != "" {
        return fmt.Sprintf("%s:%d", n.Hostname, n.ConPort)
    }
    return fmt.Sprintf("%s:%d", n.ConIp, n.ConPort)
}
```

#### NodeManager 新增索引

```go
type K8sNodeManager struct {
    Nodes      []*RuntimeNode
    IPToNode   map[string]*RuntimeNode   // retained for backward compat
    HostToNode map[string]*RuntimeNode   // NEW: hostname → node lookup
}

func (m *K8sNodeManager) GetNodeByHost(host string) *RuntimeNode {
    if m.HostToNode != nil {
        if n, ok := m.HostToNode[host]; ok {
            return n
        }
    }
    // fallback: try IP-based lookup
    return m.GetNodeByIP(host)
}
```

#### 配置扩展

```yaml
# config/conf.yaml
dns:
  enabled: true
  domain: "svc.cluster.local"     # K8s DNS domain
  naming_template: "{{.ClusterName}}-redis-{{.Index}}.{{.ClusterName}}-svc.{{.Namespace}}"
  # Expands to: mycluster-redis-0.mycluster-svc.default.svc.cluster.local
```

```go
type DNSConfig struct {
    Enabled        bool   `yaml:"enabled"`
    Domain         string `yaml:"domain"`
    NamingTemplate string `yaml:"naming_template"`
}
```

---

## 3. 各后端实现

### 3.1 K8s 后端 (StatefulSet → Headless Service)

K8s 原生支持 DNS 主机名:

```
Pod 名称:              mycluster-redis-0
Headless Service:      mycluster-svc
完整 DNS:              mycluster-redis-0.mycluster-svc.default.svc.cluster.local
```

变更:
1. **Pod 创建从裸 Pod 改为 StatefulSet** (可选, 也可给裸 Pod 设 `hostname` + `subdomain`)
2. **Service 从 NodePort 改为 Headless** (`clusterIP: None`) + NodePort Service 分开创建
3. **在 Pod Spec 中设置 `hostname` 和 `subdomain`**
4. **Redis 启动参数添加** `--cluster-announce-ip $(hostname).$(subdomain).$(namespace).svc.cluster.local`

```
创建顺序:
1. Headless Service (mycluster-svc)     → 提供 DNS 记录
2. NodePort Service (mycluster-redis-0) → 提供管理端口映射
3. StatefulSet / Pod                     → 自动获得 DNS 名称
```

### 3.2 Podman 后端 (容器名 → DNS)

Podman 默认使用容器名作为 DNS 名 (通过 aardvark-dns / CNI):

```bash
podman run --name mycluster-redis-0 ...
podman run --name mycluster-redis-1 ...

# 容器间可用名称互访
ping mycluster-redis-0   # → OK
```

变更:
1. **容器名使用可预测的模板** `<clusterName>-redis-<index>`
2. **Redis 启动参数添加** `--cluster-announce-ip <containerName>`
3. **Hostname 字段** = 容器名

### 3.3 Containerd 后端 (不变)

Containerd 后端使用主机网络, 所有节点共享 `127.0.0.1`。DNS 模式对此后端无影响，保持 `localHost` 寻址。

---

## 4. CLUSTER MEET 流程变更

### 变更前 (IP-based)

```go
func (c *ClusterManager) MeetNodes(...) error {
    for _, node := range nodes {
        client.ClusterMeet(ctx, node.ConIp, strconv.Itoa(int(node.ConPort))).Result()
    }
}
```

### 变更后 (DNS-aware)

```go
func (c *ClusterManager) MeetNodes(...) error {
    for _, node := range nodes {
        meetAddr := node.ClusterMeetAddr()  // hostname:port or ip:port
        host, port := utils.ParseIPPort(meetAddr)
        client.ClusterMeet(ctx, host, port).Result()
    }
}
```

### ClusterNode 解析适配

`CLUSTER NODES` 输出中, `cluster-announce-ip` 设置了主机名时, IP 字段会显示主机名:

```
# before: a1b2c3... 10.244.1.5:6379@16379 master - 0 ...
# after:  a1b2c3... mycluster-redis-0.mycluster-svc...:6379@16379 master - 0 ...
```

解析器 `parseClusterNodeLines` 需兼容 IP 与 hostname:

```go
// hostOrIP 可以是 "10.244.1.5" 或 "mycluster-redis-0.svc.cluster.local"
node.IP = hostOrIP
// GetNodeByHost 先查 HostToNode, fallback IPToNode
runtime := c.nodeManager.GetNodeByHost(hostOrIP)
```

---

## 5. 缓存子系统 (GeeCache) 影响

**零影响。** GeeCache 使用独立的 memberlist gossip 协议进行对等发现，不依赖 Redis 寻址。两者完全正交。

如需统一 DNS 环境，可在 `cache.seeds` 中配置主机名而非 IP:

```yaml
cache:
  seeds:
    - "cache-node-0.cache-svc.default.svc.cluster.local:8001"
    - "cache-node-1.cache-svc.default.svc.cluster.local:8001"
```

---

## 6. 实施计划

### Phase 1: 基础数据模型 (1 天)

| 任务 | 文件 |
|------|------|
| 添加 `Hostname` 字段到 `RuntimeNode` | `model/k8s_manager.go` |
| 添加 `HostToNode` 索引 + `GetNodeByHost()` | 三个后端文件 |
| 添加 `ClusterMeetAddr()` 方法 | `model/k8s_manager.go` |
| 添加 `DNSConfig` 到 `Config` / `RuntimeConfig` | `model/config.go`, `config/conf.yaml` |
| 更新 `Store` (SQLite) 持久化 Hostname | `model/store.go` |

### Phase 2: 启动参数 (1 天)

| 任务 | 文件 |
|------|------|
| 修改 `redis.conf` 模板 启用 `cluster-announce-ip` | `setup/redis/config/redis.conf` |
| 各后端 `CreatePods` 传入 `--cluster-announce-ip` | 三个后端文件 |
| K8s: 设置 `hostname` + `subdomain` | `model/k8s_manager.go` |
| Podman: 设置固定容器名 | `model/podman_manager.go` |

### Phase 3: CLUSTER MEET 适配 (1 天)

| 任务 | 文件 |
|------|------|
| `MeetNodes` 使用 `ClusterMeetAddr()` | `model/cluster_manager.go` |
| `parseClusterNodeLines` 兼容 hostname | `model/cluster_node.go` |
| 全局替换 `GetNodeByIP` → `GetNodeByHost` (8 处) | `model/cluster_manager.go` 等 |
| `clusterFilter` 改用 `GetNodeByHost` | `model/cluster_manager.go` |

### Phase 4: 测试 & 验证 (1 天)

| 测试场景 | 验证点 |
|----------|--------|
| K8s + DNS | Pod 重启后集群自动恢复 (DNS 名称不变) |
| Podman + DNS | 容器名互 ping, Redis 集群形成 |
| 混合模式 | `dns.enabled=false` 回退到 IP 模式 |
| 槽迁移 | DNS 模式下迁移正确完成 |

---

## 7. 风险 & 缓解

| 风险 | 缓解 |
|------|------|
| DNS 解析延迟导致 CLUSTER MEET 失败 | 重试逻辑 (已有 `waitForMeetSync`) |
| 主机名长度超 Redis 限制 (256 chars) | 模板校验, 截断 + hash 兜底 |
| `CLUSTER NODES` 输出混杂 IP 和 hostname | `GetNodeByHost` 双查 (hostname → IP fallback) |
| 旧集群数据 (SQLite) 无 hostname 字段 | `Store` v2 迁移, 空 hostname 回退 IP 模式 |
| Containerd 主机网络无 DNS | DNS 模式自动跳过 Containerd 后端 |
