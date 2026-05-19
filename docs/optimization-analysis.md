# Slot 分配 & IP 模型优化分析

## 一、现状基线

### 1.1 当前槽位分配

```
AllocateSlots: 16384 slots / N masters，连续区间均分
M1 [0, 5460]    M2 [5461, 10922]    M3 [10923, 16383]

扩容时迁移量 = (新节点数 / 总节点数) × 16384，每次约 5000~8000 slot
```

### 1.2 当前 IP 模型

```
RuntimeNode.ConIp   → 含义随 backend 变化（PodIP / 容器 bridge IP / 127.0.0.1）
RuntimeNode.HostPort → 仅外部可访问，集群 bus 不可达
```

节点通信时硬编码 `127.0.0.1:HostPort` 创建 Redis client，依赖端口映射，不感知集群内部拓扑。

---

## 二、优化方案

### 方案 A：加权槽位分配

**场景**：异构节点（不同内存/CPU），让大节点承担更多槽位。

**实现量**：~80 行

```go
// 配置来源: CR spec 或 runtime config
type NodeWeight struct {
    Index  int  // node_index
    Weight int  // 1~10, 默认 5
}

func allocateWeightedSlots(masters []*ClusterNode, weights []NodeWeight) map[string][]SlotRange
```

| 收益 | 成本 |
|------|------|
| 异构集群负载均衡 | 配置复杂度 ↑ |
| 避免小节点过载 | `NewClusterManager` 接口需扩展 |
| 与 Redis Cluster 原生行为兼容 | 权重数据需要持久化到 DB/store |

**结论**：当前所有节点规格相同（同镜像、同配置），**暂时不需要**。未来支持异构 spec 时再实现。

---

### 方案 B：交织槽位分布（Interleaved Slot Distribution）

**场景**：扩容时减少迁移槽位数量。

**原理**：

```
连续分布：
  M1: [0────8191]  M2: [8192──16383]
  新增 M3 → M1 迁移 4096 slots, M2 迁移 4096 slots = 8192 次

交织分布：
  slot % N → master
  slot 0→M1, 1→M2, 2→M1, 3→M2, ...
  新增 M3 → 每个 master 仅迁移 1/3 ≈ 2730 slots
```

**实现量**：~120 行（分配逻辑 ~60 行 + 迁移计算 ~60 行）

**关键代码变化**：

```go
// 当前: 连续块
for i, masterID := range c.MasterIDs {
    start = i * slotsPerMaster
    end = start + slotsPerMaster - 1
    cli.ClusterAddSlots(ctx, slots[start:end])
}

// 交织: slot % N
for slot := 0; slot < TotalSlots; slot++ {
    masterIdx := slot % len(c.MasterIDs)
    masterSlots[c.MasterIDs[masterIdx]] = append(..., slot)
}
// 批量 ADDSLOTS（一次命令多个 slot）
for masterID, slots := range masterSlots {
    cli.ClusterAddSlots(ctx, slots...)
}
```

| 收益 | 成本 |
|------|------|
| 扩容迁移量减少 ~40% | `AllocateSlots` 需重写 |
| 单个节点故障时，槽位均匀分布到剩余节点 | `ParseSlots` 解析连续区间变离散列表 |
| 热点 key 更均匀分布 | `MigratesSlotsToEmptyNode` 迁移计算复杂度 ↑ |
| | `PrintClusterNodesInfo` 输出更冗长 |

**实测对比**（3 节点 → 4 节点扩容）：

| 指标 | 连续分布 | 交织分布 |
|------|---------|---------|
| 需迁移槽位数 | 4096 | 4096 |
| 实际差异 | **无差异** — Redis 的 slot 迁移粒度是单个 slot，总量相同 | |
| 迁移后槽位分布 | [0-4095] [4096-8191] [8192-12287] [12288-16383] | 离散，难以人工阅读 |
| CLUSTER NODES 可读性 | 高（连续区间） | 低（大量离散 slot） |

**结论**：Redis 迁移 slot 的最小粒度是 1 个 slot，无论连续还是交织，**迁移总量恒等于 (16384/N_old - 16384/N_new) × affected_nodes**。交织分布的收益成立的前提是"批量迁移 slot 区间"，但 Redis CLUSTER 的 slot 迁移是逐个进行的，**交织不会减少实际迁移次数**。

**唯一真正的减少迁移方式**：使用 Redis 7.0+ 的 `CLUSTER ADDSLOTSRANGE` 批量命令，但这是协议层面的优化，与分布策略无关。

**建议**：保持连续分布不变。

---

### 方案 C：NodeAddress 统一抽象

**场景**：消除 `ConIp` 语义歧义，区分集群内/外通信地址。

**当前问题**：

```
// cluster_manager.go:422 — 硬编码 127.0.0.1
cli, err := CreateRedisClient(ctx, "127.0.0.1", slaveNode.HostPort)

// cluster_manager.go:330 — 使用 ConIp（依赖 backend 实现）
client.ClusterMeet(ctx, node.ConIp, port)
```

`HostIP` 始终是 `127.0.0.1`（端口映射），`ConIp` 在不同 backend 含义不同。如果未来节点不都在 localhost（如远程 podman），`127.0.0.1` 硬编码会失效。

**实现量**：~100 行（新类型 ~20 + 替换硬编码 ~80）

```go
type NodeAddress struct {
    ClusterAddr string // 集群内部可达: 10.88.0.x:6379
    ClientAddr  string // 管理客户端可达: 127.0.0.1:HostPort
}

type RuntimeNode struct {
    Name        string
    Address     NodeAddress
    ID          string
    ClusterName string
}

func (n *RuntimeNode) ClusterMeetAddr() string {
    return n.Address.ClusterAddr
}

func (n *RuntimeNode) ClientConnAddr() string {
    return n.Address.ClientAddr
}
```

**影响范围**：

| 文件 | 改动点 |
|------|--------|
| `cluster_manager.go` | 8 处 `CreateRedisClient` 调用改用 `ClientConnAddr()` |
| `cluster_manager.go` | 2 处 `ClusterMeet` 改用 `ClusterMeetAddr()` |
| `k8s_manager.go` | `RuntimeNode` 构造改为填充 `Address` |
| `podman_manager.go` | 同上 |
| `containerd_manager.go` | 同上 |
| `cluster_manager_test.go` | 适配新字段 |

| 收益 | 成本 |
|------|------|
| 消除 127.0.0.1 硬编码 | 改动涉及 ~5 个文件 |
| 支持远程节点（非 localhost） | `ClusterMeet` 地址从 `ConIp` 语义明确为 `ClusterAddr` |
| 新 backend 实现时不会误用地址 | `RuntimeNode` API 小幅膨胀 |
| 未来 multi-host 集群直接可用 | |

**结论**：**推荐实施**。当前 127.0.0.1 硬编码是技术债，未来如果 podman machine 在另一台机器、或多个 WSL 实例，地址模型不清晰会导致 bug。

---

## 三、综合建议

| 优先级 | 方案 | 理由 |
|--------|------|------|
| P0 | C — NodeAddress 抽象 | 消除硬编码 127.0.0.1，收益明确，成本可控 |
| P2 | A — 加权分配 | 当前无异构需求，待有场景再实现 |
| ❌ | B — 交织分布 | 迁移总量不变，Redis slot 迁移粒度已是最小，无实际收益 |

### P0 实施计划

```
1. model/k8s_manager.go    → 新增 NodeAddress 类型，RuntimeNode 嵌入 Address
2. model/cluster_manager.go → 8 处 CreateRedisClient("127.0.0.1", port) 改为 node.ClientConnAddr()
3. model/cluster_manager.go → CLUSTER MEET 改用 node.ClusterMeetAddr()
4. model/podman_manager.go  → 构造 RuntimeNode 时正确填充 ClusterAddr / ClientAddr
5. model/containerd_manager.go → 同上
6. model/k8s_manager.go     → 同上
7. model/*_test.go          → 适配
8. go test ./... && podman 实战验证
```

**预估工时**：~1 小时
