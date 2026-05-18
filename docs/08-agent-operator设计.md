# Agent + Operator 设计文档

## 1. 目标

将 `clusterManager` 从纯 CLI 工具升级为 **Agent + Operator** 架构：

- **Operator**：声明式管理 Redis Cluster，持续协调期望状态
- **Agent**：自然语言交互层，负责任务理解和诊断分析

```
User (自然语言)
   │
   ▼
Agent (Go HTTP Server)
   │  Tool: create_cluster / get_status / scale / diagnose / list / events
   │
   ▼
K8s API Server
   │  RedisCluster CRD
   │  Pods / Services / ConfigMaps / Events
   │
   ▼
Operator (Go Controller)
   │  Watch CR → Reconcile → Update Status
   │
   ▼
Redis Nodes (CLUSTER MEET / 主从 / slot)
```

## 2. Agent：语言与框架

### 方案对比

| 方案 | Agent 层 | Backend 层 | 通信 |
|------|---------|-----------|------|
| **A. 纯 Go** | Go HTTP server | Go model 层 | 进程内调用 |
| **B. Python LangGraph + Go backend** | Python LangGraph | Go Tool server | HTTP API |
| **C. Python LangGraph 全栈** | Python LangGraph | Python 重写 | 进程内调用 |

### 推荐：方案 B — Python LangGraph + Go Backend

**理由：**

LangGraph 提供的不是"调个 API"，而是一套 **agent 工作流引擎**：

| 能力 | 纯 Go HTTP Server | LangGraph |
|------|-------------------|-----------|
| Tool calling | ✅ HTTP endpoint | ✅ @tool 装饰器 |
| 多步推理 | ❌ 依赖 LLM 自身 | ✅ StateGraph 编排 |
| 状态记忆 | ❌ 无状态 | ✅ Checkpointer 持久化 |
| 人在回路（Human-in-the-loop） | ❌ | ✅ interrupt / approve |
| 错误重试 + 回退 | ❌ 手动实现 | ✅ 内置 |
| 并行工具调用 | ❌ | ✅ Send API |
| 流式输出 | ❌ | ✅ Streaming |
| 可视化调试 | ❌ | ✅ LangSmith |

纯 Go HTTP server 只是一个 **tool server**——LLM 决定调用哪个 tool，Go 执行。所有推理逻辑在 LLM 一侧。

LangGraph 让你可以 **在 agent 侧编排工作流**：比如 `diagnose` 不是一个 tool，而是一个 multi-step agent——先查 CR Status，发现问题再查 Redis INFO，分析后给出建议，必要时请求人类确认才执行修复。

### 架构

```
┌──────────────────────────────────────────────────┐
│  Agent (Python - LangGraph)                       │
│                                                   │
│  workflow.py          StateGraph 编排              │
│  ├── create_cluster:  构建 CR → 提交 → 轮询状态    │
│  ├── diagnose:        查 Status → 查 Redis → 分析  │
│  └── scale:           验证 → 执行 → 监控进度       │
│                                                   │
│  tools.py             @tool 定义                   │
│  ├── k8s_create_cr(...)       → HTTP → Go Server  │
│  ├── k8s_get_status(name)     → HTTP → Go Server  │
│  ├── k8s_list_clusters()      → HTTP → Go Server  │
│  ├── redis_cluster_nodes(ns)  → HTTP → Go Server  │
│  └── redis_info(ns, cmd)      → HTTP → Go Server  │
│                                                   │
│  依赖: langgraph, langchain, httpx, pydantic       │
└──────────────────┬───────────────────────────────┘
                   │ HTTP (localhost:8080)
┌──────────────────▼───────────────────────────────┐
│  Tool Server (Go - net/http)                      │
│                                                   │
│  POST /tools/*         薄封装层                    │
│  ├── model.CreateClusterAction()                  │
│  ├── model.ParseRedisClusterNodes()               │
│  ├── k8s clientset (CRUD)                         │
│  └── redis client (INFO, CLUSTER NODES)           │
│                                                   │
│  依赖: net/http（零新增依赖）                       │
└──────────────────────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────┐
│  Operator (Go - client-go)                        │
│                                                   │
│  10s 轮询 → Reconcile → Update CR Status           │
│  （不变）                                          │
└──────────────────────────────────────────────────┘
```

### 两层职责

| 层 | 语言 | 做什么 | 不做什么 |
|----|------|--------|---------|
| **LangGraph Agent** | Python | 理解意图、工作流编排、诊断推理、人机交互、记忆状态 | 不直接操作 K8s/Redis |
| **Go Tool Server** | Go | 执行原子操作：创建 CR、查询 Status、执行 Redis 命令 | 不做推理决策 |
| **Go Operator** | Go | 持续协调期望状态、健康检查 | 不交互、不推理 |

### 关键设计：LangGraph 不直接调 K8s

LangGraph 通过调用 Go Tool Server 的 HTTP API 间接操作 K8s/Redis。这样：

- Go Tool Server 做 **安全校验和错误处理**
- LangGraph 只处理 **推理和编排**
- 两层独立部署、独立扩缩
- 如果 LangGraph 挂了，Operator 继续工作，集群不受影响

### 最小可行 Agent 示例

```python
# agent/workflow.py
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

class ClusterState(TypedDict):
    messages: list
    cluster_name: str
    action: str          # create | scale | delete | diagnose
    cr_manifest: dict
    status: dict

def create_cluster(state):
    """Step 1: 提交 CR"""
    resp = httpx.post("http://localhost:8080/tools/create_cluster",
        json={"name": state["cluster_name"], "shards": 3, "nodes_per_shard": 2})
    return {"cr_manifest": resp.json()}

def wait_ready(state):
    """Step 2: 轮询直到 Ready"""
    while True:
        resp = httpx.post("http://localhost:8080/tools/get_status",
            json={"name": state["cluster_name"]})
        status = resp.json()
        if status["phase"] == "Ready":
            return {"status": status}
        time.sleep(3)

graph = StateGraph(ClusterState)
graph.add_node("create", create_cluster)
graph.add_node("wait", wait_ready)
graph.add_edge("create", "wait")
graph.add_edge("wait", END)
graph.set_entry_point("create")

app = graph.compile(checkpointer=MemorySaver())
```

### Agent 目录结构

```
agent/
├── pyproject.toml        # Python 依赖
├── src/
│   ├── __init__.py
│   ├── tools.py           # @tool 定义（HTTP 调用 Go server）
│   ├── workflows.py       # StateGraph 编排（create/diagnose/scale）
│   ├── prompts.py         # System prompt 模板
│   └── server.py          # LangServe 或 FastAPI 暴露 agent endpoint
└── tests/
    └── test_workflows.py

cmd/agent/
└── main.go                # Go Tool Server 入口（薄封装）
```

### Agent 依赖总结

```
Python 侧（LangGraph Agent）:
  langgraph          工作流编排
  langchain-core     LLM 抽象 + tool 定义
  httpx              HTTP client（调 Go server）
  pydantic           类型校验
  langgraph-checkpoint  状态持久化（可选）

Go 侧（Tool Server + Operator）:
  net/http           标准库（零新增）
  client-go          已有
  model/*            已有
```
```

## 3. Operator：K8s 调整

### 架构对比

| 维度 | CLI 模式（当前） | Operator 模式 |
|------|-----------------|---------------|
| 触发方式 | 人执行命令 | K8s Watch 事件驱动 |
| 状态存储 | 本地文件 `runtime/<name>/` | CR Status 字段 |
| 资源管理 | 同步创建/删除 | Reconcile 循环持续协调 |
| 故障恢复 | 人工重新执行命令 | 自动检测 + 修复 |
| 多集群 | 按名称区分 | 按 CR 实例区分 |

### CRD 设计

```yaml
apiVersion: cache.example.com/v1
kind: RedisCluster
metadata:
  name: dev-cluster
spec:
  shards: 3              # master 数量
  nodesPerShard: 2       # 每个 shard 节点数（含 master）
  redisPort: 6379        # Redis 端口
  image: myredis:latest  # 容器镜像
  passwordSecret: redis-auth  # 密码 Secret（可选）
status:
  phase: Ready           # "" → Creating → Ready → Degraded
  masterCount: 3
  totalNodes: 6
  slotBalance: balanced
  nodes:
  - id: "a1b2..."
    ip: "10.88.3.59"
    role: master
    slots: ["0-5460"]
    healthy: true
  conditions:
  - type: Ready
    status: "True"
    reason: AllNodesHealthy
```

### Controller 实现

基于 client-go，不引入 controller-runtime：

```
controller/
└── rediscluster_controller.go
    ├── RedisClusterController struct
    │   ├── clientset  kubernetes.Interface
    │   ├── crClient   *rest.RESTClient  // 操作 CR
    │   └── namespace  string
    ├── Run(ctx)                    // 主循环
    │   └── for { list CRs → reconcile each → sleep(10s) }
    ├── reconcile(cr)               // 核心协调逻辑（幂等）
    │   ├── Phase "" → Creating:
    │   │   ├── 构建 RuntimeConfig
    │   │   ├── K8sNodeManager.CreatePods()
    │   │   └── 更新 phase=Creating
    │   ├── Phase Creating:
    │   │   ├── ListPodsByCluster()
    │   │   ├── 全部 Ready → MEET + 主从 + slot
    │   │   └── 更新 phase=Ready
    │   ├── Phase Ready:
    │   │   ├── CLUSTER NODES + INFO 健康检查
    │   │   ├── 健康 → 不更新
    │   │   └── 异常 → phase=Degraded
    │   └── Phase Degraded:
    │       ├── 诊断问题（节点离线、主从漂移、slot 不平衡）
    │       └── 尝试自愈
    └── updateStatus(cr)            // 写回 Status
```

### 与现有代码的复用关系

```
Operator Reconcile                    使用现有函数
─────────────────────────────────────────────────────
构建 K8s 配置                      NewK8sClientset(cfg.KubeConfigPath)
创建 Pods                          K8sNodeManager.CreatePods(ctx, n, name)
销毁资源                          K8sNodeManager.DeleteResources(ctx, name)
列出已有 Pods                     K8sNodeManager.ListPodsByCluster(ctx, name)
创建 Redis 客户端                 RuntimeNode.CreateRedisClient()
集群握手                          ClusterManager.MeetNodes(client, ctx, name)
设置主从                          ClusterManager.SetAllNodeRole(ctx, name)
分配 slots                        ClusterManager.AllocateSlots(ctx, name)
解析集群状态                      ParseRedisClusterNodes(ctx, data, name)
健康检查                          RuntimeNode.CreateRedisClient() + INFO
```

### 部署资源

```yaml
# RBAC: operator 需要的权限
rules:
- apiGroups: ["cache.example.com"]      # CRD
  resources: ["redisclusters"]
  verbs: ["get", "list", "watch", "update", "patch"]
- apiGroups: ["cache.example.com"]
  resources: ["redisclusters/status"]
  verbs: ["get", "update", "patch"]
- apiGroups: [""]                        # core
  resources: ["pods", "services", "configmaps", "events"]
  verbs: ["get", "list", "watch", "create", "update", "delete"]

# Deployment
spec:
  replicas: 1
  template:
    containers:
    - name: operator
      image: cluster-operator:latest
      command: ["/cluster-operator"]
```

### Operator 主循环（轮询模式）

不引入 informer 的复杂性，使用简单的定时轮询（适合中小规模）：

```go
func (c *Controller) Run(ctx context.Context) {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            crs, _ := c.listCRs(ctx)
            for _, cr := range crs {
                c.reconcile(ctx, &cr)
            }
        }
    }
}
```

后续可升级为 informer/watch 模式，但轮询已满足当前规模。

## 4. 实施阶段

### Phase 1: API Types（已完成）

- `api/v1/rediscluster_types.go` — CRD Go 类型 ✅
- `config/crd/rediscluster.yaml` — CRD manifest

### Phase 2: Operator（2 文件）

- `controller/rediscluster_controller.go` — 约 200 行，Reconcile 核心
- `cmd/operator/main.go` — 约 30 行，入口

### Phase 3: Agent（5 文件：2 Go + 3 Python）

**Go Tool Server**（薄封装，复用 model 层）：
- `agent/tools.go` — 6 个 tool 实现
- `agent/server.go` — HTTP server + 路由
- `cmd/agent/main.go` — 入口

**Python LangGraph Agent**（工作流编排）：
- `agent/src/tools.py` — @tool 定义（调 Go HTTP API）
- `agent/src/workflows.py` — StateGraph 编排（create/diagnose/scale）
- `agent/pyproject.toml` — 依赖声明

### Phase 4: 部署配置（3 文件）

- `config/deploy/rbac.yaml`
- `config/deploy/operator.yaml`
- `config/deploy/agent.yaml`

### Phase 5: 验证

```bash
go build ./...     # 3 个二进制全部编译
go vet ./...       # 无告警
go test ./...      # 现有测试通过

# 部署测试
kubectl apply -f config/crd/
kubectl apply -f config/deploy/
kubectl apply -f config/examples/dev-cluster.yaml
kubectl get rediscluster dev-cluster -w  # 观察状态变化

# Agent 测试
curl localhost:8080/health
curl -X POST localhost:8080/tools/list_clusters
curl -X POST localhost:8080/tools/get_status -d '{"name":"dev-cluster"}'
```

## 5. 关键决策总结

| 决策 | 选择 | 理由 |
|------|------|------|
| Agent 推理层语言 | **Python** (LangGraph) | AI 生态最优，StateGraph 编排、Checkpointer 持久化、Human-in-the-loop 开箱即用 |
| Agent 工具层语言 | **Go** (net/http) | 与 model 层同语言，零开销复用 K8s/Redis 操作 |
| Agent ↔ Backend 通信 | HTTP JSON | 标准协议，两层独立部署互不耦合 |
| Operator 框架 | 手写 client-go | 已有深度集成，避免 controller-runtime 复杂度 |
| Watch 机制 | 定时轮询（10s） | 简单可靠，后续可升级 informer |
| 状态存储 | CR Status | K8s 原生，kubectl describe 可见 |
| 文件状态 | **废弃** | 旧 YAML/JSON 文件不再需要 |
| 新增 Go 依赖 | **零** | client-go 已有，net/http 标准库 |
| 新增 Python 依赖 | langgraph, langchain-core, httpx | Agent 推理层必需 |

## 6. 四个组件

```
                    ┌─────────────────────────┐
                    │  Agent (Python)          │
                    │  LangGraph + StateGraph  │
                    │  推理 / 编排 / 记忆      │
                    └────────────┬────────────┘
                                 │ HTTP
                    ┌────────────▼────────────┐
                    │  Tool Server (Go)        │
                    │  net/http thin wrapper   │
                    │  原子操作 / 安全校验      │
                    └────────────┬────────────┘
                                 │ client-go
                    ┌────────────▼────────────┐
                    │  K8s API Server          │
                    │  RedisCluster CRD        │
                    └────────────┬────────────┘
                                 │ Watch
                    ┌────────────▼────────────┐
                    │  Operator (Go)           │
                    │  Reconcile / HealthCheck │
                    └─────────────────────────┘

main.go              → cluster            CLI（已有，不变）
cmd/operator/main.go → cluster-operator   Operator
cmd/agent/main.go    → cluster-toolserver Go Tool Server
agent/               → Python Agent       LangGraph 推理层
```

四者独立编译/运行，共享 `model/` 和 CRD。
