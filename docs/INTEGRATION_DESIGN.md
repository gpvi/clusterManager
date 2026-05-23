# Integration Design: GeeCache + clusterManager

## 1. Current State Analysis

### Project A: GeeCache (`E:\Projects\Cache`)
| Aspect | Detail |
|--------|--------|
| Module | `GeeCache` (Go 1.20) |
| Purpose | Distributed in-memory cache library (groupcache-like) |
| Key Features | LRU+TTL, consistent hashing, gRPC transport (TLS/mTLS), SWIM gossip (memberlist), hot-key detection & replication, single-flight coalescing, DB rate limiting |
| Entry Points | `main/main.go` – standalone cache node binary |
| Transport | gRPC (peer-to-peer), HTTP (health check, API gateway on :9999) |

### Project B: clusterManager (`E:\Projects\clusterManager`)
| Aspect | Detail |
|--------|--------|
| Module | `redisClusterManager` (Go 1.25) |
| Purpose | Redis Cluster orchestration (create, scale, delete) |
| Key Features | Multi-backend (K8s/Podman/Containerd), CLI + Operator + gRPC server, SQLite persistence, slot migration, Python AI agent |
| Entry Points | `main.go` (CLI), `cluster/cmd/operator/main.go` (K8s Operator), `cluster/cmd/clusterd/main.go` (gRPC server) |
| Transport | gRPC (clusterd), Cobra CLI |

### Dependency Conflicts to Resolve
| Dependency | GeeCache | clusterManager | Resolution |
|------------|----------|----------------|------------|
| `go-redis/redis` | v6 (legacy) | v8 | Upgrade to v9 (unified API) |
| `yaml` | v3 | v2 | Unify on v3 |
| `protobuf` | v1.33.0 | v1.36.11 | Use latest (v1.36.x) |
| `grpc` | v1.64.0 | v1.81.1 | Use latest (v1.81.x) |
| `golang.org/x/time` | v0.5.0 | v0.12.0 | Use latest (v0.12.x) |

---

## 2. Architecture: Redis Data Platform (RDP)

The merged project becomes a **unified Redis data platform** with two complementary subsystems:

```
┌─────────────────────────────────────────────────────────┐
│                  Redis Data Platform                     │
│                                                         │
│  ┌──────────────────┐    ┌──────────────────────────┐   │
│  │   Cache Layer    │    │   Cluster Orchestrator   │   │
│  │   (GeeCache)     │◄───│   (clusterManager)      │   │
│  │                  │    │                          │   │
│  │  • L1 in-memory  │    │  • Redis cluster lifecycle│  │
│  │  • Hot-key repl. │    │  • Slot migration        │   │
│  │  • gRPC peers    │    │  • Multi-backend pods    │   │
│  │  • SWIM gossip   │    │  • Health monitoring     │   │
│  └────────┬─────────┘    └────────────┬─────────────┘   │
│           │                           │                  │
│           │  Getter callback          │  go-redis        │
│           ▼                           ▼                  │
│  ┌──────────────────────────────────────────────────┐    │
│  │              Redis Cluster (L2)                  │    │
│  │         Managed by clusterManager                │    │
│  └──────────────────────────────────────────────────┘    │
│                                                         │
│  ┌──────────────────────────────────────────────────┐    │
│  │           SQLite Persistence                     │    │
│  │    (cluster state + cache metadata)              │    │
│  └──────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

### Data Flow (Read Path)
```
Client Request
    │
    ▼
┌─────────────┐   hit    ┌──────────┐
│  GeeCache   │─────────►│  Return  │  ~1μs (L1 in-memory)
│  (L1)       │          └──────────┘
└──────┬──────┘
       │ miss
       ▼
┌─────────────┐   hit    ┌──────────┐
│  Redis      │─────────►│  Return  │  ~0.5ms (L2 network)
│  Cluster    │          └──────────┘
└──────┬──────┘
       │ miss
       ▼
┌─────────────┐
│  Source     │  Getter callback (DB / API)
│  of Truth   │
└─────────────┘
```

---

## 3. Directory Structure (Post-Merge)

```
clusterManager/                    # repo root (renamed or kept as-is)
├── go.mod                         # unified module: redis-data-platform
├── go.sum
├── main.go                        # unified CLI entry point
├── Makefile
├── README.md
│
├── cluster/
│   ├── cmd/                           # subcommands
│   │   ├── root.go                    # root command with shared flags
│   │   ├── create.go                  # cluster create
│   │   ├── scale.go                   # cluster scale
│   │   ├── delete.go                  # cluster delete
│   │   ├── cache.go                   # cache node subcommand (NEW)
│   │   ├── operator/                  # K8s operator entry point
│   │   │   └── main.go
│   │   └── clusterd/                  # gRPC server entry point
│   │       └── main.go
│   │
│   ├── model/                         # core business logic (existing)
│   │   ├── interfaces.go              # PodManager interface
│   │   ├── factory.go                 # backend factory
│   │   ├── config.go                  # unified config (extended)
│   │   ├── cluster_manager.go         # Redis cluster orchestrator
│   │   ├── cluster_node.go            # cluster domain types
│   │   ├── k8s_manager.go             # K8s backend
│   │   ├── podman_manager.go          # Podman backend
│   │   ├── containerd_manager.go      # Containerd backend
│   │   ├── k8s_client.go              # K8s client helpers
│   │   ├── store.go                   # SQLite persistence
│   │   └── errors.go                  # sentinel errors
│   │
│   ├── proto/                         # protobuf definitions (extended)
│   │   ├── rediscluster.proto         # clusterd gRPC service
│   │   ├── cache.proto                # cache peer gRPC service (moved)
│   │   ├── rediscluster.pb.go
│   │   ├── rediscluster_grpc.pb.go
│   │   ├── cache.pb.go
│   │   └── cache_grpc.pb.go
│   │
│   ├── api/v1/                        # K8s CRD types
│   │   └── rediscluster_types.go
│   │
│   ├── controller/                    # K8s operator reconcile loop
│   │   └── rediscluster_controller.go
│   │
│   ├── config/                        # config files
│   │   ├── conf.yaml                  # default config (extended)
│   │   ├── crd/
│   │   ├── deploy/
│   │   └── examples/
│   │
│   └── utils/                         # shared utilities
│       └── utils.go
    └── test.ps1
```

---

## 4. Unified Go Module

```go
module github.com/your-org/redis-data-platform

go 1.25.0

require (
    // --- Shared ---
    google.golang.org/grpc v1.81.1
    google.golang.org/protobuf v1.36.11
    gopkg.in/yaml.v3 v3.0.1

    // --- clusterManager ---
    github.com/go-redis/redis/v9 v9.x.x       // upgraded from v8
    github.com/spf13/cobra v1.8.1
    github.com/containerd/containerd v1.7.31
    k8s.io/client-go v0.31.0
    modernc.org/sqlite v1.50.1

    // --- GeeCache ---
    github.com/hashicorp/memberlist v0.5.1
    golang.org/x/time v0.12.0                  // unified version
)
```

---

## 5. Unified Entry Point

### 5.1 Subcommand Structure

```
redis-data-platform
├── create       # Create a Redis cluster
├── scale        # Scale a Redis cluster
├── delete       # Delete a Redis cluster
├── list         # List clusters
├── status       # Get cluster status
├── cache        # Start a cache node
│   ├── --port       (gRPC port, default 8001)
│   ├── --gossip     (gossip port, default 9001)
│   ├── --api        (enable API gateway, default false)
│   └── --seeds      (seed node addresses)
├── operator     # Run as K8s operator
└── clusterd     # Run as gRPC server (existing)
```

### 5.2 `cluster/cmd/cache.go` (New Subcommand)

```go
// cluster/cmd/cache.go
package cmd

import (
    "github.com/your-org/redis-data-platform/cache"
    "github.com/spf13/cobra"
)

var cacheCmd = &cobra.Command{
    Use:   "cache",
    Short: "Start a cache node",
    RunE: func(cmd *cobra.Command, args []string) error {
        cfg := cache.LoadConfig()
        return cache.Run(cfg)
    },
}
```

### 5.3 `cache/run.go` (Extracted from GeeCache's `main/main.go`)

The existing `main/main.go` in GeeCache becomes the library function `cache.Run(cfg *Config) error`, callable from both the CLI `cache` subcommand and programmatically from other components.

---

## 6. Integration Points

### 6.1 Cache Getter → Redis Cluster

GeeCache's `Getter` interface fetches from the Redis cluster managed by clusterManager:

```go
// cache/redis_getter.go
package cache

import (
    "context"
    "github.com/go-redis/redis/v9"
    "github.com/your-org/redis-data-platform/model"
)

// RedisGetter implements Getter using a managed Redis cluster.
type RedisGetter struct {
    cluster *model.ClusterManager  // provides Redis client
}

func (g *RedisGetter) Get(ctx context.Context, key string) ([]byte, error) {
    client := g.cluster.GetRedisClient()
    val, err := client.Get(ctx, key).Bytes()
    if err == redis.Nil {
        return nil, ErrKeyNotFound
    }
    return val, err
}
```

### 6.2 Unified Configuration

Extend `cluster/model/config.go` with cache section:

```yaml
# cluster/config/conf.yaml (extended)
backend: k8s
namespace: default
image: redis:7-alpine
baseNodePort: 30000

# --- Cache subsystem (NEW) ---
cache:
  enabled: true
  maxBytes: 1073741824        # 1GB per node
  ttl: 3600                   # default TTL in seconds
  hotKeyThreshold: 100        # requests per 10s window
  hotReplicas: 2              # extra replicas for hot keys
  dbRateLimit: 200            # DB queries per second

  server:
    ip: "0.0.0.0"
    port: 8001                # gRPC port
    gossip: 9001              # memberlist gossip port
    api: false                # enable HTTP API gateway

  seeds:                      # initial cluster seeds
    - "10.0.1.1:8001"
    - "10.0.1.2:8001"

  tls:
    mode: "insecure"          # insecure | server | mutual
    certFile: ""
    keyFile: ""
    caFile: ""
```

### 6.3 Shared Discovery

When cache nodes and clusterd nodes co-exist in K8s, both use the same memberlist for discovery:

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  Cache Node  │   │  Cache Node  │   │  Cache Node  │
│  + Redis     │   │  + Redis     │   │  + Redis     │
│    Client    │   │    Client    │   │    Client    │
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │                  │                  │
       └──────────────────┼──────────────────┘
                          │
              SWIM Gossip (memberlist)
                          │
                          ▼
              ┌───────────────────┐
              │   Redis Cluster   │
              │  (orchestrated by  │
              │   clusterManager) │
              └───────────────────┘
```

### 6.4 Cache Invalidation Broadcast

When clusterManager performs slot migration (scale up/down), it can trigger cache invalidation for affected key ranges:

```go
// cluster/model/cluster_manager.go
func (cm *ClusterManager) MigrateSlot(slot int, from, to string) error {
    // ... existing migration logic ...

    // Invalidate cache entries for this slot's key range
    if cm.cacheInvalidator != nil {
        cm.cacheInvalidator.InvalidateSlot(slot)
    }
    return nil
}
```

---

## 7. Migration Phases

### Phase 1: Module Merge (1-2 days)
1. Copy `GeeCache/src/` → `clusterManager/cache/`
2. Copy `GeeCache/geecachepb/` → `clusterManager/cluster/proto/`
3. Merge `go.mod` dependencies, resolve conflicts
4. Fix import paths throughout cache package
5. `go build ./...` succeeds

### Phase 2: Unified CLI (1 day)
1. Add `cluster/cmd/cache.go` subcommand
2. Extract `main/main.go` → `cache/run.go` as library function
3. Extend `cluster/model/config.go` with `CacheConfig` struct
4. Wire up config loading

### Phase 3: Redis Getter Integration (1 day)
1. Implement `cache/redis_getter.go`
2. Wire `ClusterManager.GetRedisClient()` into cache group creation
3. Add cache invalidation hook to slot migration

### Phase 4: Testing & Polish (1-2 days)
1. Integration tests: cache → Redis cluster round-trip
2. Performance benchmarks
3. Update documentation
4. Update CI/CD

---

## 8. What Stays Independent

Not everything needs tight coupling. These remain independent:

| Component | Independence |
|-----------|-------------|
| Cache node binary | Can run standalone (no Redis needed for pure cache use) |
| clusterManager CLI | Can run standalone (no cache layer for pure orchestration) |
| K8s Operator | Reconciles RedisCluster CRDs independently |
| Python AI Agent | Consumes clusterd gRPC API independently |

The integration is **additive** — new capabilities compose with existing ones without breaking them.

---

## 9. Benefits

| Benefit | Detail |
|---------|--------|
| **Reduced Redis load** | Hot keys served from memory (~1μs vs ~0.5ms), reducing Redis query volume by 80-90% for skewed workloads |
| **Thundering herd protection** | Single-flight coalescing + DB rate limiter prevent cascade failures during cache misses |
| **Fault tolerance** | Virtual replication ensures zero cache penetration when a cache node fails |
| **Unified deployment** | Single binary, single config, single CI pipeline |
| **Shared dependencies** | Deduplicate gRPC, protobuf, go-redis, yaml across both subsystems |
| **Redis-aware caching** | Slot migration triggers targeted invalidation, not TTL-based guessing |

---

## 10. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| GeeCache's go-redis v6 API differs significantly from v9 | `RedisGetter` is self-contained; rewrite the ~20 lines of Redis access code |
| memberlist gossip may conflict with K8s DNS-based discovery | memberlist only runs on cache nodes; clusterd uses K8s API for pod discovery |
| Increased binary size (K8s deps + containerd + memberlist) | Acceptable; build tags can disable containerd in cache-only builds |
| Two gRPC servers (cache peer + clusterd) on one process | Use different ports; document port layout clearly |
