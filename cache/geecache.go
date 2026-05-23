package cache

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	pb "redisClusterManager/proto"
	"redisClusterManager/cache/peer"
	"redisClusterManager/cache/singleflight"
)

const defaultReplicationFactor = 2

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)

	// replicationSem bounds concurrent replication goroutines to prevent storms.
	replicationSem = make(chan struct{}, 100)
)

// Getter loads data from the source of truth.
type Getter interface {
	Get(key string) ([]byte, error)
}

// GetterFunc adapts a function to the Getter interface.
type GetterFunc func(key string) ([]byte, error)

func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// Group is a cache namespace that orchestrates local cache, peer fetching,
// virtual replication, hot key replication, and source-of-truth loading.
type Group struct {
	name              string
	getter            Getter
	mainCache         *cache
	peers             PeerPicker
	loader            *singleFlight.Group
	registry          *peer.Registry
	replicationFactor int // 1=primary only, 2=primary+secondary
	hotTracker        *HotKeyTracker
	dbLimiter         *DBLoaderLimiter
	cacheDirty        atomic.Bool
}

// NewGroup creates a cache group. Returns an error instead of panicking.
func NewGroup(name string, cacheBytes int64, getter Getter) (*Group, error) {
	if getter == nil {
		return nil, fmt.Errorf("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:              name,
		getter:            getter,
		mainCache:         newCache(cacheBytes),
		loader:            &singleFlight.Group{},
		replicationFactor: defaultReplicationFactor,
	}
	groups[name] = g
	return g, nil
}

// RegisterPeers injects the PeerPicker implementation.
// Returns an error if peers are already registered.
func (g *Group) RegisterPeers(peers PeerPicker) error {
	if g.peers != nil {
		return fmt.Errorf("peer picker already registered for group %q", g.name)
	}
	g.peers = peers
	return nil
}

// SetRegistry stores the peer registry for invalidation broadcast.
func (g *Group) SetRegistry(r *peer.Registry) {
	g.registry = r
}

// SetReplicationFactor sets how many nodes each key is stored on.
// 1 = primary only (default), 2 = primary + secondary, etc.
func (g *Group) SetReplicationFactor(n int) {
	if n < 1 {
		n = 1
	}
	g.replicationFactor = n
}

// SetHotKeyTracker enables hot key detection and replication.
func (g *Group) SetHotKeyTracker(t *HotKeyTracker) {
	g.hotTracker = t
}

// SetDBLimiter enables rate limiting on DB loads.
func (g *Group) SetDBLimiter(l *DBLoaderLimiter) {
	g.dbLimiter = l
}

// markSlotMigrated marks the local cache as dirty after a slot migration.
// On the next Get(), stale entries are purged to avoid serving post-migration data.
func (g *Group) markSlotMigrated(slot int) {
	g.cacheDirty.Store(true)
}

// Registry returns the peer registry.
func (g *Group) Registry() *peer.Registry {
	return g.registry
}

// GetGroup returns the named group.
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// --- Read Path ---

// Get retrieves a key's value.
//
//  1. local LRU → return (includes hot replicas pushed by other nodes)
//  2. load → PickPeerWithFallback tries primary → secondary → … → getLocally
func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}

	g.hotTracker.Record(key)

	if g.cacheDirty.Swap(false) {
		g.mainCache.purge()
	}

	if v, ok := g.mainCache.get(key); ok {
		log.Println("[LocalCache] hit")
		return v, nil
	}

	return g.load(key)
}

// --- Write / Delete Path ---

// Delete removes a key locally and broadcasts invalidation to all peers.
func (g *Group) Delete(key string) {
	g.mainCache.delete(key)
	if g.registry != nil {
		BroadcastInvalidation(g.registry, g.name, key)
	}
}

// DeleteLocal removes a key from local cache only (no broadcast).
func (g *Group) DeleteLocal(key string) {
	g.mainCache.delete(key)
}

// StoreReplica stores a value pushed by another node. Does not invoke
// the getter or trigger further replication.
func (g *Group) StoreReplica(key string, value ByteView) {
	g.mainCache.add(key, value)
}

// --- Load Pipeline ---

func (g *Group) load(key string) (value ByteView, err error) {
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		// Virtual Replication: try up to replicationFactor peers.
		if g.registry != nil {
			candidates := g.registry.PickPeerWithFallback(key, g.replicationFactor)
			for _, pg := range candidates {
				v, e := g.fetchFromPeer(pg, key)
				if e == nil {
					return v, nil
				}
				log.Printf("[RemoteCache] peer %s failed: %v, trying next", pg.Addr(), e)
			}
		}

		// All peers failed or key belongs to this node.
		return g.getLocally(key)
	})
	if err == nil {
		return viewi.(ByteView), nil
	}
	return
}

// fetchFromPeer uses the new peer.PeerGetter interface directly.
func (g *Group) fetchFromPeer(pg peer.PeerGetter, key string) (ByteView, error) {
	req := &pb.Request{Group: g.name, Key: key}
	resp, err := pg.Get(req)
	if err != nil {
		return ByteView{}, err
	}
	v := ByteView{b: resp.Value}
	g.populateCache(key, v)
	return v, nil
}

// getFromPeer uses the legacy PeerGetter interface.
func (g *Group) getFromPeer(pg PeerGetter, key string) (ByteView, error) {
	req := &pb.Request{Group: g.name, Key: key}
	res := &pb.Response{}
	err := pg.Get(req, res)
	if err != nil {
		return ByteView{}, err
	}
	v := ByteView{b: res.Value}
	g.populateCache(key, v)
	return v, nil
}

func (g *Group) getLocally(key string) (ByteView, error) {
	// Rate-limit DB loads.
	if g.dbLimiter != nil {
		ctx, cancel := context.WithTimeout(context.Background(), dbLoadTimeout)
		defer cancel()
		if err := g.dbLimiter.Wait(ctx); err != nil {
			return ByteView{}, fmt.Errorf("db load rate limited: %w", err)
		}
	}

	bytes, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{cloneBytes(bytes)}
	g.populateCache(key, value)

	// Virtual Replication: push to secondary nodes (fault tolerance).
	g.replicateToSecondaries(key, value)

	// Hot Key Replication: push to additional nodes (load distribution).
	g.replicateIfHot(key, value)

	return value, nil
}

func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}

// --- Replication ---

// replicateToSecondaries pushes a newly loaded key to the secondary nodes
// on the hash ring, ensuring another node can serve it if this one fails.
func (g *Group) replicateToSecondaries(key string, value ByteView) {
	if g.replicationFactor < 2 || g.registry == nil {
		return
	}

	candidates := g.registry.PickPeerWithFallback(key, g.replicationFactor)
	for _, pg := range candidates {
		if pg.Addr() == g.registry.LocalAddr() {
			continue
		}
		if pp, ok := pg.(peer.ReplicaPusher); ok {
			// Use a bounded goroutine via the replication semaphore.
			replicationSem <- struct{}{}
			go func(pg peer.PeerGetter) {
				defer func() { <-replicationSem }()
				ctx, cancel := context.WithTimeout(context.Background(), dbLoadTimeout)
				defer cancel()
				if err := pp.PushReplica(ctx, g.name, key, value.ByteSlice()); err != nil {
					log.Printf("[VR] push secondary %s: %v", pg.Addr(), err)
				}
			}(pg)
		}
		break
	}
}

// replicateIfHot pushes a hot key to additional neighbors for load distribution.
func (g *Group) replicateIfHot(key string, value ByteView) {
	if g.hotTracker == nil || !g.hotTracker.IsHot(key) {
		return
	}

	candidates := g.hotTracker.ReplicateCandidates(key)
	for _, addr := range candidates {
		replicationSem <- struct{}{}
		go func(target string) {
			defer func() { <-replicationSem }()
			allPeers := g.registry.All()
			for _, p := range allPeers {
				if p.Addr() == target {
					if pp, ok := p.(peer.ReplicaPusher); ok {
						ctx, cancel := context.WithTimeout(context.Background(), dbLoadTimeout)
						defer cancel()
						if err := pp.PushReplica(ctx, g.name, key, value.ByteSlice()); err != nil {
							log.Printf("[HotKey] push replica to %s: %v", target, err)
						}
					}
					return
				}
			}
		}(addr)
	}
}
