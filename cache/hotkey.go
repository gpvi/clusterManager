package cache

import (
	"sync"
	"sync/atomic"
	"time"

	"redisClusterManager/cache/peer"
)

// HotKeyTracker detects keys receiving disproportionately high request rates
// and triggers replication to neighboring nodes on the hash ring.
type HotKeyTracker struct {
	mu           sync.Mutex
	counters     map[string]*hotCounter
	hotKeys      map[string]time.Time
	threshold    int           // requests per window to mark as hot
	sampleWindow time.Duration // observation window
	cooldown     time.Duration // how long a key stays "hot" after last detection
	replicas     int           // how many extra nodes to replicate to
	registry     *peer.Registry
	localAddr    string
	stopCh       chan struct{} // signals the pruneLoop to exit
}

type hotCounter struct {
	count atomic.Int64
}

// HotKeyConfig configures hot key detection.
type HotKeyConfig struct {
	Threshold    int           // QPS threshold to mark as hot (default 100)
	SampleWindow time.Duration // sampling window (default 10s)
	CoolDown     time.Duration // cooldown after last spike (default 60s)
	Replicas     int           // number of extra replicas (default 2)
}

// DefaultHotKeyConfig returns sensible defaults.
func DefaultHotKeyConfig() HotKeyConfig {
	return HotKeyConfig{
		Threshold:    100,
		SampleWindow: 10 * time.Second,
		CoolDown:     60 * time.Second,
		Replicas:     2,
	}
}

// NewHotKeyTracker creates a hot key detector.
func NewHotKeyTracker(cfg HotKeyConfig, registry *peer.Registry, localAddr string) *HotKeyTracker {
	t := &HotKeyTracker{
		counters:     make(map[string]*hotCounter),
		hotKeys:      make(map[string]time.Time),
		threshold:    cfg.Threshold,
		sampleWindow: cfg.SampleWindow,
		cooldown:     cfg.CoolDown,
		replicas:     cfg.Replicas,
		registry:     registry,
		localAddr:    localAddr,
		stopCh:       make(chan struct{}),
	}
	go t.pruneLoop()
	return t
}

// Stop shuts down the hot key tracker and its background goroutine.
func (t *HotKeyTracker) Stop() {
	if t == nil {
		return
	}
	close(t.stopCh)
}

// Record increments the access counter for a key. Returns true if the key
// just became hot (caller should trigger replication).
func (t *HotKeyTracker) Record(key string) bool {
	if t == nil {
		return false
	}

	t.mu.Lock()
	c, ok := t.counters[key]
	if !ok {
		c = &hotCounter{}
		t.counters[key] = c
	}
	t.mu.Unlock()

	n := c.count.Add(1)

	// Check threshold at powers of 2 to reduce lock contention.
	if n%64 != 0 {
		return false
	}

	return t.checkHot(key, int(n))
}

// IsHot returns true if the key is currently marked as hot.
func (t *HotKeyTracker) IsHot(key string) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.hotKeys[key]
	return ok
}

// ReplicateCandidates returns the addresses of nodes that should hold replicas
// of a hot key, excluding the key's primary owner.
func (t *HotKeyTracker) ReplicateCandidates(key string) []string {
	if t == nil || t.registry == nil {
		return nil
	}
	peers := t.registry.PickPeerWithFallback(key, t.replicas+1)
	candidates := make([]string, 0, len(peers))
	for _, p := range peers {
		addr := p.Addr()
		if addr != t.localAddr {
			candidates = append(candidates, addr)
		}
	}
	return candidates
}

func (t *HotKeyTracker) checkHot(key string, count int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	c := t.counters[key]
	if c == nil {
		return false
	}

	// Freshness: if the counter has been accumulated over too long, reset it.
	// We approximate this with the count magnitude.
	if count >= t.threshold {
		if _, already := t.hotKeys[key]; !already {
			t.hotKeys[key] = time.Now()
			return true // just became hot
		}
		t.hotKeys[key] = time.Now() // refresh timestamp
	}
	return false
}

func (t *HotKeyTracker) pruneLoop() {
	ticker := time.NewTicker(t.sampleWindow)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.prune()
		case <-t.stopCh:
			return
		}
	}
}

func (t *HotKeyTracker) prune() {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()

	// Reset all counters and remove entries that dropped to zero to prevent unbounded growth.
	for k, c := range t.counters {
		c.count.Store(0)
		// Remove keys that have been idle for an entire cooldown period (they won't become hot).
		if _, isHot := t.hotKeys[k]; !isHot {
			delete(t.counters, k)
		}
	}

	// Remove stale hot keys.
	for k, lastSeen := range t.hotKeys {
		if now.Sub(lastSeen) > t.cooldown {
			delete(t.hotKeys, k)
		}
	}
}
