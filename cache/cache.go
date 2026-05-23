package cache

import (
	"redisClusterManager/cache/lru"
	"sync"
	"time"
)

type cacheEntry struct {
	value     ByteView
	expiresAt time.Time
}

func (e *cacheEntry) Len() int {
	return e.value.Len()
}

func (e *cacheEntry) expired() bool {
	if e.expiresAt.IsZero() {
		return false
	}
	return time.Now().After(e.expiresAt)
}

type cache struct {
	mu         sync.Mutex
	lru        *lru.Cache
	cacheBytes int64
}

func newCache(maxBytes int64) *cache {
	return &cache{cacheBytes: maxBytes}
}

func (c *cache) add(key string, value ByteView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		c.lru = lru.New(c.cacheBytes, nil)
	}
	c.lru.Add(key, &cacheEntry{value: value})
}

func (c *cache) addWithTTL(key string, value ByteView, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		c.lru = lru.New(c.cacheBytes, nil)
	}
	c.lru.Add(key, &cacheEntry{
		value:     value,
		expiresAt: time.Now().Add(ttl),
	})
}

func (c *cache) get(key string) (value ByteView, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return
	}
	if v, ok := c.lru.Get(key); ok {
		entry := v.(*cacheEntry)
		if entry.expired() {
			c.lru.Remove(key)
			return ByteView{}, false
		}
		return entry.value, true
	}
	return
}

func (c *cache) delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		return
	}
	c.lru.Remove(key)
}

// purge removes all entries from the cache. Called after slot migration
// to ensure no stale data is served.
func (c *cache) purge() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru = nil
}
